import asyncio
import collections
import itertools
import logging
import pathlib
import pickle
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from async_generator import asynccontextmanager
import trio
import trio_typing

from lahja import constants
from lahja.base import (
    BaseEndpoint,
    BaseRemoteEndpoint,
    ConnectionAPI,
    EndpointAPI,
    RemoteEndpointAPI,
    TResponse,
    TStreamEvent,
    TSubscribeEvent,
)
from lahja.common import (
    BaseEvent,
    BaseRequestResponseEvent,
    Broadcast,
    BroadcastConfig,
    ConnectionConfig,
    Message,
    Msg,
    Subscription,
    should_endpoint_receive_item,
)
from lahja.exceptions import (
    ConnectionAttemptRejected,
    LifecycleError,
    RemoteDisconnected,
    UnexpectedResponse,
)
from lahja.typing import ConditionAPI, RequestID

from .future import Future


class TrioConnection(ConnectionAPI):
    logger = logging.getLogger("lahja.trio.Connection")

    def __init__(self, socket: trio.SocketStream) -> None:
        self._socket = socket
        self._msg_send_channel, self._msg_receive_channel = cast(
            Tuple[trio.abc.SendChannel[Message], trio.abc.ReceiveChannel[Message]],
            trio.open_memory_channel(100),
        )
        self._write_lock = trio.Lock()
        super().__init__()

    def __str__(self) -> str:
        return f"TrioConnection[{self._socket}]"

    def __repr__(self) -> str:
        return f"<{self}>"

    async def close(self) -> None:
        await self._socket.aclose()

    #
    # Connection API
    #
    @classmethod
    async def connect_to(cls, path: pathlib.Path) -> "TrioConnection":
        socket = await trio.open_unix_socket(str(path))
        cls.logger.debug("Opened connection to %s: %s", path, socket)
        return cls(socket)

    async def send_message(self, message: Msg) -> None:
        msg_data = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
        size = len(msg_data)
        try:
            async with self._write_lock:
                await self._socket.send_all(size.to_bytes(4, "little") + msg_data)
        except (trio.ClosedResourceError, trio.BrokenResourceError) as err:
            raise RemoteDisconnected from err

    async def read_message(self) -> Message:
        buffer = bytearray()

        while len(buffer) < 4:
            try:
                data = await self._socket.receive_some(4 - len(buffer))
            except (trio.ClosedResourceError, trio.BrokenResourceError) as err:
                raise RemoteDisconnected from err

            if data == b"":
                raise RemoteDisconnected()

            buffer.extend(data)

        t_size = 4 + int.from_bytes(buffer[:4], "little")

        while len(buffer) < t_size:
            try:
                data = await self._socket.receive_some(t_size - len(buffer))
            except (trio.ClosedResourceError, trio.BrokenResourceError) as err:
                raise RemoteDisconnected from err

            if data == b"":
                raise RemoteDisconnected()

            buffer.extend(data)

        msg = cast(Message, pickle.loads(buffer[4:t_size]))
        return msg


class TrioRemoteEndpoint(BaseRemoteEndpoint):
    def __init__(
        self,
        local_name: str,
        conn: ConnectionAPI,
        subscriptions_changed: ConditionAPI,
        new_msg_func: Callable[[Broadcast], Awaitable[Any]],
    ) -> None:
        super().__init__(local_name, conn, new_msg_func)

        self._notify_lock = trio.Lock()  # type: ignore

        self._received_response = trio.Condition()  # type: ignore
        self._received_subscription = trio.Condition()  # type: ignore

        self._running = trio.Event()  # type: ignore
        self._stopped = trio.Event()  # type: ignore

        self._received_subscription = subscriptions_changed

        self._subscriptions_initialized = trio.Event()  # type: ignore

        self._running = trio.Event()  # type: ignore
        self._stopped = trio.Event()  # type: ignore
        self._ready = trio.Event()  # type: ignore

    @asynccontextmanager
    async def run(self) -> AsyncIterator[RemoteEndpointAPI]:
        async with trio.open_nursery() as nursery:
            await self._start(nursery)
            try:
                yield self
            finally:
                await self.stop()

    async def _start(self, nursery: trio_typing.Nursery) -> None:
        if self.is_running:
            raise LifecycleError("RemoteEndpoint is already running")
        elif self.is_stopped:
            raise LifecycleError("RemoteEndpoint has already been run and stopped")

        nursery.start_soon(self._process_incoming_messages)
        # this is a trade-off between allowing flexibility in how long it takes
        # for a `Runnable` to start and getting good errors in the event that
        # the `_run` method forgets to set the `_running` event.
        with trio.fail_after(2):
            await self.wait_started()

    async def stop(self) -> None:
        if self.is_stopped:
            return
        self._stopped.set()
        await self.conn.close()


async def _wait_for_path(path: trio.Path) -> None:
    """
    Wait for the path to appear at ``path``
    """
    while not await path.exists():
        await trio.sleep(0.05)


WireBroadcastChannelPair = Tuple[
    trio.abc.SendChannel[Broadcast], trio.abc.ReceiveChannel[Broadcast]
]

OutboundBroadcast = Tuple[
    Optional[trio.Event], BaseEvent, Optional[BroadcastConfig], Optional[RequestID]
]
OutboundBroadcastChannelPair = Tuple[
    trio.abc.SendChannel[OutboundBroadcast], trio.abc.ReceiveChannel[OutboundBroadcast]
]

ConnectionChannelPair = Tuple[
    trio.abc.SendChannel[ConnectionConfig], trio.abc.ReceiveChannel[ConnectionConfig]
]
StreamChannelPair = Tuple[
    trio.abc.SendChannel[BaseEvent], trio.abc.ReceiveChannel[BaseEvent]
]


class TrioEndpoint(BaseEndpoint):
    _stream_channels: DefaultDict[Type[BaseEvent], Set[trio.abc.SendChannel[BaseEvent]]]
    _pending_requests: Dict[RequestID, Future[BaseEvent]]

    # Number of unaccepted connections that we allow before refusing new ones.
    _connection_backlog = 10

    _sync_handlers: DefaultDict[Type[BaseEvent], List[Callable[[TSubscribeEvent], Any]]]
    _async_handlers: DefaultDict[
        Type[BaseEvent], List[Callable[[TSubscribeEvent], Awaitable[Any]]]
    ]

    logger = logging.getLogger("lahja.trio.TrioEndpoint")

    _subscriptions_changed: trio.Event

    def __init__(self, name: str):
        super().__init__(name)

        self._running = trio.Event()
        self._stopped = trio.Event()

        # Temporary storage for SendChannels used in the `stream` API.  Uses a
        # `WeakSet` to automate cleanup.
        self._stream_channels = collections.defaultdict(set)
        self._pending_requests = {}
        self._sync_handlers = collections.defaultdict(list)
        self._async_handlers = collections.defaultdict(list)

        self._run_lock = trio.Lock()

        # Signal when a new remote connection is established
        self._remote_connections_changed = trio.Condition()  # type: ignore

        # Signal when at least one remote has had a subscription change.
        self._remote_subscriptions_changed = trio.Condition()  # type: ignore

        # events used to signal that the endpoint has fully booted up.
        self._message_processing_loop_running = trio.Event()
        self._connection_loop_running = trio.Event()
        self._process_broadcasts_running = trio.Event()

        # internal signal that local subscriptions have changed.
        self._subscriptions_changed = trio.Event()

        self._socket_bound = trio.Event()
        self._server_stopped = trio.Event()

    #
    # Running and endpoint
    #
    @property
    def is_running(self) -> bool:
        return not self.is_stopped and self._running.is_set()

    @property
    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    async def wait_started(self) -> None:
        await self._running.wait()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    @asynccontextmanager
    async def run(self) -> AsyncGenerator[EndpointAPI, None]:
        async with trio.open_nursery() as nursery:
            await self._start(nursery)
            try:
                yield self
            finally:
                await self._stop()

    async def _start(self, nursery: trio_typing.Nursery) -> None:
        if self.is_running:
            raise LifecycleError("Endpoint is already running")
        elif self.is_stopped:
            raise LifecycleError("Endpoint has already been run and stopped")

        with trio.fail_after(2):
            await nursery.start(self._run)

            # this is a trade-off between allowing flexibility in how long it takes
            # for a `Runnable` to start and getting good errors in the event that
            # the `_run` method forgets to set the `_running` event.
            await self.wait_started()

    async def _stop(self) -> None:
        if self.is_stopped:
            return
        self._stopped.set()
        await self._cleanup()

    async def _run(self, task_status: trio_typing.TaskStatus[None]) -> None:
        # This send/receive channel is fed any event/config pair that should be
        # broadcast.
        # Those messages are then retrieved from the receive channel by the
        # `_process_outbound_messages` daemon and broadcast to the appropriate
        # `RemoteEndpoint`.
        (self._outbound_send_channel, outbound_receive_channel) = cast(
            OutboundBroadcastChannelPair, trio.open_memory_channel(100)
        )

        # This send/receive channel is fed by connected `RemoteEndpoint`
        # objects which feed received events into the send side of the channel.
        # Those messages are then retrieved from the receive channel by the
        # `_process_inbound_messages` daemon.
        (self._inbound_send_channel, inbound_receive_channel) = cast(
            WireBroadcastChannelPair, trio.open_memory_channel(100)
        )

        # This send/receive channel is fed by
        # `Endpoint.connect_to_endpoint` which places a 2-tuple of
        # (ConnectionConfig, trio.Event) which is retrieved by
        # `_process_connections`.
        (self._connection_send_channel, connection_receive_channel) = cast(
            ConnectionChannelPair, trio.open_memory_channel(100)
        )

        self.logger.debug("%s: starting", self)

        async with trio.open_nursery() as nursery:
            #
            # _process_outbound_messages:
            #     Manages a channel which all outgoing
            #     event broadcasts are placed on, running them each through the
            #     appropriate `RemoteEndpoint.send_message`
            #
            nursery.start_soon(
                self._process_outbound_messages, outbound_receive_channel
            )

            #
            # _process_inbound_messages:
            #     Manages a channel which all incoming
            #     event broadcasts are placed on, running each event through
            #     the internal `_process_item` handler which handles all of the
            #     internal logic for the various
            #     `request/response/subscribe/stream/wait_for` API.
            #
            nursery.start_soon(self._process_inbound_messages, inbound_receive_channel)

            #
            # _process_connections:
            #     When `Endpoint.connect_to_endpoint` is called, the actual
            #     connection process is done asynchronously by putting the
            #     `ConnectionConfig` onto a queue which this process
            #     retrieves to establish the new connection.  This includes
            #     handing the `RemoteEndpoint` oblect off to the `RemoteManager`
            #     which takes care of the connection lifecycle.
            #
            nursery.start_soon(
                self._process_connections, connection_receive_channel, nursery
            )

            #
            # _monitor_subscription_changes
            #    Monitors an event for local changes to subscriptions to
            #    propagate those changes to remotes.
            nursery.start_soon(self._monitor_subscription_changes)

            # mark the endpoint as running.
            self._running.set()
            # tell the nursery that we are started.
            task_status.started()

            await self.wait_stopped()

            nursery.cancel_scope.cancel()

    async def _cleanup(self) -> None:
        # Cleanup stateful things.
        await self._outbound_send_channel.aclose()
        await self._inbound_send_channel.aclose()
        await self._connection_send_channel.aclose()

        del self._outbound_send_channel
        del self._inbound_send_channel
        del self._connection_send_channel

        self.logger.debug("%s: stopped", self)

    #
    # Background processes
    #
    async def _process_outbound_messages(
        self, channel: trio.abc.ReceiveChannel[OutboundBroadcast]
    ) -> None:
        """
        Consume events that have been received from a remote endpoint and
        process them.
        """
        async for (done, item, config, id) in channel:
            item.bind(self, id)

            is_eligible = should_endpoint_receive_item(
                item, config, self.name, self.get_subscribed_events()
            )
            is_internal = config is not None and config.internal

            if is_eligible:
                await self._process_item(item, config)

            if is_internal:
                # if the event is flagged as internal we exit early since it should
                # not be broadcast beyond this endpoint
                return

            remotes_for_broadcast = tuple(
                remote
                for remote in self._connections
                if should_endpoint_receive_item(
                    item, config, remote.name, remote.get_subscribed_events()
                )
            )

            compressed_item = self._compress_event(item)
            message = Broadcast(compressed_item, config)
            for remote in remotes_for_broadcast:
                try:
                    await remote.send_message(message)
                except RemoteDisconnected as err:
                    self.logger.debug(
                        "%s: dropping disconnected remote %s: %s", remote, err
                    )
                    await remote.stop()

            # This Future is used to signal back to the `broadcast` method that
            # this event has been broadcast and it is safe to return from the
            # method.
            if done is not None:
                done.set()

    async def _process_inbound_messages(
        self, channel: trio.abc.ReceiveChannel[Broadcast]
    ) -> None:
        """
        Consume events that have been received from a remote endpoint and
        process them.
        """
        async for (item, config) in channel:
            event = self._decompress_event(item)
            await self._process_item(event, config)

    async def _process_connections(
        self,
        channel: trio.abc.ReceiveChannel[ConnectionConfig],
        nursery: trio_typing.Nursery,
    ) -> None:
        """
        Long running process that establishes connections to endpoint servers
        and runs the handler for receiving events sent by the server over that
        connection.
        """
        self.logger.debug("%s: starting new connection channel", self)
        self._connection_loop_running.set()
        async for config in channel:
            # Allow some time for for the IPC socket to appear
            with trio.fail_after(constants.IPC_WAIT_SECONDS):
                await _wait_for_path(trio.Path(config.path))

            await trio.sleep(0.001)
            # Establish the socket connection
            connection = await TrioConnection.connect_to(config.path)

            # Create the remote
            remote = TrioRemoteEndpoint(
                self.name,
                connection,
                self._remote_subscriptions_changed,
                self._inbound_send_channel.send,
            )
            nursery.start_soon(self._run_remote_endpoint, remote)

    async def _monitor_subscription_changes(self) -> None:
        while not self.is_stopped:
            # We wait for the event to change and then immediately replace it
            # with a new event.  This **must** occur before any additional
            # `await` calls to ensure that any *new* changes to the
            # subscriptions end up operating on the *new* event and will be
            # picked up in the next iteration of the loop.
            await self._subscriptions_changed.wait()
            self._subscriptions_changed = trio.Event()

            # make a copy so that the set doesn't change while we iterate
            # over it
            subscribed_events = self.get_subscribed_events()

            async with trio.open_nursery() as nursery:
                async with self._remote_connections_changed:
                    for remote in self._connections:
                        nursery.start_soon(
                            remote.notify_subscriptions_updated,
                            subscribed_events,
                            False,
                        )
            async with self._remote_subscriptions_changed:
                self._remote_subscriptions_changed.notify_all()

    async def _process_item(
        self, item: BaseEvent, config: Optional[BroadcastConfig]
    ) -> None:
        event_type = type(item)

        # handle request/response
        if config is not None and config.filter_event_id in self._pending_requests:
            fut = self._pending_requests.pop(config.filter_event_id)
            fut.set_result(item)

        # handle stream channel
        if event_type in self._stream_channels:
            channels = tuple(self._stream_channels[event_type])
            for send_channel in channels:
                try:
                    await send_channel.send(item)
                except trio.ClosedResourceError:
                    self._stream_channels[event_type].remove(send_channel)

        # handle subscriptions
        if event_type in self._sync_handlers:
            for handler_fn in self._sync_handlers[event_type]:
                try:
                    handler_fn(item)
                except Exception as err:
                    self.logger.debug(
                        "%s: handler function %s error: %s", self, handler_fn, err
                    )
        if event_type in self._async_handlers:
            for handler_fn in self._async_handlers[event_type]:
                try:
                    await handler_fn(item)
                except Exception as err:
                    self.logger.debug(
                        "%s: handler function %s error: %s", self, handler_fn, err
                    )

    #
    # Server API
    #
    @property
    def is_serving(self) -> bool:
        # NOTE: This does not guarantee that we are actually serving requests since _run_server()
        # does not have a way of notifying us when trio.serve_listeners() has actually been
        # scheduled.
        return not self.is_server_stopped and self._socket_bound.is_set()

    @property
    def is_server_stopped(self) -> bool:
        return self._server_stopped.is_set()

    @classmethod
    @asynccontextmanager
    async def serve(cls, config: ConnectionConfig) -> AsyncIterator["TrioEndpoint"]:
        endpoint = cls(config.name)
        async with endpoint.run():
            async with trio.open_nursery() as nursery:
                await endpoint._start_serving(nursery, config.path)
                try:
                    yield endpoint
                finally:
                    await endpoint._stop_serving()

    async def _start_serving(
        self, nursery: trio_typing.Nursery, ipc_path: pathlib.Path
    ) -> None:
        if not self.is_running:
            raise LifecycleError("Cannot start server if endpoint is not running")
        elif self.is_stopped:
            raise LifecycleError("Endpoint has already been run and stopped")
        elif self.is_serving:
            raise LifecycleError("Endpoint is already serving")
        elif self.is_server_stopped:
            raise LifecycleError("Endpoint server already ran and was stopped")

        self.ipc_path = ipc_path
        nursery.start_soon(self._run_server)

        # Wait until the ipc socket has appeared and is accepting connections.
        with trio.fail_after(constants.IPC_WAIT_SECONDS):
            await _wait_for_path(trio.Path(ipc_path))

        await self._socket_bound.wait()

    async def _stop_serving(self) -> None:
        if self.is_server_stopped:
            return

        self._server_stopped.set()
        self._server_nursery.cancel_scope.cancel()

        del self._server_nursery

        try:
            self.ipc_path.unlink()
        except FileNotFoundError:
            pass
        self.logger.debug(f"%s: server stopped", self)

    async def _run_server(self) -> None:
        async with trio.open_nursery() as nursery:
            # Store nursery on self so that we can access it for cancellation
            self._server_nursery = nursery

            self.logger.debug("%s: server starting", self)
            socket = trio.socket.socket(trio.socket.AF_UNIX, trio.socket.SOCK_STREAM)
            await socket.bind(self.ipc_path.__fspath__())
            socket.listen(self._connection_backlog)
            listener = trio.SocketListener(socket)

            async def set_socket_bound() -> None:
                self._socket_bound.set()

            # Use start_soon here so that we give serve_listeners() below a chance to run before
            # other endpoints start connecting to us.
            nursery.start_soon(set_socket_bound)

            try:
                await trio.serve_listeners(
                    handler=self._accept_conn,
                    listeners=(listener,),
                    handler_nursery=nursery,
                )
            finally:
                self.logger.debug("%s: server finished", self)

    async def _accept_conn(self, socket: trio.SocketStream) -> None:
        self.logger.debug("%s: starting client handler for %s", self, socket)
        connection = TrioConnection(socket)
        remote = TrioRemoteEndpoint(
            self.name,
            connection,
            self._remote_subscriptions_changed,
            self._inbound_send_channel.send,
        )
        await self._run_remote_endpoint(remote)

    #
    # Establishing connections
    #
    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints and await until all connections are established.
        """
        if not self.is_running:
            raise ConnectionAttemptRejected(
                "Cannot establish connections if endpoint isn't running"
            )

        for config in endpoints:
            # Ensure we are not already connected to the named endpoint
            if self.is_connected_to(config.name):
                raise ConnectionAttemptRejected(
                    f"Already connected to endpoint with name: {config.name}"
                )

            # Feed the `ConnectionConfig` through a channel where
            # `_process_connections` will pick it up and actually establish the
            # connection.
            await self._connection_send_channel.send(config)

        for config in endpoints:
            with trio.fail_after(constants.ENDPOINT_CONNECT_TIMEOUT):
                await self.wait_until_connected_to(config.name)

    #
    # Primary endpoint API
    #
    async def broadcast(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.common.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        self.maybe_raise_no_subscribers_exception(config, type(item))
        done = trio.Event()
        await self._outbound_send_channel.send((done, item, config, None))
        await done.wait()

    def broadcast_nowait(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        self.maybe_raise_no_subscribers_exception(config, type(item))
        # FIXME: Ignoring type check because of https://github.com/python-trio/trio/issues/1327
        self._outbound_send_channel.send_nowait(  # type: ignore
            (None, item, config, None)
        )

    TResponse = TypeVar("TResponse", bound=BaseEvent)

    async def request(
        self,
        item: BaseRequestResponseEvent[TResponse],
        config: Optional[BroadcastConfig] = None,
    ) -> TResponse:
        """
        Broadcast an instance of
        :class:`~lahja.common.BaseRequestResponseEvent` on the event bus and
        immediately wait on an expected answer of type
        :class:`~lahja.common.BaseEvent`. Optionally pass a second parameter of
        :class:`~lahja.common.BroadcastConfig` to decide where the request
        should be broadcasted to. By default, requests are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        self.maybe_raise_no_subscribers_exception(config, type(item))
        request_id = next(self._get_request_id)

        # Create an asynchronous generator that we use to pipe the result
        fut: Future[TResponse] = Future()  # type: ignore

        # place the send channel where the message processing loop can find it.
        self._pending_requests[request_id] = fut

        await self._outbound_send_channel.send((None, item, config, request_id))

        # await for the result to be sent through the channel.
        result: TResponse = await fut  # type: ignore
        expected_response_type = item.expected_response_type()
        if not isinstance(result, expected_response_type):
            raise UnexpectedResponse(
                f"The type of the response is {type(result)}, expected: "
                f"{expected_response_type}"
            )

        return result

    TSubscribeEvent = TypeVar("TSubscribeEvent", bound=BaseEvent)

    def subscribe(
        self,
        event_type: Type[TSubscribeEvent],
        handler: Callable[[TSubscribeEvent], Union[Any, Awaitable[Any]]],
    ) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.common.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        if asyncio.iscoroutinefunction(handler):
            self._async_handlers[event_type].append(handler)
            subscription = Subscription(
                lambda: self._async_handlers[event_type].remove(handler)
            )
        else:
            self._sync_handlers[event_type].append(handler)
            subscription = Subscription(
                lambda: self._sync_handlers[event_type].remove(handler)
            )

        # notify subscriptions have been updated.
        self._subscriptions_changed.set()

        return subscription

    TStreamEvent = TypeVar("TStreamEvent", bound=BaseEvent)

    async def stream(
        self, event_type: Type[TStreamEvent], num_events: Optional[int] = None
    ) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        (send_channel, receive_channel) = cast(
            StreamChannelPair, trio.open_memory_channel(100)
        )

        self._stream_channels[event_type].add(send_channel)

        # notify subscriptions have been updated.
        self._subscriptions_changed.set()

        if num_events is None:
            # iterate forever
            counter = itertools.count()
        else:
            # fixed number of iterations
            counter = iter(range(max(0, num_events - 1)))

        async for event in receive_channel:
            yield event  # type: ignore  # mypy doesn't recognize this having a correct type

            try:
                next(counter)
            except StopIteration:
                await send_channel.aclose()
                break

        # We only need to trigger a subscription change if there are no more
        # streams for this event.
        self._stream_channels[event_type].remove(send_channel)
        if not self._stream_channels[event_type]:
            self._stream_channels.pop(event_type)
            self._subscriptions_changed.set()

    #
    # Subscriptions API
    #
    def get_subscribed_events(self) -> Set[Type[BaseEvent]]:
        """
        Return the set of events this Endpoint is currently listening for
        """
        return (
            set(self._sync_handlers.keys())
            .union(self._async_handlers.keys())
            .union(self._stream_channels.keys())
        )
