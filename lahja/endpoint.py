import abc
import asyncio
from asyncio import (
    StreamReader,
    StreamWriter,
)
import functools
import itertools
import logging
import pathlib
import pickle
import time
import traceback
from typing import (  # noqa: F401
    Any,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
import uuid

from async_generator import (
    asynccontextmanager,
)

from lahja._snappy import (
    check_has_snappy_support,
)
from lahja.base import (
    BaseEndpoint,
    TResponse,
    TStreamEvent,
    TSubscribeEvent,
)

from .exceptions import (
    ConnectionAttemptRejected,
    NotServing,
    RemoteDisconnected,
    UnexpectedResponse,
)
from .misc import (
    TRANSPARENT_EVENT,
    BaseEvent,
    BaseRequestResponseEvent,
    Broadcast,
    BroadcastConfig,
    ConnectionConfig,
    Subscription,
)


async def wait_for_path(path: pathlib.Path, timeout: int = 2) -> None:
    """
    Wait for the path to appear at ``path``
    """
    start_at = time.monotonic()
    while time.monotonic() - start_at < timeout:
        if path.exists():
            return
        await asyncio.sleep(0.05)

    raise TimeoutError(f"IPC socket file {path} has not appeared in {timeout} seconds")


class Message(abc.ABC):
    """
    Base class for all valid message types that an ``Endpoint`` can handle.
    ``NamedTuple`` breaks multiple inheritance which means, instead of regular subclassing,
    derived message types need to derive from ``NamedTuple`` directly and call
    Message.register(DerivedType) in order to allow isinstance(obj, Message) checks.
    """
    pass


class SubscriptionsUpdated(NamedTuple):
    subscriptions: Set[Type[BaseEvent]]
    response_expected: bool


class SubscriptionsAck:
    pass


Message.register(Broadcast)
Message.register(SubscriptionsUpdated)
Message.register(SubscriptionsAck)


# mypy doesn't appreciate the ABCMeta trick
Msg = Union[Broadcast, SubscriptionsUpdated, SubscriptionsAck]


SIZE_MARKER_LENGTH = 4


class Connection:
    logger = logging.getLogger('lahja.endpoint.Connection')

    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.writer = writer
        self.reader = reader
        self._drain_lock = asyncio.Lock()

    @classmethod
    async def connect_to(cls, path: str) -> 'Connection':
        reader, writer = await asyncio.open_unix_connection(path)
        return cls(reader, writer)

    async def send_message(self, message: Msg) -> None:
        assert isinstance(message, Message)
        pickled = pickle.dumps(message)
        size = len(pickled)

        try:
            self.writer.write(size.to_bytes(SIZE_MARKER_LENGTH, 'little'))
            self.writer.write(pickled)
            async with self._drain_lock:
                # Use a lock to serialize drain() calls. Circumvents this bug:
                # https://bugs.python.org/issue29930
                await self.writer.drain()
        except (BrokenPipeError, ConnectionResetError):
            raise RemoteDisconnected()

    async def read_message(self) -> Message:
        if self.reader.at_eof():
            raise RemoteDisconnected()

        try:
            raw_size = await self.reader.readexactly(SIZE_MARKER_LENGTH)
            size = int.from_bytes(raw_size, 'little')
            message = await self.reader.readexactly(size)
            obj = pickle.loads(message)
            assert isinstance(obj, Message)
            return obj
        except (asyncio.IncompleteReadError, BrokenPipeError, ConnectionResetError):
            raise RemoteDisconnected()


class InboundConnection:
    """
    A local Endpoint might have several ``InboundConnection``s, each of them represents a remote
    Endpoint which has connected to the given Endpoint and is attempting to send it messages.
    """
    def __init__(self, conn: Connection, new_msg_func: Callable[[Broadcast], None]) -> None:
        self.conn = conn
        self.new_msg_func = new_msg_func

        self.logger = logging.getLogger('lahja.endpoint.InboundConnection')

        self._notify_lock = asyncio.Lock()
        self._received_response = asyncio.Condition()

    async def run(self) -> None:
        while True:
            try:
                message = await self.conn.read_message()
            except RemoteDisconnected:
                async with self._received_response:
                    self._received_response.notify_all()
                return

            if isinstance(message, Broadcast):
                self.new_msg_func(message)
            elif isinstance(message, SubscriptionsAck):
                async with self._received_response:
                    self._received_response.notify_all()
            else:
                self.logger.error(f'received unexpected message: {message}')

    async def notify_subscriptions_updated(self,
                                           subscriptions: Set[Type[BaseEvent]],
                                           block: bool = True) -> None:
        """
        Alert the ``Endpoint`` which has connected to us that our subscription set has
        changed. If ``block`` is ``True`` then this function will block until the remote
        endpoint has acknowledged the new subscription set. If ``block`` is ``False`` then this
        function will return immediately after the send finishes.
        """
        async with self._notify_lock:
            # The lock ensures only one coroutine can notify this endpoint at any one time
            # and that no replies are accidentally received by the wrong coroutines.
            async with self._received_response:
                try:
                    await self.conn.send_message(SubscriptionsUpdated(subscriptions, block))
                except RemoteDisconnected:
                    return
                if block:
                    await self._received_response.wait()


class OutboundConnection:
    """
    The local Endpoint might have several ``OutboundConnection``s, each of them represents a
    remote ``Endpoint`` which has been connected to. The remote endpoint occasionally sends
    special message "backwards" to the local endpoint that connected to it.

    Those messages (``SubscriptionsUpdated``) specify which kinds of messages the remote
    Endpoint is subscribed to. No other message types are allowed to flow "backwards" from
    an outbound connection and otherwise are dropped.
    """
    def __init__(self, name: str, conn: Connection) -> None:
        self.conn = conn
        self.name = name
        self.subscribed_messages: Set[Type[BaseEvent]] = set()

        self.logger = logging.getLogger('lahja.endpoint.OutboundConnection')
        self._received_subscription = asyncio.Condition()

    async def run(self) -> None:
        while True:
            try:
                message = await self.conn.read_message()
            except RemoteDisconnected:
                return

            if not isinstance(message, SubscriptionsUpdated):
                self.logger.error(
                    f'Endpoint {self.name} sent back an unexpected message: {type(message)}'
                )
                return

            self.subscribed_messages = message.subscriptions
            async with self._received_subscription:
                self._received_subscription.notify_all()
            if message.response_expected:
                await self.send_message(SubscriptionsAck())

    def can_send_item(self, item: BaseEvent, config: Optional[BroadcastConfig]) -> bool:
        is_response = config is not None and config.filter_event_id is not None
        passes_config = config is None or config.allowed_to_receive(self.name)
        passes_filter = type(item) in self.subscribed_messages

        if not passes_config:
            return False

        return is_response or passes_filter

    async def send_message(self, message: Msg) -> None:
        await self.conn.send_message(message)

    async def wait_until_subscription_received(self) -> None:
        async with self._received_subscription:
            await self._received_subscription.wait()

    async def wait_until_subscribed_to(self, event: Type[BaseEvent]) -> None:
        while event not in self.subscribed_messages:
            await self.wait_until_subscription_received()


TFunc = TypeVar('TFunc', bound=Callable[..., Any])


class Endpoint(BaseEndpoint):
    """
    The :class:`~lahja.endpoint.Endpoint` enables communication between different processes
    as well as within a single process via various event-driven APIs.
    """

    _name: str
    _ipc_path: pathlib.Path

    _receiving_queue: 'asyncio.Queue[Tuple[Union[bytes, BaseEvent], Optional[BroadcastConfig]]]'
    _receiving_loop_running: asyncio.Event

    _internal_queue: 'asyncio.Queue[Tuple[BaseEvent, Optional[BroadcastConfig]]]'
    _internal_loop_running: asyncio.Event

    _server_running: asyncio.Event

    _loop: Optional[asyncio.AbstractEventLoop]

    def __init__(self) -> None:
        self._outbound_connections: Dict[str, OutboundConnection] = {}
        self._inbound_connections: Set[InboundConnection] = set()
        self._futures: Dict[Optional[str], 'asyncio.Future[BaseEvent]'] = {}
        self._handler: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]] = {}
        self._queues: Dict[Type[BaseEvent], List['asyncio.Queue[BaseEvent]']] = {}

        self._child_tasks: Set['asyncio.Future[Any]'] = set()

        self._running = False
        self._loop = None

    @property
    def ipc_path(self) -> pathlib.Path:
        return self._ipc_path

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise NotServing("Endpoint isn't serving yet. Call `start_serving` first.")

        return self._loop

    def check_event_loop(func: TFunc) -> TFunc:  # type: ignore
        """
        All Endpoint methods must be called from the same event loop.
        """
        @functools.wraps(func)
        def run(self, *args, **kwargs):  # type: ignore
            if not self._loop:
                self._loop = asyncio.get_event_loop()

            if self._loop != asyncio.get_event_loop():
                raise Exception(
                    'All endpoint methods must be called from the same event loop'
                )

            return func(self, *args, **kwargs)
        return cast(TFunc, run)

    @property
    def name(self) -> str:  # type: ignore  # mypy thinks the signature does not match EndpointAPI
        return self._name

    # This property gets assigned during class creation.  This should be ok
    # since snappy support is defined as the module being importable and that
    # should not change during the lifecycle of the python process.
    has_snappy_support = check_has_snappy_support()

    @check_event_loop
    async def start_serving(self, connection_config: ConnectionConfig) -> None:
        """
        Start serving this :class:`~lahja.endpoint.Endpoint` so that it can receive events. Await
        until the :class:`~lahja.endpoint.Endpoint` is ready.
        """
        self._name = connection_config.name
        self._ipc_path = connection_config.path
        self._internal_loop_running = asyncio.Event()
        self._receiving_loop_running = asyncio.Event()
        self._server_running = asyncio.Event()
        self._internal_queue = asyncio.Queue()
        self._receiving_queue = asyncio.Queue()

        asyncio.ensure_future(self._connect_receiving_queue())
        asyncio.ensure_future(self._connect_internal_queue())
        asyncio.ensure_future(self._start_server())

        self._running = True

        await self.wait_until_serving()

    @check_event_loop
    async def wait_until_serving(self) -> None:
        """
        Await until the ``Endpoint`` is ready to receive events.
        """
        await asyncio.gather(
            self._receiving_loop_running.wait(),
            self._internal_loop_running.wait(),
            self._server_running.wait(),
        )

    async def _start_server(self) -> None:
        self._server = await asyncio.start_unix_server(
            self._accept_conn,
            path=str(self.ipc_path),
        )
        self._server_running.set()

    def receive_message(self, message: Broadcast) -> None:
        self._receiving_queue.put_nowait((message.event, message.config))

    async def _accept_conn(self, reader: StreamReader, writer: StreamWriter) -> None:
        conn = Connection(reader, writer)
        remote = InboundConnection(conn, self.receive_message)
        self._inbound_connections.add(remote)

        task = asyncio.ensure_future(remote.run())
        task.add_done_callback(lambda _: self._inbound_connections.remove(remote))
        task.add_done_callback(self._child_tasks.remove)
        self._child_tasks.add(task)

        # the Endpoint on the other end blocks until it receives this message
        await remote.notify_subscriptions_updated(self.subscribed_events)

    @property
    def subscribed_events(self) -> Set[Type[BaseEvent]]:
        """
        Return the set of events this Endpoint is currently listening for
        """
        return set(self._handler.keys()) | set(self._queues.keys())

    async def _notify_subscriptions_changed(self) -> None:
        """
        Tell all inbound connections of our new subscriptions
        """
        # make a copy so that the set doesn't change while we iterate over it
        for inbound_connection in self._inbound_connections.copy():
            await inbound_connection.notify_subscriptions_updated(self.subscribed_events)

    async def wait_until_any_connection_subscribed_to(self,
                                                      event: Type[BaseEvent]) -> None:
        """
        Block until any other endpoint has subscribed to the ``event`` from this endpoint.
        """
        if len(self._outbound_connections) == 0:
            raise Exception('there are no outbound connections!')

        for outbound in self._outbound_connections.values():
            if event in outbound.subscribed_messages:
                return

        coros = [
            outbound.wait_until_subscribed_to(event)
            for outbound in self._outbound_connections.values()
        ]
        _, pending = await asyncio.wait(coros, return_when=asyncio.FIRST_COMPLETED)
        (task.cancel() for task in pending)

    async def wait_until_all_connections_subscribed_to(self,
                                                       event: Type[BaseEvent]) -> None:
        """
        Block until all other endpoints that we are connected to are subscribed to the ``event``
        from this endpoint.
        """
        if len(self._outbound_connections) == 0:
            raise Exception('there are no outbound connections!')

        coros = [
            outbound.wait_until_subscribed_to(event)
            for outbound in self._outbound_connections.values()
        ]
        await asyncio.wait(coros, return_when=asyncio.ALL_COMPLETED)

    def _compress_event(self, event: BaseEvent) -> Union[BaseEvent, bytes]:
        if self.has_snappy_support:
            import snappy
            return cast(bytes, snappy.compress(pickle.dumps(event)))
        else:
            return event

    def _decompress_event(self, data: Union[BaseEvent, bytes]) -> BaseEvent:
        if isinstance(data, BaseEvent):
            return data
        else:
            import snappy
            return cast(BaseEvent, pickle.loads(snappy.decompress(data)))

    def _throw_if_already_connected(self, *endpoints: ConnectionConfig) -> None:
        seen: Set[str] = set()

        for config in endpoints:
            if config.name in seen:
                raise ConnectionAttemptRejected(
                    f"Trying to connect to {config.name} twice. Names must be uniqe."
                )
            elif config.name in self._outbound_connections.keys():
                raise ConnectionAttemptRejected(
                    f"Already connected to {config.name} at {config.path}. Names must be unique."
                )
            else:
                seen.add(config.name)

    async def _connect_receiving_queue(self) -> None:
        self._receiving_loop_running.set()
        while self._running:
            (item, config) = await self._receiving_queue.get()
            try:
                event = self._decompress_event(item)
                self._process_item(event, config)
            except Exception:
                traceback.print_exc()

    async def _connect_internal_queue(self) -> None:
        self._internal_loop_running.set()
        while self._running:
            (item, config) = await self._internal_queue.get()
            try:
                self._process_item(item, config)
            except Exception:
                traceback.print_exc()

    @check_event_loop
    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints and await until all connections are established.
        """
        self._throw_if_already_connected(*endpoints)
        await asyncio.gather(
            *(self._await_connect_to_endpoint(endpoint) for endpoint in endpoints),
        )

    def connect_to_endpoints_nowait(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints as soon as they become available but do not block.
        """
        self._throw_if_already_connected(*endpoints)
        for endpoint in endpoints:
            asyncio.ensure_future(self._await_connect_to_endpoint(endpoint))

    async def _await_connect_to_endpoint(self, endpoint: ConnectionConfig) -> None:
        await wait_for_path(endpoint.path)
        # sleep for a moment to give the server time to be ready to accept
        # connections
        await asyncio.sleep(0.01)
        await self.connect_to_endpoint(endpoint)

    async def connect_to_endpoint(self, config: ConnectionConfig) -> None:
        if config.name in self._outbound_connections.keys():
            self.logger.warning(
                "Tried to connect to %s but we are already connected to that Endpoint",
                config.name
            )
            return

        conn = await Connection.connect_to(str(config.path))
        remote = OutboundConnection(config.name, conn)
        self._outbound_connections[config.name] = remote

        task = asyncio.ensure_future(remote.run())
        task.add_done_callback(lambda _: self._outbound_connections.pop(config.name, None))
        task.add_done_callback(self._child_tasks.remove)
        self._child_tasks.add(task)

        # don't return control until the caller can safely call broadcast()
        await remote.wait_until_subscription_received()

    def is_connected_to(self, endpoint_name: str) -> bool:
        return endpoint_name in self._outbound_connections

    def _process_item(self, item: BaseEvent, config: Optional[BroadcastConfig]) -> None:
        if item is TRANSPARENT_EVENT:
            return

        event_type = type(item)

        if config is not None and config.filter_event_id in self._futures:
            future = self._futures[config.filter_event_id]
            if not future.done():
                future.set_result(item)
            self._futures.pop(config.filter_event_id, None)

        if event_type in self._queues:
            for queue in self._queues[event_type]:
                queue.put_nowait(item)

        if event_type in self._handler:
            for handler in self._handler[event_type]:
                handler(item)

    def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.Endpoint` from receiving further events.
        """
        if not self._running:
            return

        self._running = False
        self._receiving_queue.put_nowait((TRANSPARENT_EVENT, None))
        self._internal_queue.put_nowait((TRANSPARENT_EVENT, None))
        for task in self._child_tasks:
            task.cancel()
        self._server.close()
        self.ipc_path.unlink()

    @asynccontextmanager  # type: ignore
    async def run(self) -> AsyncGenerator['Endpoint', None]:
        if not self._loop:
            self._loop = asyncio.get_event_loop()

        try:
            yield self
        finally:
            self.stop()

    @classmethod
    @asynccontextmanager  # type: ignore
    async def serve(cls, config: ConnectionConfig) -> AsyncGenerator['Endpoint', None]:
        endpoint = cls()
        async with endpoint.run():
            await endpoint.start_serving(config)
            yield endpoint

    async def broadcast(self, item: BaseEvent, config: Optional[BroadcastConfig] = None) -> None:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.misc.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        item._origin = self.name
        if config is not None and config.internal:
            # Internal events simply bypass going through the central event bus
            # and are directly put into the local receiving queue instead.
            self._internal_queue.put_nowait((item, config))
            return

        # Broadcast to every connected Endpoint that is allowed to receive the event
        compressed_item = self._compress_event(item)
        disconnected_endpoints = []
        for name, remote in list(self._outbound_connections.items()):
            # list() makes a copy, so changes to _outbount_connections don't cause errors
            if remote.can_send_item(item, config):
                try:
                    await remote.send_message(Broadcast(compressed_item, config))
                except RemoteDisconnected:
                    self.logger.debug(f'Remote endpoint {name} no longer exists')
                    disconnected_endpoints.append(name)
        for name in disconnected_endpoints:
            self._outbound_connections.pop(name, None)

    def broadcast_nowait(self,
                         item: BaseEvent,
                         config: Optional[BroadcastConfig] = None) -> None:
        """
        A non-async :meth:`~lahja.Endpoint.broadcast` (see :meth:`~lahja.Endpoint.broadcast`
        for more)

        Instead of blocking the calling coroutine this function schedules the broadcast
        and immediately returns.

        CAUTION: You probably don't want to use this. broadcast() doesn't return until the
        write socket has finished draining, meaning that the OS has accepted the message.
        This prevents us from sending more data than the remote process can handle.
        broadcast_nowait has no such backpressure. Even after the remote process stops
        accepting new messages this function will continue to accept them, which in the
        worst case could lead to runaway memory usage.
        """
        asyncio.ensure_future(self.broadcast(item, config))

    @check_event_loop
    async def request(self,
                      item: BaseRequestResponseEvent[TResponse],
                      config: Optional[BroadcastConfig] = None) -> TResponse:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseRequestResponseEvent` on the event bus and
        immediately wait on an expected answer of type :class:`~lahja.misc.BaseEvent`. Optionally
        pass a second parameter of :class:`~lahja.misc.BroadcastConfig` to decide where the request
        should be broadcasted to. By default, requests are broadcasted across all connected
        endpoints with their consuming call sites.
        """
        item._origin = self.name
        item._id = str(uuid.uuid4())

        future: 'asyncio.Future[TResponse]' = asyncio.Future()
        self._futures[item._id] = future  # type: ignore
        # mypy can't reconcile the TResponse with the declared type of
        # `self._futures`.

        await self.broadcast(item, config)

        future.add_done_callback(functools.partial(self._remove_cancelled_future, item._id))

        result = await future

        expected_response_type = item.expected_response_type()
        if not isinstance(result, expected_response_type):
            raise UnexpectedResponse(
                f"The type of the response is {type(result)}, expected: {expected_response_type}"
            )

        return result

    def _remove_cancelled_future(self, id: str, future: 'asyncio.Future[Any]') -> None:
        try:
            future.exception()
        except asyncio.CancelledError:
            self._futures.pop(id, None)

    def subscribe(self,
                  event_type: Type[TSubscribeEvent],
                  handler: Callable[[TSubscribeEvent], None]) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.misc.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        if event_type not in self._handler:
            self._handler[event_type] = []

        casted_handler = cast(Callable[[BaseEvent], Any], handler)

        self._handler[event_type].append(casted_handler)
        # It's probably better to make subscribe() async and await this coro
        asyncio.ensure_future(self._notify_subscriptions_changed())

        def remove() -> None:
            self._handler[event_type].remove(casted_handler)
            # this is asynchronous because that's a better user experience than making
            # the user `await subscription.remove()`. This means this Endpoint will keep
            # getting events for a little while after it stops listening for them but
            # that's a performance problem, not a correctness problem.
            asyncio.ensure_future(self._notify_subscriptions_changed())

        return Subscription(remove)

    async def stream(self,
                     event_type: Type[TStreamEvent],
                     num_events: Optional[int] = None) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        queue: 'asyncio.Queue[TStreamEvent]' = asyncio.Queue()
        casted_queue = cast('asyncio.Queue[BaseEvent]', queue)
        if event_type not in self._queues:
            self._queues[event_type] = []

        self._queues[event_type].append(casted_queue)
        await self._notify_subscriptions_changed()

        if num_events is None:
            # loop forever
            iterations = itertools.repeat(True)
        else:
            iterations = itertools.repeat(True, num_events)

        try:
            for _ in iterations:
                try:
                    yield await queue.get()
                except GeneratorExit:
                    break
                except asyncio.CancelledError:
                    break
        finally:
            self._queues[event_type].remove(casted_queue)
            await self._notify_subscriptions_changed()
