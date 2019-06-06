import asyncio
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
import functools
import itertools
import logging
from pathlib import Path
import pickle
import time
import traceback
from typing import (  # noqa: F401
    Any,
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
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

from async_generator import asynccontextmanager

from lahja._snappy import check_has_snappy_support
from lahja.base import (
    BaseEndpoint,
    BaseRemoteEndpoint,
    ConnectionAPI,
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
    RequestIDGenerator,
    Subscription,
    should_endpoint_receive_item,
)
from lahja.exceptions import (
    ConnectionAttemptRejected,
    RemoteDisconnected,
    UnexpectedResponse,
)
from lahja.typing import ConditionAPI, RequestID


async def wait_for_path(path: Path, timeout: int = 2) -> None:
    """
    Wait for the path to appear at ``path``
    """
    start_at = time.monotonic()
    while time.monotonic() - start_at < timeout:
        if path.exists():
            return
        await asyncio.sleep(0.05)

    raise TimeoutError(f"IPC socket file {path} has not appeared in {timeout} seconds")


SIZE_MARKER_LENGTH = 4


class Connection(ConnectionAPI):
    logger = logging.getLogger("lahja.endpoint.asyncio.Connection")

    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.writer = writer
        self.reader = reader
        self._drain_lock = asyncio.Lock()

    @classmethod
    async def connect_to(cls, path: Path) -> "ConnectionAPI":
        reader, writer = await asyncio.open_unix_connection(str(path))
        return cls(reader, writer)

    async def send_message(self, message: Msg) -> None:
        pickled = pickle.dumps(message)
        size = len(pickled)

        try:
            self.writer.write(size.to_bytes(SIZE_MARKER_LENGTH, "little") + pickled)
            async with self._drain_lock:
                # Use a lock to serialize drain() calls. Circumvents this bug:
                # https://bugs.python.org/issue29930
                await self.writer.drain()
        except (BrokenPipeError, ConnectionResetError):
            raise RemoteDisconnected()
        except RuntimeError as err:
            # We don't do a pre-check for a closed writer since this is a performance critical
            # path. We also don't want to swallow runtime errors unrelated to closed handlers.
            if "handler is closed" in str(err):
                self.logger.warning("Failed to send %s. Handler closed.", message)
                raise RemoteDisconnected from err
            raise

    async def read_message(self) -> Message:
        if self.reader.at_eof():
            raise RemoteDisconnected()

        try:
            raw_size = await self.reader.readexactly(SIZE_MARKER_LENGTH)
            size = int.from_bytes(raw_size, "little")
            message = await self.reader.readexactly(size)
            obj = cast(Message, pickle.loads(message))
            return obj
        except (asyncio.IncompleteReadError, BrokenPipeError, ConnectionResetError):
            raise RemoteDisconnected()


class AsyncioRemoteEndpoint(BaseRemoteEndpoint):
    def __init__(
        self,
        local_name: str,
        conn: ConnectionAPI,
        subscriptions_changed: ConditionAPI,
        new_msg_func: Callable[[Broadcast], Awaitable[Any]],
    ) -> None:
        super().__init__(local_name, conn, new_msg_func)

        self._notify_lock = asyncio.Lock()  # type: ignore

        self._received_response = asyncio.Condition()  # type: ignore
        self._received_subscription = asyncio.Condition()  # type: ignore

        self._running = asyncio.Event()  # type: ignore
        self._stopped = asyncio.Event()  # type: ignore

        self._received_subscription = subscriptions_changed

        self._subscriptions_initialized = asyncio.Event()  # type: ignore

        self._running = asyncio.Event()  # type: ignore
        self._stopped = asyncio.Event()  # type: ignore
        self._ready = asyncio.Event()  # type: ignore

    async def start(self) -> None:
        self._task = asyncio.ensure_future(self._run())
        await self.wait_started()

    async def stop(self) -> None:
        if self.is_stopped:
            return
        self._stopped.set()
        self._task.cancel()


TFunc = TypeVar("TFunc", bound=Callable[..., Any])


SubscriptionAsyncHandler = Callable[[BaseEvent], Awaitable[Any]]
SubscriptionSyncHandler = Callable[[BaseEvent], Any]


class AsyncioEndpoint(BaseEndpoint):
    """
    The :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` enables communication
    between different processes as well as within a single process via various
    event-driven APIs.
    """

    _ipc_path: Path

    _receiving_queue: "asyncio.Queue[Tuple[Union[bytes, BaseEvent], Optional[BroadcastConfig]]]"
    _receiving_loop_running: asyncio.Event

    _futures: Dict[RequestID, "asyncio.Future[BaseEvent]"]

    _async_handler: DefaultDict[Type[BaseEvent], List[SubscriptionAsyncHandler]]
    _sync_handler: DefaultDict[Type[BaseEvent], List[SubscriptionSyncHandler]]

    _loop: Optional[asyncio.AbstractEventLoop] = None
    _get_request_id: Iterator[RequestID]

    _subscriptions_changed: asyncio.Event

    def __init__(self, name: str) -> None:
        self.name = name
        try:
            self._get_request_id = RequestIDGenerator(name.encode("ascii") + b":")
        except UnicodeDecodeError:
            raise Exception(
                f"TODO: Invalid endpoint name: '{name}'. Must be ASCII encodable string"
            )

        # Signal when a new remote connection is established
        self._remote_connections_changed = asyncio.Condition()  # type: ignore

        # Signal when at least one remote has had a subscription change.
        self._remote_subscriptions_changed = asyncio.Condition()  # type: ignore

        # storage containers for inbound and outbound connections to other
        # endpoints
        self._connections = set()

        # event for signaling local subscriptions have changed.
        self._subscriptions_changed = asyncio.Event()

        # storage for futures which are waiting for a response.
        self._futures: Dict[RequestID, "asyncio.Future[BaseEvent]"] = {}

        # handlers for event subscriptions.  These are
        # intentionally stored separately so that the cost of
        # `inspect.iscoroutine` is incurred once when the subscription is
        # created instead of for each event that is processed
        self._async_handler = defaultdict(list)
        self._sync_handler = defaultdict(list)

        # queues for stream handlers
        self._queues: Dict[Type[BaseEvent], List["asyncio.Queue[BaseEvent]"]] = {}

        # background tasks that are started as part of the process of running
        # the endpoint.
        self._endpoint_tasks: Set["asyncio.Future[Any]"] = set()

        # background tasks that are started as part of serving the endpoint
        # over an IPC socket.
        self._server_tasks: Set["asyncio.Future[Any]"] = set()

        self._running = False
        self._serving = False

    def __str__(self) -> str:
        return f"Endpoint[{self.name}]"

    def __repr__(self) -> str:
        return f"<{self.name}>"

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_serving(self) -> bool:
        return self._serving

    @property
    def ipc_path(self) -> Path:
        return self._ipc_path

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise AttributeError("Endpoint does not have an event loop set.")

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
                    "All endpoint methods must be called from the same event loop"
                )

            return func(self, *args, **kwargs)

        return cast(TFunc, run)

    # This property gets assigned during class creation.  This should be ok
    # since snappy support is defined as the module being importable and that
    # should not change during the lifecycle of the python process.
    has_snappy_support = check_has_snappy_support()

    @check_event_loop
    async def start(self) -> None:
        if self.is_running:
            raise RuntimeError(f"Endpoint {self.name} is already running")
        self._receiving_loop_running = asyncio.Event()
        self._receiving_queue = asyncio.Queue()

        self._running = True

        self._endpoint_tasks.add(asyncio.ensure_future(self._connect_receiving_queue()))
        self._endpoint_tasks.add(
            asyncio.ensure_future(self._monitor_subscription_changes())
        )

        await self._receiving_loop_running.wait()
        self.logger.debug("Endpoint[%s]: running", self.name)

    @check_event_loop
    async def start_server(self, ipc_path: Path) -> None:
        """
        Start serving this :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` so that it
        can receive events. Await until the
        :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` is ready.
        """
        if not self.is_running:
            raise RuntimeError(f"Endpoint {self.name} must be running to start server")
        elif self.is_serving:
            raise RuntimeError(f"Endpoint {self.name} is already serving")

        self._ipc_path = ipc_path

        self._serving = True

        self._server = await asyncio.start_unix_server(
            self._accept_conn, path=str(self.ipc_path)
        )
        self.logger.debug("Endpoint[%s]: server started", self.name)

    async def _accept_conn(self, reader: StreamReader, writer: StreamWriter) -> None:
        conn = Connection(reader, writer)
        remote = AsyncioRemoteEndpoint(
            local_name=self.name,
            conn=conn,
            subscriptions_changed=self._remote_subscriptions_changed,
            new_msg_func=self._receiving_queue.put,
        )

        self._server_tasks.add(asyncio.ensure_future(self._run_remote_endpoint(remote)))

    def get_subscribed_events(self) -> Set[Type[BaseEvent]]:
        """
        Return the set of events this Endpoint is currently listening for
        """
        return (
            set(self._sync_handler.keys())
            .union(self._async_handler.keys())
            .union(self._queues.keys())
        )

    async def _monitor_subscription_changes(self) -> None:
        while self.is_running:
            # We wait for the event to change and then immediately replace it
            # with a new event.  This **must** occur before any additional
            # `await` calls to ensure that any *new* changes to the
            # subscriptions end up operating on the *new* event and will be
            # picked up in the next iteration of the loop.
            await self._subscriptions_changed.wait()
            self._subscriptions_changed = asyncio.Event()

            # make a copy so that the set doesn't change while we iterate
            # over it
            subscribed_events = self.get_subscribed_events()

            async with self._remote_connections_changed:
                await asyncio.gather(
                    *(
                        remote.notify_subscriptions_updated(
                            subscribed_events, block=False
                        )
                        for remote in self._connections
                    )
                )

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
            if config.name == self.name:
                raise ConnectionAttemptRejected(
                    f"Cannot connect an endpoint to itself."
                )
            elif config.name in seen:
                raise ConnectionAttemptRejected(
                    f"Trying to connect to {config.name} twice. Names must be uniqe."
                )
            elif self.is_connected_to(config.name):
                raise ConnectionAttemptRejected(
                    f"Already connected to {config.name} at {config.path}. Names must be unique."
                )
            else:
                seen.add(config.name)

    async def _connect_receiving_queue(self) -> None:
        self._receiving_loop_running.set()
        while self.is_running:
            try:
                (item, config) = await self._receiving_queue.get()
            except RuntimeError as err:
                # do explicit check since RuntimeError is a bit generic and we
                # only want to catch the closed event loop case here.
                if str(err) == "Event loop is closed":
                    break
                raise
            try:
                event = self._decompress_event(item)
                await self._process_item(event, config)
            except Exception:
                traceback.print_exc()

    @check_event_loop
    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints and await until all connections are established.
        """
        self._throw_if_already_connected(*endpoints)
        await asyncio.gather(
            *(self._await_connect_to_endpoint(endpoint) for endpoint in endpoints)
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
        self._throw_if_already_connected(config)

        conn = await Connection.connect_to(config.path)
        remote = AsyncioRemoteEndpoint(
            local_name=self.name,
            conn=conn,
            subscriptions_changed=self._remote_subscriptions_changed,
            new_msg_func=self._receiving_queue.put,
        )

        task = asyncio.ensure_future(self._run_remote_endpoint(remote))

        task.add_done_callback(self._endpoint_tasks.remove)

        self._endpoint_tasks.add(task)

        # block until we've received the other endpoint's subscriptions to
        # ensure that it is safe to broadcast
        await self.wait_until_connected_to(config.name)

    async def _run_remote_endpoint(self, remote: RemoteEndpointAPI) -> None:
        await remote.start()

        try:
            await remote.wait_ready()
            await self._add_connection(remote)
            await remote.wait_until_subscription_initialized()
            await remote.wait_stopped()
        finally:
            await remote.stop()
            if remote in self._connections:
                await self._remove_connection(remote)

    async def _add_connection(self, remote: RemoteEndpointAPI) -> None:
        if remote in self._connections:
            raise Exception("TODO: remote is already tracked")
        elif self.is_connected_to(remote.name):
            raise Exception(
                f"TODO: already connected to remote with name {remote.name}"
            )

        async with self._remote_connections_changed:
            # then add them to our set of connections and signal that both
            # the remote connections and subscriptions have been updated.
            await remote.notify_subscriptions_updated(self.get_subscribed_events())
            self._connections.add(remote)
            self._remote_connections_changed.notify_all()
        async with self._remote_subscriptions_changed:
            self._remote_subscriptions_changed.notify_all()

    async def _remove_connection(self, remote: RemoteEndpointAPI) -> None:
        async with self._remote_connections_changed:
            self._connections.remove(remote)
            self._remote_connections_changed.notify_all()
        async with self._remote_subscriptions_changed:
            self._remote_subscriptions_changed.notify_all()

    async def _run_async_subscription_handler(
        self, handler: Callable[[BaseEvent], Awaitable[Any]], item: BaseEvent
    ) -> None:
        try:
            await handler(item)
        except Exception:
            self.logger.debug(
                "%s: Error in subscription handler %s", self, handler, exc_info=True
            )

    async def _process_item(
        self, item: BaseEvent, config: Optional[BroadcastConfig]
    ) -> None:
        event_type = type(item)

        if config is not None and config.filter_event_id in self._futures:
            future = self._futures[config.filter_event_id]
            if not future.done():
                future.set_result(item)
            self._futures.pop(config.filter_event_id, None)

        if event_type in self._queues:
            for queue in self._queues[event_type]:
                queue.put_nowait(item)

        if event_type in self._sync_handler:
            for handler in self._sync_handler[event_type]:
                try:
                    handler(item)
                except Exception:
                    self.logger.debug(
                        "%s: Error in subscription handler %s",
                        self,
                        handler,
                        exc_info=True,
                    )

        if event_type in self._async_handler:
            await asyncio.gather(
                *(
                    self._run_async_subscription_handler(handler, item)
                    for handler in self._async_handler[event_type]
                )
            )

    def stop_server(self) -> None:
        if not self.is_serving:
            return
        self._serving = False

        self._server.close()

        for task in self._server_tasks:
            task.cancel()

        self.ipc_path.unlink()
        self.logger.debug("Endpoint[%s]: server stopped", self.name)

    def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.asyncio.AsyncioEndpoint` from receiving further events.
        """
        if not self.is_running:
            return

        self.stop_server()

        self._running = False

        for task in self._endpoint_tasks:
            task.cancel()

        self.logger.debug("Endpoint[%s]: stopped", self.name)

    @asynccontextmanager  # type: ignore
    async def run(self) -> AsyncIterator["AsyncioEndpoint"]:
        if not self._loop:
            self._loop = asyncio.get_event_loop()

        await self.start()

        try:
            yield self
        finally:
            self.stop()

    @classmethod
    @asynccontextmanager  # type: ignore
    async def serve(cls, config: ConnectionConfig) -> AsyncIterator["AsyncioEndpoint"]:
        endpoint = cls(config.name)
        async with endpoint.run():
            await endpoint.start_server(config.path)
            yield endpoint

    async def broadcast(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
        """
        Broadcast an instance of :class:`~lahja.common.BaseEvent` on the event bus. Takes
        an optional second parameter of :class:`~lahja.common.BroadcastConfig` to decide
        where this event should be broadcasted to. By default, events are broadcasted across
        all connected endpoints with their consuming call sites.
        """
        await self._broadcast(item, config, None)

    async def _broadcast(
        self,
        item: BaseEvent,
        config: Optional[BroadcastConfig],
        id: Optional[RequestID],
    ) -> None:
        item.bind(self, id)

        if should_endpoint_receive_item(
            item, config, self.name, self.get_subscribed_events()
        ):
            await self._process_item(item, config)

        if config is not None and config.internal:
            # if the event is flagged as internal we exit early since it should
            # not be broadcast beyond this endpoint
            return

        # Broadcast to every connected Endpoint that is allowed to receive the event
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
            except RemoteDisconnected:
                self.logger.debug(f"Remote endpoint {remote} no longer connected")
                await remote.stop()

    def broadcast_nowait(
        self, item: BaseEvent, config: Optional[BroadcastConfig] = None
    ) -> None:
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
        asyncio.ensure_future(self._broadcast(item, config, None))

    @check_event_loop
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
        request_id = next(self._get_request_id)

        future: "asyncio.Future[TResponse]" = asyncio.Future()
        self._futures[request_id] = future  # type: ignore
        # mypy can't reconcile the TResponse with the declared type of
        # `self._futures`.

        await self._broadcast(item, config, request_id)

        future.add_done_callback(
            functools.partial(self._remove_cancelled_future, request_id)
        )

        result = await future

        expected_response_type = item.expected_response_type()
        if not isinstance(result, expected_response_type):
            raise UnexpectedResponse(
                f"The type of the response is {type(result)}, expected: {expected_response_type}"
            )

        return result

    def _remove_cancelled_future(
        self, id: RequestID, future: "asyncio.Future[Any]"
    ) -> None:
        try:
            future.exception()
        except asyncio.CancelledError:
            self._futures.pop(id, None)

    def _remove_async_subscription(
        self, event_type: Type[BaseEvent], handler_fn: SubscriptionAsyncHandler
    ) -> None:
        self._async_handler[event_type].remove(handler_fn)
        if not self._async_handler[event_type]:
            self._async_handler.pop(event_type)
        # this is asynchronous because that's a better user experience than making
        # the user `await subscription.remove()`. This means this Endpoint will keep
        # getting events for a little while after it stops listening for them but
        # that's a performance problem, not a correctness problem.
        self._subscriptions_changed.set()

    def _remove_sync_subscription(
        self, event_type: Type[BaseEvent], handler_fn: SubscriptionSyncHandler
    ) -> None:
        self._sync_handler[event_type].remove(handler_fn)
        if not self._sync_handler[event_type]:
            self._sync_handler.pop(event_type)
        # this is asynchronous because that's a better user experience than making
        # the user `await subscription.remove()`. This means this Endpoint will keep
        # getting events for a little while after it stops listening for them but
        # that's a performance problem, not a correctness problem.
        self._subscriptions_changed.set()

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
            casted_handler = cast(SubscriptionAsyncHandler, handler)
            self._async_handler[event_type].append(casted_handler)
            unsubscribe_fn = functools.partial(
                self._remove_async_subscription, event_type, casted_handler
            )
        else:
            casted_handler = cast(SubscriptionSyncHandler, handler)
            self._sync_handler[event_type].append(casted_handler)
            unsubscribe_fn = functools.partial(
                self._remove_sync_subscription, event_type, casted_handler
            )

        self._subscriptions_changed.set()

        return Subscription(unsubscribe_fn)

    async def stream(
        self, event_type: Type[TStreamEvent], num_events: Optional[int] = None
    ) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        queue: "asyncio.Queue[TStreamEvent]" = asyncio.Queue()
        casted_queue = cast("asyncio.Queue[BaseEvent]", queue)
        if event_type not in self._queues:
            self._queues[event_type] = []

        self._queues[event_type].append(casted_queue)
        self._subscriptions_changed.set()

        iterations: Iterable[int]

        if num_events is None:
            # loop forever
            iterations = itertools.count()
        else:
            iterations = range(num_events)

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
            if not self._queues[event_type]:
                del self._queues[event_type]
            self._subscriptions_changed.set()
