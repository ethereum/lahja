import asyncio
from asyncio import (
    StreamReader,
    StreamWriter,
)
import functools
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
    ReaderAtEof,
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
from .typing import (
    AsyncEndpointContextManager,
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


class RemoteEndpoint:
    logger = logging.getLogger('lahja.endpoint.RemoteEndpoint')

    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.writer = writer
        self.reader = reader
        self._drain_lock = asyncio.Lock()

    @classmethod
    async def connect_to(cls, path: str) -> 'RemoteEndpoint':
        reader, writer = await asyncio.open_unix_connection(path)
        return cls(reader, writer)

    async def send_message(self, message: Broadcast) -> None:
        await _write_message(self.writer, message)
        async with self._drain_lock:
            # Use a lock to serialize drain() calls. Circumvents this bug:
            # https://bugs.python.org/issue29930
            await self.writer.drain()

    async def read_message(self) -> Broadcast:
        if self.reader.at_eof():
            raise ReaderAtEof()

        try:
            return await _read_message(self.reader)
        except asyncio.IncompleteReadError:
            raise ReaderAtEof()


SIZE_MARKER_LENGTH = 4


async def _read_message(reader: StreamReader) -> Broadcast:
    raw_size = await reader.readexactly(SIZE_MARKER_LENGTH)
    size = int.from_bytes(raw_size, 'little')
    message = await reader.readexactly(size)
    obj = pickle.loads(message)
    assert isinstance(obj, Broadcast)
    return obj


async def _write_message(writer: StreamWriter, message: Broadcast) -> None:
    assert isinstance(message, Broadcast)
    pickled = pickle.dumps(message)
    size = len(pickled)
    writer.write(size.to_bytes(SIZE_MARKER_LENGTH, 'little'))
    writer.write(pickled)


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
        self._connected_endpoints: Dict[str, RemoteEndpoint] = {}
        self._futures: Dict[Optional[str], 'asyncio.Future[BaseEvent]'] = {}
        self._handler: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]] = {}
        self._queues: Dict[Type[BaseEvent], List['asyncio.Queue[BaseEvent]']] = {}

        self._child_coros: Set['asyncio.Future[Any]'] = set()

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
    def name(self) -> str:
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

    async def _accept_conn(self, reader: StreamReader, writer: StreamWriter) -> None:
        remote = RemoteEndpoint(reader, writer)
        coro = asyncio.ensure_future(self._handle_client(remote))
        coro.add_done_callback(lambda fut: self._child_coros.remove(fut))
        self._child_coros.add(coro)

    async def _handle_client(self, remote: RemoteEndpoint) -> None:
        while self._running:
            try:
                message = await remote.read_message()
            except ReaderAtEof:
                return

            if isinstance(message, Broadcast):
                await self._receiving_queue.put((message.event, message.config))
            else:
                self.logger.warning(
                    f'[{self.ipc_path}] received unexpected message: {message}'
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
            if config.name in seen:
                raise ConnectionAttemptRejected(
                    f"Trying to connect to {config.name} twice. Names must be uniqe."
                )
            elif config.name in self._connected_endpoints.keys():
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
        if config.name in self._connected_endpoints.keys():
            self.logger.warning(
                "Tried to connect to %s but we are already connected to that Endpoint",
                config.name
            )
            return

        remote = await RemoteEndpoint.connect_to(str(config.path))
        self._connected_endpoints[config.name] = remote

    def is_connected_to(self, endpoint_name: str) -> bool:
        return endpoint_name in self._connected_endpoints

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
        for coro in self._child_coros:
            coro.cancel()
        self._server.close()
        self.ipc_path.unlink()

    @asynccontextmanager  # type: ignore
    async def run(self) -> AsyncIterable['Endpoint']:  # type: ignore
        if not self._loop:
            self._loop = asyncio.get_event_loop()
        try:
            yield self
        finally:
            self.stop()
    run = cast(Callable[[None], AsyncEndpointContextManager], run)

    @classmethod
    @asynccontextmanager  # type: ignore
    async def serve(cls, config: ConnectionConfig) -> AsyncIterable['Endpoint']:
        endpoint = cls()
        async with endpoint.run():
            await endpoint.start_serving(config)
            yield endpoint
    serve = cast(Callable[[None], AsyncEndpointContextManager], serve)

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
        else:
            # Broadcast to every connected Endpoint that is allowed to receive the event
            compressed_item = self._compress_event(item)
            disconnected_endpoints = []
            for name, remote in self._connected_endpoints.items():
                allowed = (config is None) or config.allowed_to_receive(name)
                if allowed:
                    try:
                        await remote.send_message(Broadcast(compressed_item, config))
                    except ConnectionResetError:
                        self.logger.debug(f'Remote endpoint {name} no longer exists')
                        # the remote has disconnected from us
                        disconnected_endpoints.append(name)
            for name in disconnected_endpoints:
                del self._connected_endpoints[name]

    def broadcast_nowait(self,
                         item: BaseEvent,
                         config: Optional[BroadcastConfig] = None) -> None:
        """
        A non-async `broadcast()` (see the docstring for `broadcast()` for more)

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

        return Subscription(lambda: self._handler[event_type].remove(casted_handler))

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

        if event_type not in self._queues:
            self._queues[event_type] = []

        # mypy cannot reconcile BaseEvent with TStreamEvent
        self._queues[event_type].append(queue)  # type: ignore
        i = None if num_events is None else 0
        while True:
            try:
                yield await queue.get()
            except (GeneratorExit, asyncio.CancelledError):
                # mypy cannot reconcile BaseEvent with TStreamEvent
                self._queues[event_type].remove(queue)  # type: ignore
                break
            else:
                if i is None:
                    continue

                i += 1

                if i >= cast(int, num_events):
                    # mypy cannot reconcile BaseEvent with TStreamEvent
                    self._queues[event_type].remove(queue)  # type: ignore
                    break
