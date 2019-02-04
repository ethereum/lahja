import asyncio
import functools
from multiprocessing.managers import (  # type: ignore # Typeshed definition is lacking `BaseProxy`
    BaseManager,
    BaseProxy,
)
import pathlib
import threading
from typing import (  # noqa: F401
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)
import uuid

from ._utils import (
    wait_for_path,
    wait_for_path_blocking,
)
from .exceptions import (
    UnexpectedResponse,
)
from .misc import (
    TRANSPARENT_EVENT,
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
    Subscription,
)


class ConnectionConfig(NamedTuple):
    """
    Configuration class needed to establish :class:`~lahja.endpoint.Endpoint` connections.
    """
    name: str
    path: pathlib.Path

    @staticmethod
    def from_name(name: str) -> 'ConnectionConfig':
        return ConnectionConfig(name=name, path=pathlib.Path(f"{name}.ipc"))


class EndpointConnector:
    """
    Expose the receiving queue of an :class:`~lahja.endpoint.Endpoint` so that any other
    :class:`~lahja.endpoint.Endpoint` that wants to push events into this
    :class:`~lahja.endpoint.Endpoint` can do so via the
    :class:`~lahja.endpoint.ProxyEndpointConnector`.
    """

    def __init__(self, endpoint: 'Endpoint') -> None:
        self._endpoint = endpoint

    def put_nowait(self, item_and_config: Tuple[BaseEvent, Optional[BroadcastConfig]]) -> None:
        loop = self._endpoint._loop
        # We need to wrap this in `call_soon_threadsafe` since otherwise, the event loop
        # won't pick it up until some other task moves the loop forward
        loop.call_soon_threadsafe(self._endpoint._receiving_queue.put_nowait, item_and_config)


class ProxyEndpointConnector(BaseProxy):  # type: ignore # TypeShed is missing BaseProxy
    """
    Proxy that connects to an :class:`~lahja.endpoint.EndpointConnector`
    """

    def put_nowait(self, item_and_config: Tuple[BaseEvent, Optional[BroadcastConfig]]) -> None:
        self._callmethod('put_nowait', (item_and_config,))


def get_connector_manager() -> Type[BaseManager]:
    class ConnectorManager(BaseManager):
        pass

    return ConnectorManager


class Endpoint:
    """
    The :class:`~lahja.endpoint.Endpoint` enables communication between different processes
    as well as within a single process via various event-driven APIs.
    """

    _name: str
    _ipc_path: pathlib.Path

    _receiving_queue: asyncio.Queue
    _receiving_loop_running: asyncio.Event

    _internal_queue: asyncio.Queue
    _internal_loop_running: asyncio.Event

    _loop: asyncio.AbstractEventLoop

    def __init__(self) -> None:
        self._connected_endpoints: Dict[str, BaseProxy] = {}
        self._futures: Dict[Optional[str], asyncio.Future] = {}
        self._handler: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]] = {}
        self._queues: Dict[Type[BaseEvent], List[asyncio.Queue]] = {}
        self._running = False

    @property
    def ipc_path(self) -> pathlib.Path:
        return self._ipc_path

    @property
    def name(self) -> str:
        return self._name

    def connect_no_wait(self,
                        connection_config: ConnectionConfig,
                        loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        Connect the :class:`~lahja.endpoint.Endpoint` to the :class:`~lahja.eventbus.EventBus`
        instance that created this endpoint.
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        self._name = connection_config.name
        self._ipc_path = connection_config.path
        self._create_external_api(self._ipc_path)
        self._loop = loop
        self._internal_loop_running = asyncio.Event(loop=self._loop)
        self._receiving_loop_running = asyncio.Event(loop=self._loop)
        self._internal_queue = asyncio.Queue(loop=self._loop)
        self._receiving_queue = asyncio.Queue(loop=self._loop)

        # Using `gather` (over e.g. `wait` or plain `ensure_future`) ensures that the inner futures
        # are automatically cancelled as soon as the parent task is cancelled
        asyncio.gather(
            asyncio.ensure_future(self._connect_receiving_queue(), loop=self._loop),
            asyncio.ensure_future(self._connect_internal_queue(), loop=self._loop),
            loop=self._loop
        )

        self._running = True

    async def connect(self,
                      connection_config: ConnectionConfig,
                      loop: Optional[asyncio.AbstractEventLoop] = None) -> None:

        self.connect_no_wait(connection_config, loop)
        await self.wait_for_connection()

    async def wait_for_connection(self) -> None:
        """
        Wait until the ``Endpoint`` has established a connection to the ``EventBus``
        """
        await asyncio.gather(
            self._receiving_loop_running.wait(),
            self._internal_loop_running.wait(),
            loop=self._loop
        )

    async def _connect_receiving_queue(self) -> None:
        self._receiving_loop_running.set()
        while self._running:
            (item, config) = await self._receiving_queue.get()
            self._process_item(item, config)

    async def _connect_internal_queue(self) -> None:
        self._internal_loop_running.set()
        while self._running:
            (item, config) = await self._internal_queue.get()

            self._process_item(item, config)

    def _create_external_api(self, ipc_path: pathlib.Path) -> None:

        receiver = EndpointConnector(self)

        ConnectorManager = get_connector_manager()

        ConnectorManager.register('get_connector', callable=lambda: receiver)  # type: ignore

        manager = ConnectorManager(address=str(ipc_path))  # type: ignore
        server = manager.get_server()   # type: ignore
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def connect_to_endpoints_blocking(self, *endpoints: ConnectionConfig, timeout: int=30) -> None:
        """
        Connect to the given endpoints and block until the connection to every endpoint is
        established. Raises a ``TimeoutError`` if connections do not become available within
        ``timeout`` seconds (default 30 seconds).
        """
        for endpoint in endpoints:
            wait_for_path_blocking(endpoint.path, timeout)
            self._connect_to_endpoint(endpoint)

    async def connect_to_endpoints(self, *endpoints: ConnectionConfig) -> None:
        """
        Asynchronously connect to the given endpoints and await until all connections are
        established.
        """

        await asyncio.gather(
            *(self._await_connect_to_endpoint(endpoint) for endpoint in endpoints),
            loop=self._loop
        )

    def connect_to_endpoints_nowait(self, *endpoints: ConnectionConfig) -> None:
        """
        Connect to the given endpoints as soon as they become available but do not block.
        """
        for endpoint in endpoints:
            asyncio.ensure_future(self._await_connect_to_endpoint(endpoint))

    async def _await_connect_to_endpoint(self, endpoint: ConnectionConfig) -> None:
        await wait_for_path(endpoint.path)
        self._connect_to_endpoint(endpoint)

    def _connect_to_endpoint(self, endpoint: ConnectionConfig) -> None:
        ConnectorManager = get_connector_manager()

        ConnectorManager.register(  # type: ignore
            'get_connector',
            proxytype=ProxyEndpointConnector
        )

        manager = ConnectorManager(address=str(endpoint.path))  # type: ignore
        manager.connect()
        self._connected_endpoints[endpoint.name] = manager.get_connector()  # type: ignore

    def is_connected_to(self, endpoint_name: str) -> bool:
        return endpoint_name in self._connected_endpoints

    def _process_item(self, item: BaseEvent, config: BroadcastConfig) -> None:
        if item is TRANSPARENT_EVENT:
            return

        has_config = config is not None

        event_type = type(item)
        in_futures = has_config and config.filter_event_id in self._futures
        in_queue = event_type in self._queues
        in_handler = event_type in self._handler

        if not in_queue and not in_handler and not in_futures:
            return

        if in_futures:
            future = self._futures[config.filter_event_id]
            if not future.done():
                future.set_result(item)
            self._futures.pop(config.filter_event_id, None)

        if in_queue:
            for queue in self._queues[event_type]:
                queue.put_nowait(item)

        if in_handler:
            for handler in self._handler[event_type]:
                handler(item)

    def stop(self) -> None:
        """
        Stop the :class:`~lahja.endpoint.Endpoint` from receiving further events, effectively
        disconnecting it from the to the :class:`~lahja.eventbus.EventBus` that created it.
        """
        if not self._running:
            return

        self._running = False
        self._receiving_queue.put_nowait((TRANSPARENT_EVENT, None))
        self._internal_queue.put_nowait((TRANSPARENT_EVENT, None))

    def broadcast(self, item: BaseEvent, config: Optional[BroadcastConfig] = None) -> None:
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
            for name, connector in self._connected_endpoints.items():
                verboten = config is not None and not config.allowed_to_receive(name)
                if not verboten:
                    connector.put_nowait((item, config))

    TResponse = TypeVar('TResponse', bound=BaseEvent)

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

        future: asyncio.Future = asyncio.Future(loop=self._loop)
        self._futures[item._id] = future

        self.broadcast(item, config)

        future.add_done_callback(functools.partial(self._remove_cancelled_future, item._id))

        result = await future

        expected_response_type = item.expected_response_type()
        if not isinstance(result, expected_response_type):
            raise UnexpectedResponse(
                f"The type of the response is {type(result)}, expected: {expected_response_type}"
            )

        return result

    TSubscribeEvent = TypeVar('TSubscribeEvent', bound=BaseEvent)

    def _remove_cancelled_future(self, id: str, future: asyncio.Future) -> None:
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

    TStreamEvent = TypeVar('TStreamEvent', bound=BaseEvent)

    async def stream(self,
                     event_type: Type[TStreamEvent],
                     num_events: Optional[int] = None) -> AsyncGenerator[TStreamEvent, None]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``num_events`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        queue: asyncio.Queue = asyncio.Queue()

        if event_type not in self._queues:
            self._queues[event_type] = []

        self._queues[event_type].append(queue)
        i = None if num_events is None else 0
        while True:
            try:
                yield await queue.get()
            except GeneratorExit:
                self._queues[event_type].remove(queue)
                break
            except asyncio.CancelledError:
                self._queues[event_type].remove(queue)
                break
            else:
                if i is None:
                    continue

                i += 1

                if i >= cast(int, num_events):
                    self._queues[event_type].remove(queue)
                    break

    TWaitForEvent = TypeVar('TWaitForEvent', bound=BaseEvent)

    async def wait_for(self, event_type: Type[TWaitForEvent]) -> TWaitForEvent:  # type: ignore
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        # mypy thinks we are missing a return statement but this seems fair to do
        async for event in self.stream(event_type, num_events=1):
            return event
