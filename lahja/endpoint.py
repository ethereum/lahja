import asyncio
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
import functools
import multiprocessing
from typing import (  # noqa: F401
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)
import uuid

from .async_util import (
    async_get,
)
from .exceptions import (
    NoConnection,
    UnexpectedResponse,
)
from .misc import (
    TRANSPARENT_EVENT,
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
    Subscription,
)


class Endpoint:

    def __init__(self,
                 name: str,
                 sending_queue: multiprocessing.Queue,
                 receiving_queue: multiprocessing.Queue) -> None:

        self.name = name
        self._sending_queue = sending_queue
        self._receiving_queue = receiving_queue
        self._futures: Dict[Optional[str], asyncio.Future] = {}
        self._handler: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]] = {}
        self._queues: Dict[Type[BaseEvent], List[asyncio.Queue]] = {}
        self._executor: Optional[ThreadPoolExecutor] = None
        self._running = False
        self._optional_internal_queue: Optional[asyncio.Queue] = None
        self._optional_loop: Optional[asyncio.AbstractEventLoop] = None
        self._optional_receiving_loop_running: Optional[asyncio.Event] = None
        self._optional_internal_loop_running: Optional[asyncio.Event] = None

    @property
    def _loop(self) -> asyncio.AbstractEventLoop:
        if self._optional_loop is None:
            raise NoConnection("Need to connect first")
        return self._optional_loop

    @property
    def _internal_queue(self) -> asyncio.Queue:
        if self._optional_internal_queue is None:
            raise NoConnection("Need to connect first")
        return self._optional_internal_queue

    @property
    def _receiving_loop_running(self) -> asyncio.Event:
        if self._optional_receiving_loop_running is None:
            raise NoConnection("Need to connect first")
        return self._optional_receiving_loop_running

    @property
    def _internal_loop_running(self) -> asyncio.Event:
        if self._optional_internal_loop_running is None:
            raise NoConnection("Need to connect first")
        return self._optional_internal_loop_running

    def connect_no_wait(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        Connect the :class:`~lahja.endpoint.Endpoint` to the :class:`~lahja.eventbus.EventBus`
        instance that created this endpoint.
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        self._optional_loop = loop
        self._optional_internal_loop_running = asyncio.Event(loop=self._loop)
        self._optional_receiving_loop_running = asyncio.Event(loop=self._loop)
        self._optional_internal_queue = asyncio.Queue()

        # Using `gather` (over e.g. `wait` or plain `ensure_future`) ensures that the inner futures
        # are automatically cancelled as soon as the parent task is cancelled
        asyncio.gather(
            asyncio.ensure_future(self._connect_receiving_queue(), loop=self._loop),
            asyncio.ensure_future(self._connect_internal_queue(), loop=self._loop),
            loop=self._loop
        )

        self._running = True

    async def connect(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.connect_no_wait(loop)
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
        self._executor = ThreadPoolExecutor()
        while self._running:
            (item, config) = await async_get(self._receiving_queue, executor=self._executor)

            self._process_item(item, config)

    async def _connect_internal_queue(self) -> None:
        self._internal_loop_running.set()
        while self._running:
            (item, config) = await self._internal_queue.get()

            self._process_item(item, config)

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
            future.set_result(item)
            self._futures.pop(config.filter_event_id)

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
        if self._executor is not None:
            self._executor.shutdown()
        self._receiving_queue.close()

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
            self._sending_queue.put_nowait((item, config))

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

        self._sending_queue.put_nowait((item, config))

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
            del self._futures[id]

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
