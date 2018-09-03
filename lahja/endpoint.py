import asyncio
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
import multiprocessing
from typing import (  # noqa: F401
    Any,
    AsyncIterable,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    cast,
)
import uuid

from .async_util import (
    async_get,
)
from .misc import (
    TRANSPARENT_EVENT,
    BaseEvent,
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
        self._futures: Dict[str, asyncio.Future] = {}
        self._handler: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]] = {}
        self._queues: Dict[Type[BaseEvent], List[asyncio.Queue]] = {}
        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None

    def connect(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        Connect the :class:`~lahja.endpoint.Endpoint` to the :class:`~lahja.eventbus.EventBus`
        instance that created this endpoint.
        """
        # mypy doesn't recognize loop as Optional[AbstractEventLoop].
        asyncio.ensure_future(self._try_connect(loop), loop=loop)  # type: ignore

    async def _try_connect(self, loop: asyncio.AbstractEventLoop) -> None:
        # We need to handle exceptions here to not get `Task exception was never retrieved`
        # errors in case the `connect()` fails (e.g. because the remote process is shutting down)
        try:
            await asyncio.ensure_future(self._connect(), loop=loop)
        except Exception as exc:
            raise Exception("Exception from Endpoint.connect()") from exc

    async def _connect(self) -> None:
        self._running = True
        self._executor = ThreadPoolExecutor()
        while self._running:
            (item, config) = await async_get(self._receiving_queue, executor=self._executor)

            if item is TRANSPARENT_EVENT:
                continue

            has_config = config is not None

            event_type = type(item)
            in_futures = has_config and config.filter_event_id in self._futures
            in_queue = event_type in self._queues
            in_handler = event_type in self._handler

            if not in_queue and not in_handler and not in_futures:
                continue

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
        self._sending_queue.put_nowait((item, config))

    async def request(self, item: BaseEvent) -> BaseEvent:
        """
        Broadcast an instance of :class:`~lahja.misc.BaseEvent` on the event bus and immediately
        wait on an expected answer of type :class:`~lahja.misc.BaseEvent`.
        """
        item._origin = self.name
        item._id = str(uuid.uuid4())

        future: asyncio.Future = asyncio.Future()
        self._futures[item._id] = future

        self._sending_queue.put_nowait((item, None))

        result = await future

        return cast(BaseEvent, result)

    def subscribe(self,
                  event_type: Type[BaseEvent],
                  handler: Callable[[BaseEvent], None]) -> Subscription:
        """
        Subscribe to receive updates for any event that matches the specified event type.
        A handler is passed as a second argument an :class:`~lahja.misc.Subscription` is returned
        to unsubscribe from the event if needed.
        """
        if event_type not in self._handler:
            self._handler[event_type] = []

        self._handler[event_type].append(handler)

        return Subscription(lambda: self._handler[event_type].remove(handler))

    async def stream(self,
                     event_type: Type[BaseEvent],
                     max: Optional[int] = None) -> AsyncIterable[BaseEvent]:
        """
        Stream all events that match the specified event type. This returns an
        ``AsyncIterable[BaseEvent]`` which can be consumed through an ``async for`` loop.
        An optional ``max`` parameter can be passed to stop streaming after a maximum amount
        of events was received.
        """
        queue: asyncio.Queue = asyncio.Queue()

        if event_type not in self._queues:
            self._queues[event_type] = []

        self._queues[event_type].append(queue)

        i = None if max is None else 0
        while True:
            event = await queue.get()
            if i is not None:
                i += 1
            try:
                yield event
            except GeneratorExit:
                self._queues[event_type].remove(queue)
            else:
                if i is not None and i >= cast(int, max):
                    break

    async def wait_for(self, event_type: Type[BaseEvent]) -> BaseEvent:  # type: ignore
        """
        Wait for a single instance of an event that matches the specified event type.
        """
        # mypy thinks we are missing a return statement but this seems fair to do
        async for event in self.stream(event_type, max=1):
            return event
