import asyncio
from concurrent.futures.thread import (
    ThreadPoolExecutor
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
        self._executor = None

    def broadcast(self, item: BaseEvent, config: Optional[BroadcastConfig] = None) -> None:
        item._origin = self.name
        self._sending_queue.put_nowait((item, config))

    async def request(self, item: BaseEvent) -> BaseEvent:
        item._origin = self.name
        item._id = str(uuid.uuid4())

        future: asyncio.Future = asyncio.Future()
        self._futures[item._id] = future

        self._sending_queue.put_nowait((item, None))

        result = await future

        return cast(BaseEvent, result)

    def connect(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
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

    def subscribe(self,
                  event_type: Type[BaseEvent],
                  handler: Callable[[BaseEvent], None]) -> Subscription:

        if event_type not in self._handler:
            self._handler[event_type] = []

        self._handler[event_type].append(handler)

        return Subscription(lambda: self._handler[event_type].remove(handler))

    async def stream(self, event_type: Type[BaseEvent]) -> AsyncIterable[BaseEvent]:
        queue: asyncio.Queue = asyncio.Queue()

        if event_type not in self._queues:
            self._queues[event_type] = []

        self._queues[event_type].append(queue)

        while True:
            event = await queue.get()
            try:
                yield event
            except GeneratorExit:
                self._queues[event_type].remove(queue)

    def stop(self) -> None:
        self._running = False
        self._receiving_queue.put_nowait((TRANSPARENT_EVENT, None))
        if self._executor is not None:
            self._executor.shutdown()
        self._receiving_queue.close()
