import asyncio
import aioprocessing

from typing import (
    Any,
    AsyncIterable,
    Callable,
    Dict,
    List,
    Type,
    TypeVar,
)

class Subscription:

    def __init__(self, unsubscribe_fn: Callable[[], Any]) -> None:
        self._unsubscribe_fn = unsubscribe_fn

    def unsubscribe(self) -> None:
        self._unsubscribe_fn()


class BaseEvent:

    def __init__(self, payload: Any) -> None:
        self._origin = ''
        self.payload = payload

class Endpoint:

    def __init__(self,
                 name: str,
                 sending_queue: aioprocessing.AioQueue,
                 receiving_queue: aioprocessing.AioQueue) -> None:

        self.name = name
        self.sending_queue = sending_queue
        self.receiving_queue = receiving_queue
        self._handler: Dict[Type[BaseEvent], List[Callable[[BaseEvent], Any]]] = {}
        self._queues: Dict[Type[BaseEvent], List[asyncio.Queue]] = {}

    def broadcast(self, item: BaseEvent) -> None:
        item.origin = self.name
        self.sending_queue.coro_put(item)

    def connect(self) -> None:
        asyncio.ensure_future(self._connect())

    async def _connect(self) -> None:
        while True:
            item = await self.receiving_queue.coro_get()

            event_type = type(item)
            in_queue = event_type in self._queues
            in_handler = event_type in self._handler

            if not in_queue and not in_handler:
                continue

            if in_queue:
                for queue in self._queues[event_type]:
                    queue.put_nowait(item)

            if in_handler:
                for handler in self._handler[event_type]:
                    handler(item)


    def subscribe(self, event_type: Type[BaseEvent], handler: Callable[[BaseEvent], None]) -> Subscription:
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


class EventBus:

    def __init__(self) -> None:
        self._queues: List[aioprocessing.AioQueue] = []
        self._endpoints: Dict[str, Endpoint] = {}
        self._incoming_queue = aioprocessing.AioQueue()

    def create_endpoint(self, name: str) -> Endpoint:
        if name in self._endpoints:
            raise ValueError("An endpoint with that name does already exist")

        receiving_queue = aioprocessing.AioQueue()
        self._queues.append(receiving_queue)
        endpoint = Endpoint(name, self._incoming_queue, receiving_queue)
        self._endpoints[name] = endpoint
        return endpoint


    def start(self) -> None:
        asyncio.ensure_future(self._start())

    async def _start(self) -> None:
        while True:
            item = await self._incoming_queue.coro_get()
            for queue in self._queues:
                queue.coro_put(item)

