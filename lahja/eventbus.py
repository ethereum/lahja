import asyncio
import aioprocessing

from typing import (
    Any,
    Callable,
    Dict,
    List,
)

class Subscription:

    def __init__(self, unsubscribe_fn: Callable[[], Any]) -> None:
        self._unsubscribe_fn = unsubscribe_fn

    def unsubscribe(self) -> None:
        self._unsubscribe_fn()


class EventWrapper:

    # TODO: stringly typed events suck
    def __init__(self, origin: str, event_type: str, payload: Any) -> None:
        self.origin = origin
        self.event_type = event_type
        self.payload = payload


class Endpoint:

    def __init__(self,
                 name: str,
                 sending_queue: aioprocessing.AioQueue,
                 receiving_queue: aioprocessing.AioQueue) -> None:

        self.name = name
        self.sending_queue = sending_queue
        self.receiving_queue = receiving_queue
        self._handler: Dict[str, List[Callable[[EventWrapper], Any]]] = {}

    def broadcast(self, event_type:str , item: Any) -> None:
        self.sending_queue.coro_put(EventWrapper(
            self.name,
            event_type,
            item,
        ))

    def connect(self) -> None:
        asyncio.ensure_future(self._connect())

    async def _connect(self) -> None:
        while True:
            item = await self.receiving_queue.coro_get()
            if item.event_type not in self._handler:
                continue
            for handler in self._handler[item.event_type]:
                handler(item)

    def subscribe(self, event_type: str, handler: Callable[[EventWrapper], None]) -> Subscription:
        if event_type not in self._handler:
            self._handler[event_type] = []

        self._handler[event_type].append(handler)

        return Subscription(lambda: self._handler[event_type].remove(handler))

    def _unwrap_event(self, event_wrapper: EventWrapper) -> Any:
        return event_wrapper.payload



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

