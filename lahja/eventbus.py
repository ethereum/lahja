import asyncio
import aioprocessing


class Subscription:

    def __init__(self, unsubscribe_fn):
        self._unsubscribe_fn = unsubscribe_fn

    def unsubscribe(self):
        self._unsubscribe_fn()


class Endpoint:

    def __init__(self, name, sending_queue, receiving_queue):
        self.name = name
        self.sending_queue = sending_queue
        self.receiving_queue = receiving_queue
        self._handler = {}

    def broadcast(self, event_type, item):
        self.sending_queue.coro_put(EventWrapper(
            self.name,
            event_type,
            item,
        ))

    def connect(self):
        asyncio.ensure_future(self._connect())

    async def _connect(self):
        while True:
            item = await self.receiving_queue.coro_get()
            if item.event_type not in self._handler:
                continue
            for handler in self._handler[item.event_type]:
                handler(item)

    def subscribe(self, event_type, handler):
        if event_type not in self._handler:
            self._handler[event_type] = []

        self._handler[event_type].append(handler)

        return Subscription(lambda: self._handler[event_type].remove(handler))

    def _unwrap_event(self, event_wrapper):
        return event_wrapper.payload

    async def receive(self, predicate=None):
        while True:
            item = await self.receiving_queue.coro_get()

            should_receive = True if predicate is None else predicate(item)
            if should_receive:
                yield self._unwrap_event(item)

    async def receive_others(self):
        async for item in self.receive(lambda e: e.origin != self.name):
            yield item


class EventWrapper:

    # TODO: stringly typed events suck
    def __init__(self, origin, event_type, payload):
        self.origin = origin
        self.event_type = event_type
        self.payload = payload


class EventBus:

    def __init__(self):
        self._queues = []
        self._endpoints = {}
        self._incoming_queue = aioprocessing.AioQueue()

    def create_endpoint(self, name):
        if name in self._endpoints:
            raise ValueError("An endpoint with that name does already exist")

        receiving_queue = aioprocessing.AioQueue()
        self._queues.append(receiving_queue)
        endpoint = Endpoint(name, self._incoming_queue, receiving_queue)
        self._endpoints[name] = endpoint
        return endpoint


    def run_forever(self):
        asyncio.ensure_future(self.start())

    async def start(self):
        while True:
            item = await self._incoming_queue.coro_get()
            for queue in self._queues:
                queue.coro_put(item)

