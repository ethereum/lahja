import asyncio
from typing import (  # noqa: F401
    Dict,
    List,
)

import aioprocessing

from .endpoint import (
    Endpoint,
)


class EventBus:

    def __init__(self) -> None:
        self._queues: List[aioprocessing.AioQueue] = []
        self._endpoints: Dict[str, Endpoint] = {}
        self._incoming_queue = aioprocessing.AioQueue()
        self._running = False

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
        self._running = True
        while self._running:
            item = await self._incoming_queue.coro_get()
            for queue in self._queues:
                queue.coro_put(item)

    def stop(self) -> None:
        self._running = False
        self._incoming_queue.close()
