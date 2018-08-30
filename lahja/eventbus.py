import asyncio
import multiprocessing  # noqa: F401
from multiprocessing.queues import (
    Queue,
)
from types import (
    ModuleType,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor
)
from typing import (  # noqa: F401
    Dict,
    List,
    Optional,
)

from .async_util import (
    async_get,
)
from .endpoint import (
    Endpoint,
)
from .misc import (
    TRANSPARENT_EVENT,
    BroadcastConfig,
)


class EventBus:

    def __init__(self, ctx: ModuleType = multiprocessing) -> None:
        self.ctx = ctx
        self._queues: List[multiprocessing.Queue] = []
        self._endpoints: Dict[str, Endpoint] = {}
        self._incoming_queue: Queue = Queue(0, ctx=self.ctx)
        self._running = False
        self._executor = ThreadPoolExecutor()

    def create_endpoint(self, name: str) -> Endpoint:
        if name in self._endpoints:
            raise ValueError("An endpoint with that name does already exist")

        receiving_queue: Queue = Queue(0, ctx=self.ctx)

        endpoint = Endpoint(name, self._incoming_queue, receiving_queue)
        self._endpoints[name] = endpoint
        return endpoint

    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        if loop is not None:
            asyncio.ensure_future(self._start(), loop=loop)
        else:
            asyncio.ensure_future(self._start())

    async def _start(self) -> None:
        self._running = True
        while self._running:
            (item, config) = await async_get(self._incoming_queue, executor=self._executor)

            if item is TRANSPARENT_EVENT:
                continue

            for endpoint in self._endpoints.values():

                if not self._is_allowed_to_receive(config, endpoint.name):
                    continue

                endpoint._receiving_queue.put_nowait((item, config))

    def _is_allowed_to_receive(self, config: BroadcastConfig, endpoint: str) -> bool:
        return config is None or config.allowed_to_receive(endpoint)

    def stop(self) -> None:
        self._running = False
        self._incoming_queue.put_nowait((TRANSPARENT_EVENT, None))
        self._incoming_queue.close()
        self._executor.shutdown()

    def shutdown(self) -> None:
        for endpoint in self._endpoints.values():
            endpoint.stop()

        self.stop()
