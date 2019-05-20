from abc import ABC, abstractmethod
from typing import Any, Type

from lahja.base import BaseEndpoint
from lahja.endpoint import Endpoint as LegacyEndpoint


class BaseBackend(ABC):
    name: str
    Endpoint: Type[BaseEndpoint]

    @staticmethod
    @abstractmethod
    def run(coro: Any, *args: Any) -> None:
        pass

    @staticmethod
    @abstractmethod
    async def sleep(seconds: float) -> None:
        pass


class AsyncioBackend(BaseBackend):
    name = "asyncio"
    Endpoint = LegacyEndpoint

    @staticmethod
    def run(coro: Any, *args: Any) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        import asyncio

        loop = asyncio.get_event_loop()
        loop.run_until_complete(coro(*args))
        loop.stop()

    @staticmethod
    async def sleep(seconds: float) -> None:
        import asyncio

        await asyncio.sleep(seconds)
