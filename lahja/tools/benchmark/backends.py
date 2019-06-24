from abc import ABC, abstractmethod
from typing import Any, Type

from lahja.asyncio import AsyncioEndpoint
from lahja.base import BaseEndpoint
from lahja.trio import TrioEndpoint


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
    Endpoint = AsyncioEndpoint

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


class TrioBackend(BaseBackend):
    name = "trio"
    Endpoint = TrioEndpoint

    @staticmethod
    def run(coro: Any, *args: Any) -> None:
        # UNCOMMENT FOR DEBUGGING
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        import trio

        trio.run(coro, *args)

    @staticmethod
    async def sleep(seconds: float) -> None:
        import trio

        await trio.sleep(seconds)
