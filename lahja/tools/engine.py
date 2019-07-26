from abc import ABC, abstractmethod
import asyncio
from typing import Any, Awaitable, Callable, Type

import trio

from lahja import AsyncioEndpoint, EndpointAPI, TrioEndpoint
from lahja.base import EventAPI


class EngineAPI(ABC):
    endpoint_class: Type[EndpointAPI]
    Event: Type[EventAPI]

    @abstractmethod
    def run_drivers(self, *drivers: Callable[["EngineAPI"], Awaitable[None]]) -> None:
        ...

    async def run_with_timeout(
        self, coro: Callable[..., Awaitable[Any]], *args: Any
    ) -> None:
        ...

    async def sleep(self, seconds: int) -> None:
        ...


class AsyncioEngine(EngineAPI):
    endpoint_class = AsyncioEndpoint
    Event = asyncio.Event

    async def run_drivers(
        self, *drivers: Callable[[EngineAPI], Awaitable[None]]
    ) -> None:
        await asyncio.gather(*(driver(self) for driver in drivers))

    async def run_with_timeout(
        self, coro: Callable[..., Awaitable[Any]], *args: Any, timeout: int
    ) -> None:
        try:
            await asyncio.wait_for(coro(*args), timeout=timeout)
        except asyncio.TimeoutError as err:
            raise TimeoutError from err

    async def sleep(self, seconds: int) -> None:
        await asyncio.sleep(seconds)


class TrioEngine(EngineAPI):
    endpoint_class = TrioEndpoint
    Event = asyncio.Event

    async def run_drivers(
        self, *drivers: Callable[[EngineAPI], Awaitable[None]]
    ) -> None:
        async with trio.open_nursery() as nursery:
            for driver in drivers:
                nursery.start_soon(driver, self)

    async def run_with_timeout(
        self, coro: Callable[..., Awaitable[Any]], *args: Any, timeout: int
    ) -> None:
        try:
            with trio.fail_after(timeout):
                await coro(*args)
        except trio.TooSlowError as err:
            raise TimeoutError from err

    async def sleep(self, seconds: int) -> None:
        await trio.sleep(seconds)
