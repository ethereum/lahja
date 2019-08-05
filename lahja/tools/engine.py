from abc import ABC, abstractmethod
import asyncio
from typing import Any, Awaitable, Callable, Type, cast

import trio

from lahja import AsyncioEndpoint, EndpointAPI, TrioEndpoint
from lahja.base import EventAPI

Driver = Callable[["EngineAPI"], Awaitable[None]]


class EngineAPI(ABC):
    endpoint_class: Type[EndpointAPI]
    Event: Type[EventAPI]

    @abstractmethod
    def run_drivers(self, *drivers: Driver) -> Awaitable[None]:
        """
        Performs the actual *running* of the drivers executing them with in a
        manner appropriate for the individual endpoint implementation.
        """
        ...

    @abstractmethod
    async def run_with_timeout(
        self, coro: Callable[..., Awaitable[Any]], *args: Any, timeout: int
    ) -> None:
        """
        Runs a coroutine with the specifid positional ``args`` with a timeout.
        **must** raise the built-in ``TimeoutError`` when a timeout occurs.
        """
        ...

    @abstractmethod
    async def sleep(self, seconds: float) -> None:
        """
        Sleep for the provide number of seconds in a manner appropriate for the
        individual endpoint implementation.
        """
        ...


class AsyncioEngine(EngineAPI):
    endpoint_class = AsyncioEndpoint
    Event = cast(Type[EventAPI], asyncio.Event)

    async def run_drivers(self, *drivers: Driver) -> None:
        await asyncio.gather(*(driver(self) for driver in drivers))

    async def run_with_timeout(
        self, coro: Callable[..., Awaitable[Any]], *args: Any, timeout: int
    ) -> None:
        try:
            await asyncio.wait_for(coro(*args), timeout=timeout)
        except asyncio.TimeoutError as err:
            raise TimeoutError from err

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class TrioEngine(EngineAPI):
    endpoint_class = TrioEndpoint
    Event = cast(Type[EventAPI], trio.Event)

    async def run_drivers(self, *drivers: Driver) -> None:
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

    async def sleep(self, seconds: float) -> None:
        await trio.sleep(seconds)
