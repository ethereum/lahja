from abc import ABC, abstractmethod
import asyncio
import multiprocessing
from typing import Any, Awaitable, Callable, Sequence, Type

import trio

from lahja import AsyncioEndpoint, EndpointAPI, TrioEndpoint
from lahja.base import EventAPI


class EngineAPI(ABC):
    endpoint_class: Type[EndpointAPI]
    Event: Type[EventAPI]

    @abstractmethod
    def run(self, *drivers: Callable[["EngineAPI"], Awaitable[None]]) -> None:
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

    def run(self, *drivers: Callable[[EngineAPI], Awaitable[None]]) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._run_drivers(drivers))

    async def _run_drivers(
        self, drivers: Sequence[Callable[[EngineAPI], Awaitable[None]]]
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

    def run(self, *drivers: Callable[[EngineAPI], Awaitable[None]]) -> None:
        trio.run(self._run_drivers, drivers)

    async def _run_drivers(
        self, drivers: Sequence[Callable[[EngineAPI], Awaitable[None]]]
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


class IsolatedProcessEngine(EngineAPI):
    Event = multiprocessing.Event

    def __init__(self, engine: EngineAPI) -> None:
        self.endpoint_class = engine.endpoint_class
        self._engine = engine

    def run(self, coro: Callable[..., Awaitable[Any]], *args: Any) -> None:
        proc = multiprocessing.Process(target=self._engine.run, args=(coro,) + args)
        proc.start()
        proc.join()

    async def run_drivers(
        self, *drivers: Callable[[EngineAPI], Awaitable[None]]
    ) -> None:
        procs = tuple(
            multiprocessing.Process(
                target=self._engine.run, args=(driver, self._engine)
            )
            for driver in drivers
        )
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()

    async def run_with_timeout(
        self, coro: Callable[..., Awaitable[Any]], *args: Any, timeout: int
    ) -> None:
        raise NotImplementedError

    async def sleep(self, seconds: int) -> None:
        raise NotImplementedError
