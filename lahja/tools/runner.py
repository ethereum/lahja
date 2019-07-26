from abc import ABC, abstractmethod
import asyncio
import itertools
import multiprocessing
from typing import Awaitable, Callable

import trio

from .engine import EngineAPI, AsyncioEngine, TrioEngine


class RunnerAPI(ABC):
    @abstractmethod
    def __call__(self, *drivers: Callable[[EngineAPI], Awaitable[None]]) -> None:
        ...


class AsyncioRunner(RunnerAPI):
    def __init__(self):
        self._engine = AsyncioEngine()

    def __call__(self, *drivers: Callable[[EngineAPI], Awaitable[None]]) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._engine.run_drivers(*drivers))


class TrioRunner(RunnerAPI):
    def __init__(self):
        self._engine = TrioEngine()

    def __call__(self, *drivers: Callable[[EngineAPI], Awaitable[None]]) -> None:
        trio.run(self._engine.run_drivers, *drivers)


class BaseIsolatedProcessRunner(RunnerAPI):
    def __call__(self, *drivers: Callable[[EngineAPI], Awaitable[None]]) -> None:
        drivers_and_runners = tuple(
            (driver, self.get_runner())
            for driver in drivers
        )
        procs = tuple(
            multiprocessing.Process(
                target=runner,
                args=(driver,)
            )
            for driver, runner in drivers_and_runners
        )
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()

    @abstractmethod
    def get_runner(self) -> RunnerAPI:
        ...


class IsolatedHomogenousRunner(BaseIsolatedProcessRunner):
    def __init__(self, runner: RunnerAPI):
        self._runner = runner

    def get_runner(self) -> RunnerAPI:
        return self._runner


class IsolatedHeterogenousRunner(BaseIsolatedProcessRunner):
    def __init__(self, *choices: RunnerAPI):
        self._choices = tuple(choices)
        self._choices_iter = itertools.cycle(self._choices)

    def get_runner(self) -> RunnerAPI:
        return next(self._choices_iter)
