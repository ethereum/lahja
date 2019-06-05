from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Callable, NewType, Type

RequestID = NewType("RequestID", bytes)


class LockAPI(ABC):
    @abstractmethod
    async def __aenter__(self) -> None:
        ...

    @abstractmethod
    async def __aexit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, tb: TracebackType
    ) -> None:
        ...

    @abstractmethod
    async def acquire(self) -> None:
        ...

    @abstractmethod
    def locked(self) -> bool:
        ...

    @abstractmethod
    def release(self) -> None:
        ...


class ConditionAPI(ABC):
    @abstractmethod
    async def __aenter__(self) -> None:
        ...

    @abstractmethod
    async def __aexit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, tb: TracebackType
    ) -> None:
        ...

    @abstractmethod
    async def acquire(self) -> None:
        ...

    @abstractmethod
    def notify_all(self) -> None:
        ...

    @abstractmethod
    def notify(self, n: int) -> None:
        ...

    @abstractmethod
    def release(self, n: int) -> None:
        ...

    @abstractmethod
    async def wait(self) -> None:
        ...

    @abstractmethod
    async def wait_for(self, predicate: Callable[[], Any]) -> None:
        ...


class EventAPI(ABC):
    @abstractmethod
    def is_set(self) -> bool:
        ...

    @abstractmethod
    def set(self) -> None:
        ...

    @abstractmethod
    def clear(self) -> None:
        ...

    @abstractmethod
    async def wait(self) -> None:
        ...
