from concurrent.futures import CancelledError
from types import TracebackType
from typing import Any, Generator, Generic, Tuple, TypeVar

import trio


class InvalidState(Exception):
    pass


TResult = TypeVar("TResult")


class Future(Generic[TResult]):
    _error: Tuple[BaseException, TracebackType]
    _result: TResult

    _cancelled: bool
    _did_error: bool

    _done: trio.Event

    def __init__(self) -> None:
        self._cancelled = False
        self._did_error = False
        self._done = trio.Event()

    def cancelled(self) -> bool:
        return self._cancelled

    def done(self) -> bool:
        return self._done.is_set()

    def result(self) -> TResult:
        if self.cancelled():
            raise CancelledError
        elif self.done():
            if self._did_error:
                exc_value, exc_tb = self._error
                raise exc_value.with_traceback(exc_tb)
            else:
                return self._result
        else:
            raise InvalidState()

    def set_result(self, result: TResult) -> None:
        self._result = result
        self._done.set()

    def set_exception(self, exc_value: Exception, exc_tb: TracebackType) -> None:
        self._error = exc_value, exc_tb
        self._done.set()
        self._did_error = True

    def cancel(self) -> bool:
        if self.done():
            return False
        self._done.set()
        self._cancelled = True
        return True

    def exception(self) -> BaseException:
        if self.cancelled():
            raise CancelledError
        elif self.done() and self._did_error:
            return self._error[0]
        else:
            raise InvalidState()

    async def _await(self) -> TResult:
        await self._done.wait()
        return self.result()

    def __await__(self) -> Generator[Any, None, TResult]:
        return self._await().__await__()
