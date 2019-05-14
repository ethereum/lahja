from abc import (
    ABC,
    abstractmethod,
)
from types import (
    TracebackType,
)
from typing import (
    TYPE_CHECKING,
    Optional,
    Type,
)

if TYPE_CHECKING:
    from lahja.endpoint import Endpoint  # noqa: F401


class AsyncEndpointContextManager(ABC):
    @abstractmethod
    def __aenter__(self) -> 'Endpoint':
        pass

    @abstractmethod
    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        pass
