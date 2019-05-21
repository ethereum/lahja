from abc import ABC, abstractmethod
from pathlib import Path
from typing import (  # noqa: F401
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    NamedTuple,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)

from lahja.exceptions import BindError

if TYPE_CHECKING:
    from lahja.base import EndpointAPI  # noqa: F401


class Subscription:
    def __init__(self, unsubscribe_fn: Callable[[], Any]) -> None:
        self._unsubscribe_fn = unsubscribe_fn

    def unsubscribe(self) -> None:
        self._unsubscribe_fn()


class BroadcastConfig:
    def __init__(
        self,
        filter_endpoint: Optional[str] = None,
        filter_event_id: Optional[str] = None,
        internal: bool = False,
    ) -> None:

        self.filter_endpoint = filter_endpoint
        self.filter_event_id = filter_event_id
        self.internal = internal

        if self.internal and self.filter_endpoint is not None:
            raise ValueError("`internal` can not be used with `filter_endpoint")

    def __str__(self) -> str:
        return (
            "BroadcastConfig["
            f"{'internal' if self.internal else 'external'} / "
            f"endpoint={self.filter_endpoint if self.filter_endpoint else 'N/A'} / "
            f"  id={self.filter_event_id if self.filter_event_id else 'N/A'}"
            "]"
        )

    def allowed_to_receive(self, endpoint: str) -> bool:
        return self.filter_endpoint is None or self.filter_endpoint == endpoint


class BaseEvent:

    _origin = ""
    _id: Optional[str] = None

    is_bound = False

    def bind(self, endpoint: "EndpointAPI", id: Optional[str]) -> None:
        if self.is_bound:
            raise BindError("Event is already bound")
        self._origin = endpoint.name
        self._id = id
        self.is_bound = True

    def broadcast_config(self, internal: bool = False) -> BroadcastConfig:
        if internal:
            return BroadcastConfig(internal=True, filter_event_id=self._id)

        return BroadcastConfig(filter_endpoint=self._origin, filter_event_id=self._id)


TResponse = TypeVar("TResponse", bound=BaseEvent)


class BaseRequestResponseEvent(ABC, BaseEvent, Generic[TResponse]):
    @staticmethod
    @abstractmethod
    def expected_response_type() -> Type[TResponse]:
        """
        Return the type that is expected to be send back for this request.
        This ensures that at runtime, only expected responses can be send
        back to callsites that issued a `BaseRequestResponseEvent`
        """
        raise NotImplementedError("Must be implemented by subsclasses")


class RemoteSubscriptionChanged(BaseEvent):
    pass


class ConnectionConfig(NamedTuple):
    """
    Configuration class needed to establish :class:`~lahja.endpoint.Endpoint` connections.
    """

    name: str
    path: Path

    @classmethod
    def from_name(
        cls, name: str, base_path: Optional[Path] = None
    ) -> "ConnectionConfig":
        if base_path is None:
            return cls(name=name, path=Path(f"{name}.ipc"))
        elif base_path.is_dir():
            return cls(name=name, path=base_path / f"{name}.ipc")
        else:
            raise TypeError("Provided `base_path` must be a directory")


class Broadcast(NamedTuple):
    event: Union[BaseEvent, bytes]
    config: Optional[BroadcastConfig]


class Message(ABC):
    """
    Base class for all valid message types that an ``Endpoint`` can handle.
    ``NamedTuple`` breaks multiple inheritance which means, instead of regular subclassing,
    derived message types need to derive from ``NamedTuple`` directly and call
    Message.register(DerivedType) in order to allow isinstance(obj, Message) checks.
    """

    pass


class SubscriptionsUpdated(NamedTuple):
    subscriptions: Set[Type[BaseEvent]]
    response_expected: bool


class SubscriptionsAck:
    pass


Message.register(Broadcast)
Message.register(SubscriptionsUpdated)
Message.register(SubscriptionsAck)


# mypy doesn't appreciate the ABCMeta trick
Msg = Union[Broadcast, SubscriptionsUpdated, SubscriptionsAck]
