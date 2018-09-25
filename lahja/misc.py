from abc import (
    ABC,
    abstractmethod,
)
from typing import (  # noqa: F401
    Any,
    Callable,
    Generic,
    Optional,
    Type,
    TypeVar,
)


class Subscription:

    def __init__(self, unsubscribe_fn: Callable[[], Any]) -> None:
        self._unsubscribe_fn = unsubscribe_fn

    def unsubscribe(self) -> None:
        self._unsubscribe_fn()


class BroadcastConfig:

    def __init__(self,
                 filter_endpoint: Optional[str] = None,
                 filter_event_id: Optional[str] = None,
                 internal: bool = False) -> None:

        self.filter_endpoint = filter_endpoint
        self.filter_event_id = filter_event_id
        self.internal = internal

        if self.internal and self.filter_endpoint is not None:
            raise ValueError("`internal` can not be used with `filter_endpoint")

    def allowed_to_receive(self, endpoint: str) -> bool:
        return self.filter_endpoint is None or self.filter_endpoint == endpoint


class BaseEvent:

    _origin = ''
    _id: Optional[str] = None
    _config: Optional[BroadcastConfig] = None

    def broadcast_config(self, internal: bool = False) -> BroadcastConfig:
        if internal:
            return BroadcastConfig(
                internal=True,
                filter_event_id=self._id
            )

        return BroadcastConfig(
            filter_endpoint=self._origin,
            filter_event_id=self._id
        )


TResponse = TypeVar('TResponse', bound=BaseEvent)


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


class TransparentEvent(BaseEvent):
    """
    This event is used to create artificial activity so that code that
    blocks on a :meth:`~multiprocessing.queues.Queue.get` unblocks and
    gets a chance to revalidate if it should continue to block for reading.
    """
    pass


TRANSPARENT_EVENT = TransparentEvent()
