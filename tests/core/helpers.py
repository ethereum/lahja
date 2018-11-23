from typing import (  # noqa: F401,
    Any,
    Callable,
    Set,
    Type,
)

from cytoolz import (
    curry,
)
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
    Endpoint,
)


class DummyRequest(BaseEvent):
    property_of_dummy_request = None


class DummyResponse(BaseEvent):
    property_of_dummy_response = None

    def __init__(self, something: Any) -> None:
        pass


class DummyRequestPair(BaseRequestResponseEvent[DummyResponse]):
    property_of_dummy_request_pair = None

    @staticmethod
    def expected_response_type() -> Type[DummyResponse]:
        return DummyResponse


class Tracker:

    def __init__(self) -> None:
        self._tracker: Set[int] = set()

    def exists(self, track_id: int) -> bool:
        return track_id in self._tracker

    def track_and_run(self, track_id: int, continue_fn: Callable[..., Any]) -> Any:
        """
        Add ``track_id`` to the internal accounting and continue with ``continue_fn``
        """
        self._tracker.add(track_id)
        return continue_fn()

    @curry
    def track_and_broadcast_dummy(self,
                                  track_id: int,
                                  endpoint: Endpoint,
                                  ev: DummyRequestPair) -> None:
        self.track_and_run(
            track_id,
            lambda: endpoint.broadcast(
                DummyResponse(ev.property_of_dummy_request_pair), ev.broadcast_config()
            )
        )
