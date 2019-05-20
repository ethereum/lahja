from typing import Any, Callable, Set, Type  # noqa: F401,

from cytoolz import curry

from lahja import BaseEvent, BaseRequestResponseEvent


class DummyRequest(BaseEvent):
    property_of_dummy_request = None


class DummyResponse(BaseEvent):
    property_of_dummy_response = None

    def __init__(self, something):
        pass


class DummyRequestPair(BaseRequestResponseEvent[DummyResponse]):
    property_of_dummy_request_pair = None

    @staticmethod
    def expected_response_type():
        return DummyResponse


class Tracker:
    def __init__(self):
        self._tracker = set()

    def exists(self, track_id):
        return track_id in self._tracker

    def track_and_run(self, track_id, continue_fn):
        """
        Add ``track_id`` to the internal accounting and continue with ``continue_fn``
        """
        self._tracker.add(track_id)
        return continue_fn()

    @curry
    def track_and_broadcast_dummy(self, track_id, endpoint, ev):
        self.track_and_run(
            track_id,
            lambda: endpoint.broadcast_nowait(
                DummyResponse(ev.property_of_dummy_request_pair), ev.broadcast_config()
            ),
        )
