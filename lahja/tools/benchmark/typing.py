from typing import NamedTuple, Type

from lahja import BaseEvent, BaseRequestResponseEvent


class RawMeasureEntry(NamedTuple):
    sent_at: float
    received_at: float


class CrunchedMeasureEntry(NamedTuple):
    sent_at: float
    received_at: float
    duration: float


class PerfMeasureEvent(BaseEvent):
    def __init__(self, payload: bytes, index: int, sent_at: float) -> None:
        self.payload = payload
        self.index = index
        self.sent_at = sent_at


class PerfMeasureResponse(BaseEvent):
    pass


class PerfMeasureRequest(BaseRequestResponseEvent[PerfMeasureResponse]):
    def __init__(self, payload: bytes, index: int, sent_at: float) -> None:
        self.payload = payload
        self.index = index
        self.sent_at = sent_at

    @staticmethod
    def expected_response_type() -> Type[PerfMeasureResponse]:
        return PerfMeasureResponse


class ShutdownEvent(BaseEvent):
    pass


class Total(NamedTuple):
    caption: str
    num_total: int
    duration_fastest: float
    duration_slowest: float
    duration_avg: float
    total_aggregated_time: float
    total_duration: float
    first_sent: float
    last_received: float


class TotalRecordedEvent(BaseEvent):
    def __init__(self, total: Total) -> None:
        self.total = total
