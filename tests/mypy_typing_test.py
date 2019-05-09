from typing import AsyncIterable, Type
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
    Endpoint,
)


class Event(BaseEvent):
    pass


class RequestEvent(BaseRequestResponseEvent[Event]):
    @staticmethod
    def expected_response_type() -> Type[Event]:
        return Event


async def verify_wait_for_type(endpoint: Endpoint) -> Event:
    return await endpoint.wait_for(Event)


async def verify_stream_type(endpoint: Endpoint) -> AsyncIterable[Event]:
    async for event in endpoint.stream(Event):
        yield event


async def verify_request_response_type(endpoint: Endpoint) -> Event:
    return await endpoint.request(RequestEvent())
