from typing import AsyncIterable, Type

from lahja import AsyncioEndpoint, BaseEvent, BaseRequestResponseEvent


class Event(BaseEvent):
    pass


class RequestEvent(BaseRequestResponseEvent[Event]):
    @staticmethod
    def expected_response_type() -> Type[Event]:
        return Event


async def verify_wait_for_type(endpoint: AsyncioEndpoint) -> Event:
    return await endpoint.wait_for(Event)


async def verify_stream_type(endpoint: AsyncioEndpoint) -> AsyncIterable[Event]:
    async for event in endpoint.stream(Event):
        yield event


async def verify_request_response_type(endpoint: AsyncioEndpoint) -> Event:
    return await endpoint.request(RequestEvent())


async def verify_subscribe_type(endpoint: AsyncioEndpoint) -> None:
    def handler(event: Event) -> None:
        pass

    endpoint.subscribe(Event, handler)
