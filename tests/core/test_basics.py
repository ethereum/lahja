import asyncio
from typing import (
    Any,
)

import pytest

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
    EventBus,
)


class DummyRequest(BaseEvent):
    property_of_dummy_request = None


class DummyResponse(BaseEvent):
    property_of_dummy_response = None

    def __init__(self, something: Any) -> None:
        pass


class DummyRequestPair(BaseRequestResponseEvent[DummyResponse]):
    property_of_dummy_request_pair = None


@pytest.mark.asyncio
async def test_request() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()

    endpoint.subscribe(
        DummyRequestPair,
        lambda ev: endpoint.broadcast(
            # Accessing `ev.property_of_dummy_request_pair` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            DummyResponse(ev.property_of_dummy_request_pair), ev.broadcast_config()
        )
    )

    response = await endpoint.request(DummyRequestPair())
    # Accessing `ev.property_of_dummy_response` here allows us to validate
    # mypy has the type information we think it has. We run mypy on the tests.
    print(response.property_of_dummy_response)
    assert isinstance(response, DummyResponse)
    endpoint.stop()
    bus.stop()


@pytest.mark.asyncio
async def test_stream_with_max() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()
    stream_counter = 0

    async def stream_response() -> None:
        async for event in endpoint.stream(DummyRequest, max=2):
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            nonlocal stream_counter
            stream_counter += 1

    asyncio.ensure_future(stream_response())

    # we broadcast one more item than what we consume and test for that
    for i in range(3):
        endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.01)
    endpoint.stop()
    bus.stop()
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_wait_for() -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()
    received = None

    async def stream_response() -> None:
        request = await endpoint.wait_for(DummyRequest)
        # Accessing `ev.property_of_dummy_request` here allows us to validate
        # mypy has the type information we think it has. We run mypy on the tests.
        print(request.property_of_dummy_request)
        nonlocal received
        received = request

    asyncio.ensure_future(stream_response())
    endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.01)
    endpoint.stop()
    bus.stop()
    assert isinstance(received, DummyRequest)
