import asyncio
from typing import (
    Any,
    Type,
)

import pytest

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
    Endpoint,
    EventBus,
    UnexpectedResponse,
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


@pytest.mark.asyncio
async def test_request(endpoint: Endpoint) -> None:
    endpoint.subscribe(
        DummyRequestPair,
        lambda ev: endpoint.broadcast(
            # Accessing `ev.property_of_dummy_request_pair` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            DummyResponse(ev.property_of_dummy_request_pair), ev.broadcast_config()
        )
    )

    item = DummyRequestPair()
    response = await endpoint.request(item)
    # Accessing `ev.property_of_dummy_response` here allows us to validate
    # mypy has the type information we think it has. We run mypy on the tests.
    print(response.property_of_dummy_response)
    assert isinstance(response, DummyResponse)
    # Ensure the registration was cleaned up
    assert item._id not in endpoint._futures


@pytest.mark.asyncio
async def test_response_must_match(endpoint: Endpoint) -> None:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()

    endpoint.subscribe(
        DummyRequestPair,
        lambda ev: endpoint.broadcast(
            # We intentionally broadcast an unexpected response. Mypy can't catch
            # this but we ensure it is caught and raised during the processing.
            DummyRequest(), ev.broadcast_config()
        )
    )

    with pytest.raises(UnexpectedResponse):
        await endpoint.request(DummyRequestPair())
    endpoint.stop()
    bus.stop()


@pytest.mark.asyncio
async def test_stream_with_break(endpoint: Endpoint) -> None:
    stream_counter = 0

    async def stream_response() -> None:
        async for event in endpoint.stream(DummyRequest):
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            nonlocal stream_counter
            stream_counter += 1

            if stream_counter == 2:
                break

    asyncio.ensure_future(stream_response())

    # we broadcast one more item than what we consume and test for that
    for i in range(5):
        endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.1)
    # Ensure the registration was cleaned up
    assert len(endpoint._queues[DummyRequest]) == 0
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_stream_with_max(endpoint: Endpoint) -> None:
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
    # Ensure the registration was cleaned up
    assert len(endpoint._queues[DummyRequest]) == 0
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_wait_for(endpoint: Endpoint) -> None:
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
    assert isinstance(received, DummyRequest)
