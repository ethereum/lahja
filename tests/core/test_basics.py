import asyncio
import pytest

from lahja import (
    BaseEvent,
    EventBus,
)


class DummyRequest(BaseEvent):
    pass


class DummyResponse(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_request():
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()

    endpoint.subscribe(
        DummyRequest,
        lambda ev: endpoint.broadcast(DummyResponse(), ev.broadcast_config())
    )

    response = await endpoint.request(DummyRequest())
    assert isinstance(response, DummyResponse)
    bus.shutdown()


@pytest.mark.asyncio
async def test_stream_with_max():
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start()
    endpoint.connect()
    stream_counter = 0
    async def stream_response():
        async for ev in endpoint.stream(DummyRequest, max=2):
            nonlocal stream_counter
            stream_counter += 1

    asyncio.ensure_future(stream_response())

    # we broadcast one more item than what we consume and test for that
    for i in range(3):
        endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.01)
    bus.shutdown()
    assert stream_counter == 2


