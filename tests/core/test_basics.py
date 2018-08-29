import asyncio
import pytest

from lahja import (
    BaseEvent,
    Endpoint,
    EventBus,
)

class DummyRequest(BaseEvent):
    pass

class DummyResponse(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_foo():
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