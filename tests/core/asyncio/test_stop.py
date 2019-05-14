import pytest

from conftest import generate_unique_name
from helpers import DummyResponse
from lahja import AsyncioEndpoint, ConnectionConfig


@pytest.mark.asyncio
async def test_can_stop():
    first = ConnectionConfig.from_name(generate_unique_name())
    first_endpoint = AsyncioEndpoint()
    await first_endpoint.start_serving(first)

    second = ConnectionConfig.from_name(generate_unique_name())
    second_endpoint = AsyncioEndpoint()
    await second_endpoint.start_serving(second)

    await first_endpoint.connect_to_endpoints(second)
    await second_endpoint.connect_to_endpoints(first)

    first_endpoint.stop()

    await second_endpoint.broadcast(DummyResponse(None))

    second_endpoint.stop()
