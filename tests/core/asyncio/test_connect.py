import pytest

from conftest import generate_unique_name
from lahja import AsyncioEndpoint, ConnectionAttemptRejected, ConnectionConfig


@pytest.mark.asyncio
async def test_connect_to_endpoint():
    config = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(config):
        async with AsyncioEndpoint("client").run() as client:
            await client.connect_to_endpoint(config)
            assert client.is_connected_to(config.name)


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting():
    own = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(own) as endpoint:
        await endpoint.connect_to_endpoint(own)

        assert endpoint.is_connected_to(own.name)
        with pytest.raises(ConnectionAttemptRejected):
            await endpoint.connect_to_endpoint(own)
