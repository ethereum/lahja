import asyncio

import pytest

from conftest import generate_unique_name
from lahja import AsyncioEndpoint, ConnectionAttemptRejected, ConnectionConfig


@pytest.mark.asyncio
async def test_wait_until_connected_to(endpoint):
    config = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(config):
        asyncio.ensure_future(endpoint.connect_to_endpoints(config))

        assert not endpoint.is_connected_to(config.name)
        await endpoint.wait_until_connected_to(config.name)
        assert endpoint.is_connected_to(config.name)


@pytest.mark.asyncio
async def test_endpoint_rejects_self_connection(endpoint_server, server_config):
    with pytest.raises(ConnectionAttemptRejected):
        await endpoint_server.connect_to_endpoints(server_config)
