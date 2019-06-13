import asyncio

import pytest

from conftest import generate_unique_name
from lahja import AsyncioEndpoint, ConnectionConfig


@pytest.mark.asyncio
async def test_base_wait_until_connections_changed():
    config = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(config):
        async with AsyncioEndpoint("client").run() as client:
            asyncio.ensure_future(client.connect_to_endpoints(config))

            assert not client.is_connected_to(config.name)
            await asyncio.wait_for(client.wait_until_connections_change(), timeout=0.1)
            assert client.is_connected_to(config.name)
