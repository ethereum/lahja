import asyncio

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
async def test_wait_until_connected_to():
    config = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(config):
        async with AsyncioEndpoint("client").run() as client:
            asyncio.ensure_future(client.connect_to_endpoint(config))

            assert not client.is_connected_to(config.name)
            await client.wait_until_connected_to(config.name)
            assert client.is_connected_to(config.name)


@pytest.mark.asyncio
async def test_server_establishes_reverse_connection():
    config = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(config) as server:
        async with AsyncioEndpoint("client").run() as client:
            await client.connect_to_endpoint(config)
            assert client.is_connected_to(config.name)
            await asyncio.wait_for(
                server.wait_until_connected_to(client.name), timeout=4
            )


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting():
    config = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(config):
        async with AsyncioEndpoint("client").run() as client:
            await client.connect_to_endpoint(config)

            assert client.is_connected_to(config.name)
            with pytest.raises(ConnectionAttemptRejected):
                await client.connect_to_endpoint(config)
