import pytest

from conftest import generate_unique_name
from lahja import ConnectionAttemptRejected, ConnectionConfig
from lahja.trio.endpoint import TrioEndpoint


@pytest.mark.trio
async def test_connecting_to_other_trio_endpoint(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)

    async with TrioEndpoint.serve(config):
        async with TrioEndpoint("client").run() as client:
            await client.connect_to_endpoints(config)

            assert client.is_connected_to(config.name)


@pytest.mark.trio
async def test_trio_duplicate_endpoint_connection_is_error(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)

    async with TrioEndpoint.serve(config):
        async with TrioEndpoint("client").run() as client:
            await client.connect_to_endpoints(config)

            assert client.is_connected_to(config.name)

            with pytest.raises(ConnectionAttemptRejected):
                await client.connect_to_endpoints(config)
