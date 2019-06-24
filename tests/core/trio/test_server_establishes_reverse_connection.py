import pytest
import trio

from conftest import generate_unique_name
from lahja import ConnectionConfig
from lahja.trio.endpoint import TrioEndpoint


@pytest.mark.trio
async def test_trio_server_endpoint_establishes_reverse_connection_to_client(
    ipc_base_path
):
    unique_name = generate_unique_name()
    config = ConnectionConfig.from_name(
        f"server-{unique_name}", base_path=ipc_base_path
    )

    async with TrioEndpoint.serve(config) as server:
        async with TrioEndpoint(f"client-{unique_name}").run() as client:
            await client.connect_to_endpoints(config)

            assert client.is_connected_to(config.name)
            with trio.fail_after(2):
                await server.wait_until_connected_to(client.name)
