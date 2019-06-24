import uuid

import pytest
import pytest_trio
import trio

from lahja import ConnectionConfig
from lahja.trio.endpoint import TrioEndpoint


def generate_unique_name() -> str:
    # We use unique names to avoid clashing of IPC pipes
    return str(uuid.uuid4())


@pytest.fixture
def endpoint_server_config(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    return config


@pytest_trio.trio_fixture
async def endpoint_server(endpoint_server_config):
    async with TrioEndpoint.serve(endpoint_server_config) as endpoint:
        yield endpoint


@pytest_trio.trio_fixture
async def endpoint_client(endpoint_server_config, endpoint_server):
    async with TrioEndpoint("client-for-testing").run() as client:
        await client.connect_to_endpoints(endpoint_server_config)
        while not endpoint_server.is_connected_to("client-for-testing"):
            await trio.sleep(0)
        yield client


@pytest_trio.trio_fixture
async def endpoint(nursery):
    async with TrioEndpoint("endpoint-for-testing").run() as client:
        yield client


@pytest.fixture(params=("server_client", "client_server"))
def endpoint_pair(request, endpoint_client, endpoint_server):
    if request.param == "server_client":
        return (endpoint_server, endpoint_client)
    elif request.param == "client_server":
        return (endpoint_client, endpoint_server)
    else:
        raise Exception(f"unknown param: {request.param}")
