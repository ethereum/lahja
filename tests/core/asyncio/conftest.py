import uuid

import pytest

from lahja import AsyncioEndpoint, ConnectionConfig


def generate_unique_name():
    # We use unique names to avoid clashing of IPC pipes
    return str(uuid.uuid4())


@pytest.fixture
def unique_name():
    return generate_unique_name()


@pytest.fixture
def server_config(unique_name, ipc_base_path):
    return ConnectionConfig.from_name(f"server-{unique_name}", base_path=ipc_base_path)


@pytest.fixture
async def endpoint():
    async with AsyncioEndpoint(generate_unique_name()).run() as endpoint:
        yield endpoint


@pytest.fixture
async def endpoint_server(server_config):
    async with AsyncioEndpoint.serve(server_config) as server:
        yield server


@pytest.fixture
async def endpoint_client(server_config, endpoint_server, unique_name):
    async with AsyncioEndpoint(f"client-{unique_name}").run() as client:
        await client.connect_to_endpoints(server_config)
        await endpoint_server.wait_until_connected_to(client.name)
        yield client


@pytest.fixture(params=("client-server", "server-client"))
def endpoint_pair(request, endpoint_client, endpoint_server):
    if request.param == "client-server":
        return endpoint_client, endpoint_server
    elif request.param == "server-client":
        return endpoint_server, endpoint_client
    else:
        raise Exception(f"Unknown param: {request.param}")


@pytest.fixture(scope="function")
async def server_with_two_clients(
    unique_name, server_config, endpoint_server, endpoint_client
):
    async with AsyncioEndpoint(f"3rd-wheel-{unique_name}").run() as client_b:
        await client_b.connect_to_endpoints(server_config)
        await endpoint_server.wait_until_connected_to(client_b.name)
        yield endpoint_server, endpoint_client, client_b
