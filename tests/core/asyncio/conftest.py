import uuid

import pytest

from lahja import AsyncioEndpoint, ConnectionConfig


def generate_unique_name():
    # We use unique names to avoid clashing of IPC pipes
    return str(uuid.uuid4())


@pytest.fixture
async def endpoint(event_loop, ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    async with AsyncioEndpoint.serve(config) as endpoint:
        # We need to connect to our own Endpoint if we care about receiving
        # the events we broadcast. Many tests use the same Endpoint for
        # broadcasting and receiving which is a valid use case so we hook it up
        await endpoint.connect_to_endpoint(config)
        yield endpoint


@pytest.fixture
async def pair_of_endpoints(event_loop, ipc_base_path):
    config_1 = ConnectionConfig.from_name(
        generate_unique_name(), base_path=ipc_base_path
    )
    config_2 = ConnectionConfig.from_name(
        generate_unique_name(), base_path=ipc_base_path
    )

    async with AsyncioEndpoint.serve(config_1) as endpoint1:
        async with AsyncioEndpoint.serve(config_2) as endpoint2:
            await endpoint1.connect_to_endpoints(config_2)
            await endpoint2.connect_to_endpoints(config_1)
            yield endpoint1, endpoint2


@pytest.fixture
async def triplet_of_endpoints(event_loop, ipc_base_path):
    config_1 = ConnectionConfig.from_name(
        generate_unique_name(), base_path=ipc_base_path
    )
    config_2 = ConnectionConfig.from_name(
        generate_unique_name(), base_path=ipc_base_path
    )
    config_3 = ConnectionConfig.from_name(
        generate_unique_name(), base_path=ipc_base_path
    )

    async with AsyncioEndpoint.serve(config_1) as endpoint1:
        async with AsyncioEndpoint.serve(config_2) as endpoint2:
            async with AsyncioEndpoint.serve(config_3) as endpoint3:
                await endpoint1.connect_to_endpoints(config_2, config_3)
                await endpoint2.connect_to_endpoints(config_1, config_3)
                await endpoint3.connect_to_endpoints(config_1, config_2)

                yield endpoint1, endpoint2, endpoint3
