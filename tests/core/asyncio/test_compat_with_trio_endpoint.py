import multiprocessing

import pytest
import trio

from lahja import BaseEvent
from lahja.asyncio.endpoint import ConnectionConfig
from lahja.trio.endpoint import TrioEndpoint


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


async def _do_trio_client_endpoint(name, ipc_path):
    config = ConnectionConfig(name, ipc_path)
    async with TrioEndpoint(name + "-client").run() as client:
        await client.connect_to_endpoints(config)

        assert client.is_connected_to(name)
        await client.wait_until_endpoint_subscribed_to(config.name, EventTest)
        event = EventTest("test")

        await client.broadcast(event)


@pytest.mark.asyncio
async def test_legacy_endpoint_serving_trio_endpoint(endpoint_server, server_config):
    name = server_config.name
    path = server_config.path

    proc = multiprocessing.Process(
        target=trio.run, args=(_do_trio_client_endpoint, name, path)
    )
    proc.start()

    result = await endpoint_server.wait_for(EventTest)
    assert isinstance(result, EventTest)
    assert result.value == "test"

    proc.join()
