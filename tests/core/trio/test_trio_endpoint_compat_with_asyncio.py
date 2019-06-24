import asyncio
import multiprocessing

import pytest

from lahja.asyncio import AsyncioEndpoint
from lahja.common import BaseEvent, ConnectionConfig


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


def run_asyncio(coro, *args):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro(*args))
    loop.close()


async def _do_asyncio_client_endpoint(name, ipc_path):
    config = ConnectionConfig(name, ipc_path)
    async with AsyncioEndpoint(name + "client").run() as client:
        await client.connect_to_endpoints(config)
        assert client.is_connected_to(name)

        await client.wait_until_endpoint_subscribed_to(config.name, EventTest)

        event = EventTest("test")
        await client.broadcast(event)


@pytest.mark.trio
async def test_trio_endpoint_serving_asyncio_endpoint(
    endpoint_server, endpoint_server_config
):
    name = endpoint_server_config.name
    path = endpoint_server_config.path

    proc = multiprocessing.Process(
        target=run_asyncio, args=(_do_asyncio_client_endpoint, name, path)
    )
    proc.start()

    result = await endpoint_server.wait_for(EventTest)
    assert isinstance(result, EventTest)
    assert result.value == "test"

    proc.join()
