import asyncio
from pathlib import Path
import tempfile
import uuid

import pytest

from lahja import AsyncioEndpoint, BaseEvent, ConnectionConfig


@pytest.fixture
def config():
    with tempfile.TemporaryDirectory() as temp_dir:
        name = str(uuid.uuid4())
        ipc_path = Path(temp_dir) / name
        yield ConnectionConfig(name, ipc_path)


@pytest.fixture(scope="function")
async def server(config):
    # TODO< verify with test client.broadcast() -> server and server.broadcast() -> client works
    endpoint = AsyncioEndpoint()
    await endpoint.start_serving(config)
    # We need to connect to our own Endpoint if we care about receiving
    # the events we broadcast. Many tests use the same Endpoint for
    # broadcasting and receiving which is a valid use case so we hook it up
    with endpoint:
        yield endpoint


@pytest.fixture(scope="function")
async def client(server, config):
    # TODO< verify with test client.broadcast() -> server and server.broadcast() -> client works
    endpoint = AsyncioEndpoint()
    await endpoint.connect_to_endpoint(config)
    return endpoint


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Client has no `_name`")
async def test_broadcast_client_to_server(client, server) -> None:
    fut = asyncio.ensure_future(server.wait_for(EventTest))

    await client.broadcast(EventTest("test"))

    result = await fut

    assert isinstance(result, EventTest)
    assert result.value == "test"


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Client has no mechanism to recieve when not running")
async def test_broadcast_server_to_client(client, server) -> None:
    fut = asyncio.ensure_future(client.wait_for(EventTest))

    await server.broadcast(EventTest("test"))

    try:
        result = await asyncio.wait_for(fut, timeout=0.01)
    except TimeoutError:
        await fut.cancel()
        assert False

    assert isinstance(result, EventTest)
    assert result.value == "test"
