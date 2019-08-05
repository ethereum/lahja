import asyncio
import uuid

import pytest

from lahja import BaseEvent, ConnectionConfig
from lahja.tools.driver import (
    broadcast,
    connect_to_endpoints,
    driver,
    run_endpoint,
    serve_endpoint,
    wait_for,
    wait_until_any_endpoint_subscribed_to,
)


def generate_unique_name():
    # We use unique names to avoid clashing of IPC pipes
    return str(uuid.uuid4())


class Event(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_endpoint_driver(ipc_base_path):
    server_config = ConnectionConfig.from_name("server", base_path=ipc_base_path)

    server_done = asyncio.Event()

    server = driver(
        serve_endpoint(server_config),
        wait_for(Event, on_event=lambda endpoint, event: server_done.set()),
    )

    client = driver(
        run_endpoint("client"),
        connect_to_endpoints(server_config),
        wait_until_any_endpoint_subscribed_to(Event),
        broadcast(Event()),
    )

    asyncio.ensure_future(server())
    asyncio.ensure_future(client())

    await asyncio.wait_for(server_done.wait(), timeout=0.1)
