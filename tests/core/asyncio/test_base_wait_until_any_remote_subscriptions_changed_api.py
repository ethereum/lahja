import asyncio

import pytest

from conftest import generate_unique_name
from lahja import AsyncioEndpoint, BaseEvent, ConnectionConfig


class SubscriptionEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_base_wait_until_any_endpoint_subscriptions_changed():
    config = ConnectionConfig.from_name(generate_unique_name())
    async with AsyncioEndpoint.serve(config) as server:
        async with AsyncioEndpoint("client").run() as client:
            await client.connect_to_endpoints(config)
            assert client.is_connected_to(config.name)

            server.subscribe(SubscriptionEvent, lambda e: None)

            assert not client.is_any_endpoint_subscribed_to(SubscriptionEvent)
            await asyncio.wait_for(
                client.wait_until_endpoint_subscriptions_change(), timeout=0.1
            )
            assert client.is_any_endpoint_subscribed_to(SubscriptionEvent)
