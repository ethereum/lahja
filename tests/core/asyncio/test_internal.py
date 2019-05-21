import asyncio

import pytest

from lahja import BaseEvent, BroadcastConfig


class Internal(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_internal_propagation(pair_of_endpoints):
    endpoint_a, endpoint_b = pair_of_endpoints

    async def do_wait_for(endpoint, event):
        await endpoint.wait_for(Internal)
        event.set()

    got_by_endpoint_a = asyncio.Event()
    got_by_endpoint_b = asyncio.Event()

    asyncio.ensure_future(do_wait_for(endpoint_a, got_by_endpoint_a))
    will_not_finish = asyncio.ensure_future(do_wait_for(endpoint_b, got_by_endpoint_b))

    # give subscriptions time to update
    await endpoint_a.wait_until_all_remotes_subscribed_to(Internal)
    await endpoint_b.wait_until_all_remotes_subscribed_to(Internal)

    # now broadcast a few over the internal bus on `A`
    for _ in range(5):
        await endpoint_a.broadcast(Internal(), BroadcastConfig(internal=True))
    await asyncio.sleep(0.01)

    assert got_by_endpoint_a.is_set() is True
    assert got_by_endpoint_b.is_set() is False

    will_not_finish.cancel()
