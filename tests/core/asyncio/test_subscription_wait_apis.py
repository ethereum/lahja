import asyncio

import pytest

from helpers import DummyRequestPair, DummyResponse


@pytest.mark.asyncio
async def test_wait_until_any_subscribed(triplet_of_endpoints):
    endpoint1, endpoint2, endpoint3 = triplet_of_endpoints

    await endpoint1.subscribe(DummyRequestPair, lambda _: None)

    await endpoint3.wait_until_any_remote_subscribed_to(DummyRequestPair)

    subscriptions = endpoint3.get_connected_endpoints_and_subscriptions()

    assert (endpoint1.name, {DummyRequestPair}) in subscriptions

    assert endpoint3.is_remote_subscribed_to(endpoint1.name, DummyRequestPair)
    assert not endpoint3.is_remote_subscribed_to(endpoint2.name, DummyRequestPair)
    assert not endpoint3.is_remote_subscribed_to("nonexistent", DummyRequestPair)

    assert not endpoint3.is_remote_subscribed_to(endpoint1.name, DummyResponse)
    assert not endpoint3.is_remote_subscribed_to(endpoint2.name, DummyResponse)

    endpoint1.stop()
    endpoint2.stop()
    endpoint3.stop()


@pytest.mark.asyncio
async def test_wait_until_all_subscribed(triplet_of_endpoints):
    endpoint1, endpoint2, endpoint3 = triplet_of_endpoints

    await endpoint1.subscribe(DummyRequestPair, lambda _: None)

    await endpoint2.subscribe(DummyRequestPair, lambda _: None)

    await endpoint3.wait_until_all_remotes_subscribed_to(DummyRequestPair)

    subscriptions = endpoint3.get_connected_endpoints_and_subscriptions()

    assert (endpoint1.name, {DummyRequestPair}) in subscriptions
    assert (endpoint2.name, {DummyRequestPair}) in subscriptions

    assert endpoint3.is_remote_subscribed_to(endpoint1.name, DummyRequestPair)
    assert endpoint3.is_remote_subscribed_to(endpoint2.name, DummyRequestPair)
    assert not endpoint3.is_remote_subscribed_to("nonexistent", DummyRequestPair)

    assert not endpoint3.is_remote_subscribed_to(endpoint1.name, DummyResponse)
    assert not endpoint3.is_remote_subscribed_to(endpoint2.name, DummyResponse)

    endpoint1.stop()
    endpoint2.stop()
    endpoint3.stop()


@pytest.mark.asyncio
async def test_wait_until_specific_subscribed(triplet_of_endpoints):
    endpoint1, endpoint2, endpoint3 = triplet_of_endpoints

    await endpoint1.subscribe(DummyRequestPair, lambda _: None)

    await endpoint3.wait_until_remote_subscribed_to(endpoint1.name, DummyRequestPair)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            endpoint3.wait_until_all_remotes_subscribed_to(DummyRequestPair), 0.01
        )

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            endpoint3.wait_until_remote_subscribed_to(endpoint2.name, DummyRequestPair),
            0.01,
        )

    subscriptions = endpoint3.get_connected_endpoints_and_subscriptions()

    assert (endpoint1.name, {DummyRequestPair}) in subscriptions
    assert not (endpoint2.name, {DummyRequestPair}) in subscriptions

    assert endpoint3.is_remote_subscribed_to(endpoint1.name, DummyRequestPair)
    assert not endpoint3.is_remote_subscribed_to(endpoint2.name, DummyRequestPair)

    assert not endpoint3.is_remote_subscribed_to("nonexistent", DummyRequestPair)

    endpoint1.stop()
    endpoint2.stop()
    endpoint3.stop()
