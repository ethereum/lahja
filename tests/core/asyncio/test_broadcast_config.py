import pytest

from helpers import DummyRequestPair, DummyResponse, Tracker
from lahja import BroadcastConfig


@pytest.mark.asyncio
async def test_broadcasts_to_all_endpoints(triplet_of_endpoints):
    endpoint1, endpoint2, endpoint3 = triplet_of_endpoints

    tracker = Tracker()

    await endpoint1.subscribe(
        DummyRequestPair, tracker.track_and_broadcast_dummy(1, endpoint1)
    )

    await endpoint2.subscribe(
        DummyRequestPair, tracker.track_and_broadcast_dummy(2, endpoint2)
    )

    await endpoint3.wait_until_all_remotes_subscribed_to(DummyRequestPair)

    item = DummyRequestPair()
    response = await endpoint3.request(item)
    assert isinstance(response, DummyResponse)
    assert tracker.exists(1)
    assert tracker.exists(2)
    # Ensure the registration was cleaned up
    assert item._id not in endpoint3._futures

    endpoint1.stop()
    endpoint2.stop()
    endpoint3.stop()


@pytest.mark.asyncio
async def test_broadcasts_to_specific_endpoint(triplet_of_endpoints):

    endpoint1, endpoint2, endpoint3 = triplet_of_endpoints

    tracker = Tracker()

    await endpoint1.subscribe(
        DummyRequestPair, tracker.track_and_broadcast_dummy(1, endpoint1)
    )

    await endpoint2.subscribe(
        DummyRequestPair, tracker.track_and_broadcast_dummy(2, endpoint1)
    )

    await endpoint3.wait_until_all_remotes_subscribed_to(DummyRequestPair)

    item = DummyRequestPair()
    response = await endpoint3.request(
        item, BroadcastConfig(filter_endpoint=endpoint1.name)
    )
    assert isinstance(response, DummyResponse)
    assert tracker.exists(1)
    assert not tracker.exists(2)
    # Ensure the registration was cleaned up
    assert item._id not in endpoint3._futures
