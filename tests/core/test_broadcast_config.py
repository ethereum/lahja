from typing import (
    Tuple,
)

import pytest

from helpers import (
    DummyRequestPair,
    DummyResponse,
    Tracker,
)
from lahja import (
    BroadcastConfig,
    Endpoint,
)


@pytest.mark.asyncio
async def test_broadcasts_to_all_endpoints(
        triplet_of_endpoints: Tuple[Endpoint, Endpoint, Endpoint]) -> None:

    endpoint1, endpoint2, endpoint3 = triplet_of_endpoints

    tracker = Tracker()

    endpoint1.subscribe(
        DummyRequestPair,
        tracker.track_and_broadcast_dummy(1, endpoint1)
    )

    endpoint2.subscribe(
        DummyRequestPair,
        tracker.track_and_broadcast_dummy(2, endpoint2)
    )

    item = DummyRequestPair()
    response = await endpoint3.request(item)
    print(response.property_of_dummy_response)
    assert isinstance(response, DummyResponse)
    assert tracker.exists(1)
    assert tracker.exists(2)
    # Ensure the registration was cleaned up
    assert item._id not in endpoint3._futures


@pytest.mark.asyncio
async def test_broadcasts_to_specific_endpoint(
        triplet_of_endpoints: Tuple[Endpoint, Endpoint, Endpoint]) -> None:

    endpoint1, endpoint2, endpoint3 = triplet_of_endpoints

    tracker = Tracker()

    endpoint1.subscribe(
        DummyRequestPair,
        tracker.track_and_broadcast_dummy(1, endpoint1)
    )

    endpoint2.subscribe(
        DummyRequestPair,
        tracker.track_and_broadcast_dummy(2, endpoint1)
    )

    item = DummyRequestPair()
    response = await endpoint3.request(item, BroadcastConfig(filter_endpoint=endpoint1.name))
    print(response.property_of_dummy_response)
    assert isinstance(response, DummyResponse)
    assert tracker.exists(1)
    assert not tracker.exists(2)
    # Ensure the registration was cleaned up
    assert item._id not in endpoint3._futures
