import pytest
import trio

from lahja import BaseEvent, BroadcastConfig


class EventTest(BaseEvent):
    pass


class EventUnexpected(BaseEvent):
    pass


class EventInherited(EventTest):
    pass


@pytest.mark.trio
async def test_trio_endpoint_subscribe(endpoint_pair):
    alice, bob = endpoint_pair

    results = []

    alice.subscribe(EventTest, results.append)

    await bob.wait_until_endpoint_subscribed_to(alice.name, EventTest)

    await bob.broadcast(EventTest())
    await bob.broadcast(EventUnexpected(), BroadcastConfig(require_subscriber=False))
    await bob.broadcast(EventInherited(), BroadcastConfig(require_subscriber=False))
    await bob.broadcast(EventTest())

    # enough cycles to allow the alice to process the event
    await trio.sleep(0.05)

    assert len(results) == 2
    assert all(type(event) is EventTest for event in results)


@pytest.mark.trio
async def test_trio_endpoint_unsubscribe(endpoint_pair):
    alice, bob = endpoint_pair

    results = []

    subscription = alice.subscribe(EventTest, results.append)

    await bob.wait_until_endpoint_subscribed_to(alice.name, EventTest)

    await bob.broadcast(EventTest())
    await bob.broadcast(EventUnexpected(), BroadcastConfig(require_subscriber=False))
    await bob.broadcast(EventInherited(), BroadcastConfig(require_subscriber=False))
    await bob.broadcast(EventTest())

    # enough cycles to allow the alice to process the event
    await trio.sleep(0.05)

    subscription.unsubscribe()

    await bob.broadcast(EventTest(), BroadcastConfig(require_subscriber=False))
    await bob.broadcast(EventUnexpected(), BroadcastConfig(require_subscriber=False))
    await bob.broadcast(EventInherited(), BroadcastConfig(require_subscriber=False))
    await bob.broadcast(EventTest(), BroadcastConfig(require_subscriber=False))

    # enough cycles to allow the alice to process the event
    await trio.sleep(0.05)

    assert len(results) == 2
    assert all(type(event) is EventTest for event in results)
