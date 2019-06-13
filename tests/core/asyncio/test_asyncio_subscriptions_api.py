import asyncio

import pytest

from lahja import AsyncioEndpoint, BaseEvent, ConnectionConfig


class StreamEvent(BaseEvent):
    pass


def _get_remote(endpoint, name):
    for remote in endpoint._connections:
        if remote.name == name:
            return remote
    else:
        raise Exception("Not found")


@pytest.mark.asyncio
async def test_asyncio_stream_api_updates_subscriptions(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = _get_remote(other, subscriber.name)

    assert StreamEvent not in remote.get_subscribed_events()
    assert StreamEvent not in subscriber.get_subscribed_events()

    stream_agen = subscriber.stream(StreamEvent, num_events=2)
    # start the generator in the background and give it a moment to start (so
    # that the subscription can get setup and propogated)
    fut = asyncio.ensure_future(stream_agen.asend(None))
    await asyncio.sleep(0.01)

    # broadcast the first event and grab and validate the first streamed
    # element.
    await other.broadcast(StreamEvent())
    event_1 = await fut
    assert isinstance(event_1, StreamEvent)

    # Now that we are within the stream, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.get_subscribed_events()
    assert StreamEvent in subscriber.get_subscribed_events()

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(StreamEvent())
    event_2 = await stream_agen.asend(None)
    assert isinstance(event_2, StreamEvent)
    await stream_agen.aclose()
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert StreamEvent not in remote.get_subscribed_events()
    assert StreamEvent not in subscriber.get_subscribed_events()


@pytest.mark.asyncio
async def test_asyncio_wait_for_updates_subscriptions(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = _get_remote(other, subscriber.name)

    assert StreamEvent not in remote.get_subscribed_events()
    assert StreamEvent not in subscriber.get_subscribed_events()

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    task = asyncio.ensure_future(subscriber.wait_for(StreamEvent))
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.get_subscribed_events()
    assert StreamEvent in subscriber.get_subscribed_events()

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(StreamEvent())
    event = await task
    assert isinstance(event, StreamEvent)
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert StreamEvent not in remote.get_subscribed_events()
    assert StreamEvent not in subscriber.get_subscribed_events()


class InheretedStreamEvent(StreamEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_subscription_api_does_not_match_inherited_classes(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = _get_remote(other, subscriber.name)

    assert StreamEvent not in remote.get_subscribed_events()
    assert StreamEvent not in subscriber.get_subscribed_events()

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    task = asyncio.ensure_future(subscriber.wait_for(StreamEvent))
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.get_subscribed_events()
    assert StreamEvent in subscriber.get_subscribed_events()

    # Broadcast two of the inherited events and then the correct event.
    await other.broadcast(InheretedStreamEvent())
    await other.broadcast(InheretedStreamEvent())
    await other.broadcast(StreamEvent())

    # wait for a received event, finishing the stream and
    # consequently the subscription
    event = await task
    assert isinstance(event, StreamEvent)


class SubscribeEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_subscribe_updates_subscriptions(endpoint_pair):
    subscriber, other = endpoint_pair
    remote = _get_remote(other, subscriber.name)

    assert SubscribeEvent not in remote.get_subscribed_events()
    assert SubscribeEvent not in subscriber.get_subscribed_events()

    received_events = []

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    subscription = subscriber.subscribe(SubscribeEvent, received_events.append)
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert SubscribeEvent in remote.get_subscribed_events()
    assert SubscribeEvent in subscriber.get_subscribed_events()

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(SubscribeEvent())
    # give time for propagation
    await asyncio.sleep(0.01)
    assert len(received_events) == 1
    event = received_events[0]
    assert isinstance(event, SubscribeEvent)

    # Ensure the event is still in the subscriptions.
    assert SubscribeEvent in remote.get_subscribed_events()
    assert SubscribeEvent in subscriber.get_subscribed_events()

    subscription.unsubscribe()
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert SubscribeEvent not in remote.get_subscribed_events()
    assert SubscribeEvent not in subscriber.get_subscribed_events()


@pytest.fixture
async def client_with_three_connections(ipc_base_path):
    config_a = ConnectionConfig.from_name("server-a", base_path=ipc_base_path)
    config_b = ConnectionConfig.from_name("server-b", base_path=ipc_base_path)
    config_c = ConnectionConfig.from_name("server-c", base_path=ipc_base_path)

    async with AsyncioEndpoint.serve(config_a) as server_a:
        async with AsyncioEndpoint.serve(config_b) as server_b:
            async with AsyncioEndpoint.serve(config_c) as server_c:
                async with AsyncioEndpoint("client").run() as client:
                    await client.connect_to_endpoints(config_a)
                    await client.connect_to_endpoints(config_b)
                    await client.connect_to_endpoints(config_c)

                    yield client, server_a, server_b, server_c


class WaitSubscription(BaseEvent):
    pass


def noop(event):
    pass


@pytest.mark.asyncio
async def test_asyncio_wait_until_any_endpoint_subscribed_to(
    client_with_three_connections
):
    client, server_a, server_b, server_c = client_with_three_connections

    server_a.subscribe(WaitSubscription, noop)

    # verify it's not currently subscribed.
    assert not client.is_any_endpoint_subscribed_to(WaitSubscription)

    await asyncio.wait_for(
        client.wait_until_any_endpoint_subscribed_to(WaitSubscription), timeout=1
    )
    assert client.is_any_endpoint_subscribed_to(WaitSubscription)


@pytest.mark.asyncio
async def test_asyncio_wait_until_all_connection_subscribed_to(
    client_with_three_connections
):
    client, server_a, server_b, server_c = client_with_three_connections

    got_subscription = asyncio.Event()

    async def do_wait_subscriptions():
        await client.wait_until_all_endpoints_subscribed_to(WaitSubscription)
        got_subscription.set()

    asyncio.ensure_future(do_wait_subscriptions())

    assert len(client._connections) == 3

    server_c.subscribe(WaitSubscription, noop)
    await asyncio.sleep(0.01)
    assert got_subscription.is_set() is False
    server_a.subscribe(WaitSubscription, noop)
    await asyncio.sleep(0.01)
    assert got_subscription.is_set() is False
    server_b.subscribe(WaitSubscription, noop)
    await asyncio.sleep(0.01)
    assert got_subscription.is_set() is False

    assert client.are_all_endpoints_subscribed_to(WaitSubscription, include_self=False)
    # test both default and explicit argument
    assert not client.are_all_endpoints_subscribed_to(
        WaitSubscription, include_self=True
    )
    assert not client.are_all_endpoints_subscribed_to(WaitSubscription)

    client.subscribe(WaitSubscription, noop)
    await asyncio.sleep(0.01)
    assert got_subscription.is_set() is True

    assert client.are_all_endpoints_subscribed_to(WaitSubscription)


@pytest.mark.asyncio
async def test_wait_until_specific_subscribed(client_with_three_connections):
    client, server_a, server_b, server_c = client_with_three_connections

    got_subscription = asyncio.Event()

    async def do_wait_subscriptions():
        await client.wait_until_endpoint_subscribed_to(server_b.name, WaitSubscription)
        got_subscription.set()

    asyncio.ensure_future(do_wait_subscriptions())

    server_a.subscribe(WaitSubscription, lambda _: None)
    await asyncio.sleep(0.01)
    assert not got_subscription.is_set()

    server_c.subscribe(WaitSubscription, lambda _: None)
    await asyncio.sleep(0.01)
    assert not got_subscription.is_set()

    server_b.subscribe(WaitSubscription, lambda _: None)
    await asyncio.sleep(0.01)
    assert got_subscription.is_set()

    assert client.is_endpoint_subscribed_to(server_b.name, WaitSubscription)
