import asyncio

import pytest

from lahja import AsyncioEndpoint, BaseEvent, ConnectionConfig


class StreamEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_stream_api_updates_subscriptions(pair_of_endpoints):
    subscriber, other = pair_of_endpoints
    remote = other._full_connections[subscriber.name]

    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events

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
    assert StreamEvent in remote.subscribed_messages
    assert StreamEvent in subscriber.subscribed_events

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(StreamEvent())
    event_2 = await stream_agen.asend(None)
    assert isinstance(event_2, StreamEvent)
    await stream_agen.aclose()
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events


@pytest.mark.asyncio
async def test_asyncio_wait_for_updates_subscriptions(pair_of_endpoints):
    subscriber, other = pair_of_endpoints
    remote = other._full_connections[subscriber.name]

    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    task = asyncio.ensure_future(subscriber.wait_for(StreamEvent))
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.subscribed_messages
    assert StreamEvent in subscriber.subscribed_events

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(StreamEvent())
    event = await task
    assert isinstance(event, StreamEvent)
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events


class InheretedStreamEvent(StreamEvent):
    pass


@pytest.mark.asyncio
async def test_asyncio_subscription_api_does_not_match_inherited_classes(
    pair_of_endpoints
):
    subscriber, other = pair_of_endpoints
    remote = other._full_connections[subscriber.name]

    assert StreamEvent not in remote.subscribed_messages
    assert StreamEvent not in subscriber.subscribed_events

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    task = asyncio.ensure_future(subscriber.wait_for(StreamEvent))
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert StreamEvent in remote.subscribed_messages
    assert StreamEvent in subscriber.subscribed_events

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
async def test_asyncio_subscribe_updates_subscriptions(pair_of_endpoints):
    subscriber, other = pair_of_endpoints
    remote = other._full_connections[subscriber.name]

    assert SubscribeEvent not in remote.subscribed_messages
    assert SubscribeEvent not in subscriber.subscribed_events

    received_events = []

    # trigger a `wait_for` call to run in the background and give it a moment
    # to spin up.
    subscription = await subscriber.subscribe(SubscribeEvent, received_events.append)
    await asyncio.sleep(0.01)

    # Now that we are within the wait_for, verify that the subscription is active
    # on the remote
    assert SubscribeEvent in remote.subscribed_messages
    assert SubscribeEvent in subscriber.subscribed_events

    # Broadcast and receive the second event, finishing the stream and
    # consequently the subscription
    await other.broadcast(SubscribeEvent())
    # give time for propagation
    await asyncio.sleep(0.01)
    assert len(received_events) == 1
    event = received_events[0]
    assert isinstance(event, SubscribeEvent)

    # Ensure the event is still in the subscriptions.
    assert SubscribeEvent in remote.subscribed_messages
    assert SubscribeEvent in subscriber.subscribed_events

    subscription.unsubscribe()
    # give the subscription removal time to propagate.
    await asyncio.sleep(0.01)

    # Ensure the event is no longer in the subscriptions.
    assert SubscribeEvent not in remote.subscribed_messages
    assert SubscribeEvent not in subscriber.subscribed_events


@pytest.fixture
async def client_with_three_connections(ipc_base_path):
    config_a = ConnectionConfig.from_name("server-a", base_path=ipc_base_path)
    config_b = ConnectionConfig.from_name("server-b", base_path=ipc_base_path)
    config_c = ConnectionConfig.from_name("server-c", base_path=ipc_base_path)

    async with AsyncioEndpoint.serve(config_a) as server_a:
        async with AsyncioEndpoint.serve(config_b) as server_b:
            async with AsyncioEndpoint.serve(config_c) as server_c:
                async with AsyncioEndpoint("client").run() as client:
                    await client.connect_to_endpoint(config_a)
                    await client.connect_to_endpoint(config_b)
                    await client.connect_to_endpoint(config_c)

                    yield client, server_a, server_b, server_c


class WaitSubscription(BaseEvent):
    pass


def noop(event):
    pass


@pytest.mark.asyncio
async def test_asyncio_wait_until_any_remote_subscribed_to(
    client_with_three_connections
):
    client, server_a, server_b, server_c = client_with_three_connections

    asyncio.ensure_future(server_a.subscribe(WaitSubscription, noop))

    await asyncio.wait_for(
        client.wait_until_any_remote_subscribed_to(WaitSubscription), timeout=0.1
    )


@pytest.mark.asyncio
async def test_asyncio_wait_until_all_connection_subscribed_to(
    client_with_three_connections
):
    client, server_a, server_b, server_c = client_with_three_connections

    got_subscription = asyncio.Event()

    async def do_wait_subscriptions():
        await client.wait_until_all_remotes_subscribed_to(WaitSubscription)
        got_subscription.set()

    asyncio.ensure_future(do_wait_subscriptions())

    assert len(client._full_connections) + len(client._half_connections) == 3

    await server_c.subscribe(WaitSubscription, noop)
    assert got_subscription.is_set() is False
    await server_a.subscribe(WaitSubscription, noop)
    assert got_subscription.is_set() is False
    await server_b.subscribe(WaitSubscription, noop)
    await asyncio.sleep(0.01)
    assert got_subscription.is_set() is True
