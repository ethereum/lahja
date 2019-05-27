import asyncio

import pytest

from lahja import BaseEvent, BroadcastConfig


class BroadcastEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_broadcasts_to_all_endpoints(server_with_two_clients):
    server, client_a, client_b = server_with_two_clients

    return_queue = asyncio.Queue()

    client_a.subscribe(BroadcastEvent, return_queue.put_nowait)
    client_b.subscribe(BroadcastEvent, return_queue.put_nowait)

    await server.wait_until_all_remotes_subscribed_to(BroadcastEvent)
    await server.broadcast(BroadcastEvent())

    result_a = await asyncio.wait_for(return_queue.get(), timeout=0.1)
    result_b = await asyncio.wait_for(return_queue.get(), timeout=0.1)

    assert isinstance(result_a, BroadcastEvent)
    assert isinstance(result_b, BroadcastEvent)


class TailEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_broadcasts_to_specific_endpoint(server_with_two_clients):

    server, client_a, client_b = server_with_two_clients

    return_queue = asyncio.Queue()

    client_a.subscribe(BroadcastEvent, return_queue.put_nowait)
    client_b.subscribe(BroadcastEvent, return_queue.put_nowait)
    client_a.subscribe(TailEvent, return_queue.put_nowait)
    client_b.subscribe(TailEvent, return_queue.put_nowait)

    await server.wait_until_all_remotes_subscribed_to(BroadcastEvent)
    await server.wait_until_all_remotes_subscribed_to(TailEvent)

    # broadcast once targeted at a specific endpoint
    await server.broadcast(
        BroadcastEvent(), BroadcastConfig(filter_endpoint=client_a.name)
    )
    # broadcast again to all endpoints
    await server.broadcast(TailEvent())

    # get what should be all items out of the queues.
    # by feeding the second set of events that go through both receiving
    # endpoints we can know that our first event isn't sitting somewhere
    # waiting to be processed since it was broadcast ahead of the tail events
    result = await return_queue.get()
    tail_event_a = await return_queue.get()
    tail_event_b = await return_queue.get()

    assert isinstance(result, BroadcastEvent)
    assert isinstance(tail_event_a, TailEvent)
    assert isinstance(tail_event_b, TailEvent)

    # ensure the queues are empty as they should be.
    assert return_queue.qsize() == 0
