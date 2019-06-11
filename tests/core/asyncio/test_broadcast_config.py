import asyncio

import pytest

from lahja import BaseEvent, BaseRequestResponseEvent, BroadcastConfig


class BroadcastEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_broadcasts_to_all_endpoints(server_with_two_clients):
    server, client_a, client_b = server_with_two_clients

    return_queue = asyncio.Queue()

    client_a.subscribe(BroadcastEvent, return_queue.put_nowait)
    client_b.subscribe(BroadcastEvent, return_queue.put_nowait)

    await server.wait_until_all_endpoints_subscribed_to(
        BroadcastEvent, include_self=False
    )

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

    await server.wait_until_all_endpoints_subscribed_to(
        BroadcastEvent, include_self=False
    )
    await server.wait_until_all_endpoints_subscribed_to(TailEvent, include_self=False)

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


class Response(BaseEvent):
    def __init__(self, value):
        self.value = value


class Request(BaseRequestResponseEvent[Response]):
    @staticmethod
    def expected_response_type():
        return Response

    def __init__(self, value):
        self.value = value


@pytest.mark.asyncio
async def test_request_to_specific_endpoint(server_with_two_clients):
    server, client_a, client_b = server_with_two_clients

    async def handler_a():
        request = await client_a.wait_for(Request)
        await client_a.broadcast(Response("handler-a"), request.broadcast_config())

    async def handler_b():
        request = await client_b.wait_for(Request)
        await client_b.broadcast(Response("handler-b"), request.broadcast_config())

    asyncio.ensure_future(handler_a())
    asyncio.ensure_future(handler_b())

    await asyncio.wait_for(
        server.wait_until_all_endpoints_subscribed_to(Request, include_self=False),
        timeout=0.1,
    )

    response_a = await asyncio.wait_for(
        server.request(Request("test"), BroadcastConfig(filter_endpoint=client_a.name)),
        timeout=0.1,
    )
    response_b = await asyncio.wait_for(
        server.request(Request("test"), BroadcastConfig(filter_endpoint=client_b.name)),
        timeout=0.1,
    )

    assert response_a.value == "handler-a"
    assert response_b.value == "handler-b"
