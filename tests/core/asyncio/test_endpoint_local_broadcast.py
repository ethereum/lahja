import asyncio

import pytest

from lahja import BaseEvent, BaseRequestResponseEvent


@pytest.mark.asyncio
async def test_local_broadcast_is_connected_to_self(endpoint):
    assert endpoint.is_connected_to(endpoint.name)


@pytest.mark.asyncio
async def test_local_broadcast_wait_until_connected_to(endpoint):
    await asyncio.wait_for(endpoint.wait_until_connected_to(endpoint.name), timeout=0.1)


@pytest.mark.asyncio
async def test_local_broadcast_result_in_being_present_in_remotes(endpoint):
    names = {name for name, _ in endpoint.get_connected_endpoints_and_subscriptions()}
    assert endpoint.name in names


class BroadcastEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_local_broadcast_wait_until_endpoint_subscriptions_change(endpoint):
    ready = asyncio.Event()
    done = asyncio.Event()

    async def do_wait():
        ready.set()
        await endpoint.wait_until_endpoint_subscriptions_change()
        done.set()

    asyncio.ensure_future(do_wait())

    await ready.wait()
    assert not done.is_set()
    endpoint.subscribe(BroadcastEvent, lambda ev: None)
    await asyncio.wait_for(done.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_subscribe_and_broadcast_to_self(endpoint):
    got_event = asyncio.Event()

    endpoint.subscribe(BroadcastEvent, lambda ev: got_event.set())

    assert not got_event.is_set()
    await endpoint.broadcast(BroadcastEvent())

    await asyncio.wait_for(got_event.wait(), timeout=0.1)
    assert got_event.is_set()


@pytest.mark.asyncio
async def test_wait_for_and_broadcast_to_self(endpoint):
    ready = asyncio.Event()
    got_event = asyncio.Event()

    async def do_wait_for():
        ready.set()
        await endpoint.wait_for(BroadcastEvent)
        got_event.set()

    asyncio.ensure_future(do_wait_for())
    await ready.wait()

    assert not got_event.is_set()
    await endpoint.broadcast(BroadcastEvent())

    await asyncio.wait_for(got_event.wait(), timeout=0.1)
    assert got_event.is_set()


@pytest.mark.asyncio
async def test_stream_and_broadcast_to_self(endpoint):
    ready = asyncio.Event()
    finished_stream = asyncio.Event()

    async def do_stream():
        ready.set()
        async for ev in endpoint.stream(BroadcastEvent, num_events=3):
            pass

        finished_stream.set()

    asyncio.ensure_future(do_stream())
    await ready.wait()

    assert not finished_stream.is_set()
    await endpoint.broadcast(BroadcastEvent())
    await endpoint.broadcast(BroadcastEvent())
    await endpoint.broadcast(BroadcastEvent())

    await asyncio.wait_for(finished_stream.wait(), timeout=0.1)
    assert finished_stream.is_set()


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
async def test_request_response_and_broadcast_to_self(endpoint):
    ready = asyncio.Event()

    async def do_response():
        ready.set()
        req = await endpoint.wait_for(Request)
        await endpoint.broadcast(Response(req.value), req.broadcast_config())

    asyncio.ensure_future(do_response())
    await ready.wait()

    resp = await asyncio.wait_for(endpoint.request(Request("test")), timeout=0.1)
    assert isinstance(resp, Response)
    assert resp.value == "test"
