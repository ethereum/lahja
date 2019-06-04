import asyncio
import pickle

import pytest

from helpers import DummyRequest, DummyRequestPair
from lahja import (
    AsyncioEndpoint,
    BaseEvent,
    BaseRequestResponseEvent,
    UnexpectedResponse,
)


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
async def test_request_response(endpoint, event_loop):
    async def do_serve_response():
        req = await endpoint.wait_for(Request)
        await endpoint.broadcast(Response(req.value), req.broadcast_config())

    asyncio.ensure_future(do_serve_response())
    await endpoint.wait_until_any_remote_subscribed_to(Request)

    response = await endpoint.request(Request("test-request"))
    assert isinstance(response, Response)
    assert response.value == "test-request"


@pytest.mark.asyncio
async def test_request_can_get_cancelled(endpoint):
    item = DummyRequestPair()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(endpoint.request(item), 0.0001)
    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert item._id not in endpoint._futures


class Wrong(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_response_must_match(endpoint):
    async def do_serve_wrong_response():
        req = await endpoint.wait_for(Request)
        await endpoint.broadcast(Wrong(), req.broadcast_config())

    asyncio.ensure_future(do_serve_wrong_response())

    await endpoint.wait_until_any_remote_subscribed_to(Request)

    with pytest.raises(UnexpectedResponse):
        await endpoint.request(Request("test-wrong-response"))


@pytest.mark.asyncio
async def test_stream_with_break(endpoint):
    stream_counter = 0

    async def stream_response():
        async for event in endpoint.stream(DummyRequest):
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            nonlocal stream_counter
            stream_counter += 1

            if stream_counter == 2:
                break

    asyncio.ensure_future(stream_response())
    await endpoint.wait_until_any_remote_subscribed_to(DummyRequest)

    # we broadcast one more item than what we consume and test for that
    for i in range(5):
        await endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert DummyRequest not in endpoint.subscribed_events
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_stream_with_num_events(endpoint):
    stream_counter = 0

    async def stream_response():
        nonlocal stream_counter
        async for event in endpoint.stream(DummyRequest, num_events=2):
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            stream_counter += 1

    asyncio.ensure_future(stream_response())
    await endpoint.wait_until_any_remote_subscribed_to(DummyRequest)

    # we broadcast one more item than what we consume and test for that
    for i in range(3):
        await endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert DummyRequest not in endpoint.subscribed_events
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_stream_can_get_cancelled(endpoint):
    stream_counter = 0

    async_generator = endpoint.stream(DummyRequest)

    async def stream_response():
        nonlocal stream_counter
        async for event in async_generator:
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            stream_counter += 1
            await asyncio.sleep(0.1)

    async def cancel_soon():
        while True:
            await asyncio.sleep(0.01)
            if stream_counter == 2:
                await async_generator.aclose()

    stream_coro = asyncio.ensure_future(stream_response())
    cancel_coro = asyncio.ensure_future(cancel_soon())
    await endpoint.wait_until_any_remote_subscribed_to(DummyRequest)

    for i in range(50):
        await endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.2)
    # Ensure the registration was cleaned up
    assert DummyRequest not in endpoint.subscribed_events
    assert stream_counter == 2

    # clean up
    stream_coro.cancel()
    cancel_coro.cancel()


@pytest.mark.asyncio
async def test_stream_cancels_when_parent_task_is_cancelled(endpoint):
    stream_counter = 0

    async def stream_response():
        nonlocal stream_counter
        async for event in endpoint.stream(DummyRequest):
            # Accessing `ev.property_of_dummy_request` here allows us to validate
            # mypy has the type information we think it has. We run mypy on the tests.
            print(event.property_of_dummy_request)
            stream_counter += 1
            await asyncio.sleep(0.01)

    task = asyncio.ensure_future(stream_response())
    await endpoint.wait_until_any_remote_subscribed_to(DummyRequest)

    async def cancel_soon():
        while True:
            await asyncio.sleep(0.01)
            if stream_counter == 2:
                task.cancel()
                break

    asyncio.ensure_future(cancel_soon())

    for i in range(10):
        await endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.1)
    # Ensure the registration was cleaned up
    assert DummyRequest not in endpoint.subscribed_events
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_wait_for(endpoint):
    received = None

    async def stream_response():
        request = await endpoint.wait_for(DummyRequest)
        # Accessing `request.property_of_dummy_request` here allows us to validate
        # mypy has the type information we think it has. We run mypy on the tests.
        print(request.property_of_dummy_request)
        nonlocal received
        received = request

    asyncio.ensure_future(stream_response())
    await endpoint.wait_until_any_remote_subscribed_to(DummyRequest)
    await endpoint.broadcast(DummyRequest())

    await asyncio.sleep(0.01)
    assert isinstance(received, DummyRequest)


@pytest.mark.asyncio
async def test_wait_for_can_get_cancelled(endpoint):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(endpoint.wait_for(DummyRequest), 0.01)
    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert DummyRequest not in endpoint.subscribed_events


class RemoveItem(BaseEvent):
    def __init__(self, item):
        super().__init__()
        self.item = item


@pytest.mark.asyncio
async def test_exceptions_dont_stop_processing(capsys, endpoint):
    the_set = {1, 3}

    def handle(message):
        the_set.remove(message.item)

    endpoint.subscribe(RemoveItem, handle)
    await endpoint.wait_until_any_remote_subscribed_to(RemoveItem)

    # this call should work
    await endpoint.broadcast(RemoveItem(1))
    await asyncio.sleep(0.05)
    assert the_set == {3}

    captured = capsys.readouterr()
    assert len(captured.err) == 0

    # this call causes an exception
    await endpoint.broadcast(RemoveItem(2))
    await asyncio.sleep(0.05)
    assert the_set == {3}

    captured = capsys.readouterr()
    assert len(captured.err) > 0

    # despite the previous exception this message should get through
    await endpoint.broadcast(RemoveItem(3))
    await asyncio.sleep(0.05)
    assert the_set == set()


def test_pickle_fails():
    endpoint = AsyncioEndpoint("pickle-test")

    with pytest.raises(Exception):
        pickle.dumps(endpoint)
