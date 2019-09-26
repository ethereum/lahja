import asyncio
import logging
import pickle

import pytest

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
async def test_request_can_get_cancelled(endpoint_pair):
    alice, bob = endpoint_pair

    item = Request("test")
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(alice.request(item), 0.0001)
    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert item._id not in alice._futures


class Wrong(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_response_must_match(endpoint_pair):
    alice, bob = endpoint_pair

    async def do_serve_wrong_response():
        req = await alice.wait_for(Request)
        await alice.broadcast(Wrong(), req.broadcast_config())

    asyncio.ensure_future(do_serve_wrong_response())

    await bob.wait_until_any_endpoint_subscribed_to(Request)

    with pytest.raises(UnexpectedResponse):
        await bob.request(Request("test-wrong-response"))


class SimpleEvent(BaseEvent):
    pass


@pytest.mark.asyncio
async def test_stream_with_break(endpoint_pair):
    alice, bob = endpoint_pair

    stream_counter = 0

    async def stream_response():
        async for event in alice.stream(SimpleEvent):
            nonlocal stream_counter
            stream_counter += 1

            if stream_counter == 2:
                break

    asyncio.ensure_future(stream_response())
    await bob.wait_until_any_endpoint_subscribed_to(SimpleEvent)

    # we broadcast one more item than what we consume and test for that
    for i in range(5):
        await bob.broadcast(SimpleEvent())

    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert SimpleEvent not in alice.get_subscribed_events()
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_stream_with_num_events(endpoint_pair):
    alice, bob = endpoint_pair

    stream_counter = 0

    async def stream_response():
        nonlocal stream_counter
        async for event in alice.stream(SimpleEvent, num_events=2):
            stream_counter += 1

    asyncio.ensure_future(stream_response())
    await bob.wait_until_any_endpoint_subscribed_to(SimpleEvent)

    # we broadcast one more item than what we consume and test for that
    for i in range(3):
        await bob.broadcast(SimpleEvent())

    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert SimpleEvent not in alice.get_subscribed_events()
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_stream_can_get_cancelled(endpoint_pair):
    alice, bob = endpoint_pair

    stream_counter = 0

    async_generator = alice.stream(SimpleEvent)

    async def stream_response():
        nonlocal stream_counter
        async for event in async_generator:
            stream_counter += 1
            await asyncio.sleep(0.1)

    async def cancel_soon():
        while True:
            await asyncio.sleep(0.01)
            if stream_counter == 2:
                await async_generator.aclose()

    stream_coro = asyncio.ensure_future(stream_response())
    cancel_coro = asyncio.ensure_future(cancel_soon())
    await bob.wait_until_any_endpoint_subscribed_to(SimpleEvent)

    for i in range(50):
        await bob.broadcast(SimpleEvent())

    await asyncio.sleep(0.2)
    # Ensure the registration was cleaned up
    assert SimpleEvent not in alice.get_subscribed_events()
    assert stream_counter == 2

    # clean up
    stream_coro.cancel()
    cancel_coro.cancel()


@pytest.mark.asyncio
async def test_stream_cancels_when_parent_task_is_cancelled(endpoint_pair):
    alice, bob = endpoint_pair

    stream_counter = 0

    async def stream_response():
        nonlocal stream_counter
        async for event in alice.stream(SimpleEvent):
            stream_counter += 1
            await asyncio.sleep(0.01)

    task = asyncio.ensure_future(stream_response())
    await bob.wait_until_any_endpoint_subscribed_to(SimpleEvent)

    async def cancel_soon():
        while True:
            await asyncio.sleep(0.01)
            if stream_counter == 2:
                task.cancel()
                break

    asyncio.ensure_future(cancel_soon())

    for i in range(10):
        await bob.broadcast(SimpleEvent())

    await asyncio.sleep(0.1)
    # Ensure the registration was cleaned up
    assert SimpleEvent not in alice.get_subscribed_events()
    assert stream_counter == 2


@pytest.mark.asyncio
async def test_wait_for(endpoint_pair):
    alice, bob = endpoint_pair

    received = None

    async def stream_response():
        request = await alice.wait_for(SimpleEvent)
        nonlocal received
        received = request

    asyncio.ensure_future(stream_response())
    await bob.wait_until_any_endpoint_subscribed_to(SimpleEvent)
    await bob.broadcast(SimpleEvent())

    await asyncio.sleep(0.01)
    assert isinstance(received, SimpleEvent)


@pytest.mark.asyncio
async def test_wait_for_can_get_cancelled(endpoint_pair):
    alice, _ = endpoint_pair

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(alice.wait_for(SimpleEvent), 0.01)
    await asyncio.sleep(0.01)
    # Ensure the registration was cleaned up
    assert SimpleEvent not in alice.get_subscribed_events()


class RemoveItem(BaseEvent):
    def __init__(self, item):
        super().__init__()
        self.item = item


@pytest.mark.asyncio
async def test_exceptions_dont_stop_processing(caplog, endpoint_pair):
    caplog.set_level(logging.DEBUG)
    alice, bob = endpoint_pair

    the_set = {1, 3}

    def handle(message):
        the_set.remove(message.item)

    bob.subscribe(RemoveItem, handle)
    await alice.wait_until_any_endpoint_subscribed_to(RemoveItem)

    # this call should work
    caplog.clear()
    await alice.broadcast(RemoveItem(1))
    await asyncio.sleep(0.05)
    assert the_set == {3}

    assert "Error in subscription handler" not in caplog.text

    # this call causes an exception
    caplog.clear()
    await alice.broadcast(RemoveItem(2))
    await asyncio.sleep(0.05)
    assert the_set == {3}

    assert "Error in subscription handler" in caplog.text

    # despite the previous exception this message should get through
    await alice.broadcast(RemoveItem(3))
    await asyncio.sleep(0.05)
    assert the_set == set()


def test_pickle_fails():
    alice = AsyncioEndpoint("pickle-test")

    with pytest.raises(Exception):
        pickle.dumps(alice, protocol=pickle.HIGHEST_PROTOCOL)
