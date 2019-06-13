import pytest
import trio

from lahja import BaseEvent


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.trio
async def test_trio_endpoint_stream_without_limit(endpoint_pair):
    alice, bob = endpoint_pair

    async with trio.open_nursery() as nursery:
        done = trio.Event()

        async def _do_stream():
            results = []
            async for event in alice.stream(EventTest):
                results.append(event)
                if len(results) == 4:
                    break

            assert len(results) == 4
            assert all(isinstance(result, EventTest) for result in results)

            values = [result.value for result in results]
            assert values == [0, 1, 2, 3]
            done.set()

        nursery.start_soon(_do_stream)
        await bob.wait_until_endpoint_subscribed_to(alice.name, EventTest)

        await bob.broadcast(EventTest(0))
        await bob.broadcast(EventTest(1))
        await bob.broadcast(EventTest(2))
        await bob.broadcast(EventTest(3))

        await done.wait()


@pytest.mark.trio
async def test_trio_endpoint_stream_with_limit(endpoint_pair):
    alice, bob = endpoint_pair

    async with trio.open_nursery() as nursery:
        done = trio.Event()

        async def _do_stream():
            results = []
            async for event in alice.stream(EventTest, num_events=4):
                results.append(event)
                assert len(results) <= 4

            assert len(results) == 4
            assert all(isinstance(result, EventTest) for result in results)

            values = [result.value for result in results]
            assert values == [0, 1, 2, 3]
            done.set()

        nursery.start_soon(_do_stream)

        await bob.wait_until_endpoint_subscribed_to(alice.name, EventTest)
        await bob.broadcast(EventTest(0))
        await bob.broadcast(EventTest(1))
        await bob.broadcast(EventTest(2))
        await bob.broadcast(EventTest(3))

        await done.wait()
