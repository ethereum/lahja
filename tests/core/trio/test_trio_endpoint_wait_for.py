import pytest
import trio

from lahja import BaseEvent


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.trio
async def test_trio_endpoint_wait_for(endpoint_pair):
    alice, bob = endpoint_pair

    # NOTE: this test is the inverse of the broadcast test
    event = EventTest("test")

    done = trio.Event()

    async def _do_wait_for():
        result = await alice.wait_for(EventTest)

        assert isinstance(result, EventTest)
        assert result.value == "test"
        done.set()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_do_wait_for)

        await bob.wait_until_endpoint_subscribed_to(alice.name, EventTest)

        await bob.broadcast(event)
        await done.wait()
