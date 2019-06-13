import logging

import pytest
import trio

from lahja import BaseEvent


class EventTest(BaseEvent):
    def __init__(self, value):
        self.value = value


@pytest.mark.trio
async def test_trio_endpoint_broadcast(endpoint_pair):
    logging.info("HERE")
    alice, bob = endpoint_pair

    done = trio.Event()
    event = EventTest("test")

    async with trio.open_nursery() as nursery:

        async def _do_wait_for():
            logging.info("WAITING FOR EVENT")
            result = await alice.wait_for(EventTest)
            logging.info("GOT IT")
            assert isinstance(result, EventTest)
            assert result.value == "test"
            done.set()

        nursery.start_soon(_do_wait_for)
        await bob.wait_until_endpoint_subscribed_to(alice.name, EventTest)

        logging.info("BROADCASTING")
        await bob.broadcast(event)
        logging.info("WAITING DONE")
        await done.wait()
