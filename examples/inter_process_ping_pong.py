import asyncio
import logging
import multiprocessing
import time
import sys

from lahja import BaseEvent, AsyncioEndpoint, ConnectionConfig


class BaseExampleEvent(BaseEvent):
    def __init__(self, payload):
        super().__init__()
        self.payload = payload


# Define two different events
class FirstThingHappened(BaseExampleEvent):
    pass


class SecondThingHappened(BaseExampleEvent):
    pass


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)8s  %(asctime)s  %(message)s",
        datefmt="%m-%d %H:%M:%S",
    )


# Base functions for first process
def run_proc1():
    setup_logging()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc1_worker())


async def proc1_worker():
    async with AsyncioEndpoint.serve(ConnectionConfig.from_name("e1")) as endpoint:
        await endpoint.connect_to_endpoints(ConnectionConfig.from_name("e2"))
        endpoint.subscribe(
            SecondThingHappened,
            lambda event: logging.info(
                "Received via SUBSCRIBE API in proc1: %s", event.payload
            ),
        )
        endpoint.subscribe(
            FirstThingHappened,
            lambda event: logging.info("Receiving own event: %s", event.payload),
        )

        while True:
            logging.info("Hello from proc1")
            if is_nth_second(5):
                await endpoint.broadcast(FirstThingHappened("Hit from proc1"))
            await asyncio.sleep(1)


# Base functions for second process
def run_proc2():
    setup_logging()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc2_worker())


async def proc2_worker():
    async with AsyncioEndpoint.serve(ConnectionConfig.from_name("e2")) as endpoint:
        await endpoint.connect_to_endpoints(ConnectionConfig.from_name("e1"))
        asyncio.ensure_future(display_proc1_events(endpoint))
        endpoint.subscribe(
            FirstThingHappened,
            lambda event: logging.info(
                "Received via SUBSCRIBE API in proc2: %s", event.payload
            ),
        )
        while True:
            logging.info("Hello from proc2")
            if is_nth_second(2):
                await endpoint.broadcast(SecondThingHappened("Hit from proc2 "))
            await asyncio.sleep(1)


async def display_proc1_events(endpoint):
    async for event in endpoint.stream(FirstThingHappened):
        logging.info("Received via STREAM API in proc2: %s", event.payload)


# Helper function to send events every n seconds
def is_nth_second(interval):
    return int(time.time()) % interval is 0


if __name__ == "__main__":
    # WARNING: The `fork` method does not work well with asyncio yet.
    # This might change with Python 3.8 (See https://bugs.python.org/issue22087#msg318140)
    multiprocessing.set_start_method("spawn")

    # Start two processes
    p1 = multiprocessing.Process(target=run_proc1)
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2)
    p2.start()
    p1.join()
    p2.join()
