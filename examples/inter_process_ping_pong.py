import asyncio
import logging
import multiprocessing

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
    async with AsyncioEndpoint.serve(ConnectionConfig.from_name("e1")) as server:
        server.subscribe(
            SecondThingHappened,
            lambda event: logging.info(
                "Received via SUBSCRIBE API in proc1: %s", event.payload
            ),
        )
        await server.wait_until_any_endpoint_subscribed_to(FirstThingHappened)

        while True:
            logging.info("Hello from proc1")
            await server.broadcast(FirstThingHappened("Hit from proc1"))
            await asyncio.sleep(2)


# Base functions for second process
def run_proc2():
    setup_logging()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc2_worker())


async def proc2_worker():
    config = ConnectionConfig.from_name("e1")
    async with AsyncioEndpoint("e2").run() as client:
        await client.connect_to_endpoints(config)
        asyncio.ensure_future(display_proc1_events(client))
        client.subscribe(
            FirstThingHappened,
            lambda event: logging.info(
                "Received via SUBSCRIBE API in proc2: %s", event.payload
            ),
        )
        await client.wait_until_any_endpoint_subscribed_to(SecondThingHappened)

        while True:
            logging.info("Hello from proc2")
            await client.broadcast(SecondThingHappened("Hit from proc2 "))
            await asyncio.sleep(2)


async def display_proc1_events(endpoint):
    async for event in endpoint.stream(FirstThingHappened):
        logging.info("Received via STREAM API in proc2: %s", event.payload)


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
