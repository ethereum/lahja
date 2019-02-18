import asyncio
import multiprocessing
import time

from lahja import (
    BaseEvent,
    Endpoint,
    ConnectionConfig,
)


class BaseExampleEvent(BaseEvent):

    def __init__(self, payload):
        super().__init__()
        self.payload = payload

# Define two different events
class FirstThingHappened(BaseExampleEvent):
    pass

class SecondThingHappened(BaseExampleEvent):
    pass

# Base functions for first process
def run_proc1():
    loop = asyncio.get_event_loop()
    endpoint = Endpoint()
    endpoint.start_serving_nowait(ConnectionConfig.from_name('e1'))
    endpoint.connect_to_endpoints_blocking(
        ConnectionConfig.from_name('e2')
    )
    endpoint.subscribe(SecondThingHappened, lambda event: 
        print("Received via SUBSCRIBE API in proc1: ", event.payload)
    )
    endpoint.subscribe(FirstThingHappened, lambda event: 
        print("Receiving own event: ", event.payload)
    )

    loop.run_until_complete(proc1_worker(endpoint))

async def proc1_worker(endpoint):
    while True:
        print("Hello from proc1")
        if is_nth_second(5):
            endpoint.broadcast(
                FirstThingHappened("Hit from proc1 ({})".format(time.time()))
            )
        await asyncio.sleep(1)

# Base functions for second process
def run_proc2():
    loop = asyncio.get_event_loop()
    endpoint = Endpoint()
    endpoint.start_serving_nowait(ConnectionConfig.from_name('e2'))
    endpoint.connect_to_endpoints_blocking(
        ConnectionConfig.from_name('e1')
    )
    endpoint.subscribe(FirstThingHappened, lambda event: 
        print("Received via SUBSCRIBE API in proc2:", event.payload)
    )
    asyncio.ensure_future(display_proc1_events(endpoint))

    loop.run_until_complete(proc2_worker(endpoint))


async def proc2_worker(endpoint):
    while True:
        print("Hello from proc2")
        if is_nth_second(2):
            endpoint.broadcast(
                SecondThingHappened("Hit from proc2 ({})".format(time.time()))
            )
        await asyncio.sleep(1)

async def display_proc1_events(endpoint):
    async for event in endpoint.stream(FirstThingHappened):
        print("Received via STREAM API in proc2: ", event.payload)


# Helper function to send events every n seconds
def is_nth_second(interval):
    return int(time.time()) % interval is 0

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    # Start two processes
    p1 = multiprocessing.Process(target=run_proc1)
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2)
    p2.start()
