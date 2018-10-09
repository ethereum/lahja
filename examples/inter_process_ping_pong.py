import asyncio
import multiprocessing
import time

from lahja import (
    Endpoint,
    EventBus,
    BaseEvent
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
def run_proc1(endpoint):
    loop = asyncio.get_event_loop()
    endpoint.connect_no_wait()
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
def run_proc2(endpoint):
    loop = asyncio.get_event_loop()
    endpoint.connect_no_wait()
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
    # Configure and start event bus
    event_bus = EventBus()
    e1 = event_bus.create_endpoint('e1')
    e2 = event_bus.create_endpoint('e2')
    event_bus.start()

    # Start two processes and pass in event bus endpoints
    p1 = multiprocessing.Process(target=run_proc1, args=(e1,))
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2, args=(e2,))
    p2.start()
