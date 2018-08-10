import asyncio
import multiprocessing
import time

from lahja.eventbus import (
    Endpoint,
    EventBus,
)

PROC1_FIRED = "proc1_fired"
PROC2_FIRED = "proc2_fired"


def run_proc1(ep):
    loop = asyncio.get_event_loop()
    ep.connect()
    ep.subscribe(PROC2_FIRED, lambda item: 
        print("Received in proc1: ", item.payload)
    )
    ep.subscribe(PROC1_FIRED, lambda item: 
        print("Receiving own event: ", item.payload)
    )

    loop.run_until_complete(proc1_worker("Hello from proc1", ep))

def run_proc2(ep):
    loop = asyncio.get_event_loop()
    ep.connect()
    ep.subscribe(PROC1_FIRED, lambda item: 
        print("Received in proc2: ", item.payload)
    )
    asyncio.ensure_future(display_proc1_events(ep))

    loop.run_until_complete(proc2_worker("Hello from proc2", ep))

async def proc1_worker(term, ep):
    while True:
        print(term)
        if is_nth_second(5):
            ep.broadcast(
                PROC1_FIRED,
                "Hit from proc1 ({})".format(time.time())
            )
        await asyncio.sleep(1)

async def proc2_worker(term, ep):
    while True:
        print(term)
        if is_nth_second(2):
            ep.broadcast(
                PROC2_FIRED,
                "Hit from proc2 ({})".format(time.time())
            )
        await asyncio.sleep(1)

async def display_proc1_events(ep):
    while True:
        item = await ep.dequeue(PROC1_FIRED).get()
        print("DEQUED: ", item.payload)
        await asyncio.sleep(1)

def is_nth_second(interval):
    return int(time.time()) % interval is 0

if __name__ == "__main__":
    event_bus = EventBus()
    e1 = event_bus.create_endpoint('e1')
    e2 = event_bus.create_endpoint('e2')
    event_bus.start()

    p1 = multiprocessing.Process(target=run_proc1, args=(e1,))
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2, args=(e2,))
    p2.start()
