import asyncio
import multiprocessing
import time

from lahja import (
    Endpoint,
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
    ConnectionConfig,
)


class DeliverSomethingResponse(BaseEvent):
    def __init__(self, payload):
        super().__init__()
        self.payload = payload


# Define request / response pair
class GetSomethingRequest(BaseRequestResponseEvent[DeliverSomethingResponse]):

    @staticmethod
    def expected_response_type():
        return DeliverSomethingResponse


# Base functions for first process
def spawn_proc1():
    loop = asyncio.get_event_loop()
    loop.ensure_future(run_proc1())
    loop.run_forever()


async def run_proc1():
    endpoint = Endpoint()
    await endpoint.start_serving(ConnectionConfig.from_name('e1'))
    await endpoint.connect_to_endpoints(
        ConnectionConfig.from_name('e2'),
    )
    print("subscribing")
    # Listen for `GetSomethingRequest`'s
    endpoint.subscribe(GetSomethingRequest, lambda event:
        # Send a response back to *only* who made that request
        endpoint.broadcast_nowait(DeliverSomethingResponse("Yay"), event.broadcast_config())
    )


# Base functions for second process
def run_proc2():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc2_worker())


async def proc2_worker():
    endpoint = Endpoint()
    await endpoint.start_serving(ConnectionConfig.from_name('e2'))
    await endpoint.connect_to_endpoints(
        ConnectionConfig.from_name('e1'),
    )
    for i in range(3):
        print("Requesting")
        result = await endpoint.request(GetSomethingRequest())
        print(f"Got answer: {result.payload}")

if __name__ == "__main__":

    multiprocessing.set_start_method('spawn')

    p1 = multiprocessing.Process(target=spawn_proc1)
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2)
    p2.start()
    asyncio.get_event_loop().run_forever()
