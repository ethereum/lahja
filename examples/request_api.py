import asyncio
import multiprocessing
import time

from lahja import (
    Endpoint,
    EventBus,
    BaseEvent,
    BaseRequestResponseEvent,
    BroadcastConfig,
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
def run_proc1(endpoint):
    loop = asyncio.get_event_loop()
    endpoint.connect_no_wait()
    # Listen for `GetSomethingRequest`'s
    endpoint.subscribe(GetSomethingRequest, lambda event:
        # Send a response back to *only* who made that request
        endpoint.broadcast(DeliverSomethingResponse("Yay"), event.broadcast_config())
    )
    loop.run_forever()

# Base functions for second process
def run_proc2(endpoint):
    loop = asyncio.get_event_loop()
    endpoint.connect_no_wait()

    loop.run_until_complete(proc2_worker(endpoint))

async def proc2_worker(endpoint):
    for i in range(3):
        print("Requesting")
        result = await endpoint.request(GetSomethingRequest())
        print(f"Got answer: {result.payload}")

if __name__ == "__main__":
    # Configure and start event bus

    ctx = multiprocessing.get_context('spawn')
    event_bus = EventBus(ctx)

    e1 = event_bus.create_endpoint('e1')
    e2 = event_bus.create_endpoint('e2')
    event_bus.start()

    # Start two processes and pass in event bus endpoints
    p1 = ctx.Process(target=run_proc1, args=(e1,))
    p1.start()

    p2 = ctx.Process(target=run_proc2, args=(e2,))
    p2.start()
    asyncio.get_event_loop().run_forever()
