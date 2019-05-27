import asyncio
import multiprocessing
import time

from lahja import (
    AsyncioEndpoint,
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
    loop.run_until_complete(run_proc1())


async def run_proc1():
    config = ConnectionConfig.from_name("e1")
    async with AsyncioEndpoint.serve(config) as endpoint:
        await endpoint.connect_to_endpoints(ConnectionConfig.from_name("e2"))
        async for event in endpoint.stream(GetSomethingRequest, num_events=3):
            await endpoint.broadcast(
                DeliverSomethingResponse("Yay"), event.broadcast_config()
            )


# Base functions for second process
def run_proc2():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proc2_worker())


async def proc2_worker():
    config = ConnectionConfig.from_name("e2")
    async with AsyncioEndpoint.serve(config) as endpoint:
        await endpoint.connect_to_endpoints(ConnectionConfig.from_name("e1"))
        await endpoint.wait_until_any_remote_subscribed_to(GetSomethingRequest)

        for i in range(3):
            print("Requesting")
            result = await endpoint.request(GetSomethingRequest())
            print(f"Got answer: {result.payload}")


if __name__ == "__main__":

    multiprocessing.set_start_method("spawn")

    p1 = multiprocessing.Process(target=spawn_proc1)
    p1.start()

    p2 = multiprocessing.Process(target=run_proc2)
    p2.start()
    p1.join()
