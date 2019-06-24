import pytest
import trio

from lahja import BaseEvent, BaseRequestResponseEvent, BroadcastConfig


class DoubleResponse(BaseEvent):
    def __init__(self, result):
        self.result = result


class DoubleRequest(BaseRequestResponseEvent[DoubleResponse]):
    def __init__(self, value):
        self.value = value

    @staticmethod
    def expected_response_type():
        return DoubleResponse


async def _handle_double_request(server):
    request = await server.wait_for(DoubleRequest)
    response = DoubleResponse(request.value * 2)
    await server.broadcast(response, request.broadcast_config())


@pytest.mark.trio
async def test_trio_endpoint_request_and_response(endpoint_pair):
    alice, bob = endpoint_pair

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_handle_double_request, alice)

        config = BroadcastConfig(alice.name)

        await bob.wait_until_endpoint_subscribed_to(alice.name, DoubleRequest)

        response = await bob.request(DoubleRequest(7), config)
        assert isinstance(response, DoubleResponse)
        assert response.result == 14
