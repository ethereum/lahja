import asyncio
from typing import (
    Tuple,
)

import pytest

from helpers import (
    DummyResponse,
)
from lahja import (
    BroadcastConfig,
    Endpoint,
)


@pytest.mark.asyncio
async def test_internal_propagation(pair_of_endpoints: Tuple[Endpoint, Endpoint]) -> None:

    endpoint1, endpoint2 = pair_of_endpoints

    async def broadcast_dummies() -> None:
        while True:
            # We are broadcasting internally on endpoint1
            endpoint1.broadcast(
                DummyResponse("Dummy"),
                BroadcastConfig(internal=True)
            )
            await asyncio.sleep(0.01)

    asyncio.ensure_future(broadcast_dummies())

    e1_task = asyncio.ensure_future(endpoint1.wait_for(DummyResponse))
    # We expect that this will never receive an answer
    e2_task = asyncio.ensure_future(endpoint2.wait_for(DummyResponse))

    done, pending = await asyncio.wait(
        {e1_task, e2_task},
        timeout=0.05
    )
    assert e1_task in done
    assert e2_task not in done
    assert e2_task in pending
    assert e1_task not in pending
