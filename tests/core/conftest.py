import asyncio
from typing import (
    Generator,
    Tuple,
)

import pytest

from lahja import (
    Endpoint,
    EventBus,
)


@pytest.fixture(scope='function')
def endpoint(event_loop: asyncio.AbstractEventLoop) -> Generator[Endpoint, None, None]:
    bus = EventBus()
    endpoint = bus.create_endpoint('test')
    bus.start(event_loop)
    endpoint.connect(event_loop)
    try:
        yield endpoint
    finally:
        endpoint.stop()
        bus.stop()


@pytest.fixture(scope='function')
def pair_of_endpoints(event_loop: asyncio.AbstractEventLoop
                      ) -> Generator[Tuple[Endpoint, Endpoint], None, None]:

    bus = EventBus()
    endpoint1 = bus.create_endpoint('e1')
    endpoint2 = bus.create_endpoint('e2')
    bus.start(event_loop)
    endpoint1.connect(event_loop)
    endpoint2.connect(event_loop)
    try:
        yield endpoint1, endpoint2
    finally:
        endpoint1.stop()
        endpoint2.stop()
        bus.stop()
