import asyncio
from typing import (
    Generator,
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
