import pytest

from conftest import (
    generate_unique_name,
)
from lahja import (
    ConnectionAttemptRejected,
    ConnectionConfig,
    Endpoint,
)


@pytest.mark.asyncio
async def test_can_not_connect_conflicting_names_blocking() -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own)

    # We connect to our own Endpoint because for this test, it doesn't matter
    # if we use a foreign one or our own
    endpoint.connect_to_endpoints_blocking(own)

    # Can't connect a second time
    with pytest.raises(ConnectionAttemptRejected):
        endpoint.connect_to_endpoints_blocking(own)


@pytest.mark.asyncio
async def test_can_not_connect_conflicting_names() -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own)

    # We connect to our own Endpoint because for this test, it doesn't matter
    # if we use a foreign one or our own
    await endpoint.connect_to_endpoints(own)

    # Can't connect a second time
    with pytest.raises(ConnectionAttemptRejected):
        await endpoint.connect_to_endpoints(own)


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting_blocking() -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own)

    with pytest.raises(ConnectionAttemptRejected):
        endpoint.connect_to_endpoints_blocking(own, own)


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting() -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own)

    with pytest.raises(ConnectionAttemptRejected):
        await endpoint.connect_to_endpoints(own, own)


@pytest.mark.asyncio
async def test_rejects_duplicates_when_connecting_nowait() -> None:

    own = ConnectionConfig.from_name(generate_unique_name())
    endpoint = Endpoint()
    await endpoint.start_serving(own)

    with pytest.raises(ConnectionAttemptRejected):
        endpoint.connect_to_endpoints_nowait(own, own)
