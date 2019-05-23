import pytest

from conftest import generate_unique_name
from lahja import AsyncioEndpoint, ConnectionConfig


@pytest.mark.asyncio
async def test_endpoint_run():
    endpoint = AsyncioEndpoint("test-run")

    assert endpoint.is_running is False

    async with endpoint.run():
        assert endpoint.is_running is True
    assert endpoint.is_running is False


@pytest.mark.asyncio
async def test_endpoint_run_with_error():
    endpoint = AsyncioEndpoint("test-run")

    assert endpoint.is_running is False

    with pytest.raises(Exception, match="break out of run"):
        async with endpoint.run():
            assert endpoint.is_running is True
            raise Exception("break out of run")

    assert endpoint.is_running is False


@pytest.mark.asyncio
async def test_endpoint_serve(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)
    async with AsyncioEndpoint.serve(config) as endpoint:
        assert endpoint.is_running is True
        assert endpoint.is_serving is True

    assert endpoint.is_running is False
    assert endpoint.is_serving is False


@pytest.mark.asyncio
async def test_endpoint_serve_with_error(ipc_base_path):
    config = ConnectionConfig.from_name(generate_unique_name(), base_path=ipc_base_path)

    with pytest.raises(Exception, match="break out of serve"):
        async with AsyncioEndpoint.serve(config) as endpoint:
            assert endpoint.is_running is True
            assert endpoint.is_serving is True
            raise Exception("break out of serve")

    assert endpoint.is_running is False
    assert endpoint.is_serving is False
