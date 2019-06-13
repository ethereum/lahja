import pytest

from lahja.trio.endpoint import TrioEndpoint


@pytest.mark.trio
async def test_trio_endpoint_as_contextmanager():
    endpoint = TrioEndpoint("test")
    assert endpoint.is_running is False

    async with endpoint.run():
        assert endpoint.is_running is True
    assert endpoint.is_running is False


@pytest.mark.trio
async def test_trio_endpoint_as_contextmanager_inline():
    async with TrioEndpoint("test").run() as endpoint:
        assert endpoint.is_running is True
    assert endpoint.is_running is False
