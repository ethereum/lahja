from concurrent.futures import CancelledError
import sys

import pytest
import trio

from lahja.trio.future import Future


@pytest.mark.trio
async def test_trio_future_result():
    fut = Future()

    async def do_set():
        fut.set_result("done")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(do_set)
        result = await fut
        assert result == "done"


class ForTest(Exception):
    pass


@pytest.mark.trio
async def test_trio_future_exception():
    fut = Future()

    async def do_set():
        try:
            raise ForTest("testing")
        except ForTest:
            _, err, tb = sys.exc_info()
        fut.set_exception(err, tb)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(do_set)
        with pytest.raises(ForTest, match="testing"):
            await fut


@pytest.mark.trio
async def test_trio_future_cancelled():
    fut = Future()

    async def do_set():
        fut.cancel()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(do_set)
        with pytest.raises(CancelledError):
            await fut
