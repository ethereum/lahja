import asyncio
from functools import (
    partial,
    wraps,
)
import multiprocessing
from typing import (
    Any,
)


def aioify(func: Any) -> Any:
    @wraps(func)
    async def run(*args: Any, loop: Any=None, executor: Any=None, **kwargs: Any) -> Any:
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


@aioify
def async_get(queue: multiprocessing.Queue) -> Any:
    return queue.get()
