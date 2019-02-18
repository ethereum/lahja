import asyncio
import pathlib
import time


def wait_for_path_blocking(path: pathlib.Path, timeout: int=30) -> None:
    """
    Waits up to ``timeout`` seconds for the path to appear at path
    ``path`` otherwise raises :exc:`TimeoutError`.
    """
    start_at = time.monotonic()
    while time.monotonic() - start_at < timeout:
        if path.exists():
            return
        else:
            time.sleep(0.05)

    raise TimeoutError(f"IPC socket file {path} has not appeared in {timeout} seconds")


async def wait_for_path(path: pathlib.Path) -> None:
    """
    Wait for the path to appear at ``path``
    """
    while not path.exists():
        await asyncio.sleep(0.05)
