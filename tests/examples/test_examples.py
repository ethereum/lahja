import asyncio
import pathlib

import pytest


@pytest.fixture
def examples_dir(request):
    return pathlib.Path(request.config.rootdir) / "examples"


async def run_example(examples_dir, example_file):
    """
    Run example and yield output line by line.
    """
    proc = await asyncio.create_subprocess_exec(
        "python",
        str(examples_dir / example_file),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    while True:
        data = await proc.stdout.readline()
        if data == b"":
            break

        line = data.decode("ascii").rstrip()
        yield line


async def run_example_until(examples_dir, example_file, predicate_fn):
    """
    Run example until it has matched a criteria for being considered successful.
    """
    seen = []

    # We expose this in the lambda to improve ergonomics
    def contains_substring(word):
        return any(word in x for x in seen)

    async for line in run_example(examples_dir, example_file):
        seen.append(line)
        if predicate_fn(seen, contains_substring):
            break

    assert predicate_fn(seen, contains_substring)


@pytest.mark.asyncio
@pytest.mark.timeout(3)
async def test_request_example(examples_dir):

    await run_example_until(
        examples_dir,
        "request_api.py",
        lambda lines, contains: contains("Requesting")
        and contains("Got answer: Yay")
        and len(lines) >= 6,
    )


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_inter_process_ping_pong_example(examples_dir):

    await run_example_until(
        examples_dir,
        "inter_process_ping_pong.py",
        lambda lines, contains: contains("Hello from proc1")
        and contains("Hello from proc2")
        and contains("Received via SUBSCRIBE API in proc2")
        and contains("Received via SUBSCRIBE API in proc1")
        and contains("Received via STREAM API in proc2"),
    )
