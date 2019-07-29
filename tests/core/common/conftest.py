import pytest

from lahja.tools.runner import (
    AsyncioRunner,
    IsolatedHeterogenousRunner,
    IsolatedHomogenousRunner,
    TrioRunner,
)


@pytest.fixture(
    params=(
        "trio-runner",
        "asyncio-runner",
        "trio-isolated",
        "asyncio-isolated",
        "mixed-isolated",
    )
)
def runner(request):
    if request.param == "trio-runner":
        return TrioRunner()
    elif request.param == "asyncio-runner":
        return AsyncioRunner()
    elif request.param == "trio-isolated":
        return IsolatedHomogenousRunner(TrioRunner())
    elif request.param == "asyncio-isolated":
        return IsolatedHomogenousRunner(TrioRunner())
    elif request.param == "mixed-isolated":
        return IsolatedHeterogenousRunner(TrioRunner(), AsyncioRunner())
    else:
        raise ValueError(f"Unknown param: {request.param}")
