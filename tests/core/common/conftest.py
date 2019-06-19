import pytest

from lahja.tools.engine import AsyncioEngine, IsolatedProcessEngine, TrioEngine


@pytest.fixture(params=(AsyncioEngine, TrioEngine))
def base_engine(request):
    return request.param()


@pytest.fixture(params=(True, False))
def engine(request, base_engine):
    if request.param is False:
        return base_engine
    elif request.param is True:
        return IsolatedProcessEngine(base_engine)
    else:
        raise Exception("WHAT?!")
