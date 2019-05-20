from pathlib import Path
import tempfile
import uuid

import pytest


@pytest.fixture
def ipc_base_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def ipc_path(ipc_base_path):
    return ipc_base_path / str(uuid.uuid4())
