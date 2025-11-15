import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    tmp = Path(tempfile.mkdtemp())
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp)
