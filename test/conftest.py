import pytest
from pathlib import Path
import tempfile
import shutil


@pytest.fixture
def temp_dir():
    tmp = Path(tempfile.mkdtemp())
    try:
        yield tmp
    finally:
        shutil.rmtree(tmp)
