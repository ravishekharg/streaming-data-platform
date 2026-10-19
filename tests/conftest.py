import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture(scope="session")
def spark():
    from sdp.spark import build_session
    s = build_session("unit-tests", with_lake=False)
    yield s
    s.stop()
