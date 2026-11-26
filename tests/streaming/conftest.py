"""Skip all streaming tests when streaming extras are not installed."""

import pytest

try:
    import mmh3
    import httpx
    import dateutil
    import aiofiles

    HAS_STREAMING = True
except ImportError:
    HAS_STREAMING = False


def pytest_collection_modifyitems(config, items):
    if not HAS_STREAMING:
        skip = pytest.mark.skip(reason="streaming extras not installed")
        for item in items:
            if "streaming" in str(item.fspath):
                item.add_marker(skip)
