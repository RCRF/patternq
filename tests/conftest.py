import os
from functools import lru_cache

import pytest

import patternq as pq


def pytest_collection_modifyitems(config, items):
    if not os.getenv("PATTERNQ_API_KEY"):
        skip = pytest.mark.skip(reason="PATTERNQ_API_KEY not set")
        for item in items:
            if "live" in item.keywords:
                item.add_marker(skip)


@lru_cache(maxsize=None)
def test_db(dataset: str) -> str:
    return pq.resolve_db(dataset)
