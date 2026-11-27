"""Tests for ConcurrentPaginator."""

import multiprocessing.synchronize
from unittest.mock import patch

from mixpanel_utils.paginator import ConcurrentPaginator


def _fake_engage(total=7, page_size=2):
    """Return a get_func that serves `total` profiles split into pages."""

    def get_page(params):
        page = params.get("page", 0)
        start = page * page_size
        return {
            "results": list(range(start, min(start + page_size, total))),
            "session_id": "abc",
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    return get_page


def test_fetch_all_returns_every_page_in_order():
    paginator = ConcurrentPaginator(_fake_engage(), concurrency=3)
    assert paginator.fetch_all() == list(range(7))


def test_fetch_all_does_not_need_multiprocessing_semaphores():
    # AWS Lambda has no /dev/shm, so creating a multiprocessing lock fails.
    with patch.object(
        multiprocessing.synchronize.SemLock,
        "__init__",
        side_effect=FileNotFoundError(2, "No such file or directory"),
    ):
        paginator = ConcurrentPaginator(_fake_engage(), concurrency=2)
        assert paginator.fetch_all() == list(range(7))
