"""Tests for MixpanelUtils._dispatch_batches."""

import multiprocessing.synchronize
from unittest.mock import patch

from mixpanel_utils import MixpanelUtils


def _client():
    return MixpanelUtils(
        service_account_username="test.a12345.mp-service-account",
        service_account_password="test123TEST123abc1234testing",
        project_id=1234567,
        pool_size=2,
    )


def test_dispatch_batches_sends_every_batch_without_multiprocessing_semaphores():
    client = _client()
    sent = []

    def fake_send_batch(base_url, endpoint, batch):
        sent.append(list(batch))
        return "1"

    # AWS Lambda has no /dev/shm, so creating a multiprocessing lock fails.
    with patch.object(
        multiprocessing.synchronize.SemLock,
        "__init__",
        side_effect=FileNotFoundError(2, "No such file or directory"),
    ), patch.object(client, "_send_batch", side_effect=fake_send_batch), patch.object(
        MixpanelUtils, "_async_response_handler_callback"
    ) as callback:
        client._dispatch_batches(client.import_api, "import", list(range(5)), None, batch_size=2)

    assert sorted(sent) == [[0, 1], [2, 3], [4]]
    assert callback.call_count == 3
