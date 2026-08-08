from __future__ import annotations

from io import BytesIO
import json
import socket
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from scripts import run_apple_revocation_cron as cron


class FakeResponse:
    def __init__(self, status: int, payload: dict):
        self.status = status
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._body


def http_error(status: int, payload: dict) -> HTTPError:
    return HTTPError(
        "https://example.test/admin/process-apple-revocations",
        status,
        "failure",
        {},
        BytesIO(json.dumps(payload).encode("utf-8")),
    )


class AppleRevocationCronTests(unittest.TestCase):
    def base_environment(self) -> dict[str, str]:
        return {
            "FUELNEAR_BACKEND_URL": "https://example.test",
            "APPLE_REVOCATION_ADMIN_TOKEN": "cron-secret",
        }

    def test_builds_endpoint_from_backend_url(self):
        with patch.dict("os.environ", self.base_environment(), clear=True):
            self.assertEqual(
                cron.get_process_url(),
                "https://example.test/admin/process-apple-revocations",
            )

    def test_missing_configuration_exits_nonzero(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(cron.main(), 2)

    def test_success_exits_zero_and_does_not_log_secret(self):
        response = FakeResponse(
            200,
            {
                "status": "ok",
                "result": {
                    "eligible": 1,
                    "attempted": 1,
                    "succeeded": 1,
                    "remaining_pending": 0,
                },
            },
        )
        with (
            patch.dict("os.environ", self.base_environment(), clear=True),
            patch.object(cron, "urlopen", return_value=response),
            patch("builtins.print") as print_mock,
        ):
            self.assertEqual(cron.main(), 0)

        output = " ".join(str(call) for call in print_mock.call_args_list)
        self.assertNotIn("cron-secret", output)

    def test_retryable_http_error_is_retried(self):
        response = FakeResponse(200, {"status": "ok", "result": {}})
        with (
            patch.dict("os.environ", self.base_environment(), clear=True),
            patch.object(
                cron,
                "urlopen",
                side_effect=[http_error(503, {"detail": "temporary"}), response],
            ) as request_mock,
            patch.object(cron.time, "sleep"),
        ):
            self.assertEqual(cron.main(), 0)

        self.assertEqual(request_mock.call_count, 2)

    def test_auth_failure_is_not_retried(self):
        with (
            patch.dict("os.environ", self.base_environment(), clear=True),
            patch.object(
                cron,
                "urlopen",
                side_effect=http_error(403, {"detail": "Forbidden"}),
            ) as request_mock,
            patch.object(cron.time, "sleep") as sleep_mock,
        ):
            self.assertEqual(cron.main(), 1)

        self.assertEqual(request_mock.call_count, 1)
        sleep_mock.assert_not_called()

    def test_timeout_exhaustion_exits_nonzero(self):
        with (
            patch.dict("os.environ", self.base_environment(), clear=True),
            patch.object(cron, "urlopen", side_effect=socket.timeout()),
            patch.object(cron.time, "sleep") as sleep_mock,
        ):
            self.assertEqual(cron.main(), 1)

        self.assertEqual(sleep_mock.call_count, cron.MAX_ATTEMPTS - 1)


if __name__ == "__main__":
    unittest.main()
