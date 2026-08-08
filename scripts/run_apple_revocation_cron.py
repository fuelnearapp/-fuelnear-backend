#!/usr/bin/env python3

import json
import os
import socket
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (10, 30)
RETRYABLE_HTTP_STATUSES = {502, 503, 504}
REQUEST_TIMEOUT_SECONDS = 300


def get_process_url() -> str:
    explicit_url = os.getenv("APPLE_REVOCATION_PROCESS_URL", "").strip()
    if explicit_url:
        return explicit_url

    backend_url = os.getenv("FUELNEAR_BACKEND_URL", "").strip().rstrip("/")
    if backend_url:
        return f"{backend_url}/admin/process-apple-revocations"

    raise RuntimeError(
        "APPLE_REVOCATION_PROCESS_URL or FUELNEAR_BACKEND_URL is required"
    )


def essential_response(raw_body: bytes) -> str:
    body = raw_body.decode("utf-8", errors="replace").strip()
    if not body:
        return "empty response"

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return "non-JSON response"

    if not isinstance(parsed, dict):
        return "response received"

    summary = {
        key: parsed[key]
        for key in ("status", "message", "error_code")
        if key in parsed
    }
    result = parsed.get("result")
    if isinstance(result, dict):
        summary["result"] = {
            key: result[key]
            for key in (
                "eligible",
                "attempted",
                "succeeded",
                "temporary_failed",
                "terminal_failed",
                "expired",
                "cleaned",
                "remaining_pending",
            )
            if key in result
        }

    return json.dumps(summary or {"status": "response_received"}, ensure_ascii=True)


def main() -> int:
    try:
        process_url = get_process_url()
    except RuntimeError as exc:
        print(f"[APPLE REVOCATION CRON] Configuration error: {exc}", flush=True)
        return 2

    admin_token = os.getenv("APPLE_REVOCATION_ADMIN_TOKEN", "").strip()
    if not admin_token:
        print(
            "[APPLE REVOCATION CRON] Configuration error: "
            "APPLE_REVOCATION_ADMIN_TOKEN is required",
            flush=True,
        )
        return 2

    for attempt in range(1, MAX_ATTEMPTS + 1):
        print(
            f"[APPLE REVOCATION CRON] Attempt {attempt}/{MAX_ATTEMPTS}",
            flush=True,
        )
        request = Request(
            process_url,
            data=b"",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Admin-Token": admin_token,
            },
            method="POST",
        )

        retry_reason: str | None = None
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                response_summary = essential_response(response.read())
                print(
                    f"[APPLE REVOCATION CRON] Success status={response.status} "
                    f"response={response_summary}",
                    flush=True,
                )
                return 0
        except HTTPError as exc:
            response_summary = essential_response(exc.read())
            print(
                f"[APPLE REVOCATION CRON] HTTP failure status={exc.code} "
                f"response={response_summary}",
                flush=True,
            )
            if exc.code not in RETRYABLE_HTTP_STATUSES:
                print(
                    "[APPLE REVOCATION CRON] Final failure: "
                    "non-retryable HTTP error",
                    flush=True,
                )
                return 1
            retry_reason = f"HTTP status={exc.code}"
        except (TimeoutError, socket.timeout) as exc:
            retry_reason = f"timeout type={exc.__class__.__name__}"
        except URLError as exc:
            reason_type = (
                exc.reason.__class__.__name__
                if exc.reason is not None
                else "unknown"
            )
            retry_reason = f"connection error type={reason_type}"
        except OSError as exc:
            retry_reason = f"network error type={exc.__class__.__name__}"

        if attempt == MAX_ATTEMPTS:
            print(
                f"[APPLE REVOCATION CRON] Final failure after {MAX_ATTEMPTS} "
                f"attempts: {retry_reason}",
                flush=True,
            )
            return 1

        delay = RETRY_DELAYS_SECONDS[attempt - 1]
        print(
            f"[APPLE REVOCATION CRON] Attempt failed: {retry_reason}; "
            f"retry in {delay}s",
            flush=True,
        )
        time.sleep(delay)

    return 1


if __name__ == "__main__":
    sys.exit(main())
