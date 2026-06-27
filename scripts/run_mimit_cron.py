#!/usr/bin/env python3

import json
import os
import socket
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


RETRYABLE_HTTP_STATUSES = {502, 503, 504}
RETRY_DELAYS_SECONDS = (20, 60, 120, 180)
REQUEST_TIMEOUT_SECONDS = 300


def get_update_url() -> str:
    explicit_url = os.getenv("MIMIT_UPDATE_URL", "").strip()
    if explicit_url:
        return explicit_url

    backend_url = os.getenv("FUELNEAR_BACKEND_URL", "").strip().rstrip("/")
    if backend_url:
        return f"{backend_url}/admin/update-mimit"

    raise RuntimeError("MIMIT_UPDATE_URL or FUELNEAR_BACKEND_URL is required")


def essential_response(raw_body: bytes) -> str:
    body = raw_body.decode("utf-8", errors="replace").strip()
    if not body:
        return "empty response"

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body[:500]

    if not isinstance(parsed, dict):
        return str(parsed)[:500]

    summary = {
        key: parsed[key]
        for key in ("status", "message", "run_id")
        if key in parsed
    }
    return json.dumps(summary or parsed, ensure_ascii=True)[:500]


def main() -> int:
    try:
        update_url = get_update_url()
    except RuntimeError as exc:
        print(f"[MIMIT CRON] Configuration error: {exc}", flush=True)
        return 2

    admin_token = os.getenv("ADMIN_UPDATE_TOKEN", "").strip()
    if not admin_token:
        print("[MIMIT CRON] Configuration error: ADMIN_UPDATE_TOKEN is required", flush=True)
        return 2

    attempts = len(RETRY_DELAYS_SECONDS) + 1

    for attempt in range(1, attempts + 1):
        print(f"[MIMIT CRON] Attempt {attempt}/{attempts}", flush=True)
        request = Request(
            update_url,
            headers={
                "Accept": "application/json",
                "X-Admin-Token": admin_token,
            },
            method="GET",
        )

        retry_reason: str | None = None
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                raw_body = response.read()
                response_summary = essential_response(raw_body)

                try:
                    parsed_body = json.loads(raw_body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    parsed_body = None

                if isinstance(parsed_body, dict) and parsed_body.get("status") == "busy":
                    retry_reason = "backend reported update busy"
                else:
                    print(
                        f"[MIMIT CRON] Success status={response.status} response={response_summary}",
                        flush=True,
                    )
                    return 0
        except HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_STATUSES:
                print(
                    f"[MIMIT CRON] Final failure: non-retryable HTTP status={exc.code}",
                    flush=True,
                )
                return 1
            retry_reason = f"HTTP status={exc.code}"
        except (TimeoutError, socket.timeout) as exc:
            retry_reason = f"timeout type={exc.__class__.__name__}"
        except URLError as exc:
            reason_type = exc.reason.__class__.__name__ if exc.reason is not None else "unknown"
            retry_reason = f"connection error type={reason_type}"
        except OSError as exc:
            retry_reason = f"network error type={exc.__class__.__name__}"

        if attempt == attempts:
            print(
                f"[MIMIT CRON] Final failure after {attempts} attempts: {retry_reason}",
                flush=True,
            )
            return 1

        delay = RETRY_DELAYS_SECONDS[attempt - 1]
        print(
            f"[MIMIT CRON] Attempt failed: {retry_reason}; retry in {delay}s",
            flush=True,
        )
        time.sleep(delay)

    return 1


if __name__ == "__main__":
    sys.exit(main())
