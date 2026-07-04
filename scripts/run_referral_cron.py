#!/usr/bin/env python3

import json
import os
import socket
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (10, 30, 60)
RETRYABLE_HTTP_STATUSES = {502, 503, 504}
REQUEST_TIMEOUT_SECONDS = 60


def get_process_url() -> str:
    explicit_url = os.getenv("REFERRAL_PROCESS_URL", "").strip()
    if explicit_url:
        return explicit_url

    backend_url = os.getenv("FUELNEAR_BACKEND_URL", "").strip().rstrip("/")
    if backend_url:
        return f"{backend_url}/admin/process-referrals"

    raise RuntimeError("REFERRAL_PROCESS_URL or FUELNEAR_BACKEND_URL is required")


def essential_response(raw_body: bytes) -> str:
    body = raw_body.decode("utf-8", errors="replace").strip()
    if not body:
        return "empty response"

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body[:300]

    if not isinstance(parsed, dict):
        return str(parsed)[:300]

    summary = {
        key: parsed[key]
        for key in ("status", "message", "error_code")
        if key in parsed
    }
    result = parsed.get("result")
    if isinstance(result, dict) and "processed_count" in result:
        summary["processed_count"] = result["processed_count"]

    return json.dumps(summary or {"status": "response_received"}, ensure_ascii=True)[:300]


def main() -> int:
    try:
        process_url = get_process_url()
    except RuntimeError as exc:
        print(f"[REFERRAL CRON] Configuration error: {exc}", flush=True)
        return 2

    admin_token = (
        os.getenv("ADMIN_UPDATE_TOKEN", "").strip()
        or os.getenv("ADMIN_TOKEN", "").strip()
    )
    if not admin_token:
        print(
            "[REFERRAL CRON] Configuration error: ADMIN_UPDATE_TOKEN or ADMIN_TOKEN is required",
            flush=True,
        )
        return 2

    for attempt in range(1, MAX_ATTEMPTS + 1):
        print(f"[REFERRAL CRON] Attempt {attempt}/{MAX_ATTEMPTS}", flush=True)
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
                    f"[REFERRAL CRON] Success status={response.status} "
                    f"response={response_summary}",
                    flush=True,
                )
                return 0
        except HTTPError as exc:
            response_summary = essential_response(exc.read())
            print(
                f"[REFERRAL CRON] HTTP failure status={exc.code} "
                f"response={response_summary}",
                flush=True,
            )
            if exc.code not in RETRYABLE_HTTP_STATUSES:
                print("[REFERRAL CRON] Final failure: non-retryable HTTP error", flush=True)
                return 1
            retry_reason = f"HTTP status={exc.code}"
        except (TimeoutError, socket.timeout) as exc:
            retry_reason = f"timeout type={exc.__class__.__name__}"
        except URLError as exc:
            reason_type = exc.reason.__class__.__name__ if exc.reason is not None else "unknown"
            retry_reason = f"connection error type={reason_type}"
        except OSError as exc:
            retry_reason = f"network error type={exc.__class__.__name__}"

        if attempt == MAX_ATTEMPTS:
            print(
                f"[REFERRAL CRON] Final failure after {MAX_ATTEMPTS} attempts: {retry_reason}",
                flush=True,
            )
            return 1

        delay = RETRY_DELAYS_SECONDS[attempt - 1]
        print(
            f"[REFERRAL CRON] Attempt failed: {retry_reason}; retry in {delay}s",
            flush=True,
        )
        time.sleep(delay)

    return 1


if __name__ == "__main__":
    sys.exit(main())
