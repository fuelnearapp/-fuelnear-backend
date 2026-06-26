from __future__ import annotations

from typing import Any
import os
import time

import httpx
import jwt


APNS_KEY_ID = (os.getenv("APNS_KEY_ID") or "").strip()
APNS_TEAM_ID = (os.getenv("APNS_TEAM_ID") or "").strip()
APNS_BUNDLE_ID = (os.getenv("APNS_BUNDLE_ID") or "").strip()
APNS_AUTH_KEY = (os.getenv("APNS_AUTH_KEY") or "").strip()
APNS_ENVIRONMENT = (os.getenv("APNS_ENVIRONMENT") or "production").strip().lower()

APNS_INVALID_TOKEN_REASONS = {
    "BadDeviceToken",
    "DeviceTokenNotForTopic",
    "Unregistered",
}


class APNsConfigurationError(RuntimeError):
    pass


def apns_is_configured() -> bool:
    return bool(APNS_KEY_ID and APNS_TEAM_ID and APNS_BUNDLE_ID and APNS_AUTH_KEY)


def normalize_apns_environment(environment: str | None = None) -> str:
    selected_environment = (environment or APNS_ENVIRONMENT or "production").strip().lower()
    if selected_environment not in {"sandbox", "production"}:
        raise APNsConfigurationError("Invalid APNs environment")

    return selected_environment


def get_apns_host(environment: str) -> str:
    if environment == "sandbox":
        return "api.sandbox.push.apple.com"

    return "api.push.apple.com"


def get_apns_auth_key() -> str:
    auth_key = APNS_AUTH_KEY.replace("\\n", "\n")
    if not auth_key:
        raise APNsConfigurationError("APNs auth key is not configured")

    return auth_key


def build_apns_provider_token() -> str:
    if not apns_is_configured():
        raise APNsConfigurationError("APNs is not configured")

    return jwt.encode(
        {
            "iss": APNS_TEAM_ID,
            "iat": int(time.time()),
        },
        get_apns_auth_key(),
        algorithm="ES256",
        headers={
            "alg": "ES256",
            "kid": APNS_KEY_ID,
        },
    )


def send_apns_push(
    *,
    device_token: str,
    title: str,
    body: str,
    environment: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_environment = normalize_apns_environment(environment)
    provider_token = build_apns_provider_token()

    push_payload = dict(payload or {})
    push_payload["aps"] = {
        "alert": {
            "title": title,
            "body": body,
        },
        "sound": "default",
    }

    headers = {
        "authorization": f"bearer {provider_token}",
        "apns-topic": APNS_BUNDLE_ID,
        "apns-push-type": "alert",
        "apns-priority": "10",
    }
    url = f"https://{get_apns_host(selected_environment)}/3/device/{device_token}"

    try:
        with httpx.Client(http2=True, timeout=10.0) as client:
            response = client.post(url, headers=headers, json=push_payload)
    except httpx.HTTPError as exc:
        return {
            "success": False,
            "status_code": None,
            "reason": exc.__class__.__name__,
            "invalid_token": False,
            "temporary_error": True,
            "environment": selected_environment,
        }

    reason = None
    try:
        response_payload = response.json()
        if isinstance(response_payload, dict):
            reason = response_payload.get("reason")
    except ValueError:
        reason = None

    invalid_token = response.status_code in {400, 410} and reason in APNS_INVALID_TOKEN_REASONS

    return {
        "success": response.status_code == 200,
        "status_code": response.status_code,
        "reason": reason,
        "invalid_token": invalid_token,
        "temporary_error": response.status_code >= 500,
        "environment": selected_environment,
    }
