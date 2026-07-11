from __future__ import annotations

from typing import Any
import os
import time
import threading

import httpx
import jwt


APNS_KEY_ID = (os.getenv("APNS_KEY_ID") or "").strip()
APNS_TEAM_ID = (os.getenv("APNS_TEAM_ID") or "").strip()
APNS_BUNDLE_ID = (os.getenv("APNS_BUNDLE_ID") or "").strip()
APNS_AUTH_KEY = (os.getenv("APNS_AUTH_KEY") or "").strip()
APNS_ENVIRONMENT = (os.getenv("APNS_ENVIRONMENT") or "production").strip().lower()
APNS_SEND_MAX_ATTEMPTS = max(1, int(os.getenv("APNS_SEND_MAX_ATTEMPTS", "3")))
APNS_RETRY_BASE_SECONDS = max(0.0, float(os.getenv("APNS_RETRY_BASE_SECONDS", "0.5")))
APNS_PROVIDER_TOKEN_TTL_SECONDS = 50 * 60
APNS_PROVIDER_TOKEN_REFRESH_MARGIN_SECONDS = 5 * 60

APNS_INVALID_TOKEN_REASONS = {
    "BadDeviceToken",
    "DeviceTokenNotForTopic",
    "Unregistered",
}
APNS_TEMPORARY_STATUS_CODES = {429, 500, 502, 503, 504}

_provider_token_lock = threading.Lock()
_provider_token_value: str | None = None
_provider_token_expires_at = 0.0
_last_provider_token_reused = False


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


def get_cached_apns_provider_token() -> tuple[str, bool]:
    global _last_provider_token_reused, _provider_token_expires_at, _provider_token_value

    now = time.time()
    with _provider_token_lock:
        if (
            _provider_token_value
            and now < _provider_token_expires_at - APNS_PROVIDER_TOKEN_REFRESH_MARGIN_SECONDS
        ):
            _last_provider_token_reused = True
            return _provider_token_value, True

        _provider_token_value = build_apns_provider_token()
        _provider_token_expires_at = now + APNS_PROVIDER_TOKEN_TTL_SECONDS
        _last_provider_token_reused = False
        return _provider_token_value, False


def get_last_provider_token_reused() -> bool:
    return _last_provider_token_reused


def build_apns_payload(title: str, body: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    push_payload = dict(payload or {})
    push_payload["aps"] = {
        "alert": {
            "title": title,
            "body": body,
        },
        "sound": "default",
    }
    return push_payload


def parse_apns_response(response: httpx.Response, selected_environment: str, attempts: int) -> dict[str, Any]:
    reason = None
    try:
        response_payload = response.json()
        if isinstance(response_payload, dict):
            reason = response_payload.get("reason")
    except ValueError:
        reason = None

    invalid_token = response.status_code in {400, 410} and reason in APNS_INVALID_TOKEN_REASONS
    temporary_error = response.status_code in APNS_TEMPORARY_STATUS_CODES

    return {
        "success": response.status_code == 200,
        "status_code": response.status_code,
        "reason": reason,
        "invalid_token": invalid_token,
        "temporary_error": temporary_error,
        "environment": selected_environment,
        "attempts": attempts,
    }


class APNsPushClient:
    def __init__(
        self,
        *,
        max_attempts: int = APNS_SEND_MAX_ATTEMPTS,
        retry_base_seconds: float = APNS_RETRY_BASE_SECONDS,
    ) -> None:
        self.max_attempts = max(1, max_attempts)
        self.retry_base_seconds = max(0.0, retry_base_seconds)
        self._clients: dict[str, httpx.Client] = {}
        self._client_lock = threading.Lock()
        self.client_reused = False
        self.jwt_reused = False

    def close(self) -> None:
        for client in self._clients.values():
            client.close()
        self._clients.clear()

    def __enter__(self) -> "APNsPushClient":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()

    def _get_client(self, selected_environment: str) -> httpx.Client:
        with self._client_lock:
            client = self._clients.get(selected_environment)
            if client is not None:
                self.client_reused = True
                return client

            client = httpx.Client(http2=True, timeout=10.0)
            self._clients[selected_environment] = client
            return client

    def send_push(
        self,
        *,
        device_token: str,
        title: str,
        body: str,
        environment: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        selected_environment = normalize_apns_environment(environment)
        push_payload = build_apns_payload(title, body, payload)
        url = f"https://{get_apns_host(selected_environment)}/3/device/{device_token}"
        client = self._get_client(selected_environment)

        last_result: dict[str, Any] | None = None
        for attempt in range(1, self.max_attempts + 1):
            provider_token, token_reused = get_cached_apns_provider_token()
            self.jwt_reused = self.jwt_reused or token_reused
            headers = {
                "authorization": f"bearer {provider_token}",
                "apns-topic": APNS_BUNDLE_ID,
                "apns-push-type": "alert",
                "apns-priority": "10",
            }

            try:
                response = client.post(url, headers=headers, json=push_payload)
                result = parse_apns_response(response, selected_environment, attempt)
            except httpx.HTTPError as exc:
                result = {
                    "success": False,
                    "status_code": None,
                    "reason": exc.__class__.__name__,
                    "invalid_token": False,
                    "temporary_error": True,
                    "environment": selected_environment,
                    "attempts": attempt,
                }

            last_result = result
            if result["success"] or result["invalid_token"] or not result["temporary_error"]:
                return result
            if attempt < self.max_attempts:
                time.sleep(self.retry_base_seconds * attempt)

        return last_result or {
            "success": False,
            "status_code": None,
            "reason": "UnknownAPNsError",
            "invalid_token": False,
            "temporary_error": True,
            "environment": selected_environment,
            "attempts": self.max_attempts,
        }


def send_apns_push(
    *,
    device_token: str,
    title: str,
    body: str,
    environment: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with APNsPushClient() as client:
        return client.send_push(
            device_token=device_token,
            title=title,
            body=body,
            environment=environment,
            payload=payload,
        )
