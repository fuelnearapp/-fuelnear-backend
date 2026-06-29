from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape
import os
from typing import Any
from urllib.parse import urlencode

import httpx


EMAIL_PROVIDER = (os.getenv("EMAIL_PROVIDER") or "").strip().lower()
RESEND_API_KEY = (os.getenv("RESEND_API_KEY") or "").strip()
EMAIL_FROM = (os.getenv("EMAIL_FROM") or "").strip()
APP_PUBLIC_BASE_URL = (os.getenv("APP_PUBLIC_BASE_URL") or "").strip().rstrip("/")


def email_debug_log(message: str) -> None:
    print(f"[AUTH][EMAIL] {message}", flush=True)


@dataclass(frozen=True)
class EmailDeliveryResult:
    delivery: str
    provider: str | None = None
    status_code: int | None = None


def email_delivery_is_configured() -> bool:
    configured = EMAIL_PROVIDER == "resend" and bool(RESEND_API_KEY and EMAIL_FROM)
    email_debug_log(
        "provider detected "
        f"provider={EMAIL_PROVIDER or 'none'} "
        f"api_key_present={bool(RESEND_API_KEY)} "
        f"from_present={bool(EMAIL_FROM)} "
        f"base_url_present={bool(APP_PUBLIC_BASE_URL)} "
        f"configured={configured}"
    )

    return configured


def build_verification_link(token: str) -> str | None:
    if not APP_PUBLIC_BASE_URL:
        return None

    return f"{APP_PUBLIC_BASE_URL}/verify-email?{urlencode({'token': token})}"


def build_verification_email(
    *,
    verification_token: str,
    expires_at: datetime,
) -> tuple[str, str, str]:
    link = build_verification_link(verification_token)
    expires_text = expires_at.isoformat()
    subject = "Verifica il tuo account FuelNear"

    text_lines = [
        "Benvenuto su FuelNear.",
        "",
        "Per completare la registrazione, verifica il tuo indirizzo email.",
        "",
        f"Codice verifica: {verification_token}",
        f"Scadenza: {expires_text}",
    ]
    if link:
        text_lines.extend(["", f"Link verifica: {link}"])

    safe_token = escape(verification_token)
    safe_expires = escape(expires_text)
    link_html = ""
    if link:
        safe_link = escape(link, quote=True)
        link_html = f'<p><a href="{safe_link}">Verifica account</a></p>'

    html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;line-height:1.5">
      <h1 style="font-size:22px;margin:0 0 16px">FuelNear</h1>
      <p>Per completare la registrazione, verifica il tuo indirizzo email.</p>
      {link_html}
      <p>Codice verifica:</p>
      <p style="font-family:ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-all;background:#f3f4f6;padding:12px;border-radius:8px">{safe_token}</p>
      <p style="color:#6b7280">Scade il {safe_expires}.</p>
    </div>
    """

    return subject, "\n".join(text_lines), html


def send_verification_email(
    *,
    to_email: str,
    verification_token: str,
    expires_at: datetime,
) -> EmailDeliveryResult:
    email_debug_log("service invoked")
    if not email_delivery_is_configured():
        email_debug_log("delivery not_configured")
        return EmailDeliveryResult(delivery="not_configured", provider=EMAIL_PROVIDER or None)

    email_debug_log("send starting provider=resend")
    subject, text, html = build_verification_email(
        verification_token=verification_token,
        expires_at=expires_at,
    )
    payload: dict[str, Any] = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": subject,
        "text": text,
        "html": html,
    }
    headers = {
        "authorization": f"Bearer {RESEND_API_KEY}",
        "content-type": "application/json",
    }

    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post("https://api.resend.com/emails", headers=headers, json=payload)
        email_debug_log("HTTP request executed")
    except httpx.HTTPError as exc:
        email_debug_log(f"HTTP request failed type={exc.__class__.__name__}")
        email_debug_log("delivery failed")
        return EmailDeliveryResult(delivery="failed", provider="resend")

    email_debug_log(f"HTTP response status={response.status_code}")
    if 200 <= response.status_code < 300:
        email_debug_log("delivery sent")
        return EmailDeliveryResult(delivery="sent", provider="resend", status_code=response.status_code)

    email_debug_log("delivery failed")
    return EmailDeliveryResult(delivery="failed", provider="resend", status_code=response.status_code)
