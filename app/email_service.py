from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape
import os
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

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
    verification_code: str,
    expires_at: datetime,
) -> tuple[str, str, str]:
    link = build_verification_link(verification_token)
    month_names = (
        "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
        "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre",
    )
    local_expires_at = expires_at.astimezone(ZoneInfo("Europe/Rome"))
    expires_text = (
        f"{local_expires_at.day} {month_names[local_expires_at.month - 1]} {local_expires_at.year} "
        f"alle {local_expires_at:%H:%M} ({local_expires_at:%Z})"
    )
    subject = "Verifica il tuo account FuelNear"

    text_lines = [
        "Benvenuto su FuelNear.",
        "",
        "Per completare la registrazione, verifica il tuo indirizzo email.",
        "",
        f"Codice di verifica: {verification_code}",
        f"Scadenza: {expires_text}",
    ]
    if link:
        text_lines.extend(["", f"Link verifica: {link}"])

    safe_display_code = escape(" ".join(verification_code))
    safe_expires = escape(expires_text)
    link_html = ""
    if link:
        safe_link = escape(link, quote=True)
        link_html = f"""
        <div style="margin:28px 0">
          <a href="{safe_link}" style="display:inline-block;background:#147d55;color:#ffffff;text-decoration:none;font-weight:600;padding:12px 20px;border-radius:8px">Verifica account</a>
        </div>
        """

    html = f"""
    <div style="margin:0;background:#f4f7f5;padding:32px 16px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#17211d;line-height:1.55">
      <div style="max-width:520px;margin:0 auto;background:#ffffff;border:1px solid #dce5e0;border-radius:8px;overflow:hidden">
        <div style="background:#147d55;color:#ffffff;padding:22px 28px;font-size:24px;font-weight:700">FuelNear</div>
        <div style="padding:28px">
          <h1 style="font-size:22px;margin:0 0 12px">Verifica il tuo account</h1>
          <p style="margin:0 0 22px">Inserisci questo codice nell'app FuelNear per completare la registrazione:</p>
          <div style="font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:34px;font-weight:700;letter-spacing:0;text-align:center;background:#eef6f1;border:1px solid #c9ded2;padding:16px;border-radius:8px">{safe_display_code}</div>
          <p style="color:#52615a;margin:18px 0 0">Il codice scade il {safe_expires}.</p>
          {link_html}
          <hr style="border:0;border-top:1px solid #e3e9e6;margin:28px 0 20px">
          <p style="font-size:13px;color:#68766f;margin:0">Se non hai richiesto questa registrazione, ignora questa email.</p>
        </div>
      </div>
    </div>
    """

    return subject, "\n".join(text_lines), html


def send_verification_email(
    *,
    to_email: str,
    verification_token: str,
    verification_code: str,
    expires_at: datetime,
) -> EmailDeliveryResult:
    email_debug_log("service invoked")
    if not email_delivery_is_configured():
        email_debug_log("delivery not_configured")
        return EmailDeliveryResult(delivery="not_configured", provider=EMAIL_PROVIDER or None)

    email_debug_log("send starting provider=resend")
    subject, text, html = build_verification_email(
        verification_token=verification_token,
        verification_code=verification_code,
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
