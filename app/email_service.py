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
        "FuelNear",
        "",
        "Verifica il tuo account",
        "",
        "Inserisci questo codice nell’app FuelNear per completare la registrazione.",
        "",
        f"Codice di verifica: {verification_code}",
        f"Scadenza: {expires_text}",
        "",
        "Se non hai richiesto questa registrazione, ignora questa email.",
    ]

    safe_display_code = escape(" ".join(verification_code))
    safe_expires = escape(expires_text)

    html = f"""<!doctype html>
    <html lang="it">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta name="color-scheme" content="light dark">
        <meta name="supported-color-schemes" content="light dark">
        <style>
          @media (prefers-color-scheme: dark) {{
            .email-bg {{ background-color:#0b1018 !important; }}
            .email-card {{ background-color:#151c27 !important; border-color:#293445 !important; }}
            .email-title {{ color:#f5f8fc !important; }}
            .email-copy {{ color:#c6d0dd !important; }}
            .email-code {{ background-color:#112a46 !important; border-color:#245b91 !important; color:#67b4ff !important; }}
            .email-divider {{ border-color:#293445 !important; }}
            .email-muted {{ color:#8f9cac !important; }}
          }}
        </style>
      </head>
      <body class="email-bg" style="margin:0;padding:0;background-color:#f3f6fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#152033;">
        <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">Il tuo codice di verifica FuelNear.</div>
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" class="email-bg" style="width:100%;background-color:#f3f6fa;">
          <tr>
            <td align="center" style="padding:40px 16px;">
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" class="email-card" style="width:100%;max-width:540px;background-color:#ffffff;border:1px solid #dce4ee;border-radius:8px;overflow:hidden;">
                <tr>
                  <td style="padding:26px 30px 14px;">
                    <table role="presentation" cellspacing="0" cellpadding="0" border="0">
                      <tr>
                        <td align="center" valign="middle" style="width:38px;height:38px;background-color:#0a84ff;border-radius:50%;color:#ffffff;font-size:20px;font-weight:800;">F</td>
                        <td class="email-title" style="padding-left:12px;color:#152033;font-size:21px;font-weight:700;">FuelNear</td>
                      </tr>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td style="padding:18px 30px 30px;">
                    <h1 class="email-title" style="margin:0 0 12px;color:#152033;font-size:24px;line-height:1.25;font-weight:700;">Verifica il tuo account</h1>
                    <p class="email-copy" style="margin:0 0 26px;color:#4d5b70;font-size:16px;line-height:1.55;">Inserisci questo codice nell’app FuelNear per completare la registrazione.</p>
                    <div class="email-code" style="box-sizing:border-box;width:100%;padding:20px 12px;background-color:#edf6ff;border:1px solid #b8d9fa;border-radius:8px;color:#006bd6;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:34px;line-height:1.2;font-weight:800;letter-spacing:0;text-align:center;white-space:nowrap;">{safe_display_code}</div>
                    <p class="email-muted" style="margin:18px 0 0;color:#68778b;font-size:14px;line-height:1.5;">Il codice scade il {safe_expires}.</p>
                    <div class="email-divider" style="border-top:1px solid #e2e8f0;margin:28px 0 18px;"></div>
                    <p class="email-muted" style="margin:0;color:#7a8798;font-size:13px;line-height:1.5;">Se non hai richiesto questa registrazione, ignora questa email.</p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
      </body>
    </html>"""

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
