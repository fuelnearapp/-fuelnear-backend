from typing import Any
from email.utils import parsedate_to_datetime
import hashlib
import hmac
import os
from math import cos, isfinite, radians
import threading
import time
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import urlparse

import psycopg2
import jwt
from jwt import PyJWKClient, PyJWTError
from psycopg2.extras import RealDictCursor
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Header, Depends
from pydantic import BaseModel, EmailStr

from app.import_mimit import update_mimit_data
from app.apns_client import APNsConfigurationError, apns_is_configured, send_apns_push
from app.auth_utils import (
    generate_access_token,
    generate_refresh_token,
    generate_referral_code,
    hash_password,
    verify_password,
    hash_token,
)


DB_NAME = os.getenv("DB_NAME", "fuelnear")
DB_USER = os.getenv("DB_USER", "matteo")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DATABASE_URL = os.getenv("DATABASE_URL")
MIMIT_UPDATE_INTERVAL_SECONDS = int(os.getenv("MIMIT_UPDATE_INTERVAL_SECONDS", str(24 * 60 * 60)))
MIMIT_STALE_AFTER_SECONDS = int(os.getenv("MIMIT_STALE_AFTER_SECONDS", str(30 * 60)))
ADMIN_UPDATE_TOKEN = os.getenv("ADMIN_UPDATE_TOKEN")
PRICE_NOTIFICATION_MIN_IMPROVEMENT_EUR = max(
    0.0,
    float(os.getenv("PRICE_NOTIFICATION_MIN_IMPROVEMENT_EUR", "0.01")),
)

ACCESS_TOKEN_TTL_HOURS = int(os.getenv("ACCESS_TOKEN_TTL_HOURS", "24"))
REFRESH_TOKEN_TTL_DAYS = int(os.getenv("REFRESH_TOKEN_TTL_DAYS", "30"))
EMAIL_VERIFICATION_TTL_HOURS = int(os.getenv("EMAIL_VERIFICATION_TTL_HOURS", "24"))
APPLE_CLIENT_ID = (os.getenv("APPLE_CLIENT_ID") or os.getenv("APPLE_BUNDLE_ID") or "").strip()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_IDS = [
    client_id.strip()
    for client_id in (os.getenv("GOOGLE_CLIENT_IDS") or GOOGLE_CLIENT_ID or "").split(",")
    if client_id.strip()
]

APPLE_JWKS_URL = "https://appleid.apple.com/auth/keys"
GOOGLE_JWKS_URL = "https://www.googleapis.com/oauth2/v3/certs"
GOOGLE_ISSUERS = {"accounts.google.com", "https://accounts.google.com"}

app = FastAPI(title="FuelNear Backend")


_scheduler_started = False
_scheduler_lock = threading.Lock()
_mimit_update_lock = threading.Lock()
_mimit_state_lock = threading.Lock()
_mimit_update_started_at: datetime | None = None
_mimit_update_run_id: int | None = None
_mimit_stale_warning_logged = False
MIMIT_ADVISORY_LOCK_ID = 618_493_027


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    display_name: str | None = None
    referral_code: str | None = None
    device_info: str | None = None



class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    device_info: str | None = None


class EmailVerificationRequest(BaseModel):
    token: str


class ResendEmailVerificationRequest(BaseModel):
    email: EmailStr


class RefreshRequest(BaseModel):
    refresh_token: str


class AppleAuthRequest(BaseModel):
    identity_token: str | None = None
    identityToken: str | None = None
    authorization_code: str | None = None
    authorizationCode: str | None = None
    email: str | None = None
    full_name: Any | None = None
    fullName: Any | None = None
    display_name: str | None = None
    nonce: str | None = None
    raw_nonce: str | None = None
    rawNonce: str | None = None
    hashed_nonce: str | None = None
    hashedNonce: str | None = None
    referral_code: str | None = None
    device_info: str | None = None


class GoogleAuthRequest(BaseModel):
    id_token: str
    display_name: str | None = None
    referral_code: str | None = None
    device_info: str | None = None


class ApplyReferralCodeRequest(BaseModel):
    referral_code: str | None = None


class DeviceTokenRequest(BaseModel):
    device_token: str
    platform: str
    environment: str | None = None
    app_version: str | None = None
    device_info: str | None = None


class DeleteDeviceTokenRequest(BaseModel):
    device_token: str


class UserLocationRequest(BaseModel):
    lat: float
    lng: float
    accuracy: float | None = None
    source: str | None = None


class NotificationPreferencesRequest(BaseModel):
    price_notifications_enabled: bool | None = None
    fuel_type: str | None = None
    radius_km: float | None = None
    favorites_only: bool | None = None
    latitude: float | None = None
    longitude: float | None = None


class AdminTestPushRequest(BaseModel):
    user_id: int | None = None
    device_token: str | None = None
    title: str
    body: str
    payload: dict[str, Any] | None = None


class CommunityPriceReportRequest(BaseModel):
    fuel_type: str
    price: float
    is_self_service: bool


SUPPORTED_COMMUNITY_FUEL_TYPES = {
    "benzina",
    "benzina_premium",
    "diesel",
    "diesel_premium",
    "gpl",
    "hvo",
    "metano",
}

COMMUNITY_FUEL_TYPE_ALIASES = {
    "gasolio": "diesel",
    "lpg": "gpl",
    "gnc": "metano",
    "cng": "metano",
}


def normalize_email(value: str) -> str:
    return value.strip().lower()


def sanitize_display_name(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()
    return cleaned or None


def default_display_name_from_email(email: str) -> str:
    local_part = email.split("@", 1)[0].strip()
    return local_part or "FuelNear User"


def parse_token_email_verified(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() == "true"

    return False


def auth_debug_log(message: str) -> None:
    print(f"[AUTH][GOOGLE] {message}")


def apple_auth_debug_log(message: str) -> None:
    print(f"[AUTH][APPLE] {message}")


def account_delete_log(message: str) -> None:
    print(f"[AUTH][DELETE_ACCOUNT] {message}")


def device_token_log(message: str) -> None:
    print(f"[DEVICE_TOKEN] {message}")


def notification_preferences_log(message: str) -> None:
    print(f"[NOTIFICATION_PREFERENCES] {message}")


def user_location_log(message: str) -> None:
    print(f"[USER_LOCATION] {message}")


def normalize_device_token(value: str | None) -> str:
    token = value.strip() if value else ""
    if not token:
        raise HTTPException(status_code=400, detail="Device token is required")

    return token


def normalize_device_platform(value: str | None) -> str:
    platform = value.strip().lower() if value else ""
    if platform != "ios":
        raise HTTPException(status_code=400, detail="Unsupported device platform")

    return platform


def normalize_device_environment(value: str | None) -> str:
    environment = value.strip().lower() if value else "production"
    if environment not in {"sandbox", "production"}:
        raise HTTPException(status_code=400, detail="Unsupported device environment")

    return environment


def device_token_log_id(device_token: str) -> str:
    return hash_token(device_token)[:12]


def normalize_notification_fuel_type(value: str | None) -> str | None:
    if value is None:
        return None

    fuel_type = normalize_community_fuel_type(value)
    if fuel_type not in SUPPORTED_COMMUNITY_FUEL_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported fuel type")

    return fuel_type


def normalize_notification_radius_km(value: float | None) -> float | None:
    if value is None:
        return None

    radius_km = float(value)
    if not isfinite(radius_km) or radius_km <= 0 or radius_km > 100:
        raise HTTPException(status_code=400, detail="Invalid radius_km")

    return radius_km


def normalize_push_text(value: str | None, field_name: str) -> str:
    text = value.strip() if value else ""
    if not text:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")

    return text


def request_fields_set(payload: BaseModel) -> set[str]:
    fields_set = getattr(payload, "model_fields_set", None)
    if fields_set is not None:
        return set(fields_set)

    return set(getattr(payload, "__fields_set__", set()))


def apple_full_name_to_display_name(value: Any) -> str | None:
    if isinstance(value, str):
        return sanitize_display_name(value)

    if isinstance(value, dict):
        name_parts = [
            value.get("givenName") or value.get("given_name") or value.get("firstName") or value.get("first_name"),
            value.get("familyName") or value.get("family_name") or value.get("lastName") or value.get("last_name"),
        ]
        return sanitize_display_name(" ".join(str(part).strip() for part in name_parts if part))

    return None


def verify_jwt_with_jwks(
    token: str,
    jwks_url: str,
    audience: str | list[str],
    issuer: str | None = None,
) -> dict[str, Any]:
    jwk_client = PyJWKClient(jwks_url)
    signing_key = jwk_client.get_signing_key_from_jwt(token)
    decode_kwargs: dict[str, Any] = {
        "audience": audience,
        "algorithms": ["RS256"],
    }

    if issuer:
        decode_kwargs["issuer"] = issuer

    return jwt.decode(token, signing_key.key, **decode_kwargs)


def verify_apple_identity_token(
    identity_token: str,
    *,
    raw_nonce: str | None = None,
    expected_nonce: str | None = None,
) -> dict[str, Any]:
    if not APPLE_CLIENT_ID:
        raise HTTPException(status_code=500, detail="Apple auth not configured")

    try:
        apple_auth_debug_log("token verification started")
        claims = verify_jwt_with_jwks(
            identity_token,
            APPLE_JWKS_URL,
            audience=APPLE_CLIENT_ID,
            issuer="https://appleid.apple.com",
        )
    except PyJWTError as exc:
        apple_auth_debug_log(f"token verification failed type={exc.__class__.__name__}")
        raise HTTPException(status_code=401, detail="Invalid Apple identity token")
    except Exception as exc:
        apple_auth_debug_log(f"jwks verification failed type={exc.__class__.__name__}")
        raise HTTPException(status_code=502, detail="Apple token verification unavailable")

    provider_user_id = claims.get("sub")
    if not provider_user_id:
        apple_auth_debug_log("token verification failed type=MissingSubject")
        raise HTTPException(status_code=401, detail="Invalid Apple identity token")

    normalized_raw_nonce = raw_nonce.strip() if raw_nonce else None
    normalized_expected_nonce = expected_nonce.strip() if expected_nonce else None
    if normalized_raw_nonce or normalized_expected_nonce:
        claim_nonce = claims.get("nonce")
        if not isinstance(claim_nonce, str) or not claim_nonce:
            apple_auth_debug_log("token verification failed type=MissingNonce")
            raise HTTPException(status_code=401, detail="Invalid Apple identity token")

        hashed_raw_nonce = (
            hashlib.sha256(normalized_raw_nonce.encode("utf-8")).hexdigest()
            if normalized_raw_nonce
            else None
        )
        if (
            hashed_raw_nonce
            and normalized_expected_nonce
            and not hmac.compare_digest(hashed_raw_nonce, normalized_expected_nonce)
        ):
            apple_auth_debug_log("token verification failed type=NonceInputMismatch")
            raise HTTPException(status_code=401, detail="Invalid Apple identity token")

        nonce_to_verify = hashed_raw_nonce or normalized_expected_nonce
        if nonce_to_verify is None or not hmac.compare_digest(claim_nonce, nonce_to_verify):
            apple_auth_debug_log("token verification failed type=InvalidNonce")
            raise HTTPException(status_code=401, detail="Invalid Apple identity token")

        apple_auth_debug_log("nonce verification success")
    else:
        apple_auth_debug_log("nonce verification skipped input_present=false")

    apple_auth_debug_log("token verification success")
    return {
        "provider": "apple",
        "provider_user_id": str(provider_user_id),
        "email": normalize_email(str(claims["email"])) if claims.get("email") else None,
        "email_verified": parse_token_email_verified(claims.get("email_verified")),
        "display_name": None,
    }


def verify_google_id_token(id_token: str) -> dict[str, Any]:
    if not GOOGLE_CLIENT_IDS:
        raise HTTPException(status_code=500, detail="Google auth not configured")

    try:
        auth_debug_log("token verification started")
        claims = verify_jwt_with_jwks(
            id_token,
            GOOGLE_JWKS_URL,
            audience=GOOGLE_CLIENT_IDS,
        )
    except PyJWTError as exc:
        if exc.__class__.__name__ == "InvalidAudienceError":
            try:
                unverified_claims = jwt.decode(id_token, options={"verify_signature": False})
                auth_debug_log(f"token audience={unverified_claims.get('aud')}")
                auth_debug_log(f"token authorized_party={unverified_claims.get('azp')}")
            except Exception as decode_exc:
                auth_debug_log(f"token audience decode failed type={decode_exc.__class__.__name__}")
        auth_debug_log(f"token verification failed type={exc.__class__.__name__}")
        raise HTTPException(status_code=401, detail="Invalid Google ID token")
    except Exception as exc:
        auth_debug_log(f"jwks verification failed type={exc.__class__.__name__}")
        raise HTTPException(status_code=502, detail="Google token verification unavailable")

    if claims.get("iss") not in GOOGLE_ISSUERS:
        auth_debug_log("token verification failed type=InvalidIssuer")
        raise HTTPException(status_code=401, detail="Invalid Google ID token")

    provider_user_id = claims.get("sub")
    if not provider_user_id:
        auth_debug_log("token verification failed type=MissingSubject")
        raise HTTPException(status_code=401, detail="Invalid Google ID token")

    return {
        "provider": "google",
        "provider_user_id": str(provider_user_id),
        "email": normalize_email(str(claims["email"])) if claims.get("email") else None,
        "email_verified": parse_token_email_verified(claims.get("email_verified")),
        "display_name": sanitize_display_name(claims.get("name")),
    }


def normalize_community_fuel_type(raw_fuel_type: str) -> str:
    fuel_type = raw_fuel_type.strip().lower()
    return COMMUNITY_FUEL_TYPE_ALIASES.get(fuel_type, fuel_type)


def validate_community_price_report(payload: CommunityPriceReportRequest) -> tuple[str, float]:
    fuel_type = normalize_community_fuel_type(payload.fuel_type)
    if fuel_type not in SUPPORTED_COMMUNITY_FUEL_TYPES:
        raise HTTPException(status_code=400, detail="Unsupported fuel type")

    price = float(payload.price)
    if not isfinite(price) or price < 0.5 or price > 5.0:
        raise HTTPException(status_code=400, detail="Invalid price")

    return fuel_type, round(price, 3)


def generate_unique_referral_code(conn) -> str:
    for _ in range(20):
        candidate = generate_referral_code()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE referral_code = %s LIMIT 1;", (candidate,))
            if cur.fetchone() is None:
                return candidate

    raise RuntimeError("Unable to generate a unique referral code")


def get_active_plus_status(conn, user_id: int) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, source, status, starts_at, expires_at, original_transaction_id
            FROM user_subscriptions
            WHERE user_id = %s
              AND status = 'active'
              AND expires_at > NOW()
            ORDER BY expires_at DESC
            LIMIT 1;
            """,
            (user_id,),
        )
        subscription = cur.fetchone()

    if not subscription:
        return {
            "is_plus": False,
            "subscription": None,
        }

    return {
        "is_plus": True,
        "subscription": serialize_datetime_fields([dict(subscription)], ["starts_at", "expires_at"])[0],
    }


def build_user_payload(conn, user_row: dict[str, Any]) -> dict[str, Any]:
    plus_info = get_active_plus_status(conn, user_row["id"])

    payload = {
        "id": user_row["id"],
        "email": user_row["email"],
        "display_name": user_row["display_name"],
        "referral_code": user_row["referral_code"],
        "referred_by_user_id": user_row["referred_by_user_id"],
        "is_email_verified": user_row["is_email_verified"],
        "is_active": user_row["is_active"],
        "created_at": user_row["created_at"].isoformat() if user_row["created_at"] else None,
        "updated_at": user_row["updated_at"].isoformat() if user_row["updated_at"] else None,
        "is_plus": plus_info["is_plus"],
        "subscription": plus_info["subscription"],
    }

    return payload


# === REFERRAL REWARD & PROCESSING ===

def grant_plus_days_reward(conn, user_id: int, days: int) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("Reward days must be greater than zero")

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=days)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, starts_at, expires_at
            FROM user_subscriptions
            WHERE user_id = %s
              AND status = 'active'
              AND expires_at > NOW()
            ORDER BY expires_at DESC
            LIMIT 1;
            """,
            (user_id,),
        )
        active_subscription = cur.fetchone()

        cur.execute(
            """
            INSERT INTO rewards (
                user_id,
                reward_type,
                reward_value,
                status,
                granted_at,
                created_at,
                updated_at
            )
            VALUES (%s, 'plus_days', %s, 'granted', NOW(), NOW(), NOW())
            RETURNING id, reward_type, reward_value, status, granted_at, expires_at, created_at, updated_at;
            """,
            (user_id, str(days)),
        )
        reward_row = cur.fetchone()

        if active_subscription:
            new_expires_at = active_subscription["expires_at"] + timedelta(days=days)
            cur.execute(
                """
                UPDATE user_subscriptions
                SET expires_at = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, user_id, source, status, starts_at, expires_at, original_transaction_id, created_at, updated_at;
                """,
                (new_expires_at, active_subscription["id"]),
            )
            subscription_row = cur.fetchone()
        else:
            cur.execute(
                """
                INSERT INTO user_subscriptions (
                    user_id,
                    source,
                    status,
                    starts_at,
                    expires_at,
                    original_transaction_id,
                    created_at,
                    updated_at
                )
                VALUES (%s, 'referral_reward', 'active', %s, %s, NULL, NOW(), NOW())
                RETURNING id, user_id, source, status, starts_at, expires_at, original_transaction_id, created_at, updated_at;
                """,
                (user_id, now, expires_at),
            )
            subscription_row = cur.fetchone()

    return {
        "reward": serialize_datetime_fields([dict(reward_row)], ["granted_at", "expires_at", "created_at", "updated_at"])[0],
        "subscription": serialize_datetime_fields([dict(subscription_row)], ["starts_at", "expires_at", "created_at", "updated_at"])[0],
    }


def process_pending_referrals(conn, min_age_days: int = 7, reward_days: int = 7) -> dict[str, Any]:
    if min_age_days <= 0:
        raise ValueError("min_age_days must be greater than zero")

    processed: list[dict[str, Any]] = []

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                r.id,
                r.referrer_user_id,
                r.referred_user_id,
                r.referral_code_used,
                r.status,
                r.created_at,
                u.is_active AS referred_user_is_active,
                u.is_email_verified AS referred_user_is_email_verified
            FROM referrals r
            JOIN users u ON u.id = r.referred_user_id
            WHERE r.status = 'pending'
              AND r.created_at <= NOW() - (%s * INTERVAL '1 day')
              AND u.is_active = TRUE
              AND u.is_email_verified = TRUE
            ORDER BY r.created_at ASC;
            """,
            (min_age_days,),
        )
        pending_referrals = cur.fetchall()

    for referral in pending_referrals:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE referrals
                SET status = 'valid',
                    validated_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'pending'
                RETURNING id, referrer_user_id, referred_user_id, referral_code_used, status, validated_at, created_at, updated_at;
                """,
                (referral["id"],),
            )
            validated_referral = cur.fetchone()

        if not validated_referral:
            continue

        reward_result = grant_plus_days_reward(conn, validated_referral["referrer_user_id"], reward_days)
        processed.append(
            {
                "referral": serialize_datetime_fields([dict(validated_referral)], ["validated_at", "created_at", "updated_at"])[0],
                "reward": reward_result["reward"],
                "subscription": reward_result["subscription"],
            }
        )

    return {
        "processed_count": len(processed),
        "items": processed,
    }


def create_email_verification_token(conn, user_id: int) -> dict[str, Any]:
    token = generate_refresh_token()
    token_hash = hash_token(token)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=EMAIL_VERIFICATION_TTL_HOURS)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            UPDATE email_verification_tokens
            SET used_at = NOW()
            WHERE user_id = %s
              AND used_at IS NULL;
            """,
            (user_id,),
        )
        cur.execute(
            """
            INSERT INTO email_verification_tokens (
                user_id,
                token_hash,
                expires_at,
                created_at
            )
            VALUES (%s, %s, %s, NOW())
            RETURNING id, user_id, expires_at, created_at;
            """,
            (user_id, token_hash, expires_at),
        )
        row = cur.fetchone()

    return {
        "token": token,
        "row": dict(row),
    }


def verify_email_token(conn, token: str) -> dict[str, Any] | None:
    normalized_token = token.strip()
    if not normalized_token:
        return None

    token_hash = hash_token(normalized_token)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, user_id
            FROM email_verification_tokens
            WHERE token_hash = %s
              AND used_at IS NULL
              AND expires_at > NOW()
            LIMIT 1
            FOR UPDATE;
            """,
            (token_hash,),
        )
        token_row = cur.fetchone()
        if token_row is None:
            return None

        cur.execute(
            """
            UPDATE users
            SET is_email_verified = TRUE,
                updated_at = NOW()
            WHERE id = %s
              AND is_active = TRUE
            RETURNING id, email, password_hash, display_name, referral_code, referred_by_user_id,
                      is_email_verified, is_active, created_at, updated_at;
            """,
            (token_row["user_id"],),
        )
        user_row = cur.fetchone()
        if user_row is None:
            return None

        cur.execute(
            """
            UPDATE email_verification_tokens
            SET used_at = NOW()
            WHERE id = %s;
            """,
            (token_row["id"],),
        )

    return dict(user_row)



def create_user_session(conn, user_id: int, device_info: str | None = None, ip_address: str | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    access_token = generate_access_token()
    refresh_token = generate_refresh_token()
    access_token_hash = hash_token(access_token)
    refresh_token_hash = hash_token(refresh_token)
    access_expires_at = now + timedelta(hours=ACCESS_TOKEN_TTL_HOURS)
    refresh_expires_at = now + timedelta(days=REFRESH_TOKEN_TTL_DAYS)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO user_sessions (
                user_id,
                access_token_hash,
                refresh_token_hash,
                device_info,
                ip_address,
                access_expires_at,
                expires_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                user_id,
                access_token_hash,
                refresh_token_hash,
                device_info,
                ip_address,
                access_expires_at,
                refresh_expires_at,
            ),
        )
        session_id = cur.fetchone()[0]

    return {
        "session_id": session_id,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "access_expires_at": access_expires_at.isoformat(),
        "refresh_expires_at": refresh_expires_at.isoformat(),
    }


def refresh_user_session(conn, refresh_token: str) -> dict[str, Any]:
    refresh_token_hash = hash_token(refresh_token)
    now = datetime.now(timezone.utc)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, user_id, revoked_at, expires_at
            FROM user_sessions
            WHERE refresh_token_hash = %s
            LIMIT 1;
            """,
            (refresh_token_hash,),
        )
        session = cur.fetchone()

        if not session:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        if session["revoked_at"] is not None:
            raise HTTPException(status_code=401, detail="Refresh token has been revoked")

        if session["expires_at"] <= now:
            raise HTTPException(status_code=401, detail="Refresh token has expired")

        new_access_token = generate_access_token()
        new_refresh_token = generate_refresh_token()
        new_access_token_hash = hash_token(new_access_token)
        new_refresh_token_hash = hash_token(new_refresh_token)
        access_expires_at = now + timedelta(hours=ACCESS_TOKEN_TTL_HOURS)
        refresh_expires_at = now + timedelta(days=REFRESH_TOKEN_TTL_DAYS)

        cur.execute(
            """
            UPDATE user_sessions
            SET access_token_hash = %s,
                refresh_token_hash = %s,
                access_expires_at = %s,
                expires_at = %s,
                revoked_at = NULL
            WHERE id = %s;
            """,
            (
                new_access_token_hash,
                new_refresh_token_hash,
                access_expires_at,
                refresh_expires_at,
                session["id"],
            ),
        )

    return {
        "session_id": session["id"],
        "access_token": new_access_token,
        "refresh_token": new_refresh_token,
        "token_type": "bearer",
        "access_expires_at": access_expires_at.isoformat(),
        "refresh_expires_at": refresh_expires_at.isoformat(),
    }


def extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(status_code=401, detail="Invalid Authorization header")

    return parts[1].strip()



def get_current_user_from_token(authorization: str | None) -> dict[str, Any]:
    access_token = extract_bearer_token(authorization)
    access_token_hash = hash_token(access_token)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        u.id,
                        u.email,
                        u.display_name,
                        u.referral_code,
                        u.referred_by_user_id,
                        u.is_email_verified,
                        u.is_active,
                        u.created_at,
                        u.updated_at,
                        s.id AS session_id,
                        s.access_expires_at,
                        s.expires_at,
                        s.revoked_at
                    FROM user_sessions s
                    JOIN users u ON u.id = s.user_id
                    WHERE s.access_token_hash = %s
                      AND s.revoked_at IS NULL
                      AND s.access_expires_at IS NOT NULL
                      AND s.access_expires_at > NOW()
                    LIMIT 1;
                    """,
                    (access_token_hash,),
                )
                user = cur.fetchone()

                if not user:
                    raise HTTPException(status_code=401, detail="Invalid or expired access token")

                if not user["is_active"]:
                    raise HTTPException(status_code=403, detail="User account is inactive")

                return build_user_payload(conn, dict(user))
    finally:
        conn.close()


def revoke_current_session(authorization: str | None) -> dict[str, Any]:
    access_token = extract_bearer_token(authorization)
    access_token_hash = hash_token(access_token)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_sessions
                    SET revoked_at = NOW()
                    WHERE access_token_hash = %s
                      AND revoked_at IS NULL
                    RETURNING id;
                    """,
                    (access_token_hash,),
                )
                revoked_session = cur.fetchone()

                if revoked_session is None:
                    raise HTTPException(status_code=401, detail="Invalid or expired access token")

        return {
            "status": "ok",
            "message": "Session revoked successfully",
        }
    finally:
        conn.close()


def fetch_user_for_auth_payload(cur, user_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT id, email, password_hash, display_name, referral_code, referred_by_user_id,
               is_email_verified, is_active, created_at, updated_at
        FROM users
        WHERE id = %s
        LIMIT 1;
        """,
        (user_id,),
    )
    return cur.fetchone()


def normalize_referral_code_input(referral_code: str | None) -> str | None:
    if not referral_code:
        return None

    normalized = referral_code.strip().upper()
    return normalized or None


def resolve_active_referrer_id(cur, referral_code: str) -> int:
    cur.execute(
        "SELECT id FROM users WHERE referral_code = %s AND is_active = TRUE LIMIT 1;",
        (referral_code,),
    )
    referrer = cur.fetchone()
    if referrer is None:
        raise HTTPException(status_code=400, detail="Invalid referral code")

    return referrer["id"]


def create_pending_referral(cur, referrer_user_id: int, referred_user_id: int, referral_code: str) -> None:
    cur.execute(
        """
        INSERT INTO referrals (
            referrer_user_id,
            referred_user_id,
            referral_code_used,
            status
        )
        VALUES (%s, %s, %s, 'pending');
        """,
        (
            referrer_user_id,
            referred_user_id,
            referral_code,
        ),
    )


def authenticate_with_provider(
    provider_claims: dict[str, Any],
    display_name: str | None,
    device_info: str | None,
    referral_code: str | None = None,
    log_prefix: str | None = None,
) -> dict[str, Any]:
    provider = provider_claims["provider"]
    provider_user_id = provider_claims["provider_user_id"]
    email = provider_claims.get("email")
    email_verified = bool(provider_claims.get("email_verified"))
    provider_display_name = sanitize_display_name(display_name) or sanitize_display_name(provider_claims.get("display_name"))
    referral_code_input = normalize_referral_code_input(referral_code)
    linked_by_provider = False
    linked_by_verified_email = False

    conn = get_connection()
    try:
        with conn:
            ensure_auth_provider_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if log_prefix:
                    print(f"{log_prefix} user lookup/create started")
                    print(f"{log_prefix} provider={provider}")
                    print(f"{log_prefix} email_claim_present={bool(email)}")
                    print(f"{log_prefix} email_verified={email_verified}")
                    print(f"{log_prefix} referral_code present={bool(referral_code_input)}")
                cur.execute(
                    """
                    SELECT user_id
                    FROM user_auth_providers
                    WHERE provider = %s
                      AND provider_user_id = %s
                    LIMIT 1;
                    """,
                    (provider, provider_user_id),
                )
                provider_row = cur.fetchone()

                if provider_row:
                    linked_by_provider = True
                    if log_prefix:
                        print(f"{log_prefix} referral skipped because existing user=true")
                        print(f"{log_prefix} referral applied=false")
                    user_row = fetch_user_for_auth_payload(cur, provider_row["user_id"])
                    if user_row is None:
                        raise HTTPException(status_code=404, detail="User not found")
                else:
                    if not email:
                        if log_prefix:
                            print(f"{log_prefix} linked_by_provider=false")
                            print(f"{log_prefix} linked_by_verified_email=false")
                        raise HTTPException(status_code=400, detail="Provider email is required for first sign-in")

                    cur.execute(
                        """
                        SELECT id, email, password_hash, display_name, referral_code, referred_by_user_id,
                               is_email_verified, is_active, created_at, updated_at
                        FROM users
                        WHERE email = %s
                        LIMIT 1;
                        """,
                        (email,),
                    )
                    existing_user_row = cur.fetchone()
                    if existing_user_row is not None and not email_verified:
                        if log_prefix:
                            print(f"{log_prefix} linked_by_provider=false")
                            print(f"{log_prefix} linked_by_verified_email=false")
                        raise HTTPException(status_code=401, detail="Unable to link provider account")

                    linked_by_verified_email = bool(existing_user_row and email_verified)
                    referral_applied = False
                    referrer_user_id = None

                    if existing_user_row is None and referral_code_input:
                        referrer_user_id = resolve_active_referrer_id(cur, referral_code_input)

                    fallback_display_name = provider_display_name or default_display_name_from_email(email)

                    if existing_user_row is None:
                        cur.execute(
                            """
                            INSERT INTO users (
                                email,
                                password_hash,
                                display_name,
                                referral_code,
                                referred_by_user_id,
                                is_email_verified,
                                is_active
                            )
                            VALUES (%s, NULL, %s, %s, %s, %s, TRUE)
                            RETURNING id, email, password_hash, display_name, referral_code, referred_by_user_id,
                                      is_email_verified, is_active, created_at, updated_at;
                            """,
                            (
                                email,
                                fallback_display_name,
                                generate_unique_referral_code(conn),
                                referrer_user_id,
                                email_verified,
                            ),
                        )
                        user_row = cur.fetchone()

                        if referral_code_input and referrer_user_id is not None:
                            create_pending_referral(cur, referrer_user_id, user_row["id"], referral_code_input)
                            referral_applied = True
                    else:
                        cur.execute(
                            """
                            UPDATE users
                            SET is_email_verified = users.is_email_verified OR %s,
                                updated_at = NOW()
                            WHERE id = %s
                            RETURNING id, email, password_hash, display_name, referral_code, referred_by_user_id,
                                      is_email_verified, is_active, created_at, updated_at;
                            """,
                            (email_verified, existing_user_row["id"]),
                        )
                        user_row = cur.fetchone()

                    if log_prefix:
                        print(f"{log_prefix} referral skipped because existing user={bool(existing_user_row)}")
                        print(f"{log_prefix} referral applied={referral_applied}")

                    cur.execute(
                        """
                        INSERT INTO user_auth_providers (
                            user_id,
                            provider,
                            provider_user_id,
                            email,
                            email_verified,
                            created_at,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (provider, provider_user_id) DO UPDATE SET
                            email = COALESCE(EXCLUDED.email, user_auth_providers.email),
                            email_verified = EXCLUDED.email_verified,
                            updated_at = NOW()
                        RETURNING user_id;
                        """,
                        (user_row["id"], provider, provider_user_id, email, email_verified),
                    )
                    linked_provider = cur.fetchone()
                    user_row = fetch_user_for_auth_payload(cur, linked_provider["user_id"])
                    if user_row is None:
                        raise HTTPException(status_code=404, detail="User not found")

                if not user_row["is_active"]:
                    raise HTTPException(status_code=403, detail="User account is inactive")

                if log_prefix:
                    print(f"{log_prefix} linked_by_provider={linked_by_provider}")
                    print(f"{log_prefix} linked_by_verified_email={linked_by_verified_email}")
                    print(f"{log_prefix} session create started")
                session_payload = create_user_session(conn, user_id=user_row["id"], device_info=device_info, ip_address=None)
                user_payload = build_user_payload(conn, dict(user_row))

        return {
            "status": "ok",
            "user": user_payload,
            "session": session_payload,
        }
    except HTTPException:
        raise
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        table = getattr(getattr(exc, "diag", None), "table_name", None)
        print(
            f"[AUTH] Provider authentication failed. provider={provider} "
            f"error={exc.__class__.__name__} pgcode={pgcode} table={table} constraint={constraint}"
        )
        raise HTTPException(status_code=500, detail="Provider authentication failed")
    finally:
        conn.close()


def delete_current_account(authorization: str | None) -> dict[str, str]:
    account_delete_log("endpoint reached")
    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]
    account_delete_log(f"authenticated user id present={bool(user_id)}")

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                account_delete_log("deletion started")
                cur.execute("DELETE FROM user_device_tokens WHERE user_id = %s;", (user_id,))
                cur.execute("DELETE FROM user_sessions WHERE user_id = %s;", (user_id,))
                cur.execute(
                    """
                    DELETE FROM users
                    WHERE id = %s
                    RETURNING id;
                    """,
                    (user_id,),
                )
                deleted_user = cur.fetchone()

                if deleted_user is None:
                    account_delete_log("deletion failure type=UserNotFound")
                    raise HTTPException(status_code=404, detail="User not found")

        account_delete_log("deletion success")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        table = getattr(getattr(exc, "diag", None), "table_name", None)
        account_delete_log(
            f"deletion failure type={exc.__class__.__name__} "
            f"pgcode={pgcode} table={table} constraint={constraint}"
        )
        raise HTTPException(status_code=500, detail="Account deletion failed")
    finally:
        conn.close()






def get_mimit_runtime_state() -> dict[str, Any]:
    global _mimit_stale_warning_logged

    with _mimit_state_lock:
        started_at = _mimit_update_started_at
        run_id = _mimit_update_run_id
        lock_active = _mimit_update_lock.locked()

        duration_seconds = None
        if lock_active and started_at is not None:
            duration_seconds = max(0, int((datetime.now(timezone.utc) - started_at).total_seconds()))

        stale = bool(
            lock_active
            and duration_seconds is not None
            and duration_seconds >= MIMIT_STALE_AFTER_SECONDS
        )
        if stale and not _mimit_stale_warning_logged:
            print(
                f"[MIMIT] Stale lock detected. run_id={run_id} "
                f"duration_seconds={duration_seconds}"
            )
            _mimit_stale_warning_logged = True

    return {
        "state": "running" if lock_active else "idle",
        "update_in_progress": lock_active,
        "run_id": run_id,
        "started_at": started_at.isoformat() if started_at else None,
        "duration_seconds": duration_seconds,
        "stale": stale,
        "stale_after_seconds": MIMIT_STALE_AFTER_SECONDS,
    }


def try_acquire_mimit_update_lock(conn) -> bool:
    global _mimit_update_started_at, _mimit_update_run_id, _mimit_stale_warning_logged

    if not _mimit_update_lock.acquire(blocking=False):
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s);", (MIMIT_ADVISORY_LOCK_ID,))
            advisory_lock_acquired = bool(cur.fetchone()[0])

        if not advisory_lock_acquired:
            _mimit_update_lock.release()
            return False

        with _mimit_state_lock:
            _mimit_update_started_at = datetime.now(timezone.utc)
            _mimit_update_run_id = None
            _mimit_stale_warning_logged = False

        print("[MIMIT] Update lock acquired.")
        return True
    except Exception:
        if _mimit_update_lock.locked():
            _mimit_update_lock.release()
        raise


def set_mimit_update_run_id(run_id: int) -> None:
    global _mimit_update_run_id

    with _mimit_state_lock:
        _mimit_update_run_id = run_id


def release_mimit_update_lock(conn) -> None:
    global _mimit_update_started_at, _mimit_update_run_id, _mimit_stale_warning_logged

    runtime_state = get_mimit_runtime_state()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s);", (MIMIT_ADVISORY_LOCK_ID,))
            advisory_lock_released = bool(cur.fetchone()[0])
        if not advisory_lock_released:
            print("[MIMIT] Advisory lock was not held during release.")
    except Exception as exc:
        # Closing the PostgreSQL connection also releases its advisory locks.
        print(f"[MIMIT] Advisory lock release deferred to connection close. type={exc.__class__.__name__}")
    finally:
        with _mimit_state_lock:
            _mimit_update_started_at = None
            _mimit_update_run_id = None
            _mimit_stale_warning_logged = False
        if _mimit_update_lock.locked():
            _mimit_update_lock.release()
        print(
            f"[MIMIT] Update lock released. run_id={runtime_state['run_id']} "
            f"duration_seconds={runtime_state['duration_seconds']}"
        )


def require_admin_update_token(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")) -> None:
    if not ADMIN_UPDATE_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_UPDATE_TOKEN is not configured")

    if not x_admin_token or x_admin_token != ADMIN_UPDATE_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid admin token")


def ensure_mimit_import_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mimit_import_runs (
                id BIGSERIAL PRIMARY KEY,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ NULL,
                status TEXT NOT NULL,
                stations_imported INTEGER NULL,
                prices_imported INTEGER NULL,
                error_message TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute("ALTER TABLE mimit_import_runs ADD COLUMN IF NOT EXISTS stations_csv INTEGER NULL;")
        cur.execute("ALTER TABLE mimit_import_runs ADD COLUMN IF NOT EXISTS prices_csv INTEGER NULL;")
        cur.execute("ALTER TABLE mimit_import_runs ADD COLUMN IF NOT EXISTS source_file_timestamp TIMESTAMPTZ NULL;")
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mimit_import_runs_completed_at
            ON mimit_import_runs(completed_at DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mimit_import_runs_status
            ON mimit_import_runs(status);
            """
        )


def create_mimit_import_run(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO mimit_import_runs (status, started_at, created_at)
            VALUES ('running', NOW(), NOW())
            RETURNING id;
            """
        )
        row = cur.fetchone()

    return int(row[0])


def fail_orphaned_mimit_import_runs(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mimit_import_runs
            SET status = 'failed',
                completed_at = NOW(),
                error_message = 'Import process ended before recording completion'
            WHERE status = 'running'
              AND completed_at IS NULL;
            """
        )
        return cur.rowcount


def get_running_mimit_import_run(conn) -> dict[str, Any] | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, started_at, completed_at, status
            FROM mimit_import_runs
            WHERE status = 'running'
              AND completed_at IS NULL
            ORDER BY started_at DESC
            LIMIT 1;
            """
        )
        row = cur.fetchone()

    return dict(row) if row else None


def mark_mimit_import_run_orphaned(conn, run_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mimit_import_runs
            SET status = 'failed',
                completed_at = NOW(),
                error_message = 'Orphaned import run: advisory lock is not held'
            WHERE id = %s
              AND status = 'running'
              AND completed_at IS NULL;
            """,
            (run_id,),
        )
        return cur.rowcount == 1


def safe_int_from_import_result(result: dict[str, Any], keys: list[str]) -> int | None:
    for key in keys:
        value = result.get(key)
        if value is None:
            continue

        try:
            return int(value)
        except (TypeError, ValueError):
            continue

    return None


def get_mimit_import_payload(result: dict[str, Any]) -> dict[str, Any]:
    import_payload = result.get("import")
    if isinstance(import_payload, dict):
        return import_payload

    return result


def get_mimit_source_file_timestamp(result: dict[str, Any]) -> datetime | None:
    download_payload = result.get("download")
    if not isinstance(download_payload, dict):
        return None

    prices_payload = download_payload.get("prezzi")
    if not isinstance(prices_payload, dict):
        return None

    last_modified = prices_payload.get("last_modified")
    if not isinstance(last_modified, str) or not last_modified:
        return None

    try:
        return datetime.fromisoformat(last_modified)
    except ValueError:
        try:
            return parsedate_to_datetime(last_modified)
        except (TypeError, ValueError):
            return None


def finish_mimit_import_run(conn, run_id: int, result: dict[str, Any]) -> None:
    import_payload = get_mimit_import_payload(result)
    stations_imported = safe_int_from_import_result(import_payload, ["stations_imported", "stations_count", "stations"])
    prices_imported = safe_int_from_import_result(import_payload, ["prices_imported", "prices_count", "prices"])
    stations_csv = safe_int_from_import_result(import_payload, ["stations_csv"])
    prices_csv = safe_int_from_import_result(import_payload, ["prices_csv"])
    source_file_timestamp = get_mimit_source_file_timestamp(result)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mimit_import_runs
            SET status = 'success',
                completed_at = NOW(),
                stations_imported = %s,
                prices_imported = %s,
                stations_csv = %s,
                prices_csv = %s,
                source_file_timestamp = %s,
                error_message = NULL
            WHERE id = %s;
            """,
            (
                stations_imported,
                prices_imported,
                stations_csv,
                prices_csv,
                source_file_timestamp,
                run_id,
            ),
        )


def fail_mimit_import_run(conn, run_id: int, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mimit_import_runs
            SET status = 'failed',
                completed_at = NOW(),
                error_message = %s
            WHERE id = %s;
            """,
            (error_message[:2000], run_id),
        )


def get_connection():
    if DATABASE_URL:
        parsed = urlparse(DATABASE_URL)
        return psycopg2.connect(
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=parsed.password,
            host=parsed.hostname,
            port=parsed.port,
        )

    connection_kwargs = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "host": DB_HOST,
        "port": DB_PORT,
    }

    if DB_PASSWORD:
        connection_kwargs["password"] = DB_PASSWORD

    return psycopg2.connect(**connection_kwargs)


def ensure_community_price_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.stations');")
        if cur.fetchone()[0] is None:
            return

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_price_reports (
                id BIGSERIAL PRIMARY KEY,
                station_id BIGINT NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
                user_id BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
                fuel_type TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL CHECK (price > 0),
                is_self_service BOOLEAN NOT NULL,
                reported_at TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'accepted', 'rejected', 'superseded')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_price_reports_station_status ON user_price_reports(station_id, status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_price_reports_user_recent ON user_price_reports(user_id, station_id, fuel_type, is_self_service, created_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_price_reports_latest ON user_price_reports(station_id, fuel_type, is_self_service, reported_at DESC);")


def ensure_auth_provider_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_auth_providers (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                provider TEXT NOT NULL CHECK (provider IN ('apple', 'google')),
                provider_user_id TEXT NOT NULL,
                email TEXT NULL,
                email_verified BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (provider, provider_user_id)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_auth_providers_user_id ON user_auth_providers(user_id);")


def ensure_user_device_tokens_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_device_tokens (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                device_token TEXT NOT NULL UNIQUE,
                platform TEXT NOT NULL,
                environment TEXT NOT NULL DEFAULT 'production',
                app_version TEXT NULL,
                device_info TEXT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_device_tokens_user_id ON user_device_tokens(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_device_tokens_active ON user_device_tokens(is_active, platform, environment);")


def ensure_user_locations_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_locations (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                lat DOUBLE PRECISION NOT NULL CHECK (lat >= -90 AND lat <= 90),
                lng DOUBLE PRECISION NOT NULL CHECK (lng >= -180 AND lng <= 180),
                accuracy DOUBLE PRECISION NULL CHECK (accuracy > 0),
                source TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_locations_user_id ON user_locations(user_id);")


def ensure_price_notification_preferences_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS price_notification_preferences (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                price_notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                fuel_type TEXT NULL,
                radius_km DOUBLE PRECISION NULL,
                favorites_only BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_notification_preferences_user_id
            ON price_notification_preferences(user_id);
            """
        )
        cur.execute("ALTER TABLE price_notification_preferences ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION NULL;")
        cur.execute("ALTER TABLE price_notification_preferences ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION NULL;")
        cur.execute("ALTER TABLE price_notification_preferences ADD COLUMN IF NOT EXISTS location_updated_at TIMESTAMPTZ NULL;")


def ensure_sent_price_notifications_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.stations');")
        if cur.fetchone()[0] is None:
            return

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_price_notifications (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                mimit_run_id BIGINT NOT NULL REFERENCES mimit_import_runs(id) ON DELETE CASCADE,
                fuel_type TEXT NOT NULL,
                station_id BIGINT NULL REFERENCES stations(id) ON DELETE SET NULL,
                price DOUBLE PRECISION NULL CHECK (price > 0),
                distance_km DOUBLE PRECISION NULL CHECK (distance_km >= 0),
                sent_at TIMESTAMPTZ NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('processing', 'sent', 'failed')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, mimit_run_id)
            );
            """
        )
        cur.execute("ALTER TABLE sent_price_notifications ADD COLUMN IF NOT EXISTS price DOUBLE PRECISION NULL;")
        cur.execute("ALTER TABLE sent_price_notifications ADD COLUMN IF NOT EXISTS distance_km DOUBLE PRECISION NULL;")
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sent_price_notifications_run_status
            ON sent_price_notifications(mimit_run_id, status);
            """
        )


def ensure_auth_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT,
                display_name TEXT NOT NULL,
                referral_code TEXT NOT NULL UNIQUE,
                referred_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
                is_email_verified BOOLEAN NOT NULL DEFAULT FALSE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE users
            ALTER COLUMN password_hash DROP NOT NULL;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                id BIGSERIAL PRIMARY KEY,
                referrer_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                referred_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                referral_code_used TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                validated_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (referred_user_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS email_verification_tokens (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash TEXT NOT NULL UNIQUE,
                expires_at TIMESTAMPTZ NOT NULL,
                used_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rewards (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                reward_type TEXT NOT NULL,
                reward_value TEXT NOT NULL,
                status TEXT NOT NULL,
                granted_at TIMESTAMPTZ NULL,
                expires_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_sessions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                access_token_hash TEXT NOT NULL UNIQUE,
                refresh_token_hash TEXT NOT NULL UNIQUE,
                device_info TEXT NULL,
                ip_address TEXT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                revoked_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE user_sessions
            ADD COLUMN IF NOT EXISTS access_expires_at TIMESTAMPTZ NULL;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_subscriptions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                starts_at TIMESTAMPTZ NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                original_transaction_id TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer_user_id ON referrals(referrer_user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_email_verification_tokens_user_id ON email_verification_tokens(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_email_verification_tokens_token_hash ON email_verification_tokens(token_hash);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rewards_user_id ON rewards(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_refresh_token_hash ON user_sessions(refresh_token_hash);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_subscriptions_user_id ON user_subscriptions(user_id);")
        ensure_auth_provider_schema(conn)
        ensure_user_device_tokens_schema(conn)
        ensure_user_locations_schema(conn)
        ensure_price_notification_preferences_schema(conn)
        ensure_mimit_import_schema(conn)
        ensure_sent_price_notifications_schema(conn)
        ensure_community_price_schema(conn)

def serialize_datetime_fields(items: list[dict[str, Any]], fields: list[str]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []

    for item in items:
        row = dict(item)
        for field in fields:
            if field in row and row[field] is not None:
                row[field] = row[field].isoformat()
        serialized.append(row)

    return serialized


def merge_mimit_runtime_state(last_run: dict[str, Any] | None) -> dict[str, Any]:
    runtime_state = get_mimit_runtime_state()
    if runtime_state["update_in_progress"] or not last_run or last_run.get("status") != "running":
        return runtime_state

    started_at = last_run.get("started_at")
    duration_seconds = None
    if isinstance(started_at, datetime):
        duration_seconds = max(0, int((datetime.now(timezone.utc) - started_at).total_seconds()))

    stale = bool(
        duration_seconds is not None
        and duration_seconds >= MIMIT_STALE_AFTER_SECONDS
    )
    if stale:
        print(
            f"[MIMIT] Stale database run detected. run_id={last_run.get('id')} "
            f"duration_seconds={duration_seconds}"
        )

    return {
        "state": "running",
        "update_in_progress": True,
        "run_id": last_run.get("id"),
        "started_at": started_at.isoformat() if isinstance(started_at, datetime) else None,
        "duration_seconds": duration_seconds,
        "stale": stale,
        "stale_after_seconds": MIMIT_STALE_AFTER_SECONDS,
    }


def find_best_price_for_notification(
    conn,
    *,
    latitude: float,
    longitude: float,
    radius_km: float,
    fuel_type: str,
) -> dict[str, Any] | None:
    lat_delta = radius_km / 111.32
    lng_divisor = 111.32 * max(cos(radians(latitude)), 0.01)
    lng_delta = radius_km / lng_divisor

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH candidate_stations AS (
                SELECT
                    s.id,
                    s.name,
                    s.brand,
                    (
                        6371 * acos(
                            least(
                                1,
                                cos(radians(%s)) * cos(radians(s.latitude)) *
                                cos(radians(s.longitude) - radians(%s)) +
                                sin(radians(%s)) * sin(radians(s.latitude))
                            )
                        )
                    ) AS distance_km
                FROM stations s
                WHERE s.is_active = TRUE
                  AND s.latitude BETWEEN %s AND %s
                  AND s.longitude BETWEEN %s AND %s
            )
            SELECT
                cs.id AS station_id,
                fp.price,
                fp.fuel_type,
                fp.reported_at,
                cs.distance_km,
                COALESCE(
                    NULLIF(BTRIM(cs.brand), ''),
                    NULLIF(BTRIM(cs.name), ''),
                    'Distributore'
                ) AS station_name
            FROM candidate_stations cs
            JOIN fuel_prices fp ON fp.station_id = cs.id
            WHERE cs.distance_km <= %s
              AND fp.fuel_type = %s
            ORDER BY fp.price ASC, cs.distance_km ASC, fp.reported_at DESC
            LIMIT 1;
            """,
            (
                latitude,
                longitude,
                latitude,
                latitude - lat_delta,
                latitude + lat_delta,
                longitude - lng_delta,
                longitude + lng_delta,
                radius_km,
                fuel_type,
            ),
        )
        row = cur.fetchone()

    return dict(row) if row else None


def price_notification_fuel_label(fuel_type: str) -> str:
    labels = {
        "benzina": "Benzina",
        "benzina_premium": "Benzina premium",
        "diesel": "Diesel",
        "diesel_premium": "Diesel premium",
        "gpl": "GPL",
        "hvo": "HVO",
        "metano": "Metano",
    }
    return labels.get(fuel_type, fuel_type.capitalize())


def truncate_price_notification_station_name(value: str, max_length: int = 28) -> str:
    cleaned = " ".join(value.split())
    if len(cleaned) <= max_length:
        return cleaned

    shortened = cleaned[: max_length - 3].rstrip(" ,.-")
    return f"{shortened}..."


def process_price_notifications_for_run(mimit_run_id: int) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "mimit_run_id": mimit_run_id,
        "minimum_improvement_eur": PRICE_NOTIFICATION_MIN_IMPROVEMENT_EUR,
        "users_considered": 0,
        "sent_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "invalid_tokens_count": 0,
        "skipped_same_price": 0,
        "skipped_worse_price": 0,
        "skipped_below_threshold": 0,
        "skip_reasons": {},
    }

    def skip(reason: str) -> None:
        summary["skipped_count"] += 1
        summary["skip_reasons"][reason] = summary["skip_reasons"].get(reason, 0) + 1
        if reason in {
            "skipped_same_price",
            "skipped_worse_price",
            "skipped_below_threshold",
        }:
            summary[reason] += 1

    conn = get_connection()
    try:
        with conn:
            ensure_mimit_import_schema(conn)
            ensure_user_device_tokens_schema(conn)
            ensure_user_locations_schema(conn)
            ensure_price_notification_preferences_schema(conn)
            ensure_sent_price_notifications_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM mimit_import_runs
                    WHERE id = %s
                      AND status = 'success'
                    LIMIT 1;
                    """,
                    (mimit_run_id,),
                )
                if cur.fetchone() is None:
                    raise ValueError("MIMIT run is not successful")

                cur.execute(
                    """
                    SELECT
                        p.user_id,
                        COALESCE(NULLIF(p.fuel_type, ''), 'benzina') AS fuel_type,
                        COALESCE(p.radius_km, 3.0) AS radius_km,
                        p.favorites_only,
                        COALESCE(ul.lat, p.latitude) AS latitude,
                        COALESCE(ul.lng, p.longitude) AS longitude,
                        COALESCE(ul.updated_at, p.location_updated_at) AS location_updated_at
                    FROM price_notification_preferences p
                    LEFT JOIN user_locations ul ON ul.user_id = p.user_id
                    WHERE p.price_notifications_enabled = TRUE
                    ORDER BY p.user_id;
                    """
                )
                preferences = [dict(row) for row in cur.fetchall()]

        summary["users_considered"] = len(preferences)
        apns_configured = apns_is_configured()

        for preference in preferences:
            user_id = int(preference["user_id"])
            fuel_type = preference["fuel_type"]
            radius_km = float(preference["radius_km"])

            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT id, device_token, environment
                        FROM user_device_tokens
                        WHERE user_id = %s
                          AND platform = 'ios'
                          AND is_active = TRUE
                        ORDER BY last_seen_at DESC;
                        """,
                        (user_id,),
                    )
                    device_tokens = [dict(row) for row in cur.fetchall()]

            if not device_tokens:
                skip("skipped_no_active_device")
                continue
            if preference["favorites_only"]:
                skip("skipped_favorites_unsupported")
                continue
            if preference["latitude"] is None or preference["longitude"] is None:
                skip("skipped_no_location")
                continue
            if not apns_configured:
                skip("skipped_apns_not_configured")
                continue

            with conn:
                best_price = find_best_price_for_notification(
                    conn,
                    latitude=float(preference["latitude"]),
                    longitude=float(preference["longitude"]),
                    radius_km=radius_km,
                    fuel_type=fuel_type,
                )
            if best_price is None:
                skip("skipped_no_price")
                continue

            current_price = round(float(best_price["price"]), 3)
            distance_km = round(float(best_price["distance_km"]), 2)
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT price
                        FROM sent_price_notifications
                        WHERE user_id = %s
                          AND fuel_type = %s
                          AND status = 'sent'
                          AND price IS NOT NULL
                        ORDER BY sent_at DESC, id DESC
                        LIMIT 1;
                        """,
                        (user_id, fuel_type),
                    )
                    last_notification = cur.fetchone()

            if last_notification is not None:
                last_notified_price = round(float(last_notification[0]), 3)
                improvement = last_notified_price - current_price
                if abs(improvement) < 0.0005:
                    skip("skipped_same_price")
                    continue
                if improvement < 0:
                    skip("skipped_worse_price")
                    continue
                if improvement + 1e-9 < PRICE_NOTIFICATION_MIN_IMPROVEMENT_EUR:
                    skip("skipped_below_threshold")
                    continue

            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO sent_price_notifications (
                            user_id,
                            mimit_run_id,
                            fuel_type,
                            station_id,
                            price,
                            distance_km,
                            status,
                            created_at,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, 'processing', NOW(), NOW())
                        ON CONFLICT (user_id, mimit_run_id) DO NOTHING
                        RETURNING id;
                        """,
                        (
                            user_id,
                            mimit_run_id,
                            fuel_type,
                            best_price["station_id"],
                            current_price,
                            distance_km,
                        ),
                    )
                    notification_row = cur.fetchone()

            if notification_row is None:
                skip("skipped_duplicate_run")
                continue

            fuel_label = price_notification_fuel_label(fuel_type)
            station_name = truncate_price_notification_station_name(best_price["station_name"])
            formatted_price = f"{current_price:.3f}".replace(".", ",")
            formatted_distance = f"{distance_km:.1f}".replace(".", ",")
            body = (
                f"{fuel_label} a {formatted_price} €/L da {station_name}, "
                f"a {formatted_distance} km da te."
            )
            any_sent = False

            for token_row in device_tokens:
                device_token = token_row["device_token"]
                try:
                    result = send_apns_push(
                        device_token=device_token,
                        title="FuelNear",
                        body=body,
                        environment=token_row.get("environment"),
                        payload={
                            "type": "price_alert",
                            "station_id": int(best_price["station_id"]),
                            "fuel_type": fuel_type,
                            "price": current_price,
                            "distance_km": distance_km,
                        },
                    )
                except APNsConfigurationError as exc:
                    print(
                        f"[PRICE_NOTIFICATIONS] APNs configuration failure "
                        f"type={exc.__class__.__name__}"
                    )
                    continue

                any_sent = any_sent or bool(result["success"])
                if result["invalid_token"]:
                    summary["invalid_tokens_count"] += 1
                    with conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE user_device_tokens
                                SET is_active = FALSE,
                                    updated_at = NOW()
                                WHERE id = %s;
                                """,
                                (token_row["id"],),
                            )

                if not result["success"]:
                    print(
                        f"[PRICE_NOTIFICATIONS] APNs send failed "
                        f"status_code={result['status_code']} "
                        f"temporary={result['temporary_error']} reason={result['reason']}"
                    )

            final_status = "sent" if any_sent else "failed"
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE sent_price_notifications
                        SET status = %s,
                            sent_at = CASE WHEN %s = 'sent' THEN NOW() ELSE NULL END,
                            updated_at = NOW()
                        WHERE id = %s;
                        """,
                        (final_status, final_status, notification_row[0]),
                    )

            if any_sent:
                summary["sent_count"] += 1
            else:
                summary["failed_count"] += 1

        print(
            f"[PRICE_NOTIFICATIONS] run_id={mimit_run_id} "
            f"users_considered={summary['users_considered']} sent={summary['sent_count']} "
            f"skipped={summary['skipped_count']} failed={summary['failed_count']} "
            f"invalid_tokens={summary['invalid_tokens_count']} "
            f"skipped_same_price={summary['skipped_same_price']} "
            f"skipped_worse_price={summary['skipped_worse_price']} "
            f"skipped_below_threshold={summary['skipped_below_threshold']}"
        )
        return summary
    finally:
        conn.close()


@app.on_event("startup")
def on_startup() -> None:
    conn = get_connection()
    try:
        with conn:
            ensure_auth_schema(conn)
    finally:
        conn.close()


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "FuelNear backend is running"}


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


def run_mimit_update_background(conn, run_id: int) -> None:
    started_at = time.monotonic()
    try:
        print(f"[MIMIT] Background update started. run_id={run_id}")
        result = update_mimit_data(download=True)
        with conn:
            finish_mimit_import_run(conn, run_id, result)

        try:
            notification_summary = process_price_notifications_for_run(run_id)
            print(
                f"[MIMIT] Price notifications processed. run_id={run_id} "
                f"sent={notification_summary['sent_count']} "
                f"skipped={notification_summary['skipped_count']}"
            )
        except Exception as notification_error:
            print(
                f"[MIMIT] Price notification processing failed. run_id={run_id} "
                f"type={notification_error.__class__.__name__}"
            )

        duration_seconds = int(time.monotonic() - started_at)
        print(
            f"[MIMIT] Background update finished successfully. run_id={run_id} "
            f"duration_seconds={duration_seconds}"
        )
    except Exception as exc:
        duration_seconds = int(time.monotonic() - started_at)
        print(
            f"[MIMIT] Background update failed. run_id={run_id} "
            f"duration_seconds={duration_seconds} type={exc.__class__.__name__}"
        )
        try:
            with conn:
                fail_mimit_import_run(conn, run_id, str(exc))
        except Exception as persist_error:
            print(
                f"[MIMIT] Failed to persist background import failure. run_id={run_id} "
                f"type={persist_error.__class__.__name__}"
            )
    finally:
        release_mimit_update_lock(conn)
        conn.close()


@app.get("/admin/update-mimit")
def admin_update_mimit(
    background_tasks: BackgroundTasks,
    _: None = Depends(require_admin_update_token),
) -> dict[str, Any]:
    conn = get_connection()
    run_id: int | None = None
    lock_acquired = False
    handed_off_to_background = False

    try:
        lock_acquired = try_acquire_mimit_update_lock(conn)
        if not lock_acquired:
            with conn:
                ensure_mimit_import_schema(conn)
                running_run = get_running_mimit_import_run(conn)
            runtime_state = merge_mimit_runtime_state(running_run)
            print(
                f"[MIMIT] Update busy. run_id={runtime_state['run_id']} "
                f"duration_seconds={runtime_state['duration_seconds']}"
            )
            return {
                "status": "busy",
                "message": "MIMIT update is currently running",
                "started": False,
                "running": True,
                "update_state": "running",
                "running_run_id": runtime_state["run_id"],
                "started_at": runtime_state["started_at"],
                "duration_seconds": runtime_state["duration_seconds"],
                "stale": runtime_state["stale"],
            }

        with conn:
            ensure_mimit_import_schema(conn)
            orphaned_runs = fail_orphaned_mimit_import_runs(conn)
            run_id = create_mimit_import_run(conn)
        set_mimit_update_run_id(run_id)

        if orphaned_runs:
            print(f"[MIMIT] Reconciled orphaned runs. count={orphaned_runs}")

        background_tasks.add_task(run_mimit_update_background, conn, run_id)
        handed_off_to_background = True
        print(f"[MIMIT] Background update scheduled. run_id={run_id}")
        return {
            "status": "running",
            "message": "MIMIT update started",
            "started": True,
            "running": True,
            "run_id": run_id,
        }
    except HTTPException:
        raise
    except Exception as exc:
        error_message = str(exc)
        print(f"[MIMIT] Update finished with failure. run_id={run_id} type={exc.__class__.__name__}")

        if run_id is not None:
            try:
                with conn:
                    fail_mimit_import_run(conn, run_id, error_message)
            except Exception as persist_error:
                print(f"[MIMIT] Failed to persist import failure. error={persist_error}")

        raise HTTPException(status_code=500, detail=f"MIMIT update failed: {exc}")
    finally:
        if not handed_off_to_background:
            if lock_acquired:
                release_mimit_update_lock(conn)
            conn.close()


@app.get("/mimit/status")
def get_mimit_status() -> dict[str, Any]:
    conn = get_connection()
    try:
        with conn:
            ensure_mimit_import_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        started_at,
                        completed_at,
                        status,
                        stations_imported,
                        prices_imported,
                        stations_csv,
                        prices_csv,
                        source_file_timestamp,
                        error_message
                    FROM mimit_import_runs
                    ORDER BY started_at DESC
                    LIMIT 1;
                    """
                )
                last_run = cur.fetchone()

                cur.execute(
                    """
                    SELECT
                        id,
                        started_at,
                        completed_at,
                        status,
                        stations_imported,
                        prices_imported,
                        stations_csv,
                        prices_csv,
                        source_file_timestamp,
                        error_message
                    FROM mimit_import_runs
                    WHERE status = 'success'
                    ORDER BY completed_at DESC
                    LIMIT 1;
                    """
                )
                last_success = cur.fetchone()

                cur.execute(
                    """
                    SELECT
                        id,
                        started_at,
                        completed_at,
                        status,
                        stations_imported,
                        prices_imported,
                        stations_csv,
                        prices_csv,
                        source_file_timestamp,
                        error_message
                    FROM mimit_import_runs
                    WHERE status = 'failed'
                    ORDER BY completed_at DESC
                    LIMIT 1;
                    """
                )
                last_failed = cur.fetchone()

        runtime_state = merge_mimit_runtime_state(dict(last_run) if last_run else None)
        datetime_fields = ["started_at", "completed_at", "source_file_timestamp"]

        return {
            "status": "ok",
            "update_state": runtime_state["state"],
            "update_in_progress": runtime_state["update_in_progress"],
            "run_id": runtime_state["run_id"],
            "started_at": runtime_state["started_at"],
            "duration_seconds": runtime_state["duration_seconds"],
            "stale": runtime_state["stale"],
            "stale_after_seconds": runtime_state["stale_after_seconds"],
            "last_status": last_run["status"] if last_run else None,
            "last_run": serialize_datetime_fields([dict(last_run)], datetime_fields)[0] if last_run else None,
            "last_success": serialize_datetime_fields([dict(last_success)], datetime_fields)[0] if last_success else None,
            "last_failed": serialize_datetime_fields([dict(last_failed)], datetime_fields)[0] if last_failed else None,
            "last_successful_update_at": last_success["completed_at"].isoformat() if last_success and last_success["completed_at"] else None,
            "stations_imported": last_success["stations_imported"] if last_success else None,
            "prices_imported": last_success["prices_imported"] if last_success else None,
            "stations_csv": last_success["stations_csv"] if last_success else None,
            "prices_csv": last_success["prices_csv"] if last_success else None,
            "source_file_timestamp": last_success["source_file_timestamp"].isoformat() if last_success and last_success["source_file_timestamp"] else None,
            "last_error": last_failed["error_message"] if last_failed else None,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MIMIT status failed: {exc}")
    finally:
        conn.close()


@app.get("/debug/mimit-status")
def debug_mimit_status(_: None = Depends(require_admin_update_token)) -> dict[str, Any]:
    conn = get_connection()
    try:
        with conn:
            ensure_mimit_import_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        started_at,
                        completed_at,
                        status,
                        stations_imported,
                        prices_imported,
                        stations_csv,
                        prices_csv,
                        source_file_timestamp,
                        error_message
                    FROM mimit_import_runs
                    ORDER BY started_at DESC
                    LIMIT 1;
                    """
                )
                last_run = cur.fetchone()

                cur.execute(
                    """
                    SELECT
                        id,
                        started_at,
                        completed_at,
                        status,
                        stations_imported,
                        prices_imported,
                        stations_csv,
                        prices_csv,
                        source_file_timestamp
                    FROM mimit_import_runs
                    WHERE status = 'success'
                    ORDER BY completed_at DESC
                    LIMIT 1;
                    """
                )
                last_success = cur.fetchone()

                cur.execute("SELECT MAX(reported_at) AS max_reported_at FROM fuel_prices;")
                max_reported_at = cur.fetchone()

        datetime_fields = ["started_at", "completed_at", "source_file_timestamp"]
        runtime_state = merge_mimit_runtime_state(dict(last_run) if last_run else None)
        return {
            "status": "ok",
            "update_state": runtime_state["state"],
            "update_in_progress": runtime_state["update_in_progress"],
            "run_id": runtime_state["run_id"],
            "started_at": runtime_state["started_at"],
            "duration_seconds": runtime_state["duration_seconds"],
            "stale": runtime_state["stale"],
            "stale_after_seconds": runtime_state["stale_after_seconds"],
            "last_run": serialize_datetime_fields([dict(last_run)], datetime_fields)[0] if last_run else None,
            "last_completed_update_at": last_success["completed_at"].isoformat() if last_success and last_success["completed_at"] else None,
            "last_dataset_date": last_success["source_file_timestamp"].isoformat() if last_success and last_success["source_file_timestamp"] else None,
            "last_prices_imported": last_success["prices_imported"] if last_success else None,
            "last_stations_imported": last_success["stations_imported"] if last_success else None,
            "last_prices_csv": last_success["prices_csv"] if last_success else None,
            "last_stations_csv": last_success["stations_csv"] if last_success else None,
            "max_fuel_prices_reported_at": max_reported_at["max_reported_at"].isoformat() if max_reported_at and max_reported_at["max_reported_at"] else None,
        }
    except Exception as exc:
        print(f"[MIMIT] Debug status failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="MIMIT debug status failed")
    finally:
        conn.close()


@app.post("/admin/mimit-lock-check")
def admin_mimit_lock_check(_: None = Depends(require_admin_update_token)) -> dict[str, Any]:
    conn = get_connection()
    probe_lock_acquired = False

    try:
        with conn:
            ensure_mimit_import_schema(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s);", (MIMIT_ADVISORY_LOCK_ID,))
                probe_lock_acquired = bool(cur.fetchone()[0])

            advisory_lock_held = not probe_lock_acquired
            running_run = get_running_mimit_import_run(conn)
            orphaned_run_marked_failed = False

            if running_run and not advisory_lock_held:
                orphaned_run_marked_failed = mark_mimit_import_run_orphaned(
                    conn,
                    int(running_run["id"]),
                )

            if probe_lock_acquired:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (MIMIT_ADVISORY_LOCK_ID,))
                probe_lock_acquired = False

        started_at = running_run.get("started_at") if running_run else None
        duration_seconds = None
        if isinstance(started_at, datetime):
            duration_seconds = max(0, int((datetime.now(timezone.utc) - started_at).total_seconds()))

        run_id = int(running_run["id"]) if running_run else None
        orphaned = bool(running_run and not advisory_lock_held)
        print(
            f"[MIMIT] Lock check. run_id={run_id} advisory_lock_held={advisory_lock_held} "
            f"orphaned={orphaned} marked_failed={orphaned_run_marked_failed}"
        )

        return {
            "status": "ok",
            "running_run_id": run_id,
            "advisory_lock_held": advisory_lock_held,
            "duration_seconds": duration_seconds,
            "stale": bool(duration_seconds is not None and duration_seconds >= MIMIT_STALE_AFTER_SECONDS),
            "stale_after_seconds": MIMIT_STALE_AFTER_SECONDS,
            "last_progress_at": None,
            "last_progress_available": False,
            "orphaned": orphaned,
            "orphaned_run_marked_failed": orphaned_run_marked_failed,
        }
    except Exception as exc:
        print(f"[MIMIT] Lock check failed. type={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="MIMIT lock check failed")
    finally:
        if probe_lock_acquired:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (MIMIT_ADVISORY_LOCK_ID,))
            except Exception:
                pass
        conn.close()


@app.get("/admin/mimit-diagnostics")
def admin_mimit_diagnostics(
    _: None = Depends(require_admin_update_token),
    lat: float = Query(default=41.4477, description="Default: Anzio latitude"),
    lng: float = Query(default=12.6297, description="Default: Anzio longitude"),
    radius_km: float = Query(default=10.0, gt=0, le=100),
    fuel_type: str | None = Query(default=None, description="Optional normalized fuel type"),
    is_self_service: bool | None = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, Any]:
    normalized_fuel_type = fuel_type.strip().lower() if fuel_type else None
    endpoint_fuel_type = normalized_fuel_type or "benzina"
    lat_delta = radius_km / 111.32
    lng_divisor = 111.32 * max(cos(radians(lat)), 0.01)
    lng_delta = radius_km / lng_divisor

    nearby_prices_sql = """
        WITH candidate_stations AS (
            SELECT
                s.id,
                s.mimit_id,
                s.name,
                s.brand,
                s.operator,
                s.address,
                s.city,
                s.province,
                s.latitude,
                s.longitude,
                (
                    6371 * acos(
                        least(
                            1,
                            cos(radians(%s)) * cos(radians(s.latitude)) *
                            cos(radians(s.longitude) - radians(%s)) +
                            sin(radians(%s)) * sin(radians(s.latitude))
                        )
                    )
                ) AS distance_km
            FROM stations s
            WHERE s.latitude BETWEEN %s AND %s
              AND s.longitude BETWEEN %s AND %s
        )
        SELECT
            cs.id,
            cs.mimit_id,
            cs.name,
            cs.brand,
            cs.operator,
            cs.address,
            cs.city,
            cs.province,
            cs.latitude,
            cs.longitude,
            round(cs.distance_km::numeric, 3)::double precision AS distance_km,
            fp.fuel_type,
            fp.price,
            fp.is_self_service,
            fp.reported_at,
            fp.created_at AS imported_at_proxy
        FROM candidate_stations cs
        JOIN fuel_prices fp ON fp.station_id = cs.id
        WHERE cs.distance_km <= %s
          AND (%s IS NULL OR fp.fuel_type = %s)
          AND (%s IS NULL OR fp.is_self_service = %s)
    """
    nearby_params = (
        lat,
        lng,
        lat,
        lat - lat_delta,
        lat + lat_delta,
        lng - lng_delta,
        lng + lng_delta,
        radius_km,
        normalized_fuel_type,
        normalized_fuel_type,
        is_self_service,
        is_self_service,
    )

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_prices,
                        MIN(reported_at) AS min_reported_at,
                        MAX(reported_at) AS max_reported_at,
                        MAX(created_at) AS last_imported_at_proxy,
                        COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE) AS prices_imported_today_proxy
                    FROM fuel_prices;
                    """
                )
                summary = cur.fetchone()

                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'fuel_prices'
                          AND column_name = 'imported_at'
                    ) AS has_imported_at;
                    """
                )
                imported_at_info = cur.fetchone()

                cur.execute(
                    """
                    SELECT reported_at::date::text AS reported_date, COUNT(*) AS prices_count
                    FROM fuel_prices
                    GROUP BY reported_at::date
                    ORDER BY reported_at::date DESC
                    LIMIT 30;
                    """
                )
                reported_at_counts = cur.fetchall()

                cur.execute(
                    nearby_prices_sql + " ORDER BY fp.reported_at DESC, fp.created_at DESC LIMIT %s;",
                    nearby_params + (limit,),
                )
                newest_nearby = cur.fetchall()

                cur.execute(
                    nearby_prices_sql + " ORDER BY fp.reported_at ASC, fp.created_at DESC LIMIT %s;",
                    nearby_params + (limit,),
                )
                oldest_nearby = cur.fetchall()

                cur.execute(
                    """
                    WITH candidate_stations AS (
                        SELECT
                            s.id,
                            s.mimit_id,
                            s.name,
                            s.brand,
                            s.operator,
                            s.address,
                            s.city,
                            s.province,
                            s.latitude,
                            s.longitude,
                            (
                                6371 * acos(
                                    least(
                                        1,
                                        cos(radians(%s)) * cos(radians(s.latitude)) *
                                        cos(radians(s.longitude) - radians(%s)) +
                                        sin(radians(%s)) * sin(radians(s.latitude))
                                    )
                                )
                            ) AS distance_km
                        FROM stations s
                        WHERE s.latitude BETWEEN %s AND %s
                          AND s.longitude BETWEEN %s AND %s
                    ),
                    ranked_prices AS (
                        SELECT DISTINCT ON (cs.id)
                            cs.id,
                            cs.mimit_id,
                            cs.name,
                            cs.brand,
                            cs.operator,
                            cs.address,
                            cs.city,
                            cs.province,
                            cs.latitude,
                            cs.longitude,
                            cs.distance_km,
                            fp.fuel_type,
                            fp.price,
                            fp.is_self_service,
                            fp.reported_at,
                            fp.created_at AS imported_at_proxy
                        FROM candidate_stations cs
                        JOIN fuel_prices fp ON fp.station_id = cs.id
                        WHERE cs.distance_km <= %s
                          AND fp.fuel_type = %s
                          AND (%s IS NULL OR fp.is_self_service = %s)
                        ORDER BY cs.id, fp.reported_at DESC, fp.price ASC, fp.is_self_service DESC
                    )
                    SELECT
                        id,
                        mimit_id,
                        name,
                        brand,
                        operator,
                        address,
                        city,
                        province,
                        latitude,
                        longitude,
                        round(distance_km::numeric, 3)::double precision AS distance_km,
                        fuel_type,
                        price,
                        is_self_service,
                        reported_at,
                        imported_at_proxy
                    FROM ranked_prices
                    ORDER BY price ASC, distance_km ASC
                    LIMIT %s;
                    """,
                    (
                        lat,
                        lng,
                        lat,
                        lat - lat_delta,
                        lat + lat_delta,
                        lng - lng_delta,
                        lng + lng_delta,
                        radius_km,
                        endpoint_fuel_type,
                        is_self_service,
                        is_self_service,
                        limit,
                    ),
                )
                nearby_endpoint_equivalent = cur.fetchall()

        datetime_fields = ["min_reported_at", "max_reported_at", "last_imported_at_proxy"]
        price_datetime_fields = ["reported_at", "imported_at_proxy"]

        return {
            "status": "ok",
            "filters": {
                "lat": lat,
                "lng": lng,
                "radius_km": radius_km,
                "fuel_type": normalized_fuel_type,
                "nearby_endpoint_equivalent_fuel_type": endpoint_fuel_type,
                "is_self_service": is_self_service,
            },
            "summary": serialize_datetime_fields([dict(summary)], datetime_fields)[0] if summary else None,
            "has_imported_at": imported_at_info["has_imported_at"] if imported_at_info else False,
            "reported_at_counts": [dict(row) for row in reported_at_counts],
            "newest_nearby_prices": serialize_datetime_fields([dict(row) for row in newest_nearby], price_datetime_fields),
            "oldest_nearby_prices": serialize_datetime_fields([dict(row) for row in oldest_nearby], price_datetime_fields),
            "nearby_endpoint_equivalent": serialize_datetime_fields([dict(row) for row in nearby_endpoint_equivalent], price_datetime_fields),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MIMIT diagnostics failed: {exc}")
    finally:
        conn.close()


# === ADMIN REFERRAL PROCESSING ENDPOINT ===

@app.post("/admin/process-referrals")
def admin_process_referrals(_: None = Depends(require_admin_update_token)) -> dict[str, Any]:
    conn = get_connection()
    try:
        with conn:
            result = process_pending_referrals(conn, min_age_days=7, reward_days=7)
        return {
            "status": "ok",
            "message": "Referral processing completed",
            "result": result,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Referral processing failed: {exc}")
    finally:
        conn.close()


@app.post("/admin/test-push")
def admin_test_push(
    payload: AdminTestPushRequest,
    _: None = Depends(require_admin_update_token),
) -> dict[str, Any]:
    title = normalize_push_text(payload.title, "title")
    body = normalize_push_text(payload.body, "body")
    explicit_device_token = normalize_device_token(payload.device_token) if payload.device_token else None

    if payload.user_id is None and explicit_device_token is None:
        raise HTTPException(status_code=400, detail="user_id or device_token is required")

    print(f"[APNS] configured={apns_is_configured()}")

    targets: list[dict[str, Any]] = []
    conn = get_connection()
    try:
        with conn:
            ensure_user_device_tokens_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if explicit_device_token:
                    cur.execute(
                        """
                        SELECT id, device_token, environment
                        FROM user_device_tokens
                        WHERE device_token = %s
                        LIMIT 1;
                        """,
                        (explicit_device_token,),
                    )
                    existing_token = cur.fetchone()
                    if existing_token:
                        targets.append(dict(existing_token))
                    else:
                        targets.append(
                            {
                                "id": None,
                                "device_token": explicit_device_token,
                                "environment": None,
                            }
                        )
                elif payload.user_id is not None:
                    cur.execute(
                        """
                        SELECT id, device_token, environment
                        FROM user_device_tokens
                        WHERE user_id = %s
                          AND is_active = TRUE
                          AND platform = 'ios'
                        ORDER BY last_seen_at DESC;
                        """,
                        (payload.user_id,),
                    )
                    targets.extend([dict(row) for row in cur.fetchall()])

        unique_targets: dict[str, dict[str, Any]] = {}
        for target in targets:
            unique_targets[target["device_token"]] = target
        targets = list(unique_targets.values())

        print(f"[APNS] target token count={len(targets)}")

        sent_success_count = 0
        sent_failure_count = 0
        invalid_token_count = 0
        items: list[dict[str, Any]] = []

        for target in targets:
            device_token = target["device_token"]
            token_log_id = device_token_log_id(device_token)

            try:
                result = send_apns_push(
                    device_token=device_token,
                    title=title,
                    body=body,
                    environment=target.get("environment"),
                    payload=payload.payload,
                )
            except APNsConfigurationError as exc:
                print(f"[APNS] send failed token hash prefix={token_log_id} type={exc.__class__.__name__}")
                raise HTTPException(status_code=500, detail=str(exc))

            if result["success"]:
                sent_success_count += 1
            else:
                sent_failure_count += 1

            if result["invalid_token"]:
                invalid_token_count += 1
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE user_device_tokens
                            SET is_active = FALSE,
                                updated_at = NOW()
                            WHERE device_token = %s;
                            """,
                            (device_token,),
                        )

            print(
                "[APNS] send result "
                f"token hash prefix={token_log_id} "
                f"success={result['success']} "
                f"status_code={result['status_code']} "
                f"reason={result['reason']}"
            )
            items.append(
                {
                    "token_hash_prefix": token_log_id,
                    "success": result["success"],
                    "status_code": result["status_code"],
                    "reason": result["reason"],
                    "invalid_token": result["invalid_token"],
                    "temporary_error": result["temporary_error"],
                    "environment": result["environment"],
                }
            )

        print(f"[APNS] sent success count={sent_success_count} failure count={sent_failure_count}")
        return {
            "status": "ok",
            "apns_configured": apns_is_configured(),
            "target_token_count": len(targets),
            "sent_success_count": sent_success_count,
            "sent_failure_count": sent_failure_count,
            "invalid_token_count": invalid_token_count,
            "items": items,
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[APNS] test push failed type={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="APNs test push failed")
    finally:
        conn.close()


@app.post("/admin/process-price-notifications")
def admin_process_price_notifications(
    _: None = Depends(require_admin_update_token),
) -> dict[str, Any]:
    conn = get_connection()
    try:
        with conn:
            ensure_mimit_import_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM mimit_import_runs
                    WHERE status = 'success'
                    ORDER BY completed_at DESC
                    LIMIT 1;
                    """
                )
                row = cur.fetchone()
    except Exception as exc:
        print(f"[PRICE_NOTIFICATIONS] Latest successful run lookup failed. type={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Price notification processing failed")
    finally:
        conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail="No successful MIMIT run found")

    run_id = int(row[0])
    try:
        result = process_price_notifications_for_run(run_id)
        return {
            "status": "ok",
            "mimit_run_id": run_id,
            "result": result,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        print(
            f"[PRICE_NOTIFICATIONS] Manual processing failed. run_id={run_id} "
            f"type={exc.__class__.__name__}"
        )
        raise HTTPException(status_code=500, detail="Price notification processing failed")


# === AUTH ENDPOINTS ===


@app.post("/auth/register")
def register_user(payload: RegisterRequest) -> dict[str, Any]:
    email = normalize_email(str(payload.email))
    display_name = sanitize_display_name(payload.display_name) or default_display_name_from_email(email)
    referral_code_input = payload.referral_code.strip().upper() if payload.referral_code else None

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id FROM users WHERE email = %s LIMIT 1;", (email,))
                if cur.fetchone() is not None:
                    raise HTTPException(status_code=409, detail="Email already registered")

                referrer_user_id = None
                if referral_code_input:
                    cur.execute(
                        "SELECT id FROM users WHERE referral_code = %s AND is_active = TRUE LIMIT 1;",
                        (referral_code_input,),
                    )
                    referrer = cur.fetchone()
                    if referrer is None:
                        raise HTTPException(status_code=400, detail="Invalid referral code")
                    referrer_user_id = referrer["id"]

                password_hash_value = hash_password(payload.password)
                user_referral_code = generate_unique_referral_code(conn)

                cur.execute(
                    """
                    INSERT INTO users (
                        email,
                        password_hash,
                        display_name,
                        referral_code,
                        referred_by_user_id,
                        is_email_verified,
                        is_active
                    )
                    VALUES (%s, %s, %s, %s, %s, FALSE, TRUE)
                    RETURNING id, email, display_name, referral_code, referred_by_user_id, is_email_verified, is_active, created_at, updated_at;
                    """,
                    (
                        email,
                        password_hash_value,
                        display_name,
                        user_referral_code,
                        referrer_user_id,
                    ),
                )
                user_row = cur.fetchone()

                if referral_code_input and referrer_user_id is not None:
                    cur.execute(
                        """
                        INSERT INTO referrals (
                            referrer_user_id,
                            referred_user_id,
                            referral_code_used,
                            status
                        )
                        VALUES (%s, %s, %s, 'pending');
                        """,
                        (
                            referrer_user_id,
                            user_row["id"],
                            referral_code_input,
                        ),
                    )

                verification_token = create_email_verification_token(conn, user_row["id"])
                verification_expires_at = verification_token["row"]["expires_at"]

                user_payload = build_user_payload(conn, dict(user_row))

        return {
            "status": "email_verification_required",
            "user": user_payload,
            "session": None,
            "email_verification": {
                "required": True,
                "delivery": "not_configured",
                "expires_at": verification_expires_at.isoformat(),
            },
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except psycopg2.errors.UniqueViolation as exc:
        constraint_name = getattr(exc.diag, "constraint_name", None)
        print(f"[AUTH] Registration unique violation. constraint={constraint_name}")
        if constraint_name and "email" in constraint_name:
            raise HTTPException(status_code=409, detail="Email already registered")
        raise HTTPException(status_code=500, detail="Registration failed")
    except Exception as exc:
        print(f"[AUTH] Registration failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Registration failed")
    finally:
        conn.close()


@app.post("/auth/verify-email")
def verify_email(payload: EmailVerificationRequest) -> dict[str, Any]:
    print("[AUTH][EMAIL] verify endpoint reached")
    conn = get_connection()
    try:
        with conn:
            user_row = verify_email_token(conn, payload.token)
            if user_row is None:
                print("[AUTH][EMAIL] verify failed")
                raise HTTPException(status_code=400, detail="Invalid or expired verification token")

            user_payload = build_user_payload(conn, user_row)

        print("[AUTH][EMAIL] verify success")
        return {
            "status": "ok",
            "user": user_payload,
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[AUTH][EMAIL] verify failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Email verification failed")
    finally:
        conn.close()


@app.post("/auth/resend-verification-email")
def resend_email_verification(payload: ResendEmailVerificationRequest) -> dict[str, Any]:
    print("[AUTH][EMAIL] resend endpoint reached")
    conn = get_connection()
    fallback_expires_at = datetime.now(timezone.utc) + timedelta(hours=EMAIL_VERIFICATION_TTL_HOURS)
    expires_at = fallback_expires_at
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, is_email_verified, is_active
                    FROM users
                    WHERE email = %s
                    LIMIT 1;
                    """,
                    (normalize_email(str(payload.email)),),
                )
                user_row = cur.fetchone()

            if user_row and user_row["is_active"] and not user_row["is_email_verified"]:
                verification_token = create_email_verification_token(conn, user_row["id"])
                expires_at = verification_token["row"]["expires_at"]
                print("[AUTH][EMAIL] resend token created=true")
            else:
                print("[AUTH][EMAIL] resend token created=false")

        return {
            "status": "ok",
            "email_verification": {
                "required": True,
                "delivery": "not_configured",
                "expires_at": expires_at.isoformat(),
            },
        }
    except Exception as exc:
        print(f"[AUTH][EMAIL] resend failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Email verification resend failed")
    finally:
        conn.close()


@app.post("/auth/login")
def login_user(payload: LoginRequest) -> dict[str, Any]:
    email = normalize_email(str(payload.email))

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, email, password_hash, display_name, referral_code, referred_by_user_id,
                           is_email_verified, is_active, created_at, updated_at
                    FROM users
                    WHERE email = %s
                    LIMIT 1;
                    """,
                    (email,),
                )
                user_row = cur.fetchone()

                if user_row is None or not user_row["password_hash"]:
                    raise HTTPException(status_code=401, detail="Invalid email or password")

                if not verify_password(payload.password, user_row["password_hash"]):
                    raise HTTPException(status_code=401, detail="Invalid email or password")

                if not user_row["is_active"]:
                    raise HTTPException(status_code=403, detail="User account is inactive")

                if not user_row["is_email_verified"]:
                    raise HTTPException(status_code=403, detail="Email verification required")

                session_payload = create_user_session(
                    conn,
                    user_id=user_row["id"],
                    device_info=payload.device_info,
                    ip_address=None,
                )

                user_payload = build_user_payload(conn, dict(user_row))

        return {
            "status": "ok",
            "user": user_payload,
            "session": session_payload,
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[AUTH] Login failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Login failed")
    finally:
        conn.close()


@app.post("/auth/apple")
def apple_login(payload: AppleAuthRequest) -> dict[str, Any]:
    identity_token = payload.identityToken or payload.identity_token
    display_name = (
        sanitize_display_name(payload.display_name)
        or apple_full_name_to_display_name(payload.fullName)
        or apple_full_name_to_display_name(payload.full_name)
    )

    apple_auth_debug_log("endpoint reached")
    apple_auth_debug_log(f"config present={bool(APPLE_CLIENT_ID)}")
    apple_auth_debug_log(f"token length={len(identity_token) if identity_token else 0}")

    if not identity_token or not identity_token.strip():
        raise HTTPException(status_code=400, detail="Apple identity token is required")

    raw_nonce = payload.rawNonce or payload.raw_nonce
    expected_nonce = payload.hashedNonce or payload.hashed_nonce or payload.nonce
    if raw_nonce and len(raw_nonce) > 512:
        raise HTTPException(status_code=400, detail="Invalid Apple nonce")
    if expected_nonce and len(expected_nonce) > 512:
        raise HTTPException(status_code=400, detail="Invalid Apple nonce")

    try:
        unverified_claims = jwt.decode(identity_token, options={"verify_signature": False})
        apple_auth_debug_log(f"token audience={unverified_claims.get('aud')}")
        apple_auth_debug_log(f"token subject present={bool(unverified_claims.get('sub'))}")
    except Exception as exc:
        apple_auth_debug_log(f"token preflight decode failed type={exc.__class__.__name__}")

    claims = verify_apple_identity_token(
        identity_token,
        raw_nonce=raw_nonce,
        expected_nonce=expected_nonce,
    )

    claims["display_name"] = display_name
    return authenticate_with_provider(
        claims,
        display_name=display_name,
        device_info=payload.device_info,
        referral_code=payload.referral_code,
        log_prefix="[AUTH][APPLE]",
    )


@app.post("/auth/google")
def google_login(payload: GoogleAuthRequest) -> dict[str, Any]:
    auth_debug_log("endpoint reached")
    auth_debug_log(f"config present={bool(GOOGLE_CLIENT_IDS)}")
    auth_debug_log(f"configured audiences count={len(GOOGLE_CLIENT_IDS)}")
    auth_debug_log(f"id_token length={len(payload.id_token) if payload.id_token else 0}")

    try:
        if not payload.id_token or not payload.id_token.strip():
            raise HTTPException(status_code=400, detail="Google ID token is required")

        claims = verify_google_id_token(payload.id_token)
        return authenticate_with_provider(
            claims,
            display_name=payload.display_name,
            device_info=payload.device_info,
            referral_code=payload.referral_code,
            log_prefix="[AUTH][GOOGLE]",
        )
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[AUTH][GOOGLE] unexpected failure type={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Google authentication failed")


# Add refresh and logout endpoints after /auth/login and before /auth/me


@app.post("/auth/refresh")
def refresh_login_session(payload: RefreshRequest) -> dict[str, Any]:
    conn = get_connection()
    try:
        with conn:
            session_payload = refresh_user_session(conn, payload.refresh_token)
        return {
            "status": "ok",
            "session": session_payload,
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {exc}")
    finally:
        conn.close()


@app.post("/auth/logout")
def logout_user(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
    return revoke_current_session(authorization)



@app.get("/auth/me")
def get_current_user_profile(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
    user_payload = get_current_user_from_token(authorization)
    return {
        "status": "ok",
        "user": user_payload,
    }


@app.delete("/auth/me")
def delete_current_user_profile(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, str]:
    return delete_current_account(authorization)


@app.delete("/account")
def delete_account(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, str]:
    return delete_current_account(authorization)


# === USER REFERRALS, REWARDS, SUBSCRIPTION ENDPOINTS ===

@app.post("/user/device-token")
def upsert_current_user_device_token(
    payload: DeviceTokenRequest,
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, Any]:
    device_token_log("upsert endpoint reached")
    token_present = bool(payload.device_token and payload.device_token.strip())
    device_token_log(f"token present={token_present}")

    device_token = normalize_device_token(payload.device_token)
    token_log_id = device_token_log_id(device_token)
    device_token_log(f"token hash prefix={token_log_id}")
    platform = normalize_device_platform(payload.platform)
    environment = normalize_device_environment(payload.environment)
    app_version = sanitize_display_name(payload.app_version)
    device_info = sanitize_display_name(payload.device_info)

    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            ensure_user_device_tokens_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM user_device_tokens
                    WHERE device_token = %s
                    LIMIT 1;
                    """,
                    (device_token,),
                )
                existing_token = cur.fetchone()

                cur.execute(
                    """
                    INSERT INTO user_device_tokens (
                        user_id,
                        device_token,
                        platform,
                        environment,
                        app_version,
                        device_info,
                        is_active,
                        created_at,
                        updated_at,
                        last_seen_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW(), NOW())
                    ON CONFLICT (device_token) DO UPDATE SET
                        user_id = EXCLUDED.user_id,
                        platform = EXCLUDED.platform,
                        environment = EXCLUDED.environment,
                        app_version = EXCLUDED.app_version,
                        device_info = EXCLUDED.device_info,
                        is_active = TRUE,
                        updated_at = NOW(),
                        last_seen_at = NOW()
                    RETURNING id, platform, environment, is_active, created_at, updated_at, last_seen_at;
                    """,
                    (
                        user_id,
                        device_token,
                        platform,
                        environment,
                        app_version,
                        device_info,
                    ),
                )
                device_row = cur.fetchone()

        operation = "updated" if existing_token else "created"
        device_token_log(f"upsert {operation}=true token hash prefix={token_log_id}")
        return {
            "status": "ok",
            "operation": operation,
            "device": serialize_datetime_fields(
                [dict(device_row)],
                ["created_at", "updated_at", "last_seen_at"],
            )[0],
        }
    except HTTPException:
        raise
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        table = getattr(getattr(exc, "diag", None), "table_name", None)
        device_token_log(
            f"upsert failed type={exc.__class__.__name__} "
            f"pgcode={pgcode} table={table} constraint={constraint}"
        )
        raise HTTPException(status_code=500, detail="Device token update failed")
    finally:
        conn.close()


@app.delete("/user/device-token")
def deactivate_current_user_device_token(
    payload: DeleteDeviceTokenRequest,
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, str]:
    device_token_log("delete endpoint reached")
    token_present = bool(payload.device_token and payload.device_token.strip())
    device_token_log(f"token present={token_present}")

    device_token = normalize_device_token(payload.device_token)
    token_log_id = device_token_log_id(device_token)
    device_token_log(f"token hash prefix={token_log_id}")

    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            ensure_user_device_tokens_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_device_tokens
                    SET is_active = FALSE,
                        updated_at = NOW(),
                        last_seen_at = NOW()
                    WHERE user_id = %s
                      AND device_token = %s
                    RETURNING id;
                    """,
                    (user_id, device_token),
                )
                deactivated = cur.fetchone()

        device_token_log(f"delete deactivated={bool(deactivated)} token hash prefix={token_log_id}")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        table = getattr(getattr(exc, "diag", None), "table_name", None)
        device_token_log(
            f"delete failed type={exc.__class__.__name__} "
            f"pgcode={pgcode} table={table} constraint={constraint}"
        )
        raise HTTPException(status_code=500, detail="Device token delete failed")
    finally:
        conn.close()


@app.post("/user/location")
def upsert_current_user_location(
    payload: UserLocationRequest,
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, Any]:
    user_location_log("location update endpoint reached")
    user_payload = get_current_user_from_token(authorization)
    user_id = int(user_payload["id"])
    user_location_log(f"user_id={user_id} has_location=true")

    if not isfinite(payload.lat) or not -90 <= payload.lat <= 90:
        raise HTTPException(status_code=400, detail="Invalid latitude")
    if not isfinite(payload.lng) or not -180 <= payload.lng <= 180:
        raise HTTPException(status_code=400, detail="Invalid longitude")
    if payload.accuracy is not None and (
        not isfinite(payload.accuracy) or payload.accuracy <= 0
    ):
        raise HTTPException(status_code=400, detail="Invalid location accuracy")

    normalized_source = payload.source.strip() if payload.source else ""
    source = normalized_source or None
    if source and len(source) > 100:
        raise HTTPException(status_code=400, detail="Invalid location source")

    conn = get_connection()
    try:
        with conn:
            ensure_user_locations_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO user_locations (
                        user_id,
                        lat,
                        lng,
                        accuracy,
                        source,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id) DO UPDATE SET
                        lat = EXCLUDED.lat,
                        lng = EXCLUDED.lng,
                        accuracy = EXCLUDED.accuracy,
                        source = EXCLUDED.source,
                        updated_at = NOW()
                    RETURNING user_id, lat, lng, accuracy, source, created_at, updated_at;
                    """,
                    (
                        user_id,
                        float(payload.lat),
                        float(payload.lng),
                        float(payload.accuracy) if payload.accuracy is not None else None,
                        source,
                    ),
                )
                location = cur.fetchone()

        return {
            "status": "ok",
            "has_location": True,
            "location": serialize_datetime_fields(
                [dict(location)],
                ["created_at", "updated_at"],
            )[0],
        }
    except HTTPException:
        raise
    except Exception as exc:
        user_location_log(f"location update failed user_id={user_id} type={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Location update failed")
    finally:
        conn.close()


@app.get("/user/location")
def get_current_user_location(
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, Any]:
    user_payload = get_current_user_from_token(authorization)
    user_id = int(user_payload["id"])

    conn = get_connection()
    try:
        with conn:
            ensure_user_locations_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT user_id, lat, lng, accuracy, source, created_at, updated_at
                    FROM user_locations
                    WHERE user_id = %s
                    LIMIT 1;
                    """,
                    (user_id,),
                )
                location = cur.fetchone()

        has_location = location is not None
        user_location_log(f"location get user_id={user_id} has_location={has_location}")
        return {
            "status": "ok",
            "has_location": has_location,
            "location": serialize_datetime_fields(
                [dict(location)],
                ["created_at", "updated_at"],
            )[0] if location else None,
        }
    except Exception as exc:
        user_location_log(f"location get failed user_id={user_id} type={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Location get failed")
    finally:
        conn.close()


@app.get("/user/notification-preferences")
def get_current_user_notification_preferences(
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, Any]:
    notification_preferences_log("preferences get")
    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            ensure_price_notification_preferences_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO price_notification_preferences (
                        user_id,
                        price_notifications_enabled,
                        fuel_type,
                        radius_km,
                        favorites_only,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, TRUE, NULL, NULL, FALSE, NOW(), NOW())
                    ON CONFLICT (user_id) DO UPDATE SET
                        updated_at = price_notification_preferences.updated_at
                    RETURNING
                        user_id,
                        price_notifications_enabled,
                        fuel_type,
                        radius_km,
                        favorites_only,
                        latitude,
                        longitude,
                        location_updated_at,
                        created_at,
                        updated_at;
                    """,
                    (user_id,),
                )
                preferences = cur.fetchone()

        notification_preferences_log(f"preferences get enabled={preferences['price_notifications_enabled']}")
        return {
            "status": "ok",
            "preferences": serialize_datetime_fields(
                [dict(preferences)],
                ["created_at", "updated_at"],
            )[0],
        }
    except HTTPException:
        raise
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        table = getattr(getattr(exc, "diag", None), "table_name", None)
        notification_preferences_log(
            f"preferences get failed type={exc.__class__.__name__} "
            f"pgcode={pgcode} table={table} constraint={constraint}"
        )
        raise HTTPException(status_code=500, detail="Notification preferences get failed")
    finally:
        conn.close()


@app.put("/user/notification-preferences")
def update_current_user_notification_preferences(
    payload: NotificationPreferencesRequest,
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, Any]:
    notification_preferences_log("preferences update")
    fields_set = request_fields_set(payload)

    updates: dict[str, Any] = {}
    if "price_notifications_enabled" in fields_set:
        if payload.price_notifications_enabled is None:
            raise HTTPException(status_code=400, detail="Invalid price_notifications_enabled")
        updates["price_notifications_enabled"] = bool(payload.price_notifications_enabled)

    if "fuel_type" in fields_set:
        updates["fuel_type"] = normalize_notification_fuel_type(payload.fuel_type)

    if "radius_km" in fields_set:
        updates["radius_km"] = normalize_notification_radius_km(payload.radius_km)

    if "favorites_only" in fields_set:
        if payload.favorites_only is None:
            raise HTTPException(status_code=400, detail="Invalid favorites_only")
        updates["favorites_only"] = bool(payload.favorites_only)

    location_fields = {"latitude", "longitude"}
    if fields_set.intersection(location_fields):
        if not location_fields.issubset(fields_set):
            raise HTTPException(status_code=400, detail="Latitude and longitude must be provided together")
        if payload.latitude is None and payload.longitude is None:
            updates["latitude"] = None
            updates["longitude"] = None
            updates["location_updated_at"] = None
        elif payload.latitude is None or payload.longitude is None:
            raise HTTPException(status_code=400, detail="Invalid notification location")
        elif (
            not isfinite(payload.latitude)
            or not isfinite(payload.longitude)
            or not -90 <= payload.latitude <= 90
            or not -180 <= payload.longitude <= 180
        ):
            raise HTTPException(status_code=400, detail="Invalid notification location")
        else:
            updates["latitude"] = float(payload.latitude)
            updates["longitude"] = float(payload.longitude)
            updates["location_updated_at"] = datetime.now(timezone.utc)

    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            ensure_price_notification_preferences_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO price_notification_preferences (
                        user_id,
                        price_notifications_enabled,
                        fuel_type,
                        radius_km,
                        favorites_only,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, TRUE, NULL, NULL, FALSE, NOW(), NOW())
                    ON CONFLICT (user_id) DO NOTHING;
                    """,
                    (user_id,),
                )

                if updates:
                    set_clauses = [f"{field} = %s" for field in updates]
                    values = list(updates.values())
                    set_clauses.append("updated_at = NOW()")
                    values.append(user_id)
                    cur.execute(
                        f"""
                        UPDATE price_notification_preferences
                        SET {", ".join(set_clauses)}
                        WHERE user_id = %s
                        RETURNING
                            user_id,
                            price_notifications_enabled,
                            fuel_type,
                            radius_km,
                            favorites_only,
                            latitude,
                            longitude,
                            location_updated_at,
                            created_at,
                            updated_at;
                        """,
                        values,
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            user_id,
                            price_notifications_enabled,
                            fuel_type,
                            radius_km,
                            favorites_only,
                            latitude,
                            longitude,
                            location_updated_at,
                            created_at,
                            updated_at
                        FROM price_notification_preferences
                        WHERE user_id = %s;
                        """,
                        (user_id,),
                    )

                preferences = cur.fetchone()

        notification_preferences_log(f"preferences update enabled={preferences['price_notifications_enabled']}")
        return {
            "status": "ok",
            "preferences": serialize_datetime_fields(
                [dict(preferences)],
                ["created_at", "updated_at"],
            )[0],
        }
    except HTTPException:
        raise
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        table = getattr(getattr(exc, "diag", None), "table_name", None)
        notification_preferences_log(
            f"preferences update failed type={exc.__class__.__name__} "
            f"pgcode={pgcode} table={table} constraint={constraint}"
        )
        raise HTTPException(status_code=500, detail="Notification preferences update failed")
    finally:
        conn.close()


@app.post("/user/referral-code")
def apply_current_user_referral_code(
    payload: ApplyReferralCodeRequest,
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, str]:
    print("[REFERRAL] apply code endpoint reached")
    referral_code_input = normalize_referral_code_input(payload.referral_code)
    print(f"[REFERRAL] referral_code present={bool(referral_code_input)}")

    if not referral_code_input:
        print("[REFERRAL] referral applied=false")
        raise HTTPException(status_code=400, detail="Referral code is required")

    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, referred_by_user_id
                    FROM users
                    WHERE id = %s
                    FOR UPDATE;
                    """,
                    (user_id,),
                )
                current_user = cur.fetchone()
                if current_user is None:
                    print("[REFERRAL] referral applied=false")
                    raise HTTPException(status_code=404, detail="User not found")

                cur.execute(
                    """
                    SELECT 1
                    FROM referrals
                    WHERE referred_user_id = %s
                    LIMIT 1;
                    """,
                    (user_id,),
                )
                already_referred = bool(current_user["referred_by_user_id"] or cur.fetchone())
                print(f"[REFERRAL] already referred={already_referred}")
                if already_referred:
                    print("[REFERRAL] referral applied=false")
                    raise HTTPException(status_code=409, detail="Referral code already applied")

                try:
                    referrer_user_id = resolve_active_referrer_id(cur, referral_code_input)
                except HTTPException:
                    print("[REFERRAL] referral applied=false")
                    raise

                if referrer_user_id == user_id:
                    print("[REFERRAL] referral applied=false")
                    raise HTTPException(status_code=400, detail="Cannot use your own referral code")

                cur.execute(
                    """
                    UPDATE users
                    SET referred_by_user_id = %s,
                        updated_at = NOW()
                    WHERE id = %s;
                    """,
                    (referrer_user_id, user_id),
                )
                create_pending_referral(cur, referrer_user_id, user_id, referral_code_input)

        print("[REFERRAL] referral applied=true")
        return {"status": "ok"}
    except HTTPException:
        raise
    except psycopg2.errors.UniqueViolation:
        print("[REFERRAL] referral applied=false")
        raise HTTPException(status_code=409, detail="Referral code already applied")
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        table = getattr(getattr(exc, "diag", None), "table_name", None)
        print(
            f"[REFERRAL] apply code failed type={exc.__class__.__name__} "
            f"pgcode={pgcode} table={table} constraint={constraint}"
        )
        raise HTTPException(status_code=500, detail="Referral code apply failed")
    finally:
        conn.close()


@app.get("/user/referrals")
def get_current_user_referrals(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        r.id,
                        r.referrer_user_id,
                        r.referred_user_id,
                        r.referral_code_used,
                        r.status,
                        r.validated_at,
                        r.created_at,
                        r.updated_at,
                        u.email AS referred_user_email,
                        u.display_name AS referred_user_display_name
                    FROM referrals r
                    JOIN users u ON u.id = r.referred_user_id
                    WHERE r.referrer_user_id = %s
                    ORDER BY r.created_at DESC;
                    """,
                    (user_id,),
                )
                referrals = cur.fetchall()

                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
                        COUNT(*) FILTER (WHERE status = 'valid') AS valid_count,
                        COUNT(*) FILTER (WHERE status = 'rejected') AS rejected_count,
                        COUNT(*) AS total_count
                    FROM referrals
                    WHERE referrer_user_id = %s;
                    """,
                    (user_id,),
                )
                summary = cur.fetchone()

        return {
            "status": "ok",
            "summary": dict(summary) if summary else {
                "pending_count": 0,
                "valid_count": 0,
                "rejected_count": 0,
                "total_count": 0,
            },
            "items": serialize_datetime_fields([dict(row) for row in referrals], ["validated_at", "created_at", "updated_at"]),
        }
    finally:
        conn.close()


@app.get("/user/rewards")
def get_current_user_rewards(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        user_id,
                        reward_type,
                        reward_value,
                        status,
                        granted_at,
                        expires_at,
                        created_at,
                        updated_at
                    FROM rewards
                    WHERE user_id = %s
                    ORDER BY created_at DESC;
                    """,
                    (user_id,),
                )
                rewards = cur.fetchall()

        return {
            "status": "ok",
            "count": len(rewards),
            "items": serialize_datetime_fields([dict(row) for row in rewards], ["granted_at", "expires_at", "created_at", "updated_at"]),
        }
    finally:
        conn.close()


@app.get("/user/subscription")
def get_current_user_subscription(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        plus_info = get_active_plus_status(conn, user_id)

        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        user_id,
                        source,
                        status,
                        starts_at,
                        expires_at,
                        original_transaction_id,
                        created_at,
                        updated_at
                    FROM user_subscriptions
                    WHERE user_id = %s
                    ORDER BY created_at DESC;
                    """,
                    (user_id,),
                )
                subscriptions = cur.fetchall()

        return {
            "status": "ok",
            "is_plus": plus_info["is_plus"],
            "active_subscription": plus_info["subscription"],
            "items": serialize_datetime_fields([dict(row) for row in subscriptions], ["starts_at", "expires_at", "created_at", "updated_at"]),
        }
    finally:
        conn.close()


@app.get("/stations")
def list_stations(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    try:
        conn = get_connection()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        mimit_id,
                        name,
                        brand,
                        operator,
                        address,
                        city,
                        province,
                        latitude,
                        longitude
                    FROM stations
                    ORDER BY id ASC
                    LIMIT %s;
                    """,
                    (limit,),
                )
                stations = cur.fetchall()
        conn.close()
        return {
            "count": len(stations),
            "items": [dict(row) for row in stations],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


@app.get("/stations/search")
def search_stations(
    q: str = Query(..., min_length=2, description="Search query for station name, address, city, province, brand, or operator"),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    try:
        conn = get_connection()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                search_query = f"%{q.lower()}%"
                cur.execute(
                    """
                    SELECT
                        id,
                        mimit_id,
                        name,
                        brand,
                        operator,
                        address,
                        city,
                        province,
                        latitude,
                        longitude
                    FROM stations
                    WHERE LOWER(COALESCE(name, '')) LIKE %s
                       OR LOWER(COALESCE(address, '')) LIKE %s
                       OR LOWER(COALESCE(city, '')) LIKE %s
                       OR LOWER(COALESCE(province, '')) LIKE %s
                       OR LOWER(COALESCE(brand, '')) LIKE %s
                       OR LOWER(COALESCE(operator, '')) LIKE %s
                    ORDER BY city ASC, brand ASC NULLS LAST, address ASC
                    LIMIT %s;
                    """,
                    (
                        search_query,
                        search_query,
                        search_query,
                        search_query,
                        search_query,
                        search_query,
                        limit,
                    ),
                )
                stations = cur.fetchall()
        conn.close()
        return {
            "count": len(stations),
            "items": [dict(row) for row in stations],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


# Nearby stations endpoint
@app.get("/stations/nearby")
def get_nearby_stations(
    lat: float = Query(..., description="User latitude"),
    lng: float = Query(..., description="User longitude"),
    fuel_type: str = Query(..., description="Fuel type, for example benzina or diesel"),
    is_self_service: bool | None = Query(default=None, description="Filter by self service: true, false, or omit for both"),
    radius_km: float = Query(default=5.0, gt=0, le=100),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    try:
        lat_delta = radius_km / 111.32
        lng_divisor = 111.32 * max(cos(radians(lat)), 0.01)
        lng_delta = radius_km / lng_divisor

        conn = get_connection()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH candidate_stations AS (
                        SELECT
                            s.id,
                            s.mimit_id,
                            s.name,
                            s.brand,
                            s.operator,
                            s.address,
                            s.city,
                            s.province,
                            s.latitude,
                            s.longitude,
                            (
                                6371 * acos(
                                    least(
                                        1,
                                        cos(radians(%s)) * cos(radians(s.latitude)) *
                                        cos(radians(s.longitude) - radians(%s)) +
                                        sin(radians(%s)) * sin(radians(s.latitude))
                                    )
                                )
                            ) AS distance_km
                        FROM stations s
                        WHERE s.latitude BETWEEN %s AND %s
                          AND s.longitude BETWEEN %s AND %s
                    ),
                    ranked_prices AS (
                        SELECT DISTINCT ON (cs.id)
                            cs.id,
                            cs.mimit_id,
                            cs.name,
                            cs.brand,
                            cs.operator,
                            cs.address,
                            cs.city,
                            cs.province,
                            cs.latitude,
                            cs.longitude,
                            cs.distance_km,
                            fp.fuel_type,
                            fp.price,
                            fp.is_self_service,
                            fp.reported_at,
                            fp.reported_at AS last_reported_at
                        FROM candidate_stations cs
                        JOIN fuel_prices fp ON fp.station_id = cs.id
                        WHERE cs.distance_km <= %s
                          AND fp.fuel_type = %s
                          AND (%s IS NULL OR fp.is_self_service = %s)
                        ORDER BY cs.id, fp.reported_at DESC, fp.price ASC, fp.is_self_service DESC
                    )
                    SELECT
                        id,
                        mimit_id,
                        name,
                        brand,
                        operator,
                        address,
                        city,
                        province,
                        latitude,
                        longitude,
                        round(distance_km::numeric, 3) AS distance_km,
                        fuel_type,
                        price,
                        is_self_service,
                        reported_at,
                        last_reported_at
                    FROM ranked_prices
                    ORDER BY price ASC, distance_km ASC
                    LIMIT %s;
                    """,
                    (
                        lat,
                        lng,
                        lat,
                        lat - lat_delta,
                        lat + lat_delta,
                        lng - lng_delta,
                        lng + lng_delta,
                        radius_km,
                        fuel_type.strip().lower(),
                        is_self_service,
                        is_self_service,
                        limit,
                    ),
                )
                stations = cur.fetchall()
        conn.close()
        return {
            "count": len(stations),
            "items": serialize_datetime_fields([dict(row) for row in stations], ["reported_at", "last_reported_at"]),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


@app.get("/stations/{station_id}/community-prices")
def get_station_community_prices(station_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        with conn:
            ensure_community_price_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id FROM stations WHERE id = %s LIMIT 1;", (station_id,))
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="Station not found")

                cur.execute(
                    """
                    WITH valid_reports AS (
                        SELECT
                            fuel_type,
                            price,
                            is_self_service,
                            reported_at,
                            created_at,
                            COUNT(*) OVER (
                                PARTITION BY fuel_type, is_self_service
                            ) AS reports_count,
                            ROW_NUMBER() OVER (
                                PARTITION BY fuel_type, is_self_service
                                ORDER BY reported_at DESC, created_at DESC, id DESC
                            ) AS row_number
                        FROM user_price_reports
                        WHERE station_id = %s
                          AND status IN ('pending', 'accepted')
                    )
                    SELECT
                        fuel_type,
                        price,
                        is_self_service,
                        reported_at,
                        'community' AS source,
                        reports_count
                    FROM valid_reports
                    WHERE row_number = 1
                    ORDER BY fuel_type ASC, is_self_service DESC;
                    """,
                    (station_id,),
                )
                reports = cur.fetchall()

        return {
            "station_id": station_id,
            "items": serialize_datetime_fields([dict(row) for row in reports], ["reported_at"]),
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[COMMUNITY_PRICES] Read failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Community prices read failed")
    finally:
        conn.close()


@app.post("/stations/{station_id}/community-prices", status_code=201)
def submit_station_community_price(
    station_id: int,
    payload: CommunityPriceReportRequest,
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> dict[str, Any]:
    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]
    fuel_type, price = validate_community_price_report(payload)
    reported_at = datetime.now(timezone.utc)

    conn = get_connection()
    try:
        with conn:
            ensure_community_price_schema(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id FROM stations WHERE id = %s LIMIT 1;", (station_id,))
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="Station not found")

                cur.execute(
                    """
                    SELECT 1
                    FROM user_price_reports
                    WHERE user_id = %s
                      AND station_id = %s
                      AND fuel_type = %s
                      AND is_self_service = %s
                      AND ABS(price - %s) < 0.0001
                      AND status IN ('pending', 'accepted')
                      AND created_at >= NOW() - INTERVAL '10 minutes'
                    LIMIT 1;
                    """,
                    (user_id, station_id, fuel_type, payload.is_self_service, price),
                )
                if cur.fetchone() is not None:
                    raise HTTPException(status_code=429, detail="Duplicate report submitted recently")

                cur.execute(
                    """
                    INSERT INTO user_price_reports (
                        station_id,
                        user_id,
                        fuel_type,
                        price,
                        is_self_service,
                        reported_at,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'pending', NOW(), NOW())
                    RETURNING id, fuel_type, price, is_self_service, reported_at, status;
                    """,
                    (station_id, user_id, fuel_type, price, payload.is_self_service, reported_at),
                )
                report = cur.fetchone()

        report_payload = serialize_datetime_fields([dict(report)], ["reported_at"])[0]
        report_payload["source"] = "community"

        return {
            "status": "ok",
            "station_id": station_id,
            "report": report_payload,
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[COMMUNITY_PRICES] Submit failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Community price submission failed")
    finally:
        conn.close()


# Best station endpoint
@app.get("/stations/best")
def get_best_station(
    lat: float = Query(..., description="User latitude"),
    lng: float = Query(..., description="User longitude"),
    fuel_type: str = Query(..., description="Fuel type, for example benzina or diesel"),
    is_self_service: bool | None = Query(default=None, description="Filter by self service: true, false, or omit for both"),
    radius_km: float = Query(default=5.0, gt=0, le=100),
) -> dict[str, Any]:
    nearby_result = get_nearby_stations(
        lat=lat,
        lng=lng,
        fuel_type=fuel_type,
        is_self_service=is_self_service,
        radius_km=radius_km,
        limit=1,
    )

    items = nearby_result.get("items", [])
    if not items:
        raise HTTPException(status_code=404, detail="No stations found for the selected filters")

    return {
        "best_station": items[0]
    }


@app.get("/stations/{station_id}")
def get_station_detail(station_id: int) -> dict[str, Any]:
    try:
        conn = get_connection()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        mimit_id,
                        name,
                        brand,
                        operator,
                        address,
                        city,
                        province,
                        latitude,
                        longitude
                    FROM stations
                    WHERE id = %s;
                    """,
                    (station_id,),
                )
                station = cur.fetchone()

        conn.close()

        if not station:
            raise HTTPException(status_code=404, detail="Station not found")

        return {
            "station": dict(station)
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


@app.get("/stations/{station_id}/prices")
def get_station_prices(station_id: int) -> dict[str, Any]:
    try:
        conn = get_connection()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        mimit_id,
                        name,
                        brand,
                        address,
                        city,
                        province
                    FROM stations
                    WHERE id = %s;
                    """,
                    (station_id,),
                )
                station = cur.fetchone()

                if not station:
                    raise HTTPException(status_code=404, detail="Station not found")

                cur.execute(
                    """
                    SELECT
                        fuel_type,
                        price,
                        is_self_service,
                        reported_at
                    FROM fuel_prices
                    WHERE station_id = %s
                    ORDER BY fuel_type ASC, is_self_service DESC;
                    """,
                    (station_id,),
                )
                prices = cur.fetchall()
        conn.close()
        return {
            "station": dict(station),
            "prices": serialize_datetime_fields([dict(row) for row in prices], ["reported_at"]),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


# --- Add entrypoint for running with uvicorn ---
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
