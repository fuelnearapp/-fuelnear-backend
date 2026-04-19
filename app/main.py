from typing import Any
import os
from math import cos, radians
import threading
import time
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel, EmailStr

from app.import_mimit import update_mimit_data
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

ACCESS_TOKEN_TTL_HOURS = int(os.getenv("ACCESS_TOKEN_TTL_HOURS", "24"))
REFRESH_TOKEN_TTL_DAYS = int(os.getenv("REFRESH_TOKEN_TTL_DAYS", "30"))

app = FastAPI(title="FuelNear Backend")


_scheduler_started = False
_scheduler_lock = threading.Lock()
_mimit_update_lock = threading.Lock()


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


class RefreshRequest(BaseModel):
    refresh_token: str


def normalize_email(value: str) -> str:
    return value.strip().lower()


def sanitize_display_name(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()
    return cleaned or None


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
                u.is_active AS referred_user_is_active
            FROM referrals r
            JOIN users u ON u.id = r.referred_user_id
            WHERE r.status = 'pending'
              AND r.created_at <= NOW() - (%s * INTERVAL '1 day')
              AND u.is_active = TRUE
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
                expires_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                user_id,
                access_token_hash,
                refresh_token_hash,
                device_info,
                ip_address,
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
                expires_at = %s,
                revoked_at = NULL
            WHERE id = %s;
            """,
            (
                new_access_token_hash,
                new_refresh_token_hash,
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
                        s.expires_at,
                        s.revoked_at
                    FROM user_sessions s
                    JOIN users u ON u.id = s.user_id
                    WHERE s.access_token_hash = %s
                      AND s.revoked_at IS NULL
                      AND s.expires_at > NOW()
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






def run_mimit_update(download: bool = True) -> dict[str, object] | None:
    if not _mimit_update_lock.acquire(blocking=False):
        print("[MIMIT] Update skipped: another update is already running.")
        return None

    try:
        return update_mimit_data(download=download)
    finally:
        _mimit_update_lock.release()


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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rewards_user_id ON rewards(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_sessions_refresh_token_hash ON user_sessions(refresh_token_hash);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_subscriptions_user_id ON user_subscriptions(user_id);")

def serialize_datetime_fields(items: list[dict[str, Any]], fields: list[str]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []

    for item in items:
        row = dict(item)
        for field in fields:
            if field in row and row[field] is not None:
                row[field] = row[field].isoformat()
        serialized.append(row)

    return serialized


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


@app.get("/admin/update-mimit")
def admin_update_mimit() -> dict[str, Any]:
    try:
        result = run_mimit_update(download=True)
        if result is None:
            return {
                "status": "busy",
                "message": "MIMIT update already in progress",
            }

        return {
            "status": "ok",
            "message": "MIMIT update completed",
            "result": result,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MIMIT update failed: {exc}")


# === ADMIN REFERRAL PROCESSING ENDPOINT ===

@app.post("/admin/process-referrals")
def admin_process_referrals() -> dict[str, Any]:
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


# === AUTH ENDPOINTS ===


@app.post("/auth/register")
def register_user(payload: RegisterRequest) -> dict[str, Any]:
    email = normalize_email(str(payload.email))
    display_name = sanitize_display_name(payload.display_name)
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
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Registration failed: {exc}")
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
        raise HTTPException(status_code=500, detail=f"Login failed: {exc}")
    finally:
        conn.close()


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


# === USER REFERRALS, REWARDS, SUBSCRIPTION ENDPOINTS ===

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
                        ORDER BY cs.id, fp.price ASC, fp.is_self_service DESC, fp.reported_at DESC
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