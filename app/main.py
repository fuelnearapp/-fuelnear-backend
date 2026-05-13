from typing import Any
from email.utils import parsedate_to_datetime
import os
from math import cos, radians
import threading
import time
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query, Header, Depends
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
ADMIN_UPDATE_TOKEN = os.getenv("ADMIN_UPDATE_TOKEN")

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


def default_display_name_from_email(email: str) -> str:
    local_part = email.split("@", 1)[0].strip()
    return local_part or "FuelNear User"


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


def delete_current_account(authorization: str | None) -> dict[str, str]:
    user_payload = get_current_user_from_token(authorization)
    user_id = user_payload["id"]

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
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
                    raise HTTPException(status_code=404, detail="User not found")

        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[AUTH] Account deletion failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Account deletion failed")
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
            ALTER TABLE user_sessions
            ADD COLUMN IF NOT EXISTS access_expires_at TIMESTAMPTZ NULL;
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
        ensure_mimit_import_schema(conn)

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
def admin_update_mimit(_: None = Depends(require_admin_update_token)) -> dict[str, Any]:
    conn = get_connection()
    run_id: int | None = None

    try:
        with conn:
            ensure_mimit_import_schema(conn)
            run_id = create_mimit_import_run(conn)

        print(f"[MIMIT] Update started. run_id={run_id}")
        result = run_mimit_update(download=True)

        if result is None:
            with conn:
                fail_mimit_import_run(conn, run_id, "MIMIT update already in progress")
            return {
                "status": "busy",
                "message": "MIMIT update already in progress",
                "run_id": run_id,
            }

        with conn:
            finish_mimit_import_run(conn, run_id, result)

        print(f"[MIMIT] Update completed. run_id={run_id} result={result}")
        return {
            "status": "ok",
            "message": "MIMIT update completed",
            "run_id": run_id,
            "result": result,
        }
    except HTTPException:
        raise
    except Exception as exc:
        error_message = str(exc)
        print(f"[MIMIT] Update failed. run_id={run_id} error={error_message}")

        if run_id is not None:
            try:
                with conn:
                    fail_mimit_import_run(conn, run_id, error_message)
            except Exception as persist_error:
                print(f"[MIMIT] Failed to persist import failure. error={persist_error}")

        raise HTTPException(status_code=500, detail=f"MIMIT update failed: {exc}")
    finally:
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

        return {
            "status": "ok",
            "last_status": last_run["status"] if last_run else None,
            "last_run": serialize_datetime_fields([dict(last_run)], ["started_at", "completed_at", "source_file_timestamp"])[0] if last_run else None,
            "last_successful_update_at": last_success["completed_at"].isoformat() if last_success and last_success["completed_at"] else None,
            "stations_imported": last_success["stations_imported"] if last_success else None,
            "prices_imported": last_success["prices_imported"] if last_success else None,
            "stations_csv": last_success["stations_csv"] if last_success else None,
            "prices_csv": last_success["prices_csv"] if last_success else None,
            "source_file_timestamp": last_success["source_file_timestamp"].isoformat() if last_success and last_success["source_file_timestamp"] else None,
            "last_error": last_run["error_message"] if last_run and last_run["status"] == "failed" else None,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MIMIT status failed: {exc}")
    finally:
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
        print(f"[AUTH] Login failed. error={exc.__class__.__name__}")
        raise HTTPException(status_code=500, detail="Login failed")
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


@app.delete("/account")
def delete_account(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, str]:
    return delete_current_account(authorization)


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
