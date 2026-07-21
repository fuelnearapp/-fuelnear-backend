from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
import secrets
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from app import apple_subscription_reconciler
from app.auth_utils import hash_token
from app.db import get_connection


GUEST_SESSION_TTL_DAYS = max(1, int(os.getenv("GUEST_SESSION_TTL_DAYS", "365")))
GUEST_TOKEN_BYTES = 48


class GuestSubscriptionError(RuntimeError):
    pass


class GuestSessionInvalid(GuestSubscriptionError):
    pass


class GuestAlreadyClaimed(GuestSubscriptionError):
    pass


class GuestSubscriptionNotFound(GuestSubscriptionError):
    pass


class GuestOwnershipConflict(GuestSubscriptionError):
    pass


class GuestUserNotFound(GuestSubscriptionError):
    pass


@dataclass(frozen=True, slots=True)
class GuestSessionResult:
    guest_id: int
    access_token: str
    app_account_token: UUID
    expires_at: datetime
    created: bool


@dataclass(frozen=True, slots=True)
class GuestSubscriptionStatus:
    is_plus: bool
    product_id: str | None
    expires_at: datetime | None
    status: str


@dataclass(frozen=True, slots=True)
class AppleSubscriptionOwner:
    kind: str
    owner_id: int

    @property
    def user_id(self) -> int | None:
        return self.owner_id if self.kind == "user" else None

    @property
    def guest_id(self) -> int | None:
        return self.owner_id if self.kind == "guest" else None


@dataclass(frozen=True, slots=True)
class GuestClaimResult:
    claimed: bool
    guest_id: int
    user_id: int
    transferred_transactions: int
    is_plus: bool
    expires_at: datetime | None
    changed: bool


def ensure_guest_subscription_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS guest_identities (
                id BIGSERIAL PRIMARY KEY,
                app_account_token UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
                claimed_user_id BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
                claimed_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS guest_sessions (
                id BIGSERIAL PRIMARY KEY,
                guest_id BIGINT NOT NULL REFERENCES guest_identities(id) ON DELETE CASCADE,
                token_hash TEXT NOT NULL UNIQUE,
                expires_at TIMESTAMPTZ NOT NULL,
                revoked_at TIMESTAMPTZ NULL,
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute("ALTER TABLE apple_transactions ADD COLUMN IF NOT EXISTS guest_id BIGINT NULL;")
        cur.execute("ALTER TABLE apple_transactions ALTER COLUMN user_id DROP NOT NULL;")
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'fk_apple_transactions_guest_id'
                      AND conrelid = 'apple_transactions'::regclass
                ) THEN
                    ALTER TABLE apple_transactions
                    ADD CONSTRAINT fk_apple_transactions_guest_id
                    FOREIGN KEY (guest_id)
                    REFERENCES guest_identities(id)
                    ON DELETE RESTRICT;
                END IF;
            END
            $$;
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'apple_transactions_exactly_one_owner'
                      AND conrelid = 'apple_transactions'::regclass
                ) THEN
                    ALTER TABLE apple_transactions
                    ADD CONSTRAINT apple_transactions_exactly_one_owner
                    CHECK (num_nonnulls(user_id, guest_id) = 1) NOT VALID;
                END IF;
            END
            $$;
            """
        )
        cur.execute(
            """
            ALTER TABLE apple_transactions
            VALIDATE CONSTRAINT apple_transactions_exactly_one_owner;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_guest_sessions_guest_id
            ON guest_sessions(guest_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_guest_sessions_expires_at
            ON guest_sessions(expires_at);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_guest_identities_claimed_user_id
            ON guest_identities(claimed_user_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_apple_transactions_guest_id
            ON apple_transactions(guest_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_apple_transactions_guest_original_transaction
            ON apple_transactions(guest_id, original_transaction_id);
            """
        )


def _normalize_guest_token(token: str) -> str:
    if not isinstance(token, str) or not token.strip():
        raise GuestSessionInvalid("Guest session is invalid")
    return token.strip()


def create_guest_session() -> GuestSessionResult:
    access_token = secrets.token_urlsafe(GUEST_TOKEN_BYTES)
    token_hash = hash_token(access_token)
    expires_at = datetime.now(timezone.utc) + timedelta(days=GUEST_SESSION_TTL_DAYS)
    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO guest_identities DEFAULT VALUES
                    RETURNING id, app_account_token;
                    """
                )
                guest = cur.fetchone()
                cur.execute(
                    """
                    INSERT INTO guest_sessions (guest_id, token_hash, expires_at)
                    VALUES (%s, %s, %s)
                    RETURNING id;
                    """,
                    (guest["id"], token_hash, expires_at),
                )
        return GuestSessionResult(
            guest_id=int(guest["id"]),
            access_token=access_token,
            app_account_token=UUID(str(guest["app_account_token"])),
            expires_at=expires_at,
            created=True,
        )
    finally:
        conn.close()


def get_guest_by_token(
    token: str,
    *,
    connection: Any | None = None,
    for_update: bool = False,
    allow_claimed_session: bool = False,
    touch: bool = False,
) -> dict[str, Any]:
    normalized_token = _normalize_guest_token(token)
    conn = connection if connection is not None else get_connection()
    owns_connection = connection is None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    identity.id AS guest_id,
                    identity.app_account_token,
                    identity.claimed_user_id,
                    identity.claimed_at,
                    session.id AS session_id,
                    session.expires_at,
                    session.revoked_at
                FROM guest_sessions session
                JOIN guest_identities identity ON identity.id = session.guest_id
                WHERE session.token_hash = %s
                LIMIT 1
                {"FOR UPDATE OF session, identity" if for_update else ""};
                """,
                (hash_token(normalized_token),),
            )
            row = cur.fetchone()
            if row is None:
                raise GuestSessionInvalid("Guest session is invalid")

            claimed = row["claimed_user_id"] is not None
            session_active = row["revoked_at"] is None and row["expires_at"] > datetime.now(timezone.utc)
            if claimed and not allow_claimed_session:
                raise GuestAlreadyClaimed("Guest identity has already been claimed")
            if not session_active and not (allow_claimed_session and claimed):
                raise GuestSessionInvalid("Guest session is invalid")

            if touch and session_active:
                extended_expiry = datetime.now(timezone.utc) + timedelta(days=GUEST_SESSION_TTL_DAYS)
                cur.execute(
                    """
                    UPDATE guest_sessions
                    SET last_seen_at = NOW(),
                        expires_at = GREATEST(expires_at, %s),
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING expires_at;
                    """,
                    (extended_expiry, row["session_id"]),
                )
                row["expires_at"] = cur.fetchone()["expires_at"]
            return dict(row)
    finally:
        if owns_connection:
            if touch:
                conn.commit()
            conn.close()


def reuse_guest_session(token: str) -> GuestSessionResult:
    guest = get_guest_by_token(token, touch=True)
    return GuestSessionResult(
        guest_id=int(guest["guest_id"]),
        access_token=token.strip(),
        app_account_token=UUID(str(guest["app_account_token"])),
        expires_at=guest["expires_at"],
        created=False,
    )


def get_guest_subscription_status(
    guest_id: int,
    reference_date: datetime | None = None,
    *,
    connection: Any | None = None,
) -> GuestSubscriptionStatus:
    if isinstance(guest_id, bool) or not isinstance(guest_id, int) or guest_id <= 0:
        raise ValueError("guest_id must be a positive integer")
    reference = reference_date or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)

    conn = connection if connection is not None else get_connection()
    owns_connection = connection is None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH latest_transactions AS (
                    SELECT DISTINCT ON (original_transaction_id) *
                    FROM apple_transactions
                    WHERE guest_id = %s
                    ORDER BY original_transaction_id,
                             COALESCE(signed_date, purchase_date) DESC,
                             purchase_date DESC,
                             id DESC
                )
                SELECT *
                FROM latest_transactions
                ORDER BY (revocation_date IS NULL
                          AND (expires_date IS NULL OR expires_date > %s)) DESC,
                         expires_date DESC NULLS FIRST,
                         COALESCE(signed_date, purchase_date) DESC,
                         id DESC
                LIMIT 1;
                """,
                (guest_id, reference),
            )
            latest = cur.fetchone()
            if latest is None:
                return GuestSubscriptionStatus(False, None, None, "none")
            if latest["revocation_date"] is not None:
                status = "revoked"
                active = False
            elif latest["expires_date"] is not None and latest["expires_date"] <= reference:
                status = "expired"
                active = False
            else:
                status = "active"
                active = True
            return GuestSubscriptionStatus(
                is_plus=active,
                product_id=str(latest["product_id"]),
                expires_at=latest["expires_date"],
                status=status,
            )
    finally:
        if owns_connection:
            conn.close()


def resolve_owner_by_app_account_token(app_account_token: UUID) -> AppleSubscriptionOwner | None:
    if not isinstance(app_account_token, UUID):
        raise ValueError("app_account_token must be a UUID")
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM users WHERE app_account_token = %s LIMIT 1;
                """,
                (str(app_account_token),),
            )
            user = cur.fetchone()
            cur.execute(
                """
                SELECT id, claimed_user_id
                FROM guest_identities
                WHERE app_account_token = %s
                LIMIT 1;
                """,
                (str(app_account_token),),
            )
            guest = cur.fetchone()

        owners: set[tuple[str, int]] = set()
        if user is not None:
            owners.add(("user", int(user["id"])))
        if guest is not None:
            if guest["claimed_user_id"] is not None:
                owners.add(("user", int(guest["claimed_user_id"])))
            else:
                owners.add(("guest", int(guest["id"])))
        if not owners:
            return None
        if len(owners) != 1:
            raise GuestOwnershipConflict("Apple app account token ownership is inconsistent")
        kind, owner_id = next(iter(owners))
        return AppleSubscriptionOwner(kind, owner_id)
    finally:
        conn.close()


def get_allowed_app_account_tokens_for_user(user_id: int) -> list[UUID]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT app_account_token FROM users WHERE id = %s
                UNION
                SELECT app_account_token FROM guest_identities WHERE claimed_user_id = %s;
                """,
                (user_id, user_id),
            )
            return [UUID(str(row[0])) for row in cur.fetchall()]
    finally:
        conn.close()


def claim_guest_subscription(user_id: int, guest_token: str) -> GuestClaimResult:
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    conn = get_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id FROM users WHERE id = %s FOR UPDATE;", (user_id,))
                if cur.fetchone() is None:
                    raise GuestUserNotFound("User not found")

            guest = get_guest_by_token(
                guest_token,
                connection=conn,
                for_update=True,
                allow_claimed_session=True,
            )
            guest_id = int(guest["guest_id"])
            claimed_user_id = guest["claimed_user_id"]
            if claimed_user_id is not None:
                if int(claimed_user_id) != user_id:
                    raise GuestAlreadyClaimed("Guest identity has already been claimed")
                entitlement = apple_subscription_reconciler.reconcile_apple_entitlement(
                    user_id,
                    connection=conn,
                )
                return GuestClaimResult(
                    claimed=False,
                    guest_id=guest_id,
                    user_id=user_id,
                    transferred_transactions=0,
                    is_plus=entitlement.is_plus,
                    expires_at=entitlement.expires_at,
                    changed=entitlement.changed,
                )

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, original_transaction_id
                    FROM apple_transactions
                    WHERE guest_id = %s
                    ORDER BY original_transaction_id, id
                    FOR UPDATE;
                    """,
                    (guest_id,),
                )
                guest_transactions = cur.fetchall()
                if not guest_transactions:
                    raise GuestSubscriptionNotFound("Guest has no Apple subscription")

                original_ids = sorted({str(row["original_transaction_id"]) for row in guest_transactions})
                for original_transaction_id in original_ids:
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0));",
                        (original_transaction_id,),
                    )
                    cur.execute(
                        """
                        SELECT user_id, guest_id
                        FROM apple_transactions
                        WHERE original_transaction_id = %s
                          AND NOT (
                              guest_id IS NOT DISTINCT FROM %s
                              OR (user_id = %s AND guest_id IS NULL)
                          )
                        LIMIT 1
                        FOR UPDATE;
                        """,
                        (original_transaction_id, guest_id, user_id),
                    )
                    if cur.fetchone() is not None:
                        raise GuestOwnershipConflict(
                            "Apple subscription belongs to another owner"
                        )

                cur.execute(
                    """
                    UPDATE apple_transactions
                    SET user_id = %s,
                        guest_id = NULL,
                        updated_at = NOW()
                    WHERE guest_id = %s;
                    """,
                    (user_id, guest_id),
                )
                transferred_transactions = cur.rowcount
                cur.execute(
                    """
                    UPDATE guest_identities
                    SET claimed_user_id = %s,
                        claimed_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                      AND claimed_user_id IS NULL;
                    """,
                    (user_id, guest_id),
                )
                if cur.rowcount != 1:
                    raise GuestAlreadyClaimed("Guest identity has already been claimed")
                cur.execute(
                    """
                    UPDATE guest_sessions
                    SET revoked_at = COALESCE(revoked_at, NOW()),
                        updated_at = NOW()
                    WHERE guest_id = %s;
                    """,
                    (guest_id,),
                )

            entitlement = apple_subscription_reconciler.reconcile_apple_entitlement(
                user_id,
                connection=conn,
            )
            return GuestClaimResult(
                claimed=True,
                guest_id=guest_id,
                user_id=user_id,
                transferred_transactions=transferred_transactions,
                is_plus=entitlement.is_plus,
                expires_at=entitlement.expires_at,
                changed=entitlement.changed,
            )
    finally:
        conn.close()
