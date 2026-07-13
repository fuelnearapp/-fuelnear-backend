import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor


POSTGRES_PROCESS = None
POSTGRES_TMPDIR = None
main = None


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_checked(command: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **kwargs,
    )


def wait_for_postgres(port: int) -> None:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=15)
    last_error = None
    while datetime.now(timezone.utc) < deadline:
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                host="127.0.0.1",
                port=port,
                connect_timeout=1,
            )
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.05)
    raise RuntimeError(f"PostgreSQL test server did not start: {last_error}")


def install_import_stubs() -> None:
    jwt = types.ModuleType("jwt")

    class PyJWTError(Exception):
        pass

    class PyJWKClient:
        def __init__(self, *args, **kwargs):
            pass

    jwt.PyJWTError = PyJWTError
    jwt.PyJWKClient = PyJWKClient
    jwt.decode = lambda *args, **kwargs: {}
    jwt.encode = lambda *args, **kwargs: "jwt"
    sys.modules["jwt"] = jwt

    apns_client = types.ModuleType("app.apns_client")

    class APNsConfigurationError(Exception):
        pass

    class APNsPushClient:
        client_reused = False
        jwt_reused = False

        def close(self):
            pass

    apns_client.APNsConfigurationError = APNsConfigurationError
    apns_client.APNsPushClient = APNsPushClient
    apns_client.apns_is_configured = lambda: False
    sys.modules["app.apns_client"] = apns_client

    email_service = types.ModuleType("app.email_service")
    email_service.email_delivery_is_configured = lambda: True
    email_service.send_verification_email = lambda **_kwargs: SimpleNamespace(delivery="sent")
    sys.modules["app.email_service"] = email_service


def setUpModule() -> None:
    global POSTGRES_PROCESS, POSTGRES_TMPDIR, main

    POSTGRES_TMPDIR = tempfile.mkdtemp(prefix="fuelnear-referral-tests-", dir="/private/tmp")
    data_dir = os.path.join(POSTGRES_TMPDIR, "data")
    run_checked(["/opt/homebrew/bin/initdb", "-A", "trust", "-U", "postgres", "-D", data_dir])

    port = find_free_port()
    POSTGRES_PROCESS = subprocess.Popen(
        [
            "/opt/homebrew/bin/postgres",
            "-D",
            data_dir,
            "-h",
            "127.0.0.1",
            "-p",
            str(port),
            "-k",
            POSTGRES_TMPDIR,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for_postgres(port)

    os.environ["DATABASE_URL"] = f"postgres://postgres@127.0.0.1:{port}/postgres"
    os.environ["DB_POOL_MIN_CONNECTIONS"] = "1"
    os.environ["DB_POOL_MAX_CONNECTIONS"] = "16"
    os.environ["AUTH_REGISTER_RATE_LIMIT"] = "1000"
    os.environ["AUTH_REGISTER_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_LOGIN_RATE_LIMIT"] = "1000"
    os.environ["AUTH_LOGIN_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_VERIFY_RATE_LIMIT"] = "1000"
    os.environ["AUTH_VERIFY_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_RESEND_RATE_LIMIT"] = "1000"
    os.environ["AUTH_RESEND_RATE_WINDOW_SECONDS"] = "60"
    os.environ["REFERRAL_ADMIN_TOKEN"] = "referral-admin-token"
    os.environ["REFERRAL_ADMIN_TOKEN_PREVIOUS"] = "referral-admin-token-previous"
    os.environ["ADMIN_UPDATE_TOKEN"] = "legacy-admin-token"
    os.environ["ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK"] = "false"
    os.environ["REFERRAL_MONTHLY_REWARD_LIMIT"] = "10"
    os.environ["REFERRAL_PROCESS_BATCH_SIZE"] = "100"

    install_import_stubs()
    import app.main as imported_main
    import app.db as imported_db

    imported_db.close_connection_pool()
    imported_db.DATABASE_URL = os.environ["DATABASE_URL"]
    imported_db.DB_POOL_MIN_CONNECTIONS = int(os.environ["DB_POOL_MIN_CONNECTIONS"])
    imported_db.DB_POOL_MAX_CONNECTIONS = int(os.environ["DB_POOL_MAX_CONNECTIONS"])

    main = imported_main
    with main.get_connection() as conn:
        main.ensure_auth_schema(conn)


def tearDownModule() -> None:
    global POSTGRES_PROCESS, POSTGRES_TMPDIR
    if main is not None:
        main.close_connection_pool()
    if POSTGRES_PROCESS is not None:
        POSTGRES_PROCESS.terminate()
        try:
            POSTGRES_PROCESS.wait(timeout=5)
        except subprocess.TimeoutExpired:
            POSTGRES_PROCESS.kill()
            POSTGRES_PROCESS.wait(timeout=5)
    if POSTGRES_TMPDIR:
        shutil.rmtree(POSTGRES_TMPDIR, ignore_errors=True)


class FakeRequest:
    def __init__(self, ip: str = "127.0.0.1", path: str = "/"):
        self.headers = {}
        self.client = SimpleNamespace(host=ip)
        self.url = SimpleNamespace(path=path)


class ReferralRewardsPlusTestCase(unittest.TestCase):
    password = "CorrectHorse123"

    def setUp(self) -> None:
        self.reset_database()
        main.REFERRAL_MONTHLY_REWARD_LIMIT = 10
        main.REFERRAL_PROCESS_BATCH_SIZE = 100
        main.REFERRAL_ADMIN_TOKEN = "referral-admin-token"
        main.REFERRAL_ADMIN_TOKEN_PREVIOUS = "referral-admin-token-previous"
        main.ADMIN_UPDATE_TOKEN = "legacy-admin-token"
        main.ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK = False
        main.AUTH_REGISTER_RATE_LIMIT = 1000
        main.AUTH_LOGIN_RATE_LIMIT = 1000
        main.AUTH_VERIFY_RATE_LIMIT = 1000
        main.AUTH_RESEND_RATE_LIMIT = 1000
        main.send_verification_email = lambda **_kwargs: SimpleNamespace(delivery="sent")
        main.email_delivery_is_configured = lambda: True

    def reset_database(self) -> None:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE
                        auth_rate_limits,
                        user_device_tokens,
                        user_auth_providers,
                        user_sessions,
                        email_verification_tokens,
                        rewards,
                        referrals,
                        user_subscriptions,
                        users
                    RESTART IDENTITY CASCADE;
                    """
                )

    def create_user(
        self,
        email: str,
        *,
        verified: bool = True,
        active: bool = True,
        created_at: datetime | None = None,
    ) -> dict:
        created_at = created_at or datetime.now(timezone.utc)
        with main.get_connection() as conn:
            referral_code = main.generate_unique_referral_code(conn)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO users (
                        email,
                        password_hash,
                        display_name,
                        referral_code,
                        is_email_verified,
                        is_active,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *;
                    """,
                    (
                        email,
                        main.hash_password(self.password),
                        email.split("@", 1)[0],
                        referral_code,
                        verified,
                        active,
                        created_at,
                        created_at,
                    ),
                )
                return dict(cur.fetchone())

    def register(self, email: str, referral_code=None):
        return main.register_user(
            main.RegisterRequest(
                email=email,
                password=self.password,
                display_name=None,
                referral_code=referral_code,
            ),
            FakeRequest(path="/auth/register", ip=f"10.10.0.{len(email) % 250}"),
        )

    def bearer_for_user(self, user_id: int) -> str:
        with main.get_connection() as conn:
            session = main.create_user_session(conn, user_id=user_id)
        return f"Bearer {session['access_token']}"

    def make_referral(
        self,
        referrer_id: int,
        referred_id: int,
        *,
        days_old: int = 8,
        status: str = "pending",
        status_reason: str | None = "awaiting_eligibility",
    ) -> int:
        created_at = datetime.now(timezone.utc) - timedelta(days=days_old)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users
                    SET referred_by_user_id = %s
                    WHERE id = %s;
                    """,
                    (referrer_id, referred_id),
                )
                cur.execute(
                    """
                    INSERT INTO referrals (
                        referrer_user_id,
                        referred_user_id,
                        referral_code_used,
                        status,
                        status_reason,
                        validated_at,
                        created_at,
                        updated_at
                    )
                    SELECT %s, %s, referral_code, %s, %s,
                           CASE WHEN %s = 'valid' THEN NOW() ELSE NULL END,
                           %s, %s
                    FROM users
                    WHERE id = %s
                    RETURNING id;
                    """,
                    (
                        referrer_id,
                        referred_id,
                        status,
                        status_reason,
                        status,
                        created_at,
                        created_at,
                        referrer_id,
                    ),
                )
                return int(cur.fetchone()[0])

    def process(self, *, min_age_days: int = 7, reward_days: int = 7) -> dict:
        conn = main.get_connection()
        try:
            return main.process_pending_referrals(conn, min_age_days=min_age_days, reward_days=reward_days)
        finally:
            conn.close()

    def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        with main.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else None

    def fetch_value(self, sql: str, params: tuple = ()):
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None

    def assert_api_error(self, cm, error_code: str, status_code: int | None = None) -> None:
        exc = cm.exception
        self.assertEqual(getattr(exc, "error_code", None), error_code)
        if status_code is not None:
            self.assertEqual(exc.status_code, status_code)

    def create_mature_referral_pair(self, referrer: dict | None = None, suffix: str = "x") -> tuple[dict, dict, int]:
        referrer = referrer or self.create_user(f"referrer-{suffix}@example.com")
        referred = self.create_user(f"referred-{suffix}@example.com")
        referral_id = self.make_referral(referrer["id"], referred["id"])
        return referrer, referred, referral_id

    def test_01_registration_without_referral_code(self):
        response = self.register("plain@example.com")
        self.assertIsNone(response["user"]["referred_by_user_id"])
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM referrals;"), 0)

    def test_02_registration_with_valid_referral_code(self):
        referrer = self.create_user("referrer@example.com")
        response = self.register("invitee@example.com", referral_code=referrer["referral_code"])
        self.assertEqual(response["user"]["referred_by_user_id"], referrer["id"])
        referral = self.fetch_one("SELECT * FROM referrals WHERE referred_user_id = %s;", (response["user"]["id"],))
        self.assertEqual(referral["status"], "pending")

    def test_03_missing_referral_code_is_invalid(self):
        with self.assertRaises(main.APIError) as cm:
            self.register("invitee@example.com", referral_code="NOPE999")
        self.assert_api_error(cm, "REFERRAL_CODE_INVALID", 400)

    def test_04_self_referral_rejected(self):
        user = self.create_user("self@example.com")
        with self.assertRaises(main.APIError) as cm:
            main.apply_current_user_referral_code(
                main.ApplyReferralCodeRequest(referral_code=user["referral_code"]),
                self.bearer_for_user(user["id"]),
            )
        self.assert_api_error(cm, "REFERRAL_SELF_NOT_ALLOWED", 400)

    def test_05_one_referral_per_invitee(self):
        first = self.create_user("first@example.com")
        second = self.create_user("second@example.com")
        invitee = self.create_user("invitee@example.com")
        bearer = self.bearer_for_user(invitee["id"])
        main.apply_current_user_referral_code(main.ApplyReferralCodeRequest(referral_code=first["referral_code"]), bearer)
        with self.assertRaises(main.APIError) as cm:
            main.apply_current_user_referral_code(main.ApplyReferralCodeRequest(referral_code=second["referral_code"]), bearer)
        self.assert_api_error(cm, "REFERRAL_CODE_ALREADY_USED", 409)

    def test_06_referral_code_trim_uppercase_normalized(self):
        referrer = self.create_user("referrer@example.com")
        response = self.register("invitee@example.com", referral_code=f"  {referrer['referral_code'].lower()}  ")
        referral = self.fetch_one("SELECT referral_code_used FROM referrals WHERE referred_user_id = %s;", (response["user"]["id"],))
        self.assertEqual(referral["referral_code_used"], referrer["referral_code"])

    def test_07_post_registration_code_within_24_hours(self):
        referrer = self.create_user("referrer@example.com")
        invitee = self.create_user("invitee@example.com")
        response = main.apply_current_user_referral_code(
            main.ApplyReferralCodeRequest(referral_code=referrer["referral_code"]),
            self.bearer_for_user(invitee["id"]),
        )
        self.assertEqual(response["status"], "ok")

    def test_08_post_registration_code_after_24_hours_expires(self):
        referrer = self.create_user("referrer@example.com")
        invitee = self.create_user(
            "invitee@example.com",
            created_at=datetime.now(timezone.utc) - timedelta(hours=25),
        )
        with self.assertRaises(main.APIError) as cm:
            main.apply_current_user_referral_code(
                main.ApplyReferralCodeRequest(referral_code=referrer["referral_code"]),
                self.bearer_for_user(invitee["id"]),
            )
        self.assert_api_error(cm, "REFERRAL_WINDOW_EXPIRED", 400)

    def test_09_referral_code_already_used(self):
        referrer = self.create_user("referrer@example.com")
        invitee = self.create_user("invitee@example.com")
        bearer = self.bearer_for_user(invitee["id"])
        main.apply_current_user_referral_code(main.ApplyReferralCodeRequest(referral_code=referrer["referral_code"]), bearer)
        with self.assertRaises(main.APIError) as cm:
            main.apply_current_user_referral_code(main.ApplyReferralCodeRequest(referral_code=referrer["referral_code"]), bearer)
        self.assert_api_error(cm, "REFERRAL_CODE_ALREADY_USED", 409)

    def test_10_unverified_invitee_stays_pending(self):
        referrer = self.create_user("referrer@example.com")
        referred = self.create_user("referred@example.com", verified=False)
        referral_id = self.make_referral(referrer["id"], referred["id"])
        result = self.process()
        self.assertEqual(result["processed_count"], 0)
        self.assertEqual(self.fetch_one("SELECT status FROM referrals WHERE id = %s;", (referral_id,))["status"], "pending")

    def test_11_inactive_invitee_does_not_mature(self):
        referrer = self.create_user("referrer@example.com")
        referred = self.create_user("referred@example.com", active=False)
        referral_id = self.make_referral(referrer["id"], referred["id"])
        result = self.process()
        self.assertEqual(result["processed_count"], 0)
        self.assertEqual(self.fetch_one("SELECT status FROM referrals WHERE id = %s;", (referral_id,))["status"], "pending")

    def test_12_unverified_referrer_does_not_receive_reward(self):
        referrer = self.create_user("referrer@example.com", verified=False)
        referred = self.create_user("referred@example.com")
        referral_id = self.make_referral(referrer["id"], referred["id"])
        result = self.process()
        self.assertEqual(result["skipped_referrer_not_verified"], 1)
        referral = self.fetch_one("SELECT status, status_reason FROM referrals WHERE id = %s;", (referral_id,))
        self.assertEqual(referral["status"], "pending")
        self.assertEqual(referral["status_reason"], "referrer_not_verified")

    def test_13_inactive_referrer_does_not_receive_reward(self):
        referrer = self.create_user("referrer@example.com", active=False)
        referred = self.create_user("referred@example.com")
        referral_id = self.make_referral(referrer["id"], referred["id"])
        result = self.process()
        self.assertEqual(result["skipped_referrer_not_verified"], 1)
        self.assertEqual(self.fetch_one("SELECT status_reason FROM referrals WHERE id = %s;", (referral_id,))["status_reason"], "referrer_not_verified")

    def test_14_not_mature_referral_stays_pending(self):
        _, _, referral_id = self.create_mature_referral_pair(suffix="fresh")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE referrals SET created_at = NOW() - INTERVAL '1 day' WHERE id = %s;", (referral_id,))
        result = self.process()
        self.assertEqual(result["processed_count"], 0)
        self.assertEqual(self.fetch_one("SELECT status FROM referrals WHERE id = %s;", (referral_id,))["status"], "pending")

    def test_15_mature_referral_becomes_valid(self):
        _, _, referral_id = self.create_mature_referral_pair()
        result = self.process()
        self.assertEqual(result["processed_count"], 1)
        self.assertEqual(self.fetch_one("SELECT status FROM referrals WHERE id = %s;", (referral_id,))["status"], "valid")

    def test_16_valid_referral_grants_exactly_7_plus_days(self):
        referrer, _, _ = self.create_mature_referral_pair()
        self.process()
        subscription = self.fetch_one("SELECT starts_at, expires_at FROM user_subscriptions WHERE user_id = %s;", (referrer["id"],))
        self.assertAlmostEqual((subscription["expires_at"] - subscription["starts_at"]).total_seconds(), 7 * 24 * 3600, delta=5)

    def test_17_reward_contains_referral_id(self):
        referrer, _, referral_id = self.create_mature_referral_pair()
        self.process()
        reward = self.fetch_one("SELECT referral_id FROM rewards WHERE user_id = %s;", (referrer["id"],))
        self.assertEqual(reward["referral_id"], referral_id)

    def test_18_unique_constraint_prevents_double_reward_for_referral(self):
        referrer, _, referral_id = self.create_mature_referral_pair()
        with main.get_connection() as conn:
            first = main.grant_plus_days_reward(conn, referrer["id"], referral_id, 7)
            second = main.grant_plus_days_reward(conn, referrer["id"], referral_id, 7)
        self.assertFalse(first["already_granted"])
        self.assertTrue(second["already_granted"])
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards WHERE referral_id = %s;", (referral_id,)), 1)

    def test_19_processing_retry_does_not_duplicate_reward(self):
        referrer, _, _ = self.create_mature_referral_pair()
        self.process()
        self.process()
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards WHERE user_id = %s;", (referrer["id"],)), 1)

    def test_20_processing_retry_does_not_extend_plus_again(self):
        referrer, _, _ = self.create_mature_referral_pair()
        self.process()
        first_expiry = self.fetch_value("SELECT expires_at FROM user_subscriptions WHERE user_id = %s;", (referrer["id"],))
        self.process()
        second_expiry = self.fetch_value("SELECT expires_at FROM user_subscriptions WHERE user_id = %s;", (referrer["id"],))
        self.assertEqual(first_expiry, second_expiry)

    def test_21_first_reward_creates_active_subscription(self):
        referrer, _, _ = self.create_mature_referral_pair()
        self.process()
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (referrer["id"],)), 1)

    def test_22_next_reward_extends_same_subscription(self):
        referrer = self.create_user("referrer@example.com")
        self.create_mature_referral_pair(referrer, "a")
        self.process()
        subscription_id = self.fetch_value("SELECT id FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (referrer["id"],))
        self.create_mature_referral_pair(referrer, "b")
        self.process()
        row = self.fetch_one("SELECT id, expires_at - starts_at AS duration FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (referrer["id"],))
        self.assertEqual(row["id"], subscription_id)
        self.assertAlmostEqual(row["duration"].total_seconds(), 14 * 24 * 3600, delta=5)

    def test_23_no_two_active_subscriptions_for_user(self):
        referrer = self.create_user("referrer@example.com")
        self.create_mature_referral_pair(referrer, "a")
        self.create_mature_referral_pair(referrer, "b")
        self.process()
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (referrer["id"],)), 1)

    def test_24_expired_subscription_is_normalized(self):
        user = self.create_user("user@example.com")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_subscriptions (user_id, source, status, starts_at, expires_at)
                    VALUES (%s, 'referral_reward', 'active', NOW() - INTERVAL '10 days', NOW() - INTERVAL '1 day');
                    """,
                    (user["id"],),
                )
            main.ensure_auth_schema(conn)
        self.assertEqual(self.fetch_value("SELECT status FROM user_subscriptions WHERE user_id = %s;", (user["id"],)), "expired")

    def test_25_legacy_duplicate_active_subscriptions_are_consolidated(self):
        user = self.create_user("user@example.com")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP INDEX IF EXISTS idx_user_subscriptions_one_active;")
                cur.execute(
                    """
                    INSERT INTO user_subscriptions (user_id, source, status, starts_at, expires_at)
                    VALUES
                        (%s, 'referral_reward', 'active', NOW(), NOW() + INTERVAL '3 days'),
                        (%s, 'referral_reward', 'active', NOW(), NOW() + INTERVAL '9 days');
                    """,
                    (user["id"], user["id"]),
                )
            main.ensure_auth_schema(conn)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (user["id"],)), 1)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s AND status = 'superseded';", (user["id"],)), 1)

    def test_26_concurrent_referrals_do_not_lose_plus_days(self):
        referrer = self.create_user("referrer@example.com")
        referral_ids = [self.create_mature_referral_pair(referrer, str(i))[2] for i in range(2)]

        def process_one(referral_id):
            conn = main.get_connection()
            try:
                with conn:
                    referral = self.fetch_one("SELECT * FROM referrals WHERE id = %s;", (referral_id,))
                    return main.process_pending_referral(conn, referral, 7)
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(process_one, referral_ids))

        duration = self.fetch_value("SELECT expires_at - starts_at FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (referrer["id"],))
        self.assertAlmostEqual(duration.total_seconds(), 14 * 24 * 3600, delta=5)

    def test_27_concurrent_processing_does_not_create_two_active_subscriptions(self):
        referrer = self.create_user("referrer@example.com")
        for index in range(4):
            self.create_mature_referral_pair(referrer, str(index))

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(lambda _: self.process(), range(2)))

        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (referrer["id"],)), 1)

    def test_28_final_expiry_matches_sum_of_rewarded_days(self):
        referrer = self.create_user("referrer@example.com")
        for index in range(3):
            self.create_mature_referral_pair(referrer, str(index))
        self.process()
        duration = self.fetch_value("SELECT expires_at - starts_at FROM user_subscriptions WHERE user_id = %s AND status = 'active';", (referrer["id"],))
        self.assertAlmostEqual(duration.total_seconds(), 21 * 24 * 3600, delta=5)

    def test_29_default_monthly_limit_is_respected(self):
        referrer = self.create_user("referrer@example.com")
        for index in range(11):
            self.create_mature_referral_pair(referrer, str(index))
        result = self.process()
        self.assertEqual(result["rewarded_count"], 10)
        self.assertEqual(result["skipped_monthly_limit"], 1)

    def test_30_configurable_monthly_limit_is_respected(self):
        main.REFERRAL_MONTHLY_REWARD_LIMIT = 2
        referrer = self.create_user("referrer@example.com")
        for index in range(3):
            self.create_mature_referral_pair(referrer, str(index))
        result = self.process()
        self.assertEqual(result["rewarded_count"], 2)
        self.assertEqual(result["skipped_monthly_limit"], 1)

    def test_31_referral_over_limit_stays_pending(self):
        main.REFERRAL_MONTHLY_REWARD_LIMIT = 1
        referrer = self.create_user("referrer@example.com")
        ids = [self.create_mature_referral_pair(referrer, str(index))[2] for index in range(2)]
        self.process()
        statuses = self.fetch_value("SELECT COUNT(*) FROM referrals WHERE id = ANY(%s) AND status = 'pending';", (ids,))
        self.assertEqual(statuses, 1)

    def test_32_status_reason_for_monthly_limit(self):
        main.REFERRAL_MONTHLY_REWARD_LIMIT = 1
        referrer = self.create_user("referrer@example.com")
        for index in range(2):
            self.create_mature_referral_pair(referrer, str(index))
        self.process()
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM referrals WHERE status_reason = 'monthly_limit';"), 1)

    def test_33_italian_calendar_month_count_ignores_previous_month(self):
        main.REFERRAL_MONTHLY_REWARD_LIMIT = 1
        referrer = self.create_user("referrer@example.com")
        previous_month_in_rome = (
            datetime.now(ZoneInfo("Europe/Rome")).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            - timedelta(seconds=1)
        )
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rewards (user_id, reward_type, reward_value, status, granted_at)
                    VALUES (%s, 'plus_days', '7', 'granted', %s);
                    """,
                    (referrer["id"], previous_month_in_rome),
                )
        self.create_mature_referral_pair(referrer, "current")
        result = self.process()
        self.assertEqual(result["rewarded_count"], 1)

    def test_34_concurrent_monthly_limit_is_not_exceeded(self):
        main.REFERRAL_MONTHLY_REWARD_LIMIT = 2
        referrer = self.create_user("referrer@example.com")
        for index in range(5):
            self.create_mature_referral_pair(referrer, str(index))
        with ThreadPoolExecutor(max_workers=3) as executor:
            list(executor.map(lambda _: self.process(), range(3)))
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards WHERE user_id = %s;", (referrer["id"],)), 2)

    def test_35_only_pending_valid_invalid_statuses_are_allowed(self):
        referrer = self.create_user("referrer@example.com")
        referred = self.create_user("referred@example.com")
        with self.assertRaises(psycopg2.errors.CheckViolation):
            with main.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO referrals (referrer_user_id, referred_user_id, referral_code_used, status)
                        VALUES (%s, %s, %s, 'rejected');
                        """,
                        (referrer["id"], referred["id"], referrer["referral_code"]),
                    )

    def test_36_legacy_invalid_status_is_normalized(self):
        referrer = self.create_user("referrer@example.com")
        referred = self.create_user("referred@example.com")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE referrals DROP CONSTRAINT IF EXISTS referrals_status_check;")
                cur.execute(
                    """
                    INSERT INTO referrals (referrer_user_id, referred_user_id, referral_code_used, status)
                    VALUES (%s, %s, %s, 'rejected');
                    """,
                    (referrer["id"], referred["id"], referrer["referral_code"]),
                )
            main.ensure_auth_schema(conn)
        row = self.fetch_one("SELECT status, status_reason FROM referrals;")
        self.assertEqual(row["status"], "invalid")
        self.assertEqual(row["status_reason"], "legacy_rejected")

    def test_37_referral_summary_counts_statuses(self):
        referrer = self.create_user("referrer@example.com")
        referred_a = self.create_user("a@example.com")
        referred_b = self.create_user("b@example.com")
        referred_c = self.create_user("c@example.com")
        self.make_referral(referrer["id"], referred_a["id"], status="pending")
        self.make_referral(referrer["id"], referred_b["id"], status="valid", status_reason="reward_granted")
        self.make_referral(referrer["id"], referred_c["id"], status="invalid", status_reason="legacy_invalid_status")
        response = main.get_current_user_referrals(self.bearer_for_user(referrer["id"]))
        self.assertEqual(response["summary"]["pending_count"], 1)
        self.assertEqual(response["summary"]["valid_count"], 1)
        self.assertEqual(response["summary"]["invalid_count"], 1)

    def test_38_status_reason_is_optional_and_non_pii(self):
        referrer = self.create_user("referrer@example.com")
        referred = self.create_user("referred@example.com")
        referral_id = self.make_referral(referrer["id"], referred["id"], status_reason=None)
        row = self.fetch_one("SELECT status_reason FROM referrals WHERE id = %s;", (referral_id,))
        self.assertIsNone(row["status_reason"])
        response = main.get_current_user_referrals(self.bearer_for_user(referrer["id"]))
        self.assertNotIn("referred@example.com", str(response))

    def test_39_valid_referred_user_delete_preserves_anonymous_history(self):
        referrer, referred, referral_id = self.create_mature_referral_pair()
        self.process()
        main.delete_account(self.bearer_for_user(referred["id"]))
        referral = self.fetch_one("SELECT status, referred_user_id, referred_user_deleted FROM referrals WHERE id = %s;", (referral_id,))
        self.assertEqual(referral["status"], "valid")
        self.assertIsNone(referral["referred_user_id"])
        self.assertTrue(referral["referred_user_deleted"])
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards WHERE referral_id = %s;", (referral_id,)), 1)

    def test_40_pending_referred_user_delete_removes_pending_referral(self):
        referrer = self.create_user("referrer@example.com")
        referred = self.create_user("referred@example.com")
        referral_id = self.make_referral(referrer["id"], referred["id"], days_old=1)
        main.delete_account(self.bearer_for_user(referred["id"]))
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM referrals WHERE id = %s;", (referral_id,)), 0)

    def test_41_referrer_delete_cascades_rewards_and_subscription(self):
        referrer, _, _ = self.create_mature_referral_pair()
        self.process()
        main.delete_account(self.bearer_for_user(referrer["id"]))
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards;"), 0)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions;"), 0)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM referrals;"), 0)

    def test_42_user_referrals_does_not_return_referred_user_pii(self):
        referrer = self.create_user("referrer@example.com")
        referred = self.create_user("secret-invitee@example.com")
        self.make_referral(referrer["id"], referred["id"])
        response = main.get_current_user_referrals(self.bearer_for_user(referrer["id"]))
        self.assertNotIn("referred_user_email", response["items"][0])
        self.assertNotIn("secret-invitee@example.com", str(response))

    def test_43_multiple_batches_complete_all_work(self):
        main.REFERRAL_PROCESS_BATCH_SIZE = 2
        referrer = self.create_user("referrer@example.com")
        for index in range(5):
            self.create_mature_referral_pair(referrer, str(index))
        result = self.process()
        self.assertEqual(result["processed_count"], 5)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM referrals WHERE status = 'valid';"), 5)

    def test_44_batch_size_is_respected(self):
        main.REFERRAL_PROCESS_BATCH_SIZE = 2
        referrer = self.create_user("referrer@example.com")
        for index in range(5):
            self.create_mature_referral_pair(referrer, str(index))
        result = self.process()
        self.assertEqual(result["batch_count"], 3)

    def test_45_one_referral_error_does_not_rollback_others(self):
        referrer = self.create_user("referrer@example.com")
        referral_ids = [self.create_mature_referral_pair(referrer, str(index))[2] for index in range(3)]
        original = main.grant_plus_days_reward

        def flaky(conn, user_id, referral_id, days):
            if referral_id == referral_ids[1]:
                raise RuntimeError("simulated reward failure")
            return original(conn, user_id, referral_id, days)

        main.grant_plus_days_reward = flaky
        try:
            result = self.process()
        finally:
            main.grant_plus_days_reward = original
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["processed_count"], 2)

    def test_46_savepoint_keeps_failed_referral_pending(self):
        referrer = self.create_user("referrer@example.com")
        referral_ids = [self.create_mature_referral_pair(referrer, str(index))[2] for index in range(2)]
        original = main.grant_plus_days_reward

        def flaky(conn, user_id, referral_id, days):
            if referral_id == referral_ids[0]:
                raise RuntimeError("simulated reward failure")
            return original(conn, user_id, referral_id, days)

        main.grant_plus_days_reward = flaky
        try:
            self.process()
        finally:
            main.grant_plus_days_reward = original
        self.assertEqual(self.fetch_one("SELECT status FROM referrals WHERE id = %s;", (referral_ids[0],))["status"], "pending")
        self.assertEqual(self.fetch_one("SELECT status FROM referrals WHERE id = %s;", (referral_ids[1],))["status"], "valid")

    def test_47_skip_locked_avoids_duplicate_concurrent_processing(self):
        referrer = self.create_user("referrer@example.com")
        for index in range(6):
            self.create_mature_referral_pair(referrer, str(index))
        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(lambda _: self.process(), range(2)))
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards;"), 6)
        self.assertEqual(self.fetch_value("SELECT COUNT(DISTINCT referral_id) FROM rewards;"), 6)

    def test_48_processing_metrics_are_coherent(self):
        main.REFERRAL_PROCESS_BATCH_SIZE = 2
        referrer = self.create_user("referrer@example.com")
        for index in range(3):
            self.create_mature_referral_pair(referrer, str(index))
        result = self.process()
        self.assertEqual(result["scanned_count"], 3)
        self.assertEqual(result["processed_count"], 3)
        self.assertEqual(result["rewarded_count"], 3)
        self.assertEqual(result["skipped_count"], 0)
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(result["batch_count"], 2)

    def test_49_admin_process_referrals_with_correct_token(self):
        self.create_mature_referral_pair()
        main.require_referral_admin_token("referral-admin-token")
        response = main.admin_process_referrals(None)
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["result"]["processed_count"], 1)

    def test_50_admin_wrong_token_forbidden(self):
        with self.assertRaises(main.HTTPException) as cm:
            main.require_referral_admin_token("wrong-token")
        self.assertEqual(cm.exception.status_code, 403)

    def test_51_admin_previous_token_during_rotation_works(self):
        main.require_referral_admin_token("referral-admin-token-previous")

    def test_52_legacy_fallback_disabled_rejects_old_token(self):
        main.ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK = False
        with self.assertRaises(main.HTTPException) as cm:
            main.require_referral_admin_token("legacy-admin-token")
        self.assertEqual(cm.exception.status_code, 403)

    def test_53_admin_response_does_not_expose_raw_db_errors(self):
        original = main.process_pending_referrals

        def fail(_conn, min_age_days=7, reward_days=7):
            raise psycopg2.OperationalError("raw db password=secret SELECT * FROM users")

        main.process_pending_referrals = fail
        try:
            with self.assertRaises(main.HTTPException) as cm:
                main.admin_process_referrals(None)
        finally:
            main.process_pending_referrals = original
        self.assertEqual(cm.exception.status_code, 500)
        self.assertEqual(cm.exception.detail, "Referral processing failed")
        self.assertNotIn("SELECT", cm.exception.detail)


if __name__ == "__main__":
    unittest.main(verbosity=2)
