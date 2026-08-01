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
from uuid import UUID
from unittest.mock import patch

import psycopg2
from psycopg2.extras import RealDictCursor


POSTGRES_PROCESS = None
POSTGRES_TMPDIR = None
main = None
db = None
apns_pushes: list[dict] = []


class FakeAPNsConfigurationError(Exception):
    pass


class FakeAPNsPushClient:
    client_reused = True
    jwt_reused = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        pass

    def send_push(self, *, device_token, title, body, environment=None, payload=None):
        apns_pushes.append(
            {
                "device_token": device_token,
                "title": title,
                "body": body,
                "environment": environment,
                "payload": payload or {},
            }
        )
        return {
            "success": True,
            "status_code": 200,
            "reason": "Success",
            "invalid_token": False,
            "temporary_error": False,
            "environment": environment or "sandbox",
            "attempts": 1,
        }


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

    apns_client.APNsConfigurationError = FakeAPNsConfigurationError
    apns_client.APNsPushClient = FakeAPNsPushClient
    apns_client.apns_is_configured = lambda: True
    sys.modules["app.apns_client"] = apns_client

    email_service = types.ModuleType("app.email_service")
    email_service.email_delivery_is_configured = lambda: True
    email_service.send_verification_email = lambda **_kwargs: SimpleNamespace(delivery="sent")
    sys.modules["app.email_service"] = email_service


def setUpModule() -> None:
    global POSTGRES_PROCESS, POSTGRES_TMPDIR, main, db

    POSTGRES_TMPDIR = tempfile.mkdtemp(prefix="fuelnear-e2e-tests-", dir="/private/tmp")
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
    os.environ["DB_POOL_MAX_CONNECTIONS"] = "20"
    os.environ["AUTH_REGISTER_RATE_LIMIT"] = "1000"
    os.environ["AUTH_REGISTER_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_LOGIN_RATE_LIMIT"] = "1000"
    os.environ["AUTH_LOGIN_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_VERIFY_RATE_LIMIT"] = "1000"
    os.environ["AUTH_VERIFY_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_RESEND_RATE_LIMIT"] = "1000"
    os.environ["AUTH_RESEND_RATE_WINDOW_SECONDS"] = "60"
    os.environ["EMAIL_VERIFICATION_MAX_ATTEMPTS"] = "3"
    os.environ["REFERRAL_ADMIN_TOKEN"] = "referral-admin-token"
    os.environ["REFERRAL_ADMIN_TOKEN_PREVIOUS"] = "referral-admin-token-previous"
    os.environ["MIMIT_ADMIN_TOKEN"] = "mimit-admin-token"
    os.environ["PRICE_NOTIFICATIONS_ADMIN_TOKEN"] = "price-admin-token"
    os.environ["ADMIN_UPDATE_TOKEN"] = "legacy-admin-token"
    os.environ["ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK"] = "false"
    os.environ["REFERRAL_MONTHLY_REWARD_LIMIT"] = "10"
    os.environ["REFERRAL_PROCESS_BATCH_SIZE"] = "100"
    os.environ["APPLE_CLIENT_ID"] = "app.fuelnear.ios"
    os.environ["GOOGLE_CLIENT_ID"] = "google-client"

    install_import_stubs()
    import app.main as imported_main
    import app.db as imported_db

    imported_db.close_connection_pool()
    imported_db.DATABASE_URL = os.environ["DATABASE_URL"]
    imported_db.DB_POOL_MIN_CONNECTIONS = int(os.environ["DB_POOL_MIN_CONNECTIONS"])
    imported_db.DB_POOL_MAX_CONNECTIONS = int(os.environ["DB_POOL_MAX_CONNECTIONS"])

    main = imported_main
    db = imported_db
    with main.get_connection() as conn:
        main.ensure_auth_schema(conn)
        ensure_core_price_schema(conn)


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


def ensure_core_price_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stations (
                id BIGSERIAL PRIMARY KEY,
                mimit_id BIGINT NOT NULL UNIQUE,
                name TEXT,
                brand TEXT,
                operator TEXT,
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                province TEXT NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fuel_prices (
                id BIGSERIAL PRIMARY KEY,
                station_id BIGINT NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
                fuel_type TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                is_self_service BOOLEAN NOT NULL,
                reported_at TIMESTAMPTZ NOT NULL,
                source TEXT NOT NULL DEFAULT 'mimit',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )


class FakeRequest:
    def __init__(self, ip: str = "127.0.0.1", path: str = "/"):
        self.headers = {}
        self.client = SimpleNamespace(host=ip)
        self.url = SimpleNamespace(path=path)


class ImmediateBackgroundTasks:
    def __init__(self):
        self.tasks = []

    def add_task(self, func, *args, **kwargs):
        self.tasks.append((func, args, kwargs))
        func(*args, **kwargs)


class DeferredBackgroundTasks:
    def __init__(self):
        self.tasks = []

    def add_task(self, func, *args, **kwargs):
        self.tasks.append((func, args, kwargs))

    def run(self):
        for func, args, kwargs in self.tasks:
            func(*args, **kwargs)


class FuelNearE2ETestCase(unittest.TestCase):
    password = "CorrectHorse123"
    scenario_count = 9

    def setUp(self) -> None:
        apns_pushes.clear()
        self.reset_database()
        main.REFERRAL_MONTHLY_REWARD_LIMIT = 10
        main.REFERRAL_PROCESS_BATCH_SIZE = 100
        main.AUTH_REGISTER_RATE_LIMIT = 1000
        main.AUTH_LOGIN_RATE_LIMIT = 1000
        main.AUTH_VERIFY_RATE_LIMIT = 1000
        main.AUTH_RESEND_RATE_LIMIT = 1000
        main.AUTH_VERIFY_RATE_WINDOW_SECONDS = 60
        main.EMAIL_VERIFICATION_MAX_ATTEMPTS = 3
        main.REFERRAL_ADMIN_TOKEN = "referral-admin-token"
        main.REFERRAL_ADMIN_TOKEN_PREVIOUS = "referral-admin-token-previous"
        main.MIMIT_ADMIN_TOKEN = "mimit-admin-token"
        main.PRICE_NOTIFICATIONS_ADMIN_TOKEN = "price-admin-token"
        main.ADMIN_UPDATE_TOKEN = "legacy-admin-token"
        main.ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK = False
        main.APPLE_CLIENT_ID = "app.fuelnear.ios"
        main.APNsConfigurationError = FakeAPNsConfigurationError
        main.APNsPushClient = FakeAPNsPushClient
        main.apns_is_configured = lambda: True
        main.send_verification_email = lambda **_kwargs: SimpleNamespace(delivery="sent")
        main.email_delivery_is_configured = lambda: True

    def reset_database(self) -> None:
        with main.get_connection() as conn:
            ensure_core_price_schema(conn)
            main.ensure_auth_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE
                        sent_price_notifications,
                        price_notification_preferences,
                        user_locations,
                        auth_rate_limits,
                        user_device_tokens,
                        user_auth_providers,
                        user_sessions,
                        email_verification_tokens,
                        rewards,
                        referrals,
                        user_subscriptions,
                        fuel_prices,
                        stations,
                        mimit_import_runs,
                        users
                    RESTART IDENTITY CASCADE;
                    """
                )

    def register_user(self, email: str, referral_code=None):
        return main.register_user(
            main.RegisterRequest(
                email=email,
                password=self.password,
                display_name=None,
                referral_code=referral_code,
            ),
            FakeRequest(ip=f"10.0.0.{len(email) % 250}", path="/auth/register"),
        )

    def latest_verification_code(self, email: str) -> str:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT evt.code_hash
                    FROM email_verification_tokens evt
                    JOIN users u ON u.id = evt.user_id
                    WHERE u.email = %s
                    ORDER BY evt.id DESC
                    LIMIT 1;
                    """,
                    (email,),
                )
                code_hash = cur.fetchone()[0]
        for candidate in range(1_000_000):
            code = f"{candidate:06d}"
            if main.hash_token(code) == code_hash:
                return code
        raise AssertionError("verification code not found")

    def verify_email(self, email: str):
        return main.verify_email(
            main.EmailVerificationRequest(email=email, code=self.latest_verification_code(email)),
            FakeRequest(path="/auth/verify-email"),
        )

    def login(self, email: str, password: str | None = None):
        return main.login_user(
            main.LoginRequest(email=email, password=password or self.password, device_info="ios-e2e"),
            FakeRequest(path="/auth/login"),
        )

    def signup_verified_login(self, email: str, referral_code=None) -> dict:
        self.register_user(email, referral_code=referral_code)
        self.verify_email(email)
        return self.login(email)

    def auth_header(self, session: dict) -> str:
        return f"Bearer {session['session']['access_token']}"

    def make_referral_mature(self, referred_email: str) -> None:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE referrals r
                    SET created_at = NOW() - INTERVAL '8 days',
                        updated_at = NOW() - INTERVAL '8 days'
                    FROM users u
                    WHERE r.referred_user_id = u.id
                      AND u.email = %s;
                    """,
                    (referred_email,),
                )

    def process_referrals(self) -> dict:
        return main.admin_process_referrals(None)["result"]

    def seed_prices_and_notification_user(self, authorization: str) -> None:
        with main.get_connection() as conn:
            ensure_core_price_schema(conn)
            main.ensure_user_device_tokens_schema(conn)
            main.ensure_user_locations_schema(conn)
            main.ensure_price_notification_preferences_schema(conn)
            main.ensure_sent_price_notifications_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stations (
                        mimit_id, name, brand, operator, address, city, province,
                        latitude, longitude, is_active
                    )
                    VALUES (1001, 'FuelNear Test Station', 'Q8', 'Q8', 'Via Test 1',
                            'Anzio', 'RM', 41.4959, 12.6190, TRUE)
                    RETURNING id;
                    """
                )
                station_id = cur.fetchone()[0]
                cur.execute(
                    """
                    INSERT INTO fuel_prices (
                        station_id, fuel_type, price, is_self_service, reported_at, source
                    )
                    VALUES (%s, 'benzina', 1.814, TRUE, NOW(), 'mimit');
                    """,
                    (station_id,),
                )
        main.upsert_current_user_device_token(
            main.DeviceTokenRequest(
                device_token="apns-e2e-device-token",
                platform="ios",
                environment="sandbox",
                app_version="1.0",
                device_info="iPhone",
            ),
            authorization,
        )
        main.upsert_current_user_location(
            main.UserLocationRequest(lat=41.4959, lng=12.6190, accuracy=10.0, source="ios"),
            authorization,
        )
        main.update_current_user_notification_preferences(
            main.NotificationPreferencesRequest(
                price_notifications_enabled=True,
                fuel_type="benzina",
                radius_km=3.0,
                favorites_only=False,
            ),
            authorization,
        )

    def fake_mimit_update(self, *_args, **_kwargs) -> dict:
        with main.get_connection() as conn:
            ensure_core_price_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stations (
                        mimit_id, name, brand, operator, address, city, province,
                        latitude, longitude, is_active
                    )
                    VALUES (2001, 'Cron Station', 'Q8', 'Q8', 'Via Cron 1',
                            'Anzio', 'RM', 41.4959, 12.6190, TRUE)
                    ON CONFLICT (mimit_id) DO UPDATE SET updated_at = NOW()
                    RETURNING id;
                    """
                )
                station_id = cur.fetchone()[0]
                cur.execute(
                    """
                    INSERT INTO fuel_prices (
                        station_id, fuel_type, price, is_self_service, reported_at, source
                    )
                    VALUES (%s, 'benzina', 1.804, TRUE, NOW(), 'mimit');
                    """,
                    (station_id,),
                )
        return {
            "import": {
                "stations_imported": 1,
                "prices_imported": 1,
                "stations_csv": 1,
                "prices_csv": 1,
            },
            "download": {
                "prezzi": {
                    "last_modified": datetime.now(timezone.utc).isoformat(),
                },
            },
        }

    def count_active_pool_connections(self) -> int:
        pool_obj = db.get_connection_pool()
        return len(getattr(pool_obj, "_used", {}))

    def assert_api_error(self, cm, error_code: str, status_code: int | None = None) -> None:
        exc = cm.exception
        self.assertEqual(getattr(exc, "error_code", None), error_code)
        if status_code is not None:
            self.assertEqual(exc.status_code, status_code)

    def test_01_new_user_full_account_lifecycle(self):
        email = "new-user@example.com"
        register_response = self.register_user(email)
        self.assertEqual(register_response["status"], "email_verification_required")
        self.assertEqual(register_response["email_verification"]["delivery"], "sent")

        self.verify_email(email)
        login_response = self.login(email)
        refresh = main.refresh_login_session(
            main.RefreshRequest(refresh_token=login_response["session"]["refresh_token"])
        )
        main.logout_user(f"Bearer {refresh['session']['access_token']}")

        login_again = self.login(email)
        profile = main.get_current_user_profile(self.auth_header(login_again))
        self.assertEqual(profile["user"]["email"], email)

        old_refresh_token = login_again["session"]["refresh_token"]
        old_access_header = self.auth_header(login_again)
        delete_response = main.delete_account(old_access_header)
        self.assertEqual(delete_response["status"], "ok")
        with self.assertRaises(main.APIError):
            main.get_current_user_profile(old_access_header)
        with self.assertRaises(main.APIError):
            main.refresh_login_session(main.RefreshRequest(refresh_token=old_refresh_token))

    def test_02_complete_referral_matures_plus_reward(self):
        user_a = self.signup_verified_login("a@example.com")
        referral_code = user_a["user"]["referral_code"]

        self.register_user("b@example.com")
        user_b = self.login_after_direct_verification("b@example.com")
        main.apply_current_user_referral_code(
            main.ApplyReferralCodeRequest(referral_code=referral_code),
            self.auth_header(user_b),
        )
        self.make_referral_mature("b@example.com")
        result = self.process_referrals()

        self.assertEqual(result["processed_count"], 1)
        self.assertEqual(self.fetch_value("SELECT status FROM referrals;"), "valid")
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards;"), 1)
        duration = self.fetch_value("SELECT expires_at - starts_at FROM user_subscriptions;")
        self.assertAlmostEqual(duration.total_seconds(), 7 * 24 * 3600, delta=5)

    def login_after_direct_verification(self, email: str) -> dict:
        self.verify_email(email)
        return self.login(email)

    def fetch_value(self, sql: str, params: tuple = ()):
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None

    def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        with main.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else None

    def test_03_two_consecutive_referrals_extend_single_plus(self):
        user_a = self.signup_verified_login("a@example.com")
        referral_code = user_a["user"]["referral_code"]

        for email in ["b@example.com", "c@example.com"]:
            self.register_user(email, referral_code=referral_code)
            self.verify_email(email)
            self.make_referral_mature(email)

        result = self.process_referrals()
        self.assertEqual(result["rewarded_count"], 2)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards;"), 2)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM referrals WHERE status = 'valid';"), 2)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions WHERE status = 'active';"), 1)
        duration = self.fetch_value("SELECT expires_at - starts_at FROM user_subscriptions WHERE status = 'active';")
        self.assertAlmostEqual(duration.total_seconds(), 14 * 24 * 3600, delta=5)

    def test_04_delete_invited_account_preserves_anonymous_referral_history(self):
        user_a = self.signup_verified_login("a@example.com")
        referral_code = user_a["user"]["referral_code"]
        user_b = self.signup_verified_login("b@example.com", referral_code=referral_code)
        self.make_referral_mature("b@example.com")
        self.process_referrals()

        main.delete_account(self.auth_header(user_b))
        referral = self.fetch_one("SELECT id, status, referred_user_id, referred_user_deleted FROM referrals;")
        self.assertEqual(referral["status"], "valid")
        self.assertIsNone(referral["referred_user_id"])
        self.assertTrue(referral["referred_user_deleted"])
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards WHERE referral_id = %s;", (referral["id"],)), 1)
        self.assertTrue(self.fetch_value("SELECT expires_at > NOW() FROM user_subscriptions;"))

        response = main.get_current_user_referrals(self.auth_header(user_a))
        self.assertNotIn("b@example.com", str(response))
        self.assertNotIn("referred_user_email", str(response))

    def test_05_cron_mimit_notifications_and_referrals_complete_without_leaks(self):
        user = self.signup_verified_login("notify@example.com")
        self.seed_prices_and_notification_user(self.auth_header(user))

        referrer = self.signup_verified_login("referrer@example.com")
        self.signup_verified_login("invitee@example.com", referral_code=referrer["user"]["referral_code"])
        self.make_referral_mature("invitee@example.com")

        original_update = main.update_mimit_data
        main.update_mimit_data = self.fake_mimit_update
        try:
            response = main.admin_update_mimit(ImmediateBackgroundTasks(), None)
        finally:
            main.update_mimit_data = original_update

        self.assertTrue(response["started"])
        status = main.get_mimit_status()
        self.assertEqual(status["update_state"], "idle")
        self.assertEqual(status["last_status"], "success")
        self.assertFalse(main.get_mimit_runtime_state()["update_in_progress"])
        self.assertGreaterEqual(len(apns_pushes), 1)

        price_response = main.admin_process_price_notifications(None)
        referral_response = main.admin_process_referrals(None)
        self.assertEqual(price_response["status"], "ok")
        self.assertEqual(referral_response["status"], "ok")
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM referrals WHERE status = 'valid';"), 1)
        self.assertLessEqual(self.count_active_pool_connections(), 1)

    def test_06_restart_lifecycle_reopens_pool_and_leaves_no_advisory_lock(self):
        main.on_startup()
        self.assertIsNotNone(db.get_connection_pool())
        main.on_shutdown()

        main.on_startup()
        with main.get_connection() as conn:
            acquired = main.try_acquire_mimit_update_lock(conn)
            self.assertTrue(acquired)
            main.release_mimit_update_lock(conn)
        self.assertFalse(main.get_mimit_runtime_state()["update_in_progress"])
        main.on_shutdown()
        main.on_startup()
        self.assertIsNotNone(db.get_connection_pool())

    def test_06b_mimit_advisory_lock_connection_stays_checked_out_until_background_finishes(self):
        background_tasks = DeferredBackgroundTasks()
        original_update = main.update_mimit_data
        main.update_mimit_data = self.fake_mimit_update
        try:
            response = main.admin_update_mimit(background_tasks, None)
            self.assertTrue(response["started"])
            self.assertEqual(len(background_tasks.tasks), 1)

            lock_connection = background_tasks.tasks[0][1][0]
            self.assertFalse(lock_connection._returned)

            other_connection = main.get_connection()
            try:
                self.assertIsNot(
                    other_connection._raw_connection,
                    lock_connection._raw_connection,
                )
            finally:
                other_connection.close()

            background_tasks.run()
            self.assertTrue(lock_connection._returned)
            self.assertFalse(main.get_mimit_runtime_state()["update_in_progress"])
        finally:
            if background_tasks.tasks:
                lock_connection = background_tasks.tasks[0][1][0]
                if not lock_connection._returned:
                    background_tasks.run()
            main.update_mimit_data = original_update

    def test_07_concurrent_realistic_operations_do_not_duplicate_or_exhaust_pool(self):
        user_a = self.signup_verified_login("a@example.com")
        referral_code = user_a["user"]["referral_code"]
        for index in range(4):
            self.signup_verified_login(f"invitee-{index}@example.com", referral_code=referral_code)
            self.make_referral_mature(f"invitee-{index}@example.com")

        sessions = [self.login("a@example.com")["session"] for _ in range(4)]

        def refresh_once(session):
            try:
                main.refresh_login_session(main.RefreshRequest(refresh_token=session["refresh_token"]))
                return "ok"
            except Exception as exc:
                return getattr(exc, "error_code", exc.__class__.__name__)

        with ThreadPoolExecutor(max_workers=10) as executor:
            refresh_results = list(executor.map(refresh_once, sessions))
            referral_results = list(executor.map(lambda _: self.process_referrals(), range(3)))

        self.assertEqual(refresh_results.count("ok"), 4)
        self.assertGreaterEqual(sum(result["processed_count"] for result in referral_results), 4)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM rewards;"), 4)
        self.assertEqual(self.fetch_value("SELECT COUNT(*) FROM user_subscriptions WHERE status = 'active';"), 1)
        self.assertLessEqual(self.count_active_pool_connections(), 1)

    def test_08_security_errors_are_structured_and_safe(self):
        with self.assertRaises(main.HTTPException) as wrong_admin:
            main.require_referral_admin_token("wrong")
        self.assertEqual(wrong_admin.exception.status_code, 403)
        main.require_referral_admin_token("referral-admin-token-previous")
        main.ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK = False
        with self.assertRaises(main.HTTPException) as legacy:
            main.require_referral_admin_token("legacy-admin-token")
        self.assertEqual(legacy.exception.status_code, 403)

        email = "security@example.com"
        self.register_user(email)
        main.AUTH_VERIFY_RATE_LIMIT = 1
        with self.assertRaises(main.APIError):
            main.verify_email(
                main.EmailVerificationRequest(email=email, code="999999"),
                FakeRequest(path="/auth/verify-email"),
            )
        with self.assertRaises(main.APIError) as limited:
            main.verify_email(
                main.EmailVerificationRequest(email=email, code="999998"),
                FakeRequest(path="/auth/verify-email"),
            )
        self.assert_api_error(limited, "RATE_LIMITED", 429)

        main.AUTH_VERIFY_RATE_LIMIT = 1000
        main.EMAIL_VERIFICATION_MAX_ATTEMPTS = 2
        self.reset_rate_limits()
        self.register_user("brute@example.com")
        with self.assertRaises(main.APIError):
            main.verify_email(main.EmailVerificationRequest(email="brute@example.com", code="999999"), FakeRequest(path="/auth/verify-email"))
        with self.assertRaises(main.APIError) as brute:
            main.verify_email(main.EmailVerificationRequest(email="brute@example.com", code="999998"), FakeRequest(path="/auth/verify-email"))
        self.assert_api_error(brute, "VERIFICATION_ATTEMPTS_EXCEEDED", 400)

        user = self.signup_verified_login("refresh@example.com")
        old_refresh = user["session"]["refresh_token"]
        main.refresh_login_session(main.RefreshRequest(refresh_token=old_refresh))
        with self.assertRaises(main.APIError) as reused:
            main.refresh_login_session(main.RefreshRequest(refresh_token=old_refresh))
        self.assert_api_error(reused, "REFRESH_TOKEN_REUSED", 401)

        original_verify = main.verify_jwt_with_jwks
        main.verify_jwt_with_jwks = lambda *_args, **_kwargs: {
            "sub": "apple-sub",
            "email": "apple@example.com",
            "email_verified": "true",
            "nonce": "wrong-nonce",
        }
        try:
            with self.assertRaises(main.APIError) as nonce_required:
                main.verify_apple_identity_token("fake-token", raw_nonce=None)
            self.assert_api_error(nonce_required, "APPLE_NONCE_REQUIRED", 400)

            with self.assertRaises(main.APIError) as nonce_invalid:
                main.verify_apple_identity_token("fake-token", raw_nonce="raw-nonce")
        finally:
            main.verify_jwt_with_jwks = original_verify
        self.assert_api_error(nonce_invalid, "APPLE_NONCE_INVALID", 401)

    def test_09_apple_verify_persists_and_subscription_get_returns_plus(self):
        user = self.signup_verified_login("apple-plus@example.com")
        authorization = self.auth_header(user)
        now = datetime.now(timezone.utc)
        verified = main.apple_jws_verifier.VerifiedAppleTransaction(
            product_id="MB.FuelNear.plus.monthly",
            transaction_id="e2e-apple-transaction",
            original_transaction_id="e2e-apple-original",
            purchase_date=now,
            expires_date=now + timedelta(days=30),
            environment="Sandbox",
            ownership_type="PURCHASED",
            transaction_reason="PURCHASE",
            revocation_date=None,
            revocation_reason=None,
            app_account_token=UUID(user["user"]["app_account_token"]),
            signed_date=now,
            storefront="ITA",
            offer_type=None,
        )

        with patch.object(
            main.apple_jws_verifier,
            "verify_apple_signed_transaction",
            return_value=verified,
        ):
            response = main.verify_current_user_apple_subscription(
                main.AppleSubscriptionVerifyRequest(
                    signed_transaction="signed-sandbox-transaction"
                ),
                authorization,
            )

        subscription = main.get_current_user_subscription(authorization)

        self.assertTrue(response.is_plus)
        self.assertTrue(subscription["is_plus"])
        self.assertEqual(subscription["active_subscription"]["source"], "apple_subscription")
        self.assertEqual(
            self.fetch_value(
                "SELECT environment FROM apple_transactions WHERE transaction_id = %s;",
                ("e2e-apple-transaction",),
            ),
            "Sandbox",
        )

    def reset_rate_limits(self) -> None:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE auth_rate_limits RESTART IDENTITY;")


if __name__ == "__main__":
    unittest.main(verbosity=2)
