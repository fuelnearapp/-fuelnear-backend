import asyncio
import base64
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import psycopg2

from app.auth_utils import PBKDF2_ALGORITHM


POSTGRES_PROCESS = None
POSTGRES_TMPDIR = None
main = None
db = None


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
    global POSTGRES_PROCESS, POSTGRES_TMPDIR, main, db

    POSTGRES_TMPDIR = tempfile.mkdtemp(prefix="fuelnear-auth-tests-", dir="/private/tmp")
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
    os.environ["DB_POOL_MAX_CONNECTIONS"] = "12"
    os.environ["EMAIL_VERIFICATION_MAX_ATTEMPTS"] = "3"
    os.environ["AUTH_LOGIN_RATE_LIMIT"] = "3"
    os.environ["AUTH_LOGIN_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_RESEND_RATE_LIMIT"] = "2"
    os.environ["AUTH_RESEND_RATE_WINDOW_SECONDS"] = "60"
    os.environ["AUTH_VERIFY_RATE_LIMIT"] = "3"
    os.environ["AUTH_VERIFY_RATE_WINDOW_SECONDS"] = "60"

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
        main.ensure_auth_provider_schema(conn)
        main.ensure_user_device_tokens_schema(conn)


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
    def __init__(self, ip: str = "127.0.0.1", path: str = "/", headers=None):
        self.headers = headers or {}
        self.client = SimpleNamespace(host=ip)
        self.url = SimpleNamespace(path=path)


class AuthTestCase(unittest.TestCase):
    password = "CorrectHorse123"

    def setUp(self) -> None:
        self.reset_database()
        main.EMAIL_VERIFICATION_MAX_ATTEMPTS = 3
        main.AUTH_LOGIN_RATE_LIMIT = 3
        main.AUTH_LOGIN_RATE_WINDOW_SECONDS = 60
        main.AUTH_RESEND_RATE_LIMIT = 2
        main.AUTH_RESEND_RATE_WINDOW_SECONDS = 60
        main.AUTH_VERIFY_RATE_LIMIT = 3
        main.AUTH_VERIFY_RATE_WINDOW_SECONDS = 60
        main.send_verification_email = lambda **_kwargs: SimpleNamespace(delivery="sent")
        main.email_delivery_is_configured = lambda: True

    def reset_database(self) -> None:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE
                        apple_token_revocations,
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

    def make_register_payload(self, email: str = "user@example.com", password: str | None = None, referral_code=None):
        return main.RegisterRequest(
            email=email,
            password=password or self.password,
            display_name=None,
            referral_code=referral_code,
        )

    def register(self, email: str = "user@example.com", password: str | None = None, referral_code=None):
        return main.register_user(
            self.make_register_payload(email=email, password=password, referral_code=referral_code),
            FakeRequest(path="/auth/register"),
        )

    def get_user(self, email: str = "user@example.com") -> dict:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, email, password_hash, is_email_verified, is_active
                    FROM users
                    WHERE email = %s;
                    """,
                    (email,),
                )
                row = cur.fetchone()
        self.assertIsNotNone(row)
        return {
            "id": row[0],
            "email": row[1],
            "password_hash": row[2],
            "is_email_verified": row[3],
            "is_active": row[4],
        }

    def get_latest_verification_code(self, email: str = "user@example.com") -> str:
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

    def verify_code(self, code: str, email: str = "user@example.com"):
        return main.verify_email(
            main.EmailVerificationRequest(email=email, code=code),
            FakeRequest(path="/auth/verify-email"),
        )

    def verify_user_directly(self, email: str = "user@example.com") -> None:
        code = self.get_latest_verification_code(email)
        self.verify_code(code, email=email)

    def login(self, email: str = "user@example.com", password: str | None = None, ip: str = "127.0.0.1"):
        return main.login_user(
            main.LoginRequest(email=email, password=password or self.password, device_info=None),
            FakeRequest(ip=ip, path="/auth/login"),
        )

    def assert_api_error(self, cm, error_code: str, status_code: int | None = None) -> None:
        exc = cm.exception
        self.assertEqual(getattr(exc, "error_code", None), error_code)
        if status_code is not None:
            self.assertEqual(exc.status_code, status_code)

    def test_01_register_valid_creates_unverified_user_and_no_session(self):
        response = self.register()
        self.assertEqual(response["status"], "email_verification_required")
        self.assertIsNone(response["session"])
        self.assertEqual(response["email_verification"]["delivery"], "sent")
        user = self.get_user()
        self.assertFalse(user["is_email_verified"])

    def test_02_register_duplicate_email(self):
        self.register()
        with self.assertRaises(main.APIError) as cm:
            self.register()
        self.assert_api_error(cm, "EMAIL_ALREADY_EXISTS", 409)

    def test_03_register_invalid_email_validation_handler(self):
        from fastapi.exceptions import RequestValidationError

        response = asyncio.run(
            main.request_validation_error_handler(
                FakeRequest(path="/auth/register"),
                RequestValidationError([{"loc": ("body", "email")}]),
            )
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.body)["error_code"], "INVALID_EMAIL")

    def test_04_register_password_too_short(self):
        with self.assertRaises(main.APIError) as cm:
            self.register(password="short")
        self.assert_api_error(cm, "PASSWORD_TOO_SHORT", 400)

    def test_05_register_password_too_long(self):
        with self.assertRaises(main.APIError) as cm:
            self.register(password="x" * (main.PASSWORD_MAX_LENGTH + 1))
        self.assert_api_error(cm, "PASSWORD_TOO_LONG", 400)

    def test_06_register_allows_absent_empty_referral(self):
        self.register(email="a@example.com", referral_code=None)
        self.register(email="b@example.com", referral_code="")
        self.register(email="c@example.com", referral_code="   ")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users;")
                self.assertEqual(cur.fetchone()[0], 3)

    def test_07_verify_valid_code_marks_user_verified(self):
        self.register()
        code = self.get_latest_verification_code()
        response = self.verify_code(code)
        self.assertEqual(response["status"], "ok")
        self.assertTrue(self.get_user()["is_email_verified"])

    def test_08_wrong_code_increments_failed_attempts(self):
        self.register()
        with self.assertRaises(main.APIError) as cm:
            self.verify_code("999999")
        self.assert_api_error(cm, "VERIFICATION_CODE_INVALID", 400)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT failed_attempts FROM email_verification_tokens;")
                self.assertEqual(cur.fetchone()[0], 1)

    def test_09_max_verification_attempts_exceeded(self):
        self.register()
        for _ in range(main.EMAIL_VERIFICATION_MAX_ATTEMPTS - 1):
            with self.assertRaises(main.APIError):
                self.verify_code("999999")
        with self.assertRaises(main.APIError) as cm:
            self.verify_code("999999")
        self.assert_api_error(cm, "VERIFICATION_ATTEMPTS_EXCEEDED", 400)

    def test_10_expired_code(self):
        self.register()
        code = self.get_latest_verification_code()
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE email_verification_tokens SET expires_at = NOW() - INTERVAL '1 second';")
        with self.assertRaises(main.APIError) as cm:
            self.verify_code(code)
        self.assert_api_error(cm, "VERIFICATION_CODE_EXPIRED", 400)

    def test_11_already_verified_account(self):
        self.register()
        code = self.get_latest_verification_code()
        self.verify_code(code)
        with self.assertRaises(main.APIError) as cm:
            self.verify_code(code)
        self.assert_api_error(cm, "ACCOUNT_ALREADY_VERIFIED", 409)

    def test_12_resend_invalidates_previous_and_resets_attempts(self):
        self.register()
        with self.assertRaises(main.APIError):
            self.verify_code("999999")
        first_code = self.get_latest_verification_code()
        response = main.resend_email_verification(
            main.ResendEmailVerificationRequest(email="user@example.com"),
            FakeRequest(path="/auth/resend-verification-email"),
        )
        self.assertEqual(response["email_verification"]["delivery"], "sent")
        second_code = self.get_latest_verification_code()
        self.assertNotEqual(first_code, second_code)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM email_verification_tokens WHERE used_at IS NULL;")
                self.assertEqual(cur.fetchone()[0], 1)
                cur.execute("SELECT failed_attempts FROM email_verification_tokens WHERE used_at IS NULL;")
                self.assertEqual(cur.fetchone()[0], 0)

    def test_13_verify_and_resend_rate_limits(self):
        self.register()
        main.AUTH_VERIFY_RATE_LIMIT = 1
        with self.assertRaises(main.APIError):
            self.verify_code("999999")
        with self.assertRaises(main.APIError) as cm:
            self.verify_code("999998")
        self.assert_api_error(cm, "RATE_LIMITED", 429)

        self.reset_database()
        self.register()
        main.AUTH_RESEND_RATE_LIMIT = 1
        payload = main.ResendEmailVerificationRequest(email="user@example.com")
        main.resend_email_verification(payload, FakeRequest(path="/auth/resend-verification-email"))
        with self.assertRaises(main.APIError) as cm2:
            main.resend_email_verification(payload, FakeRequest(path="/auth/resend-verification-email"))
        self.assert_api_error(cm2, "RATE_LIMITED", 429)

    def test_short_verification_code_requires_email_context(self):
        self.register()
        code = self.get_latest_verification_code()

        with self.assertRaises(main.APIError) as cm:
            main.verify_email(
                main.EmailVerificationRequest(code=code),
                FakeRequest(path="/auth/verify-email"),
            )

        self.assert_api_error(cm, "VERIFICATION_CODE_INVALID", 400)
        self.assertFalse(self.get_user()["is_email_verified"])

    def test_short_verification_code_is_bound_to_matching_email(self):
        self.register(email="first@example.com")
        first_code = self.get_latest_verification_code("first@example.com")
        self.register(email="second@example.com")

        with self.assertRaises(main.APIError) as cm:
            self.verify_code(first_code, email="second@example.com")

        self.assert_api_error(cm, "VERIFICATION_CODE_INVALID", 400)
        self.assertFalse(self.get_user("first@example.com")["is_email_verified"])
        self.assertFalse(self.get_user("second@example.com")["is_email_verified"])

    def test_legacy_long_verification_token_remains_usable_without_email(self):
        self.register()
        conn = main.get_connection()
        try:
            with conn:
                verification = main.create_email_verification_token(
                    conn,
                    self.get_user()["id"],
                )
        finally:
            conn.close()

        response = main.verify_email(
            main.EmailVerificationRequest(token=verification["token"]),
            FakeRequest(path="/auth/verify-email"),
        )

        self.assertEqual(response["status"], "ok")
        self.assertTrue(self.get_user()["is_email_verified"])

    def test_rate_limit_uses_railway_real_ip_not_client_forwarded_for(self):
        request_a = FakeRequest(
            ip="100.64.0.1",
            path="/auth/login",
            headers={
                "x-real-ip": "203.0.113.10",
                "x-forwarded-for": "198.51.100.1",
            },
        )
        request_b = FakeRequest(
            ip="100.64.0.1",
            path="/auth/login",
            headers={
                "x-real-ip": "203.0.113.10",
                "x-forwarded-for": "198.51.100.2",
            },
        )
        main.AUTH_LOGIN_RATE_LIMIT = 1
        payload = main.LoginRequest(
            email="missing@example.com",
            password=self.password,
            device_info=None,
        )

        with self.assertRaises(main.APIError) as first:
            main.login_user(payload, request_a)
        self.assert_api_error(first, "INVALID_CREDENTIALS", 401)

        with self.assertRaises(main.APIError) as second:
            main.login_user(payload, request_b)
        self.assert_api_error(second, "RATE_LIMITED", 429)

    def test_14_login_valid_after_verification_creates_session(self):
        self.register()
        self.verify_user_directly()
        response = self.login()
        self.assertIn("access_token", response["session"])
        self.assertIn("refresh_token", response["session"])

    def test_15_login_wrong_password(self):
        self.register()
        self.verify_user_directly()
        with self.assertRaises(main.APIError) as cm:
            self.login(password="wrong-password")
        self.assert_api_error(cm, "INVALID_CREDENTIALS", 401)

    def test_16_login_unverified_email(self):
        self.register()
        with self.assertRaises(main.APIError) as cm:
            self.login()
        self.assert_api_error(cm, "EMAIL_NOT_VERIFIED", 403)

    def test_17_login_inactive_account(self):
        self.register()
        self.verify_user_directly()
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET is_active = FALSE;")
        with self.assertRaises(main.APIError) as cm:
            self.login()
        self.assert_api_error(cm, "ACCOUNT_INACTIVE", 403)

    def test_18_login_rate_limit(self):
        self.register()
        self.verify_user_directly()
        main.AUTH_LOGIN_RATE_LIMIT = 1
        with self.assertRaises(main.APIError):
            self.login(password="wrong-password", ip="10.0.0.1")
        with self.assertRaises(main.APIError) as cm:
            self.login(password="wrong-password", ip="10.0.0.1")
        self.assert_api_error(cm, "RATE_LIMITED", 429)

    def test_19_refresh_valid_rotates_tokens(self):
        self.register()
        self.verify_user_directly()
        session = self.login()["session"]
        response = main.refresh_login_session(main.RefreshRequest(refresh_token=session["refresh_token"]))
        self.assertNotEqual(session["access_token"], response["session"]["access_token"])
        self.assertNotEqual(session["refresh_token"], response["session"]["refresh_token"])

    def test_20_old_refresh_token_after_rotation_is_reused(self):
        self.register()
        self.verify_user_directly()
        old_refresh = self.login()["session"]["refresh_token"]
        main.refresh_login_session(main.RefreshRequest(refresh_token=old_refresh))
        with self.assertRaises(main.APIError) as cm:
            main.refresh_login_session(main.RefreshRequest(refresh_token=old_refresh))
        self.assert_api_error(cm, "REFRESH_TOKEN_REUSED", 401)

    def test_21_concurrent_refresh_only_one_succeeds(self):
        self.register()
        self.verify_user_directly()
        refresh = self.login()["session"]["refresh_token"]
        barrier = threading.Barrier(2)

        def do_refresh():
            barrier.wait()
            try:
                main.refresh_login_session(main.RefreshRequest(refresh_token=refresh))
                return "ok"
            except Exception as exc:
                return getattr(exc, "error_code", exc.__class__.__name__)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: do_refresh(), range(2)))
        self.assertEqual(results.count("ok"), 1)
        self.assertIn("REFRESH_TOKEN_REUSED", results)

    def test_22_refresh_inactive_user_revokes_session(self):
        self.register()
        self.verify_user_directly()
        refresh = self.login()["session"]["refresh_token"]
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET is_active = FALSE;")
        with self.assertRaises(main.APIError) as cm:
            main.refresh_login_session(main.RefreshRequest(refresh_token=refresh))
        self.assert_api_error(cm, "ACCOUNT_INACTIVE", 403)

    def test_23_refresh_legacy_unverified_user_fails(self):
        self.register()
        self.verify_user_directly()
        refresh = self.login()["session"]["refresh_token"]
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET is_email_verified = FALSE;")
        with self.assertRaises(main.APIError) as cm:
            main.refresh_login_session(main.RefreshRequest(refresh_token=refresh))
        self.assert_api_error(cm, "EMAIL_NOT_VERIFIED", 403)

    def test_24_refresh_expired_session(self):
        self.register()
        self.verify_user_directly()
        refresh = self.login()["session"]["refresh_token"]
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE user_sessions SET expires_at = NOW() - INTERVAL '1 second';")
        with self.assertRaises(Exception) as cm:
            main.refresh_login_session(main.RefreshRequest(refresh_token=refresh))
        self.assertEqual(cm.exception.status_code, 401)

    def test_25_refresh_revoked_session(self):
        self.register()
        self.verify_user_directly()
        refresh = self.login()["session"]["refresh_token"]
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE user_sessions SET revoked_at = NOW();")
        with self.assertRaises(main.APIError) as cm:
            main.refresh_login_session(main.RefreshRequest(refresh_token=refresh))
        self.assert_api_error(cm, "SESSION_REVOKED", 401)

    def test_26_logout_revokes_session(self):
        self.register()
        self.verify_user_directly()
        session = self.login()["session"]
        response = main.logout_user(f"Bearer {session['access_token']}")
        self.assertEqual(response["status"], "ok")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT revoked_at IS NOT NULL FROM user_sessions;")
                self.assertTrue(cur.fetchone()[0])

    def test_27_auth_me_with_valid_session(self):
        self.register()
        self.verify_user_directly()
        session = self.login()["session"]
        response = main.get_current_user_profile(f"Bearer {session['access_token']}")
        self.assertEqual(response["user"]["email"], "user@example.com")

    def test_28_auth_me_invalid_or_expired_token(self):
        with self.assertRaises(main.APIError) as cm:
            main.get_current_user_profile("Bearer invalid")
        self.assert_api_error(cm, "ACCESS_TOKEN_INVALID", 401)

        self.register()
        self.verify_user_directly()
        session = self.login()["session"]
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE user_sessions SET access_expires_at = NOW() - INTERVAL '1 second';")
        with self.assertRaises(main.APIError) as cm2:
            main.get_current_user_profile(f"Bearer {session['access_token']}")
        self.assert_api_error(cm2, "ACCESS_TOKEN_INVALID", 401)

    def test_29_delete_account_removes_user_and_dependents(self):
        self.register()
        self.verify_user_directly()
        session = self.login()["session"]
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_device_tokens (user_id, device_token, platform)
                    SELECT id, 'device-token', 'ios' FROM users LIMIT 1;
                    """
                )
        response = main.delete_account(f"Bearer {session['access_token']}")
        self.assertEqual(response["status"], "ok")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users;")
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("SELECT COUNT(*) FROM user_sessions;")
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("SELECT COUNT(*) FROM user_device_tokens;")
                self.assertEqual(cur.fetchone()[0], 0)

    def test_30_after_delete_refresh_and_auth_me_fail(self):
        self.register()
        self.verify_user_directly()
        session = self.login()["session"]
        main.delete_account(f"Bearer {session['access_token']}")
        with self.assertRaises(main.APIError):
            main.refresh_login_session(main.RefreshRequest(refresh_token=session["refresh_token"]))
        with self.assertRaises(main.APIError):
            main.get_current_user_profile(f"Bearer {session['access_token']}")

    def create_linked_provider_user(
        self,
        provider: str,
        *,
        token_ciphertext: str | None = None,
    ) -> tuple[int, dict]:
        self.register()
        self.verify_user_directly()
        session = self.login()["session"]
        user_id = self.get_user()["id"]
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_auth_providers (
                        user_id, provider, provider_user_id, email,
                        email_verified, apple_refresh_token_ciphertext,
                        apple_token_updated_at
                    )
                    VALUES (%s, %s, %s, NULL, TRUE, %s,
                            CASE WHEN %s IS NULL THEN NULL ELSE NOW() END);
                    """,
                    (
                        user_id,
                        provider,
                        f"{provider}-subject",
                        token_ciphertext,
                        token_ciphertext,
                    ),
                )
        return user_id, session

    def test_30a_apple_delete_revokes_token_and_cleans_account(self):
        user_id, session = self.create_linked_provider_user(
            "apple",
            token_ciphertext="encrypted-refresh-token",
        )
        with (
            patch.object(
                main.apple_auth_service,
                "decrypt_apple_refresh_token",
                return_value="refresh-token",
            ),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult(
                    "revoked",
                    1,
                ),
            ) as revoke,
        ):
            response = main.delete_account(f"Bearer {session['access_token']}")

        self.assertEqual(response["status"], "ok")
        revoke.assert_called_once_with("refresh-token")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE id = %s;", (user_id,))
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("SELECT COUNT(*) FROM user_auth_providers;")
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("SELECT COUNT(*) FROM user_sessions;")
                self.assertEqual(cur.fetchone()[0], 0)

    def test_30b_already_invalid_apple_token_still_deletes_account(self):
        _user_id, session = self.create_linked_provider_user(
            "apple",
            token_ciphertext="encrypted-refresh-token",
        )
        with (
            patch.object(
                main.apple_auth_service,
                "decrypt_apple_refresh_token",
                return_value="refresh-token",
            ),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult(
                    "already_invalid",
                    1,
                ),
            ),
        ):
            response = main.delete_account(f"Bearer {session['access_token']}")
        self.assertEqual(response["status"], "ok")

    def test_30c_temporary_apple_failure_queues_revoke_and_deletes_locally(self):
        _user_id, session = self.create_linked_provider_user(
            "apple",
            token_ciphertext="encrypted-refresh-token",
        )
        with (
            patch.object(
                main.apple_auth_service,
                "decrypt_apple_refresh_token",
                return_value="refresh-token",
            ),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                side_effect=main.apple_auth_service.AppleTokenRevocationTemporaryError(
                    "temporary"
                ),
            ),
        ):
            response = main.delete_account(f"Bearer {session['access_token']}")

        self.assertEqual(response["status"], "ok")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users;")
                self.assertEqual(cur.fetchone()[0], 0)
                cur.execute("SELECT status FROM apple_token_revocations;")
                self.assertEqual(cur.fetchone()[0], "pending")

        with (
            patch.object(
                main.apple_auth_service,
                "decrypt_apple_refresh_token",
                return_value="refresh-token",
            ),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult(
                    "revoked",
                    1,
                ),
            ),
        ):
            summary = main.apple_auth_service.process_pending_apple_revocations()
        self.assertEqual(summary["completed"], 1)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM apple_token_revocations;")
                self.assertEqual(cur.fetchone()[0], 0)

    def test_30d_email_and_google_accounts_do_not_call_apple_revoke(self):
        self.register()
        self.verify_user_directly()
        email_session = self.login()["session"]
        with patch.object(
            main.apple_auth_service,
            "revoke_apple_refresh_token",
        ) as revoke:
            main.delete_account(f"Bearer {email_session['access_token']}")
        revoke.assert_not_called()

        self.reset_database()
        _user_id, google_session = self.create_linked_provider_user("google")
        with patch.object(
            main.apple_auth_service,
            "revoke_apple_refresh_token",
        ) as revoke:
            main.delete_account(f"Bearer {google_session['access_token']}")
        revoke.assert_not_called()

    def test_30e_apple_plus_google_revokes_only_apple_and_deletes_all(self):
        user_id, session = self.create_linked_provider_user(
            "apple",
            token_ciphertext="encrypted-refresh-token",
        )
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_auth_providers (
                        user_id, provider, provider_user_id, email_verified
                    )
                    VALUES (%s, 'google', 'google-subject', TRUE);
                    """,
                    (user_id,),
                )
        with (
            patch.object(
                main.apple_auth_service,
                "decrypt_apple_refresh_token",
                return_value="refresh-token",
            ),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult(
                    "revoked",
                    1,
                ),
            ) as revoke,
        ):
            main.delete_account(f"Bearer {session['access_token']}")
        revoke.assert_called_once()
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM user_auth_providers;")
                self.assertEqual(cur.fetchone()[0], 0)

    def test_30f_legacy_apple_account_without_token_deletes_locally(self):
        _user_id, session = self.create_linked_provider_user("apple")
        with patch.object(
            main.apple_auth_service,
            "revoke_apple_refresh_token",
        ) as revoke:
            response = main.delete_account(f"Bearer {session['access_token']}")
        self.assertEqual(response["status"], "ok")
        revoke.assert_not_called()

    def test_30g_repeated_delete_is_safe_and_does_not_revoke_twice(self):
        _user_id, session = self.create_linked_provider_user(
            "apple",
            token_ciphertext="encrypted-refresh-token",
        )
        with (
            patch.object(
                main.apple_auth_service,
                "decrypt_apple_refresh_token",
                return_value="refresh-token",
            ),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult(
                    "revoked",
                    1,
                ),
            ) as revoke,
        ):
            main.delete_account(f"Bearer {session['access_token']}")
            with self.assertRaises(main.APIError):
                main.delete_account(f"Bearer {session['access_token']}")
        revoke.assert_called_once()

    def test_30h_authorization_code_exchange_saves_refresh_token(self):
        claims = {
            "provider": "apple",
            "provider_user_id": "apple-subject",
            "email": "apple@example.com",
            "email_verified": True,
            "display_name": None,
        }
        with (
            patch.object(main, "verify_apple_identity_token", return_value=claims),
            patch.object(
                main.apple_auth_service,
                "exchange_apple_authorization_code",
                return_value=main.apple_auth_service.AppleTokenExchangeResult(
                    "refresh-token"
                ),
            ),
            patch.object(
                main.apple_auth_service,
                "encrypt_apple_refresh_token",
                return_value="encrypted-refresh-token",
            ),
        ):
            response = main.apple_login(
                main.AppleAuthRequest(
                    identity_token="identity-token",
                    authorization_code="authorization-code",
                    raw_nonce="raw-nonce",
                )
            )

        self.assertEqual(response["status"], "ok")
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT apple_refresh_token_ciphertext FROM user_auth_providers "
                    "WHERE provider = 'apple';"
                )
                self.assertEqual(cur.fetchone()[0], "encrypted-refresh-token")

    def test_30i_reused_authorization_code_keeps_existing_refresh_token(self):
        user_id, _session = self.create_linked_provider_user(
            "apple",
            token_ciphertext="existing-ciphertext",
        )
        claims = {
            "provider": "apple",
            "provider_user_id": "apple-subject",
            "email": None,
            "email_verified": False,
            "display_name": None,
        }
        with (
            patch.object(main, "verify_apple_identity_token", return_value=claims),
            patch.object(
                main.apple_auth_service,
                "exchange_apple_authorization_code",
                side_effect=main.apple_auth_service.AppleAuthorizationCodeInvalid(
                    "used"
                ),
            ),
        ):
            response = main.apple_login(
                main.AppleAuthRequest(
                    identity_token="identity-token",
                    authorization_code="used-code",
                    raw_nonce="raw-nonce",
                )
            )
        self.assertEqual(response["user"]["id"], user_id)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT apple_refresh_token_ciphertext FROM user_auth_providers "
                    "WHERE provider = 'apple';"
                )
                self.assertEqual(cur.fetchone()[0], "existing-ciphertext")

    def test_30j_database_delete_error_rolls_back_local_cleanup(self):
        user_id, session = self.create_linked_provider_user(
            "apple",
            token_ciphertext="encrypted-refresh-token",
        )
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE OR REPLACE FUNCTION reject_test_user_delete()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        RAISE EXCEPTION 'test delete failure';
                    END;
                    $$ LANGUAGE plpgsql;
                    CREATE TRIGGER reject_test_user_delete_trigger
                    BEFORE DELETE ON users
                    FOR EACH ROW EXECUTE FUNCTION reject_test_user_delete();
                    """
                )
        try:
            with (
                patch.object(
                    main.apple_auth_service,
                    "decrypt_apple_refresh_token",
                    return_value="refresh-token",
                ),
                patch.object(
                    main.apple_auth_service,
                    "revoke_apple_refresh_token",
                    return_value=main.apple_auth_service.AppleTokenRevocationResult(
                        "revoked",
                        1,
                    ),
                ),
            ):
                with self.assertRaises(main.HTTPException) as context:
                    main.delete_account(f"Bearer {session['access_token']}")
            self.assertEqual(context.exception.status_code, 500)
            with main.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users WHERE id = %s;", (user_id,))
                    self.assertEqual(cur.fetchone()[0], 1)
                    cur.execute("SELECT COUNT(*) FROM user_sessions WHERE user_id = %s;", (user_id,))
                    self.assertEqual(cur.fetchone()[0], 1)
        finally:
            with main.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DROP TRIGGER IF EXISTS reject_test_user_delete_trigger ON users;")
                    cur.execute("DROP FUNCTION IF EXISTS reject_test_user_delete();")

    def test_31_new_hash_uses_at_least_600k_iterations(self):
        hash_value = main.hash_password(self.password)
        self.assertGreaterEqual(int(hash_value.split("$")[1]), 600_000)

    def test_32_legacy_hash_100k_still_verifies(self):
        salt = b"0123456789abcdef"
        key = hashlib.pbkdf2_hmac(PBKDF2_ALGORITHM, self.password.encode(), salt, 100_000)
        legacy = (
            f"pbkdf2_{PBKDF2_ALGORITHM}$100000$"
            f"{base64.b64encode(salt).decode()}${base64.b64encode(key).decode()}"
        )
        self.assertTrue(main.verify_password(self.password, legacy))
        self.assertTrue(main.password_hash_is_legacy(legacy))

    def test_33_legacy_hash_rehashes_on_valid_login(self):
        self.register()
        self.verify_user_directly()
        salt = b"0123456789abcdef"
        key = hashlib.pbkdf2_hmac(PBKDF2_ALGORITHM, self.password.encode(), salt, 100_000)
        legacy = (
            f"pbkdf2_{PBKDF2_ALGORITHM}$100000$"
            f"{base64.b64encode(salt).decode()}${base64.b64encode(key).decode()}"
        )
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET password_hash = %s;", (legacy,))
        self.login()
        self.assertFalse(main.password_hash_is_legacy(self.get_user()["password_hash"]))

    def test_34_wrong_password_does_not_rehash(self):
        self.register()
        self.verify_user_directly()
        original_hash = self.get_user()["password_hash"]
        with self.assertRaises(main.APIError):
            self.login(password="wrong-password")
        self.assertEqual(self.get_user()["password_hash"], original_hash)

    def test_35_rate_limit_buckets_are_separate_by_endpoint(self):
        bucket = main.build_rate_limit_bucket("same")
        main.check_auth_rate_limit("/auth/login", bucket, limit=1, window_seconds=60)
        main.check_auth_rate_limit("/auth/register", bucket, limit=1, window_seconds=60)
        with self.assertRaises(main.APIError) as cm:
            main.check_auth_rate_limit("/auth/login", bucket, limit=1, window_seconds=60)
        self.assert_api_error(cm, "RATE_LIMITED", 429)

    def test_36_rate_limit_window_expiry_resets_counter(self):
        bucket = main.build_rate_limit_bucket("same")
        main.check_auth_rate_limit("/auth/login", bucket, limit=1, window_seconds=60)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE auth_rate_limits SET window_start = NOW() - INTERVAL '2 minutes';")
        main.check_auth_rate_limit("/auth/login", bucket, limit=1, window_seconds=60)

    def test_37_rate_limit_concurrency_does_not_exceed_limit(self):
        bucket = main.build_rate_limit_bucket("race")
        barrier = threading.Barrier(5)

        def hit_limit():
            barrier.wait()
            try:
                main.check_auth_rate_limit("/auth/login", bucket, limit=2, window_seconds=60)
                return "ok"
            except Exception as exc:
                return getattr(exc, "error_code", exc.__class__.__name__)

        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda _: hit_limit(), range(5)))
        self.assertEqual(results.count("ok"), 2)
        self.assertEqual(results.count("RATE_LIMITED"), 3)

    def test_38_health_liveness_is_lightweight(self):
        self.assertEqual(main.health_check(), {"status": "ok"})

    def test_39_readiness_db_available_returns_200_payload(self):
        self.assertEqual(main.readiness_check(), {"status": "ready"})

    def test_40_readiness_db_unavailable_returns_503(self):
        original_get_connection = main.get_connection

        def unavailable_connection():
            raise psycopg2.OperationalError("database unavailable")

        main.get_connection = unavailable_connection
        try:
            with self.assertRaises(main.HTTPException) as cm:
                main.readiness_check()
        finally:
            main.get_connection = original_get_connection

        self.assertEqual(cm.exception.status_code, 503)
        self.assertNotIn("database unavailable", str(cm.exception.detail))

    def test_41_readiness_pool_exhausted_returns_503(self):
        original_get_connection = main.get_connection

        def exhausted_pool():
            raise main.DatabasePoolExhausted("pool exhausted")

        main.get_connection = exhausted_pool
        try:
            with self.assertRaises(main.HTTPException) as cm:
                main.readiness_check()
        finally:
            main.get_connection = original_get_connection

        self.assertEqual(cm.exception.status_code, 503)
        self.assertNotIn("pool exhausted", str(cm.exception.detail))

    def test_42_readiness_returns_connection_to_pool(self):
        pool = db.get_connection_pool()
        used_before = len(getattr(pool, "_used", {}))
        main.readiness_check()
        used_after = len(getattr(pool, "_used", {}))
        self.assertEqual(used_after, used_before)

    def test_43_new_user_receives_app_account_token(self):
        response = self.register()
        response_token = response["user"]["app_account_token"]

        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT app_account_token FROM users WHERE email = %s;",
                    ("user@example.com",),
                )
                stored_token = cur.fetchone()[0]

        self.assertEqual(str(UUID(response_token)), response_token)
        self.assertEqual(str(stored_token), response_token)

    def test_44_schema_backfills_existing_user_without_token(self):
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE users ALTER COLUMN app_account_token DROP NOT NULL;")
                cur.execute("ALTER TABLE users ALTER COLUMN app_account_token DROP DEFAULT;")
                cur.execute(
                    """
                    INSERT INTO users (
                        app_account_token,
                        email,
                        password_hash,
                        display_name,
                        referral_code
                    )
                    VALUES (NULL, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        "legacy@example.com",
                        main.hash_password(self.password),
                        "Legacy User",
                        "LEGACY01",
                    ),
                )
                user_id = cur.fetchone()[0]

            main.ensure_auth_schema(conn)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT app_account_token FROM users WHERE id = %s;",
                    (user_id,),
                )
                token = cur.fetchone()[0]

        self.assertIsNotNone(token)
        self.assertEqual(str(UUID(str(token))), str(token))

    def test_45_app_account_token_is_unique(self):
        first = self.register(email="first@example.com")["user"]["app_account_token"]
        self.register(email="second@example.com")

        with self.assertRaises(psycopg2.errors.UniqueViolation):
            with main.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET app_account_token = %s WHERE email = %s;",
                        (first, "second@example.com"),
                    )

    def test_46_client_cannot_choose_app_account_token(self):
        requested_token = str(uuid4())
        payload = main.RegisterRequest(
            email="user@example.com",
            password=self.password,
            app_account_token=requested_token,
        )
        response = main.register_user(payload, FakeRequest(path="/auth/register"))

        self.assertNotEqual(response["user"]["app_account_token"], requested_token)
        self.assertNotIn("app_account_token", main.RegisterRequest.model_fields)

    def test_47_profile_exposes_stable_app_account_token(self):
        registration = self.register()
        original_token = registration["user"]["app_account_token"]
        self.verify_user_directly()
        login = self.login()

        profile = main.get_current_user_profile(
            f"Bearer {login['session']['access_token']}"
        )
        with main.get_connection() as conn:
            main.ensure_auth_schema(conn)
        second_profile = main.get_current_user_profile(
            f"Bearer {login['session']['access_token']}"
        )

        self.assertEqual(login["user"]["app_account_token"], original_token)
        self.assertEqual(profile["user"]["app_account_token"], original_token)
        self.assertEqual(second_profile["user"]["app_account_token"], original_token)

    def queue_apple_revocation_for_test(
        self,
        ciphertext: str,
        *,
        next_attempt_delta: timedelta = timedelta(0),
        expires_delta: timedelta = timedelta(days=30),
    ) -> None:
        with main.get_connection() as conn:
            main.apple_auth_service.queue_apple_revocation(conn, ciphertext)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE apple_token_revocations
                    SET next_attempt_at = NOW() + %s,
                        expires_at = NOW() + %s
                    WHERE token_ciphertext = %s;
                    """,
                    (next_attempt_delta, expires_delta, ciphertext),
                )

    def apple_revocation_rows(self) -> list[tuple]:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT token_ciphertext, status, attempts, next_attempt_at
                    FROM apple_token_revocations
                    ORDER BY id;
                    """
                )
                return cur.fetchall()

    def test_48_empty_apple_revocation_queue_is_successful(self):
        summary = main.apple_auth_service.process_pending_apple_revocations()
        self.assertEqual(summary["eligible"], 0)
        self.assertEqual(summary["attempted"], 0)
        self.assertEqual(summary["remaining_pending"], 0)

    def test_49_pending_apple_revocation_is_removed_after_success(self):
        self.queue_apple_revocation_for_test("cipher-success")
        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult("revoked", 1),
            ),
        ):
            summary = main.apple_auth_service.process_pending_apple_revocations()

        self.assertEqual(summary["succeeded"], 1)
        self.assertEqual(summary["remaining_pending"], 0)
        self.assertEqual(self.apple_revocation_rows(), [])

    def test_50_timeout_reschedules_apple_revocation(self):
        self.queue_apple_revocation_for_test("cipher-timeout")
        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                side_effect=main.apple_auth_service.AppleTokenRevocationTemporaryError("timeout"),
            ),
        ):
            summary = main.apple_auth_service.process_pending_apple_revocations()

        row = self.apple_revocation_rows()[0]
        self.assertEqual(summary["temporary_failed"], 1)
        self.assertEqual(row[1], "pending")
        self.assertEqual(row[2], 1)
        self.assertGreater(row[3], datetime.now(timezone.utc))

    def test_51_apple_5xx_reschedules_revocation(self):
        self.queue_apple_revocation_for_test("cipher-5xx")
        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                side_effect=main.apple_auth_service.AppleTokenRevocationTemporaryError("503"),
            ),
        ):
            summary = main.apple_auth_service.process_pending_apple_revocations()

        self.assertEqual(summary["temporary_failed"], 1)
        self.assertEqual(self.apple_revocation_rows()[0][1], "pending")

    def test_52_already_invalid_apple_token_completes_queue_item(self):
        self.queue_apple_revocation_for_test("cipher-invalid")
        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult("already_invalid", 1),
            ),
        ):
            summary = main.apple_auth_service.process_pending_apple_revocations()

        self.assertEqual(summary["succeeded"], 1)
        self.assertEqual(self.apple_revocation_rows(), [])

    def test_53_not_yet_eligible_revocation_is_not_processed(self):
        self.queue_apple_revocation_for_test(
            "cipher-future",
            next_attempt_delta=timedelta(hours=1),
        )
        with patch.object(main.apple_auth_service, "revoke_apple_refresh_token") as revoke:
            summary = main.apple_auth_service.process_pending_apple_revocations()

        self.assertEqual(summary["eligible"], 0)
        self.assertEqual(summary["remaining_pending"], 1)
        revoke.assert_not_called()

    def test_54_expired_revocation_is_cleaned_without_decrypt(self):
        self.queue_apple_revocation_for_test(
            "cipher-expired",
            expires_delta=timedelta(seconds=-1),
        )
        with patch.object(main.apple_auth_service, "decrypt_apple_refresh_token") as decrypt:
            summary = main.apple_auth_service.process_pending_apple_revocations()

        self.assertEqual(summary["expired"], 1)
        self.assertEqual(summary["cleaned"], 1)
        self.assertEqual(self.apple_revocation_rows(), [])
        decrypt.assert_not_called()

    def test_55_permanent_revocation_error_becomes_terminal(self):
        self.queue_apple_revocation_for_test("cipher-terminal")
        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                side_effect=main.apple_auth_service.AppleTokenRevocationError("rejected"),
            ),
        ):
            summary = main.apple_auth_service.process_pending_apple_revocations()

        self.assertEqual(summary["terminal_failed"], 1)
        self.assertEqual(self.apple_revocation_rows()[0][1], "failed")

    def test_56_two_concurrent_revocation_runs_do_not_duplicate_processing(self):
        self.queue_apple_revocation_for_test("cipher-concurrent")
        entered = threading.Event()
        release = threading.Event()

        def revoke(_token):
            entered.set()
            self.assertTrue(release.wait(timeout=5))
            return main.apple_auth_service.AppleTokenRevocationResult("revoked", 1)

        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(main.apple_auth_service, "revoke_apple_refresh_token", side_effect=revoke) as revoke_mock,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            first = executor.submit(main.apple_auth_service.process_pending_apple_revocations)
            self.assertTrue(entered.wait(timeout=5))
            second = executor.submit(main.apple_auth_service.process_pending_apple_revocations)
            second_summary = second.result(timeout=5)
            release.set()
            first_summary = first.result(timeout=5)

        self.assertEqual(revoke_mock.call_count, 1)
        self.assertEqual(first_summary["succeeded"] + second_summary["succeeded"], 1)

    def test_57_startup_and_cron_share_concurrency_safe_processor(self):
        self.queue_apple_revocation_for_test("cipher-startup-cron")
        entered = threading.Event()
        release = threading.Event()

        def revoke(_token):
            entered.set()
            self.assertTrue(release.wait(timeout=5))
            return main.apple_auth_service.AppleTokenRevocationResult("revoked", 1)

        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(main.apple_auth_service, "revoke_apple_refresh_token", side_effect=revoke) as revoke_mock,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            startup = executor.submit(main.process_pending_apple_revocations_safely)
            self.assertTrue(entered.wait(timeout=5))
            cron = executor.submit(main.admin_process_apple_revocations, None)
            cron_result = cron.result(timeout=5)
            release.set()
            startup.result(timeout=5)

        self.assertEqual(revoke_mock.call_count, 1)
        self.assertEqual(cron_result["status"], "ok")

    def test_58_revocation_queued_during_run_is_processed_next_time(self):
        self.queue_apple_revocation_for_test("cipher-first")
        entered = threading.Event()
        release = threading.Event()

        def revoke(_token):
            entered.set()
            self.assertTrue(release.wait(timeout=5))
            return main.apple_auth_service.AppleTokenRevocationResult("revoked", 1)

        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(main.apple_auth_service, "revoke_apple_refresh_token", side_effect=revoke),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            running = executor.submit(main.apple_auth_service.process_pending_apple_revocations)
            self.assertTrue(entered.wait(timeout=5))
            self.queue_apple_revocation_for_test("cipher-during-run")
            release.set()
            running.result(timeout=5)

        with (
            patch.object(main.apple_auth_service, "decrypt_apple_refresh_token", return_value="refresh"),
            patch.object(
                main.apple_auth_service,
                "revoke_apple_refresh_token",
                return_value=main.apple_auth_service.AppleTokenRevocationResult("revoked", 1),
            ),
        ):
            summary = main.apple_auth_service.process_pending_apple_revocations()

        self.assertEqual(summary["succeeded"], 1)
        self.assertEqual(self.apple_revocation_rows(), [])

    def test_59_apple_revocation_admin_token_is_scope_specific(self):
        from fastapi.testclient import TestClient

        with (
            patch.object(main, "APPLE_REVOCATION_ADMIN_TOKEN", "current-token"),
            patch.object(main, "APPLE_REVOCATION_ADMIN_TOKEN_PREVIOUS", "previous-token"),
            patch.object(
                main.apple_auth_service,
                "process_pending_apple_revocations",
                return_value={"eligible": 0, "remaining_pending": 0},
            ),
        ):
            client = TestClient(main.app)
            rejected = client.post(
                "/admin/process-apple-revocations",
                headers={"X-Admin-Token": "wrong-token"},
            )
            accepted = client.post(
                "/admin/process-apple-revocations",
                headers={"X-Admin-Token": "current-token"},
            )
            previous = client.post(
                "/admin/process-apple-revocations",
                headers={"X-Admin-Token": "previous-token"},
            )

        self.assertEqual(rejected.status_code, 403)
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(previous.status_code, 200)

    def test_60_authorized_cron_returns_safe_summary_and_is_idempotent(self):
        safe_summary = {
            "eligible": 0,
            "attempted": 0,
            "succeeded": 0,
            "temporary_failed": 0,
            "terminal_failed": 0,
            "expired": 0,
            "cleaned": 0,
            "remaining_pending": 0,
            "processed": 0,
            "completed": 0,
        }
        with patch.object(
            main.apple_auth_service,
            "process_pending_apple_revocations",
            side_effect=[safe_summary, safe_summary],
        ) as processor:
            first = main.admin_process_apple_revocations(None)
            second = main.admin_process_apple_revocations(None)

        serialized = json.dumps(first)
        self.assertEqual(first, second)
        self.assertNotIn("token", serialized.lower())
        self.assertNotIn("email", serialized.lower())
        self.assertEqual(processor.call_count, 2)

    def test_61_database_failure_is_safe_and_does_not_corrupt_queue(self):
        self.queue_apple_revocation_for_test("cipher-db-failure")
        with patch.object(
            main.apple_auth_service,
            "get_connection",
            side_effect=RuntimeError("database unavailable"),
        ):
            with self.assertRaises(main.HTTPException) as cm:
                main.admin_process_apple_revocations(None)

        self.assertEqual(cm.exception.status_code, 500)
        self.assertNotIn("database unavailable", str(cm.exception.detail))
        self.assertEqual(len(self.apple_revocation_rows()), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
