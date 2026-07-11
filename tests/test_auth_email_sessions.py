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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import psycopg2

from app.auth_utils import PBKDF2_ALGORITHM


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

    main = imported_main
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
    def __init__(self, ip: str = "127.0.0.1", path: str = "/"):
        self.headers = {}
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
