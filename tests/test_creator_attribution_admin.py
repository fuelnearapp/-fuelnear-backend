from __future__ import annotations

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
from fastapi.testclient import TestClient


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
    email_service.send_verification_email = lambda **_kwargs: SimpleNamespace(
        delivery="sent"
    )
    sys.modules["app.email_service"] = email_service


def setUpModule() -> None:
    global POSTGRES_PROCESS, POSTGRES_TMPDIR, main, db

    POSTGRES_TMPDIR = tempfile.mkdtemp(
        prefix="fuelnear-creator-admin-tests-",
        dir="/private/tmp",
    )
    data_dir = os.path.join(POSTGRES_TMPDIR, "data")
    run_checked(
        ["/opt/homebrew/bin/initdb", "-A", "trust", "-U", "postgres", "-D", data_dir]
    )

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
    os.environ["REFERRAL_ADMIN_TOKEN"] = "creator-admin-token"
    os.environ["REFERRAL_ADMIN_TOKEN_PREVIOUS"] = "creator-admin-token-previous"
    os.environ["ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK"] = "false"

    install_import_stubs()
    import app.main as imported_main
    import app.db as imported_db

    imported_db.close_connection_pool()
    imported_db.DATABASE_URL = os.environ["DATABASE_URL"]
    imported_db.DB_POOL_MIN_CONNECTIONS = int(os.environ["DB_POOL_MIN_CONNECTIONS"])
    imported_db.DB_POOL_MAX_CONNECTIONS = int(os.environ["DB_POOL_MAX_CONNECTIONS"])

    main = imported_main
    db = imported_db
    main.REFERRAL_ADMIN_TOKEN = "creator-admin-token"
    main.REFERRAL_ADMIN_TOKEN_PREVIOUS = "creator-admin-token-previous"
    main.ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK = False
    with main.get_connection() as conn:
        main.ensure_auth_schema(conn)
        main.creator_attribution.ensure_creator_attribution_schema(conn)


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


class CreatorAttributionAdminTestCase(unittest.TestCase):
    admin_headers = {"X-Admin-Token": "creator-admin-token"}

    def setUp(self) -> None:
        main.REFERRAL_ADMIN_TOKEN = "creator-admin-token"
        main.REFERRAL_ADMIN_TOKEN_PREVIOUS = "creator-admin-token-previous"
        main.ENABLE_LEGACY_ADMIN_TOKEN_FALLBACK = False
        self.client = TestClient(main.app, raise_server_exceptions=False)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE
                        creator_conversion_events,
                        creator_attributions,
                        creator_campaigns,
                        creators,
                        users
                    RESTART IDENTITY CASCADE;
                    """
                )

    def tearDown(self) -> None:
        self.client.close()

    def create_creator(self, *, name: str = "Marzioso", slug: str = "marzioso") -> dict:
        response = self.client.post(
            "/admin/creators",
            headers=self.admin_headers,
            json={"name": name, "slug": slug},
        )
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()["creator"]

    def campaign_payload(self, creator_id: int, *, code: str = "MARZIOSO26") -> dict:
        return {
            "creator_id": creator_id,
            "name": "Marzioso - Settembre 2026",
            "code": code,
            "starts_at": "2026-09-01T00:00:00Z",
            "ends_at": "2026-10-01T00:00:00Z",
            "post_registration_window_hours": 24,
            "compensation_type": "per_qualified_user",
            "compensation_value": "0.5000",
            "compensation_currency": "eur",
        }

    def create_campaign(self, creator_id: int, *, code: str = "MARZIOSO26") -> dict:
        response = self.client.post(
            "/admin/creator-campaigns",
            headers=self.admin_headers,
            json=self.campaign_payload(creator_id, code=code),
        )
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()["campaign"]

    def insert_user(self, index: int) -> int:
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (
                        email, password_hash, display_name, referral_code,
                        is_email_verified, is_active
                    )
                    VALUES (%s, NULL, %s, %s, TRUE, TRUE)
                    RETURNING id;
                    """,
                    (
                        f"creator-admin-{index}@example.com",
                        f"Creator Admin {index}",
                        f"USR{index:05d}",
                    ),
                )
                return int(cur.fetchone()[0])

    def test_admin_authentication_is_required(self):
        payload = {"name": "Marzioso", "slug": "marzioso"}
        missing = self.client.post("/admin/creators", json=payload)
        wrong = self.client.post(
            "/admin/creators",
            headers={"X-Admin-Token": "wrong"},
            json=payload,
        )
        valid = self.client.post(
            "/admin/creators",
            headers=self.admin_headers,
            json=payload,
        )

        self.assertEqual(missing.status_code, 403)
        self.assertEqual(wrong.status_code, 403)
        self.assertEqual(valid.status_code, 201)

    def test_create_creator_normalizes_and_rejects_duplicate_slug(self):
        creator = self.create_creator(name="  Marzioso  ", slug="  MARZIOSO  ")
        self.assertEqual(creator["name"], "Marzioso")
        self.assertEqual(creator["slug"], "marzioso")
        self.assertEqual(creator["status"], "active")

        duplicate = self.client.post(
            "/admin/creators",
            headers=self.admin_headers,
            json={"name": "Another Name", "slug": "marzioso"},
        )
        self.assertEqual(duplicate.status_code, 409)
        self.assertEqual(duplicate.json()["error_code"], "CREATOR_SLUG_ALREADY_EXISTS")

    def test_create_campaign_normalizes_code_and_starts_as_draft(self):
        creator = self.create_creator()
        campaign = self.create_campaign(creator["id"], code="  marzioso26  ")

        self.assertEqual(campaign["code"], "MARZIOSO26")
        self.assertEqual(campaign["status"], "draft")
        self.assertEqual(campaign["compensation_currency"], "EUR")
        self.assertEqual(campaign["compensation_value"], "0.5000")

    def test_campaign_code_unique_under_concurrency(self):
        creator = self.create_creator()
        payload = main.AdminCreateCreatorCampaignRequest(
            **self.campaign_payload(creator["id"], code="CONCURRENT1")
        )
        barrier = threading.Barrier(2)

        def create_once():
            barrier.wait(timeout=5)
            try:
                response = main.admin_create_creator_campaign(payload, None)
                return response["campaign"]["code"]
            except main.APIError as exc:
                return exc.error_code

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _index: create_once(), range(2)))

        self.assertEqual(results.count("CONCURRENT1"), 1)
        self.assertEqual(results.count("CREATOR_CAMPAIGN_CODE_ALREADY_EXISTS"), 1)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM creator_campaigns WHERE code = 'CONCURRENT1';"
                )
                self.assertEqual(cur.fetchone()[0], 1)

    def test_campaign_validation_and_missing_creator(self):
        creator = self.create_creator()
        base = self.campaign_payload(creator["id"])
        invalid_payloads = (
            {**base, "code": "short"},
            {**base, "starts_at": "2026-10-01T00:00:00Z", "ends_at": "2026-09-01T00:00:00Z"},
            {**base, "starts_at": "2026-09-01T00:00:00", "ends_at": None},
            {**base, "compensation_type": "none", "compensation_value": "0.5000"},
            {**base, "compensation_type": "per_qualified_user", "compensation_value": None},
            {**base, "compensation_type": "per_qualified_user", "compensation_value": "0"},
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                response = self.client.post(
                    "/admin/creator-campaigns",
                    headers=self.admin_headers,
                    json=payload,
                )
                self.assertEqual(response.status_code, 400, response.text)

        invalid_window = self.client.post(
            "/admin/creator-campaigns",
            headers=self.admin_headers,
            json={**base, "post_registration_window_hours": 0},
        )
        self.assertEqual(invalid_window.status_code, 422)

        missing_creator = self.client.post(
            "/admin/creator-campaigns",
            headers=self.admin_headers,
            json={**base, "creator_id": 9999},
        )
        self.assertEqual(missing_creator.status_code, 404)
        self.assertEqual(missing_creator.json()["error_code"], "CREATOR_NOT_FOUND")

    def test_campaign_status_transitions_and_idempotency(self):
        creator = self.create_creator()
        campaign = self.create_campaign(creator["id"])
        campaign_id = campaign["id"]

        active = self.client.patch(
            f"/admin/creator-campaigns/{campaign_id}/status",
            headers=self.admin_headers,
            json={"status": "active"},
        )
        self.assertEqual(active.status_code, 200)
        self.assertTrue(active.json()["campaign"]["changed"])

        same = self.client.patch(
            f"/admin/creator-campaigns/{campaign_id}/status",
            headers=self.admin_headers,
            json={"status": "active"},
        )
        self.assertEqual(same.status_code, 200)
        self.assertFalse(same.json()["campaign"]["changed"])

        user_id = self.insert_user(1)
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO creator_attributions (
                        campaign_id, user_id, code_used, source, attributed_at
                    )
                    VALUES (%s, %s, 'MARZIOSO26', 'email_registration', NOW())
                    RETURNING id, campaign_id, code_used, attributed_at;
                    """,
                    (campaign_id, user_id),
                )
                attribution_snapshot = cur.fetchone()

        for status in ("paused", "active", "ended"):
            response = self.client.patch(
                f"/admin/creator-campaigns/{campaign_id}/status",
                headers=self.admin_headers,
                json={"status": status},
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["campaign"]["status"], status)

        terminal = self.client.patch(
            f"/admin/creator-campaigns/{campaign_id}/status",
            headers=self.admin_headers,
            json={"status": "paused"},
        )
        self.assertEqual(terminal.status_code, 409)
        self.assertEqual(
            terminal.json()["error_code"],
            "CREATOR_CAMPAIGN_STATUS_INVALID",
        )

        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, campaign_id, code_used, attributed_at
                    FROM creator_attributions;
                    """
                )
                self.assertEqual(cur.fetchone(), attribution_snapshot)

    def test_activation_requires_active_creator(self):
        creator = self.create_creator()
        campaign = self.create_campaign(creator["id"])
        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE creators SET status = 'paused' WHERE id = %s;",
                    (creator["id"],),
                )

        response = self.client.patch(
            f"/admin/creator-campaigns/{campaign['id']}/status",
            headers=self.admin_headers,
            json={"status": "active"},
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error_code"], "CREATOR_NOT_ACTIVE")

    def test_summary_uses_milestones_and_excludes_pii(self):
        creator = self.create_creator()
        campaign = self.create_campaign(creator["id"])
        campaign_id = campaign["id"]
        user_ids = [self.insert_user(index) for index in range(1, 5)]

        with main.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO creator_attributions (
                        campaign_id, user_id, code_used, source, status,
                        attributed_at, verified_at, qualified_at,
                        plus_converted_at, paid_plus_converted_at
                    ) VALUES
                        (%s, %s, 'MARZIOSO26', 'email_registration', 'active',
                         NOW() - INTERVAL '10 days', NOW() - INTERVAL '9 days',
                         NOW() - INTERVAL '8 days', NOW() - INTERVAL '7 days',
                         NOW() - INTERVAL '6 days'),
                        (%s, %s, 'MARZIOSO26', 'google_registration', 'active',
                         NOW() - INTERVAL '5 days', NOW() - INTERVAL '4 days',
                         NULL, NULL, NULL),
                        (%s, %s, 'MARZIOSO26', 'apple_registration', 'active',
                         NOW() - INTERVAL '3 days', NULL, NULL, NULL, NULL),
                        (%s, NULL, 'MARZIOSO26', 'post_registration', 'anonymized',
                         NOW() - INTERVAL '20 days', NOW() - INTERVAL '19 days',
                         NOW() - INTERVAL '18 days', NOW() - INTERVAL '17 days',
                         NOW() - INTERVAL '16 days'),
                        (%s, %s, 'MARZIOSO26', 'post_registration', 'invalid',
                         NOW() - INTERVAL '15 days', NOW() - INTERVAL '14 days',
                         NOW() - INTERVAL '13 days', NOW() - INTERVAL '12 days',
                         NOW() - INTERVAL '11 days');
                    """,
                    (
                        campaign_id,
                        user_ids[0],
                        campaign_id,
                        user_ids[1],
                        campaign_id,
                        user_ids[2],
                        campaign_id,
                        campaign_id,
                        user_ids[3],
                    ),
                )

        response = self.client.get(
            f"/admin/creator-campaigns/{campaign_id}/summary",
            headers=self.admin_headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        summary = response.json()["summary"]

        self.assertEqual(
            summary["current_funnel"],
            {
                "attributed": 3,
                "verified": 2,
                "qualified": 1,
                "plus_converted": 1,
                "paid_plus_converted": 1,
            },
        )
        self.assertEqual(
            summary["historical_funnel"],
            {
                "attributed": 4,
                "verified": 3,
                "qualified": 2,
                "plus_converted": 2,
                "paid_plus_converted": 2,
            },
        )
        self.assertEqual(
            summary["status_counts"],
            {"active_count": 3, "anonymized_count": 1, "invalid_count": 1},
        )

        serialized = json.dumps(summary)
        self.assertNotIn("user_id", serialized)
        self.assertNotIn("email", serialized)
        self.assertNotIn("transaction", serialized)
        for index in range(1, 5):
            self.assertNotIn(f"creator-admin-{index}@example.com", serialized)


if __name__ == "__main__":
    unittest.main()
