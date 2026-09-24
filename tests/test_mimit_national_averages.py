from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from email.message import Message
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import psycopg2
from fastapi.testclient import TestClient

from app import main, mimit_national_averages as averages


def section(network="stradale", today="2.154", day="24-09-2026"):
    return f"""<h2><a id="section"></a>Prezzi rete {network}</h2>
<p>Medie dei prezzi in modalit&#224; Self - Aggiornamento {day}</p>
<p><table><thead><tr><th>TIPOLOGIA</th><th>OGGI</th><th>IERI</th><th>DIFFERENZA</th></tr></thead>
<tbody><tr><th>Benzina</th><td>{today}</td><td>2.155</td><td>-0.001</td></tr>
<tr><th>Gasolio</th><td>2.338</td><td>2.340</td><td>-0.002</td></tr></tbody></table></p>
"""


HTML = section() + section("autostradale", "9.999")
START = datetime(2026, 9, 24, 10, tzinfo=timezone.utc)


class ParserTests(unittest.TestCase):
    def test_valid_snapshot_decimal_date_and_both_fuels(self):
        snapshot = averages.parse_snapshot(HTML)
        self.assertEqual(snapshot.reference_date, date(2026, 9, 24))
        self.assertEqual(snapshot.benzina, averages.Price(Decimal("2.154"), Decimal("2.155"), Decimal("-0.001")))
        self.assertEqual(snapshot.gasolio.current, Decimal("2.338"))
        self.assertEqual(snapshot.gasolio.previous.as_tuple().exponent, -3)

    def test_motorway_and_unrelated_table_before_road_are_ignored(self):
        snapshot = averages.parse_snapshot("<table><tr><td>Other</td></tr></table>" + section("autostradale") + section())
        self.assertEqual(snapshot.benzina.current, Decimal("2.154"))

    def test_decimal_comma_is_normalized_without_rounding(self):
        self.assertEqual(averages.parse_snapshot(HTML.replace("2.", "2,").replace("-0.", "-0,")).benzina.current, Decimal("2.154"))

    def test_merged_cells_outside_road_section_do_not_invalidate_snapshot(self):
        expected = averages.parse_snapshot(section())
        for attribute in ("colspan", "rowspan"):
            unrelated = f'<table><tr><td {attribute}="2">Other</td></tr></table>'
            for html in (
                unrelated + HTML,
                HTML + '<h2>Footer</h2>' + unrelated,
                unrelated + HTML + '<h2>Footer</h2>' + unrelated,
                section("autostradale").replace("<td>", f'<td {attribute}="2">') + section(),
            ):
                with self.subTest(attribute=attribute, html=html):
                    self.assertEqual(averages.parse_snapshot(html), expected)

    def test_merged_cells_in_road_table_remain_invalid(self):
        for attribute in ("colspan", "rowspan"):
            with self.subTest(attribute=attribute), self.assertRaises(ValueError):
                averages.parse_snapshot(
                    section().replace("<td>2.154", f'<td {attribute}="2">2.154')
                    + section("autostradale")
                )

    def test_nested_road_table_remains_invalid(self):
        with self.assertRaises(ValueError):
            averages.parse_snapshot(section().replace("2.154</td>", "2.154<table><tr><td>x</td></tr></table></td>"))

    def test_incomplete_ambiguous_or_changed_html_rejected(self):
        for html in (
            section("autostradale"), section() + section(),
            section().replace("Gasolio", "Benzina"),
            section().replace("Gasolio", "GPL"),
            section().replace("Self", "Servito"),
            section().replace("IERI", "PRECEDENTE"),
            section().replace("24-09-2026", "31-02-2026"),
            section().replace("<tr><th>Gasolio</th><td>2.338</td><td>2.340</td><td>-0.002</td></tr>", ""),
            section().replace("</table>", ""),
            section().replace("<td>2.154", '<td colspan="2">2.154'),
            section().replace("</p>", "</p><p>Other metadata</p>", 1),
        ):
            with self.subTest(html=html), self.assertRaises(ValueError):
                averages.parse_snapshot(html)

    def test_malformed_nonfinite_nonpositive_or_inconsistent_numbers_rejected(self):
        for value in ("NaN", "Infinity", "-1.000", "0.000", "2.15", "2.1540", "1e3", "100.000", "2.156", ""):
            with self.subTest(value=value), self.assertRaises(ValueError):
                averages.parse_snapshot(section(today=value))

    def test_fetch_size_content_type_and_retry(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.headers = Message()
        response.headers["Content-Type"] = "text/html; charset=utf-8"
        response.read.return_value = HTML.encode()
        with patch.object(averages, "urlopen", side_effect=[HTTPError("url", 503, "unavailable", {}, None), response]) as fetch, patch.object(averages.time, "sleep"):
            self.assertEqual(averages.fetch_snapshot().reference_date, date(2026, 9, 24))
            self.assertEqual(fetch.call_count, 2)
        for raw in (b"", b"x" * (averages.MAX_HTML_BYTES + 1)):
            response.read.return_value = raw
            with patch.object(averages, "urlopen", return_value=response), self.assertRaises(ValueError):
                averages.fetch_snapshot()
        response.headers.replace_header("Content-Type", "application/json")
        with patch.object(averages, "urlopen", return_value=response), self.assertRaises(ValueError):
            averages.fetch_snapshot()


class SnapshotDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        initdb, cls.pg_ctl = shutil.which("initdb"), shutil.which("pg_ctl")
        if not initdb or not cls.pg_ctl:
            raise unittest.SkipTest("Local PostgreSQL binaries unavailable")
        cls.temp = tempfile.TemporaryDirectory(prefix="fuelnear-national-averages-", dir="/private/tmp")
        cls.data = Path(cls.temp.name) / "pg"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            cls.port = sock.getsockname()[1]
        subprocess.run([initdb, "-D", str(cls.data), "-A", "trust", "-U", "postgres"], check=True, capture_output=True)
        subprocess.run([cls.pg_ctl, "-D", str(cls.data), "-o", f"-h 127.0.0.1 -p {cls.port} -k {cls.temp.name}", "-w", "start"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with closing(cls.connect()) as conn, conn:
            averages.ensure_mimit_national_averages_schema(conn)
            main.ensure_mimit_import_schema(conn)

    @classmethod
    def tearDownClass(cls):
        subprocess.run([cls.pg_ctl, "-D", str(cls.data), "-m", "fast", "-w", "stop"], check=True, capture_output=True)
        cls.temp.cleanup()

    @classmethod
    def connect(cls):
        return psycopg2.connect(host="127.0.0.1", port=cls.port, user="postgres", dbname="postgres")

    def setUp(self):
        with closing(self.connect()) as conn, conn, conn.cursor() as cur:
            cur.execute("DELETE FROM mimit_national_average_snapshot")

    def save(self, html=HTML, started=START):
        with closing(self.connect()) as conn, conn:
            return averages.save_snapshot(conn, averages.parse_snapshot(html), fetch_started_at=started, acquired_at=started + timedelta(seconds=1))

    def read(self, now=START):
        with closing(self.connect()) as conn, conn:
            return averages.read_snapshot(conn, now=now)

    def test_startup_idempotency_and_restart_read(self):
        self.save()
        with closing(self.connect()) as conn, conn:
            averages.ensure_mimit_national_averages_schema(conn)
            averages.ensure_mimit_national_averages_schema(conn)
        self.assertEqual(self.read()["prices"][0]["average_price"], Decimal("2.154"))

    def test_regressive_date_rejected_and_newer_date_accepted(self):
        self.save()
        self.assertFalse(self.save(HTML.replace("24-09", "23-09"), START + timedelta(hours=1)))
        self.assertEqual(self.read()["reference_date"], "2026-09-24")
        self.assertTrue(self.save(HTML.replace("24-09", "25-09"), START + timedelta(days=1)))

    def test_same_date_replay_correction_and_out_of_order_fetch(self):
        self.assertTrue(self.save())
        self.assertFalse(self.save())
        correction = HTML.replace("2.154", "2.153").replace("-0.001", "-0.002")
        self.assertTrue(self.save(correction, START + timedelta(minutes=1)))
        self.assertFalse(self.save(HTML, START + timedelta(seconds=30)))
        self.assertEqual(self.read()["prices"][0]["average_price"], Decimal("2.153"))
        with closing(self.connect()) as conn, conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM mimit_national_average_snapshot")
            self.assertEqual(cur.fetchone()[0], 1)

    def test_concurrent_same_date_fetches_keep_latest_started_snapshot(self):
        barrier = threading.Barrier(2)
        corrected = HTML.replace("2.154", "2.153").replace("-0.001", "-0.002")

        def save_concurrently(html, started):
            barrier.wait(timeout=5)
            return self.save(html, started)

        with ThreadPoolExecutor(max_workers=2) as pool:
            old = pool.submit(save_concurrently, HTML, START)
            new = pool.submit(save_concurrently, corrected, START + timedelta(seconds=1))
            old.result(timeout=10)
            self.assertTrue(new.result(timeout=10))
        self.assertEqual(self.read()["prices"][0]["average_price"], Decimal("2.153"))

    def test_unchanged_later_fetch_refreshes_acquisition_not_reference_date(self):
        self.save()
        self.assertTrue(self.save(started=START + timedelta(days=3)))
        result = self.read(START + timedelta(days=3))
        self.assertEqual(result["reference_date"], "2026-09-24")
        self.assertEqual(result["updated_at"], (START + timedelta(days=3, seconds=1)).isoformat())
        self.assertTrue(result["stale"])

    def test_snapshot_transaction_rolls_back_both_fuels(self):
        self.save()
        before = self.read()
        with closing(self.connect()) as conn:
            with self.assertRaises(RuntimeError), conn:
                averages.save_snapshot(conn, averages.parse_snapshot(HTML.replace("2.154", "2.153").replace("-0.001", "-0.002")), fetch_started_at=START + timedelta(hours=1), acquired_at=START + timedelta(hours=1))
                raise RuntimeError("transaction failure")
        self.assertEqual(self.read(), before)

    def test_fetch_and_parse_failure_preserve_last_good(self):
        self.save()
        before = self.read()
        with patch.object(averages, "get_connection", side_effect=self.connect):
            for error in (TimeoutError(), ValueError("bad HTML")):
                with patch.object(averages, "fetch_snapshot", side_effect=error), self.assertRaises(type(error)):
                    averages.refresh_snapshot()
        self.assertEqual(self.read(), before)

    def test_refresh_uses_own_connection_and_commits(self):
        # A past date is intentional: stale data may still be the only official snapshot.
        with patch.object(averages, "get_connection", side_effect=self.connect), patch.object(averages, "fetch_snapshot", return_value=averages.parse_snapshot(HTML.replace("2026", "2020"))):
            self.assertTrue(averages.refresh_snapshot())
        self.assertEqual(self.read()["reference_date"], "2020-09-24")

    def test_failed_station_job_still_persists_national_snapshot(self):
        with closing(self.connect()) as conn, conn:
            run_id = main.create_mimit_import_run(conn)
        with (
            patch.object(main, "update_mimit_data", side_effect=RuntimeError("station failure")),
            patch.object(averages, "get_connection", side_effect=self.connect),
            patch.object(averages, "fetch_snapshot", return_value=averages.parse_snapshot(HTML.replace("2026", "2020"))),
        ):
            main.run_mimit_update_background(self.connect(), run_id)
        self.assertEqual(self.read()["reference_date"], "2020-09-24")
        with closing(self.connect()) as conn, conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM mimit_import_runs WHERE id = %s", (run_id,))
            self.assertEqual(cur.fetchone()[0], "failed")

    def test_stale_boundary_uses_italian_midnight_and_not_acquisition(self):
        self.save()
        boundary = datetime(2026, 9, 25, 22, tzinfo=timezone.utc)
        self.assertFalse(self.read()["stale"])
        self.assertFalse(self.read(boundary)["stale"])
        self.assertTrue(self.read(boundary + timedelta(microseconds=1))["stale"])

    def test_public_api_contract_and_no_fetch(self):
        self.save()
        with patch.object(main, "get_connection", side_effect=self.connect), patch.object(averages, "fetch_snapshot", side_effect=AssertionError("request must not fetch")) as fetch:
            response = TestClient(main.app).get("/mimit/national-average-prices")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["source"], "MIMIT")
        self.assertEqual(body["network"], "road")
        self.assertEqual([p["fuel_type"] for p in body["prices"]], ["benzina", "gasolio"])
        self.assertTrue(all(p["service_mode"] == "self" and p["unit"] == "EUR/L" for p in body["prices"]))
        self.assertEqual(body["prices"][0]["average_price"], 2.154)
        self.assertEqual(body["prices"][0]["change"], -0.001)
        fetch.assert_not_called()

    def test_no_snapshot_public_api_returns_controlled_503(self):
        with patch.object(main, "get_connection", side_effect=self.connect):
            response = TestClient(main.app).get("/mimit/national-average-prices")
        self.assertEqual(response.status_code, 503)
        self.assertIn("MIMIT_NATIONAL_AVERAGES_UNAVAILABLE", response.text)


class JobIsolationTests(unittest.TestCase):
    def run_job(self, station_error=None, national_error=None):
        conn = MagicMock()
        with patch.object(main, "update_mimit_data", side_effect=station_error, return_value={}), patch.object(main, "finish_mimit_import_run") as success, patch.object(main, "fail_mimit_import_run") as failure, patch.object(main, "process_price_notifications_for_run", return_value={"sent_count": 0, "skipped_count": 0}), patch.object(main, "release_mimit_update_lock") as release, patch.object(averages, "refresh_snapshot", side_effect=national_error) as refresh:
            main.run_mimit_update_background(conn, 1)
        refresh.assert_called_once_with()
        release.assert_called_once_with(conn)
        conn.close.assert_called_once_with()
        return success, failure, conn

    def test_station_failure_still_attempts_national_refresh(self):
        success, failure, _ = self.run_job(station_error=RuntimeError("station failure"))
        success.assert_not_called()
        failure.assert_called_once()

    def test_national_failure_does_not_rollback_successful_station_import(self):
        success, failure, conn = self.run_job(national_error=RuntimeError("national failure"))
        success.assert_called_once()
        failure.assert_not_called()
        conn.rollback.assert_not_called()
        conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
