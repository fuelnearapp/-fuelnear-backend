from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import psycopg2

from app import import_mimit, main


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class MimitImportAtomicityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-mimit-atomicity-",
            dir="/private/tmp",
        )
        cls.data_dir = Path(cls.temp_dir.name) / "postgres"
        cls.socket_dir = Path(cls.temp_dir.name) / "socket"
        cls.socket_dir.mkdir()
        cls.port = find_free_port()
        cls.pg_ctl = pg_ctl

        subprocess.run(
            [initdb, "-D", str(cls.data_dir), "-A", "trust", "-U", "postgres"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                pg_ctl,
                "-D",
                str(cls.data_dir),
                "-o",
                f"-F -p {cls.port} -k {cls.socket_dir}",
                "-w",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.connection_kwargs = {
            "dbname": "postgres",
            "user": "postgres",
            "host": str(cls.socket_dir),
            "port": cls.port,
        }
        with cls.connect() as conn:
            import_mimit.ensure_core_schema(conn)
            main.ensure_mimit_import_schema(conn)

    @classmethod
    def tearDownClass(cls) -> None:
        subprocess.run(
            [cls.pg_ctl, "-D", str(cls.data_dir), "-m", "fast", "stop"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.temp_dir.cleanup()

    @classmethod
    def connect(cls):
        return psycopg2.connect(**cls.connection_kwargs)

    def setUp(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE mimit_import_runs, fuel_prices, stations "
                    "RESTART IDENTITY CASCADE;"
                )
                cur.execute(
                    """
                    INSERT INTO stations (
                        mimit_id, name, address, city, province, latitude, longitude
                    )
                    VALUES (9001, 'Previous', 'Old address', 'Roma', 'RM', 41.9, 12.5)
                    RETURNING id;
                    """
                )
                station_id = int(cur.fetchone()[0])
                for offset in range(5):
                    cur.execute(
                        """
                        INSERT INTO fuel_prices (
                            station_id, fuel_type, price, is_self_service, reported_at
                        )
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (
                            station_id,
                            "benzina" if offset % 2 == 0 else "diesel",
                            1.7 + (offset / 100),
                            bool(offset % 2),
                            datetime(2026, 7, 1 + offset, tzinfo=timezone.utc),
                        ),
                    )

    def station_frame(self, count: int) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "idImpianto": str(index),
                    "Gestore": "Operator",
                    "Bandiera": "Brand",
                    "Nome Impianto": f"Station {index}",
                    "Indirizzo": f"Address {index}",
                    "Comune": "Roma",
                    "Provincia": "RM",
                    "Latitudine": "41.9",
                    "Longitudine": str(12.5 + index / 1000),
                }
                for index in range(1, count + 1)
            ]
        )

    def price_frame(self, count: int) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "idImpianto": str(index),
                    "descCarburante": "Benzina",
                    "prezzo": str(1.8 + index / 1000),
                    "isSelf": "1",
                    "dtComu": f"{index:02d}/07/2026 08:00:00",
                }
                for index in range(1, count + 1)
            ],
            columns=["idImpianto", "descCarburante", "prezzo", "isSelf", "dtComu"],
        )

    def dataset_snapshot(self) -> list[tuple]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.mimit_id, fp.fuel_type, fp.price,
                           fp.is_self_service, fp.reported_at
                    FROM fuel_prices fp
                    JOIN stations s ON s.id = fp.station_id
                    ORDER BY s.mimit_id, fp.fuel_type, fp.price;
                    """
                )
                return list(cur.fetchall())

    def run_import(
        self,
        candidate_count: int,
        *,
        minimum: int = 3,
        prices: pd.DataFrame | None = None,
    ):
        with (
            patch.object(import_mimit, "get_connection", side_effect=self.connect),
            patch.object(
                import_mimit,
                "load_stations_dataframe",
                return_value=self.station_frame(max(candidate_count, 1)),
            ),
            patch.object(
                import_mimit,
                "load_prices_dataframe",
                return_value=(
                    prices if prices is not None else self.price_frame(candidate_count)
                ),
            ),
            patch.object(import_mimit, "read_mimit_extraction_date", return_value="2026-07-31"),
            patch.object(import_mimit, "MIMIT_MIN_FINAL_PRICE_ROWS", minimum),
        ):
            return import_mimit.import_local_mimit_files()

    def assert_failed_import_preserves_previous_dataset(self, candidate_count: int) -> None:
        previous = self.dataset_snapshot()
        with self.assertRaises(import_mimit.MimitCandidateDatasetRejected):
            self.run_import(candidate_count)
        self.assertEqual(self.dataset_snapshot(), previous)

    def test_candidate_above_threshold_replaces_previous_dataset(self) -> None:
        result = self.run_import(4)
        self.assertEqual(result["prices_imported"], 4)
        self.assertEqual(len(self.dataset_snapshot()), 4)
        self.assertNotEqual(self.dataset_snapshot()[0][0], 9001)

    def test_candidate_exactly_at_threshold_is_accepted(self) -> None:
        result = self.run_import(3)
        self.assertEqual(result["prices_imported"], 3)
        self.assertEqual(len(self.dataset_snapshot()), 3)

    def test_candidate_below_threshold_preserves_previous_dataset(self) -> None:
        self.assert_failed_import_preserves_previous_dataset(2)

    def test_single_candidate_preserves_previous_dataset(self) -> None:
        self.assert_failed_import_preserves_previous_dataset(1)

    def test_empty_candidate_preserves_previous_dataset(self) -> None:
        previous = self.dataset_snapshot()
        with self.assertRaisesRegex(RuntimeError, "Prezzi CSV vuoto"):
            self.run_import(0)
        self.assertEqual(self.dataset_snapshot(), previous)

    def test_all_nonpositive_prices_preserve_previous_dataset(self) -> None:
        previous = self.dataset_snapshot()
        prices = self.price_frame(3)
        prices["prezzo"] = "0"
        with self.assertRaises(import_mimit.MimitCandidateDatasetRejected):
            self.run_import(3, minimum=1, prices=prices)
        self.assertEqual(self.dataset_snapshot(), previous)

    def test_error_after_delete_before_insert_rolls_back(self) -> None:
        previous = self.dataset_snapshot()
        with patch.object(import_mimit, "import_prices", side_effect=RuntimeError("before insert")):
            with self.assertRaisesRegex(RuntimeError, "before insert"):
                self.run_import(3)
        self.assertEqual(self.dataset_snapshot(), previous)

    def test_error_during_insert_rolls_back(self) -> None:
        previous = self.dataset_snapshot()

        def insert_then_fail(conn, _df, station_id_map, **kwargs):
            item = kwargs["prepared_prices"][0][0]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fuel_prices (
                        station_id, fuel_type, price, is_self_service, reported_at
                    ) VALUES (%s, %s, %s, %s, %s);
                    """,
                    (
                        station_id_map[item.mimit_id],
                        item.fuel_type,
                        item.price,
                        item.is_self_service,
                        item.reported_at,
                    ),
                )
            raise RuntimeError("during insert")

        with patch.object(import_mimit, "import_prices", side_effect=insert_then_fail):
            with self.assertRaisesRegex(RuntimeError, "during insert"):
                self.run_import(3)
        self.assertEqual(self.dataset_snapshot(), previous)

    def test_post_write_count_below_threshold_rolls_back(self) -> None:
        previous = self.dataset_snapshot()

        def partial_insert(conn, _df, station_id_map, **kwargs):
            selected = kwargs["prepared_prices"][0][:2]
            with conn.cursor() as cur:
                for item in selected:
                    cur.execute(
                        """
                        INSERT INTO fuel_prices (
                            station_id, fuel_type, price, is_self_service, reported_at
                        ) VALUES (%s, %s, %s, %s, %s);
                        """,
                        (
                            station_id_map[item.mimit_id],
                            item.fuel_type,
                            item.price,
                            item.is_self_service,
                            item.reported_at,
                        ),
                    )
            return len(selected)

        with patch.object(import_mimit, "import_prices", side_effect=partial_insert):
            with self.assertRaises(import_mimit.MimitDatabaseWriteValidationError):
                self.run_import(3)
        self.assertEqual(self.dataset_snapshot(), previous)

    def test_failed_run_preserves_last_success_metadata(self) -> None:
        with self.connect() as setup_conn:
            with setup_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mimit_import_runs (
                        status, started_at, completed_at, stations_imported,
                        prices_imported, stations_csv, prices_csv
                    ) VALUES ('success', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day',
                              20000, 92000, 21000, 93000)
                    RETURNING id;
                    """
                )
                success_run_id = int(cur.fetchone()[0])
                cur.execute(
                    "INSERT INTO mimit_import_runs (status) VALUES ('running') RETURNING id;"
                )
                failed_run_id = int(cur.fetchone()[0])

        failure = import_mimit.MimitCandidateDatasetRejected(
            "Candidate dataset rejected: existing dataset preserved"
        )
        with patch.object(main, "update_mimit_data", side_effect=failure):
            main.run_mimit_update_background(self.connect(), failed_run_id)

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, status, prices_imported
                    FROM mimit_import_runs
                    WHERE status = 'success'
                    ORDER BY completed_at DESC
                    LIMIT 1;
                    """
                )
                last_success = cur.fetchone()
                cur.execute(
                    "SELECT status, completed_at, error_message "
                    "FROM mimit_import_runs WHERE id = %s;",
                    (failed_run_id,),
                )
                failed = cur.fetchone()

        self.assertEqual(last_success, (success_run_id, "success", 92000))
        self.assertEqual(failed[0], "failed")
        self.assertIsNotNone(failed[1])
        self.assertIn("Candidate dataset rejected", failed[2])

    def test_advisory_lock_is_released_after_validation_failure(self) -> None:
        lock_conn = self.connect()
        self.assertTrue(main.try_acquire_mimit_update_lock(lock_conn))
        with patch.object(
            main,
            "update_mimit_data",
            side_effect=import_mimit.MimitCandidateDatasetRejected("candidate rejected"),
        ):
            main.run_mimit_update_background(lock_conn, 999)

        probe_conn = self.connect()
        try:
            with probe_conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_lock(%s);",
                    (main.MIMIT_ADVISORY_LOCK_ID,),
                )
                self.assertTrue(cur.fetchone()[0])
                cur.execute(
                    "SELECT pg_advisory_unlock(%s);",
                    (main.MIMIT_ADVISORY_LOCK_ID,),
                )
            self.assertFalse(main.get_mimit_runtime_state()["update_in_progress"])
        finally:
            probe_conn.close()


if __name__ == "__main__":
    unittest.main()
