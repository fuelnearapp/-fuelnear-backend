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


def station_frame(rows: list[tuple[int, str, str, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "idImpianto": str(mimit_id),
                "Gestore": "Operator",
                "Bandiera": "Brand",
                "Nome Impianto": f"Station {mimit_id}",
                "Indirizzo": f"Address {mimit_id}",
                "Comune": city,
                "Provincia": province,
                "Latitudine": str(latitude),
                "Longitudine": str(longitude),
            }
            for mimit_id, city, province, latitude, longitude in rows
        ]
    )


class MimitGeodataQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initdb = shutil.which("initdb")
        pg_ctl = shutil.which("pg_ctl")
        if not initdb or not pg_ctl:
            raise unittest.SkipTest("PostgreSQL test binaries are not available")

        cls.temp_dir = tempfile.TemporaryDirectory(
            prefix="fuelnear-mimit-geodata-",
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
                cur.execute("TRUNCATE fuel_prices, stations RESTART IDENTITY CASCADE;")

    def test_valid_coordinates_remain_valid(self) -> None:
        assessed = import_mimit.assess_station_coordinate_quality(
            station_frame([(1, "Roma", "RM", 41.9028, 12.4964)])
        )
        self.assertEqual(assessed.iloc[0]["geodata_status"], "valid")
        self.assertTrue(pd.isna(assessed.iloc[0]["geodata_reason"]))

    def test_duplicate_but_plausible_coordinates_remain_valid(self) -> None:
        assessed = import_mimit.assess_station_coordinate_quality(
            station_frame(
                [
                    (1, "Roma", "RM", 41.9028, 12.4964),
                    (2, "Roma", "RM", 41.9028, 12.4964),
                ]
            )
        )
        self.assertEqual(set(assessed["geodata_status"]), {"valid"})

    def test_coordinate_incompatible_with_municipality_is_quarantined(self) -> None:
        rows = [
            (index, "Pistoia", "PT", 43.93 + index / 1000, 10.91 + index / 1000)
            for index in range(1, 6)
        ]
        rows.append((99, "Pistoia", "PT", 40.8748, 14.1525))
        assessed = import_mimit.assess_station_coordinate_quality(station_frame(rows))
        outlier = assessed.loc[assessed["idImpianto"] == "99"].iloc[0]
        self.assertEqual(outlier["geodata_status"], "quarantined")
        self.assertEqual(outlier["geodata_reason"], "municipality_outlier")

    def seed_api_stations(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stations (
                        mimit_id, name, brand, address, city, province,
                        latitude, longitude, geodata_status, geodata_reason
                    ) VALUES
                        (1001, 'Valid Roma', 'Brand', 'Via Roma', 'Roma', 'RM',
                         41.9028, 12.4964, 'valid', NULL),
                        (1002, 'Bad Pistoia', 'Brand', 'Via Pistoia', 'Pistoia', 'PT',
                         41.9028, 12.4964, 'quarantined', 'municipality_outlier');
                    """
                )
                cur.execute(
                    """
                    INSERT INTO fuel_prices (
                        station_id, fuel_type, price, is_self_service, reported_at
                    )
                    SELECT id, 'benzina',
                           CASE WHEN mimit_id = 1002 THEN 1.0 ELSE 1.8 END,
                           TRUE, %s
                    FROM stations;
                    """,
                    (datetime(2026, 8, 23, tzinfo=timezone.utc),),
                )

    def test_quarantined_station_is_excluded_from_nearby_and_best(self) -> None:
        self.seed_api_stations()
        with patch.object(main, "get_connection", side_effect=self.connect):
            nearby = main.get_nearby_stations(
                lat=41.9028,
                lng=12.4964,
                fuel_type="benzina",
                is_self_service=None,
                radius_km=1,
                limit=20,
            )
            best = main.get_best_station(
                lat=41.9028,
                lng=12.4964,
                fuel_type="benzina",
                is_self_service=None,
                radius_km=1,
            )

        self.assertEqual([item["mimit_id"] for item in nearby["items"]], [1001])
        self.assertEqual(best["best_station"]["mimit_id"], 1001)

    def test_quarantined_station_remains_text_searchable(self) -> None:
        self.seed_api_stations()
        with patch.object(main, "get_connection", side_effect=self.connect):
            result = main.search_stations(q="Pistoia", limit=20)
        self.assertEqual([item["mimit_id"] for item in result["items"]], [1002])

    def test_schema_backfill_is_idempotent(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stations (
                        mimit_id, name, address, city, province, latitude, longitude
                    ) VALUES
                        (1, 'P1', 'A1', 'Pistoia', 'PT', 43.930, 10.910),
                        (2, 'P2', 'A2', 'Pistoia', 'PT', 43.931, 10.911),
                        (3, 'P3', 'A3', 'Pistoia', 'PT', 43.932, 10.912),
                        (4, 'P4', 'A4', 'Pistoia', 'PT', 43.933, 10.913),
                        (5, 'P5', 'A5', 'Pistoia', 'PT', 43.934, 10.914),
                        (99, 'Bad', 'Bad', 'Pistoia', 'PT', 40.8748, 14.1525);
                    """
                )
            first_count = import_mimit.ensure_station_geodata_schema(conn)
            second_count = import_mimit.ensure_station_geodata_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT geodata_status, geodata_reason FROM stations WHERE mimit_id = 99;"
                )
                status = cur.fetchone()

        self.assertEqual(first_count, 6)
        self.assertEqual(second_count, 0)
        self.assertEqual(status, ("quarantined", "municipality_outlier"))


if __name__ == "__main__":
    unittest.main()
