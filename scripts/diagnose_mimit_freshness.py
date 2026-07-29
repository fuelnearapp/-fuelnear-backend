#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import psycopg2
from psycopg2.extras import RealDictCursor


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.import_mimit import ANAGRAFICA_URL, PREZZI_URL, normalize_fuel_type


HTTP_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class PriceObservation:
    fuel_type: str
    price: float
    is_self_service: bool
    reported_at: str
    imported_at: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare the latest MIMIT CSV, database and API prices for one idImpianto."
    )
    parser.add_argument("station_id", type=int, help="MIMIT idImpianto")
    parser.add_argument("--prices-csv", type=Path, help="Use an existing prices CSV")
    parser.add_argument("--stations-csv", type=Path, help="Use an existing stations CSV")
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("FUELNEAR_BACKEND_URL", "").strip(),
        help="FuelNear API base URL; defaults to FUELNEAR_BACKEND_URL",
    )
    return parser.parse_args()


def download_to(url: str, destination: Path) -> None:
    with urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response, destination.open("wb") as output:
        if response.status != 200:
            raise RuntimeError(f"Dataset download failed with HTTP {response.status}")
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "csv" not in content_type and "text/plain" not in content_type:
            raise RuntimeError("Dataset response is not CSV")
        output.write(response.read())
    if destination.stat().st_size == 0:
        raise RuntimeError("Dataset response is empty")


def parse_reported_at(raw_value: str) -> datetime:
    return datetime.strptime(raw_value.strip(), "%d/%m/%Y %H:%M:%S")


def iso_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def select_latest(observations: list[PriceObservation]) -> dict[tuple[str, bool], PriceObservation]:
    selected: dict[tuple[str, bool], PriceObservation] = {}
    for item in observations:
        key = (item.fuel_type, item.is_self_service)
        current = selected.get(key)
        if current is None or (item.reported_at, -item.price) > (
            current.reported_at,
            -current.price,
        ):
            selected[key] = item
    return selected


def read_csv_prices(path: Path, station_id: int) -> dict[tuple[str, bool], PriceObservation]:
    observations: list[PriceObservation] = []
    with path.open(newline="", encoding="utf-8") as source:
        source.readline()
        reader = csv.DictReader(source, delimiter="|")
        if reader.fieldnames is None:
            raise RuntimeError("Prices CSV has no header")
        reader.fieldnames = [field.strip() for field in reader.fieldnames]
        for row in reader:
            if int(row["idImpianto"].strip()) != station_id:
                continue
            fuel_type = normalize_fuel_type(row["descCarburante"])
            if fuel_type is None:
                continue
            observations.append(
                PriceObservation(
                    fuel_type=fuel_type,
                    price=float(row["prezzo"]),
                    is_self_service=row["isSelf"].strip() == "1",
                    reported_at=parse_reported_at(row["dtComu"]).isoformat(),
                )
            )
    return select_latest(observations)


def read_station_coordinates(path: Path, station_id: int) -> tuple[float, float] | None:
    with path.open(newline="", encoding="utf-8") as source:
        source.readline()
        reader = csv.DictReader(source, delimiter="|")
        for row in reader:
            if int(row["idImpianto"].strip()) == station_id:
                return float(row["Latitudine"]), float(row["Longitudine"])
    return None


def read_database_prices(
    station_id: int,
) -> tuple[int | None, dict[tuple[str, bool], PriceObservation]]:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        return None, {}

    conn = psycopg2.connect(database_url, connect_timeout=10)
    try:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT
                    s.id AS station_id,
                    fp.fuel_type,
                    fp.price,
                    fp.is_self_service,
                    fp.reported_at,
                    fp.created_at
                FROM stations AS s
                LEFT JOIN fuel_prices AS fp ON fp.station_id = s.id
                WHERE s.mimit_id = %s
                ORDER BY
                    fp.fuel_type,
                    fp.is_self_service,
                    fp.reported_at DESC,
                    fp.price ASC,
                    fp.id DESC;
                """,
                (station_id,),
            )
            rows = cursor.fetchall()
    finally:
        conn.rollback()
        conn.close()

    internal_station_id = int(rows[0]["station_id"]) if rows else None
    observations = [
        PriceObservation(
            fuel_type=row["fuel_type"],
            price=float(row["price"]),
            is_self_service=bool(row["is_self_service"]),
            reported_at=iso_value(row["reported_at"]) or "",
            imported_at=iso_value(row["created_at"]),
        )
        for row in rows
        if row["fuel_type"] is not None
    ]
    return internal_station_id, select_latest(observations)


def get_json(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response:
        if response.status != 200:
            raise RuntimeError(f"API request failed with HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))


def discover_api_station_id(
    api_base_url: str,
    mimit_id: int,
    coordinates: tuple[float, float] | None,
    csv_prices: dict[tuple[str, bool], PriceObservation],
) -> int | None:
    if coordinates is None or not csv_prices:
        return None
    latitude, longitude = coordinates
    fuel_type = next(iter(csv_prices))[0]
    query = urlencode(
        {
            "lat": latitude,
            "lng": longitude,
            "fuel_type": fuel_type,
            "radius_km": 1,
            "limit": 100,
        }
    )
    payload = get_json(f"{api_base_url.rstrip('/')}/stations/nearby?{query}")
    for item in payload.get("items", []):
        if int(item.get("mimit_id", -1)) == mimit_id:
            return int(item["id"])
    return None


def read_api_prices(
    api_base_url: str,
    internal_station_id: int | None,
) -> dict[tuple[str, bool], PriceObservation]:
    if not api_base_url or internal_station_id is None:
        return {}
    payload = get_json(
        f"{api_base_url.rstrip('/')}/stations/{internal_station_id}/prices"
    )
    observations = [
        PriceObservation(
            fuel_type=item["fuel_type"],
            price=float(item["price"]),
            is_self_service=bool(item["is_self_service"]),
            reported_at=item["reported_at"],
        )
        for item in payload.get("prices", [])
    ]
    return select_latest(observations)


def serialize_observation(item: PriceObservation | None) -> dict[str, Any] | None:
    return asdict(item) if item is not None else None


def build_comparison(
    csv_prices: dict[tuple[str, bool], PriceObservation],
    database_prices: dict[tuple[str, bool], PriceObservation],
    api_prices: dict[tuple[str, bool], PriceObservation],
) -> list[dict[str, Any]]:
    keys = sorted(set(csv_prices) | set(database_prices) | set(api_prices))
    result = []
    for key in keys:
        csv_item = csv_prices.get(key)
        db_item = database_prices.get(key)
        api_item = api_prices.get(key)
        result.append(
            {
                "fuel_type": key[0],
                "is_self_service": key[1],
                "csv": serialize_observation(csv_item),
                "database": serialize_observation(db_item),
                "api": serialize_observation(api_item),
                "csv_db_price_equal": (
                    csv_item is not None
                    and db_item is not None
                    and csv_item.price == db_item.price
                ),
                "csv_db_timestamp_equal": (
                    csv_item is not None
                    and db_item is not None
                    and csv_item.reported_at == db_item.reported_at
                ),
                "db_api_price_equal": (
                    db_item is not None
                    and api_item is not None
                    and db_item.price == api_item.price
                ),
                "db_api_timestamp_equal": (
                    db_item is not None
                    and api_item is not None
                    and db_item.reported_at == api_item.reported_at
                ),
            }
        )
    return result


def main() -> int:
    args = parse_args()
    with tempfile.TemporaryDirectory(prefix="fuelnear-mimit-diagnostic-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        prices_path = args.prices_csv or tmp_path / "prices.csv"
        stations_path = args.stations_csv or tmp_path / "stations.csv"
        if args.prices_csv is None:
            download_to(PREZZI_URL, prices_path)
        if args.stations_csv is None:
            download_to(ANAGRAFICA_URL, stations_path)

        csv_prices = read_csv_prices(prices_path, args.station_id)
        coordinates = read_station_coordinates(stations_path, args.station_id)
        internal_station_id, database_prices = read_database_prices(args.station_id)
        if internal_station_id is None and args.api_base_url:
            internal_station_id = discover_api_station_id(
                args.api_base_url,
                args.station_id,
                coordinates,
                csv_prices,
            )
        api_prices = read_api_prices(args.api_base_url, internal_station_id)

        print(
            json.dumps(
                {
                    "mimit_station_id": args.station_id,
                    "internal_station_id": internal_station_id,
                    "database_checked": bool(os.getenv("DATABASE_URL", "").strip()),
                    "api_checked": bool(args.api_base_url),
                    "comparison": build_comparison(
                        csv_prices,
                        database_prices,
                        api_prices,
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
