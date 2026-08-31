import csv
import json
import logging
import math
import os
import re
import socket
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen
import ssl
import shutil
import certifi

import pandas as pd

from app.db import get_connection


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
ANAGRAFICA_PATH = DATA_DIR / "anagrafica_impianti_attivi.csv"
PREZZI_PATH = DATA_DIR / "prezzo_alle_8.csv"

ANAGRAFICA_URL = os.getenv(
    "ANAGRAFICA_URL",
    "https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv",
)
PREZZI_URL = os.getenv(
    "PREZZI_URL",
    "https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv",
)
MIMIT_DOWNLOAD_TIMEOUT_SECONDS = max(1, int(os.getenv("MIMIT_DOWNLOAD_TIMEOUT_SECONDS", "120")))
MIMIT_DOWNLOAD_MAX_ATTEMPTS = max(1, int(os.getenv("MIMIT_DOWNLOAD_MAX_ATTEMPTS", "3")))
MIMIT_RETRYABLE_HTTP_STATUSES = {502, 503, 504}
MIMIT_MIN_FINAL_PRICE_ROWS = max(0, int(os.getenv("MIMIT_MIN_FINAL_PRICE_ROWS", "75000")))
MIMIT_MAX_REPORTED_AGE_DAYS = max(0, int(os.getenv("MIMIT_MAX_REPORTED_AGE_DAYS", "2")))
MIMIT_UNKNOWN_FUEL_WARNING_ROWS = max(
    0,
    int(os.getenv("MIMIT_UNKNOWN_FUEL_WARNING_ROWS", "100")),
)
GEODATA_STATUS_UNASSESSED = "unassessed"
GEODATA_STATUS_VALID = "valid"
GEODATA_STATUS_QUARANTINED = "quarantined"
GEODATA_MIN_LATITUDE = 35.0
GEODATA_MAX_LATITUDE = 48.0
GEODATA_MIN_LONGITUDE = 6.0
GEODATA_MAX_LONGITUDE = 19.5
GEODATA_MIN_MUNICIPALITY_STATIONS = 5
GEODATA_MAX_MUNICIPALITY_DISTANCE_KM = 100.0
KNOWN_UNKNOWN_MIMIT_FUEL_NAMES = frozenset(
    {
        "gasolio ecoplus",
        "gasolio artico igloo",
    }
)
LOGGER = logging.getLogger("fuelnear.mimit")


def ensure_station_geodata_schema(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.stations');")
        if cur.fetchone()[0] is None:
            return 0

        cur.execute(
            """
            ALTER TABLE stations
            ADD COLUMN IF NOT EXISTS geodata_status TEXT NOT NULL DEFAULT 'unassessed';
            """
        )
        cur.execute(
            """
            ALTER TABLE stations
            ADD COLUMN IF NOT EXISTS geodata_reason TEXT NULL;
            """
        )
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'stations_geodata_status_check'
                      AND conrelid = 'stations'::regclass
                ) THEN
                    ALTER TABLE stations
                    ADD CONSTRAINT stations_geodata_status_check
                    CHECK (geodata_status IN ('unassessed', 'valid', 'quarantined'));
                END IF;
            END
            $$;
            """
        )
        cur.execute(
            """
            WITH municipality_stats AS (
                SELECT
                    UPPER(BTRIM(city)) AS city_key,
                    UPPER(BTRIM(province)) AS province_key,
                    COUNT(*) FILTER (
                        WHERE latitude BETWEEN %s AND %s
                          AND longitude BETWEEN %s AND %s
                    ) AS reference_count,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY latitude) FILTER (
                        WHERE latitude BETWEEN %s AND %s
                          AND longitude BETWEEN %s AND %s
                    ) AS median_latitude,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY longitude) FILTER (
                        WHERE latitude BETWEEN %s AND %s
                          AND longitude BETWEEN %s AND %s
                    ) AS median_longitude
                FROM stations
                GROUP BY UPPER(BTRIM(city)), UPPER(BTRIM(province))
            ), assessed AS (
                SELECT
                    s.id,
                    CASE
                        WHEN s.latitude NOT BETWEEN %s AND %s
                          OR s.longitude NOT BETWEEN %s AND %s
                            THEN 'quarantined'
                        WHEN stats.reference_count >= %s
                          AND 6371 * acos(
                              least(1, greatest(-1,
                                  cos(radians(stats.median_latitude)) * cos(radians(s.latitude)) *
                                  cos(radians(s.longitude) - radians(stats.median_longitude)) +
                                  sin(radians(stats.median_latitude)) * sin(radians(s.latitude))
                              ))
                          ) > %s
                            THEN 'quarantined'
                        ELSE 'valid'
                    END AS status,
                    CASE
                        WHEN s.latitude NOT BETWEEN %s AND %s
                          OR s.longitude NOT BETWEEN %s AND %s
                            THEN 'outside_italy_bounds'
                        WHEN stats.reference_count >= %s
                          AND 6371 * acos(
                              least(1, greatest(-1,
                                  cos(radians(stats.median_latitude)) * cos(radians(s.latitude)) *
                                  cos(radians(s.longitude) - radians(stats.median_longitude)) +
                                  sin(radians(stats.median_latitude)) * sin(radians(s.latitude))
                              ))
                          ) > %s
                            THEN 'municipality_outlier'
                        ELSE NULL
                    END AS reason
                FROM stations s
                JOIN municipality_stats stats
                  ON stats.city_key = UPPER(BTRIM(s.city))
                 AND stats.province_key = UPPER(BTRIM(s.province))
                WHERE s.geodata_status = 'unassessed'
            )
            UPDATE stations s
            SET geodata_status = assessed.status,
                geodata_reason = assessed.reason
            FROM assessed
            WHERE s.id = assessed.id;
            """,
            (
                GEODATA_MIN_LATITUDE,
                GEODATA_MAX_LATITUDE,
                GEODATA_MIN_LONGITUDE,
                GEODATA_MAX_LONGITUDE,
                GEODATA_MIN_LATITUDE,
                GEODATA_MAX_LATITUDE,
                GEODATA_MIN_LONGITUDE,
                GEODATA_MAX_LONGITUDE,
                GEODATA_MIN_LATITUDE,
                GEODATA_MAX_LATITUDE,
                GEODATA_MIN_LONGITUDE,
                GEODATA_MAX_LONGITUDE,
                GEODATA_MIN_LATITUDE,
                GEODATA_MAX_LATITUDE,
                GEODATA_MIN_LONGITUDE,
                GEODATA_MAX_LONGITUDE,
                GEODATA_MIN_MUNICIPALITY_STATIONS,
                GEODATA_MAX_MUNICIPALITY_DISTANCE_KM,
                GEODATA_MIN_LATITUDE,
                GEODATA_MAX_LATITUDE,
                GEODATA_MIN_LONGITUDE,
                GEODATA_MAX_LONGITUDE,
                GEODATA_MIN_MUNICIPALITY_STATIONS,
                GEODATA_MAX_MUNICIPALITY_DISTANCE_KM,
            ),
        )
        assessed_count = cur.rowcount
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stations_geodata_nearby
            ON stations(latitude, longitude)
            WHERE is_active = TRUE AND geodata_status = 'valid';
            """
        )

    if assessed_count:
        LOGGER.info("geodata_backfill_assessed_count=%s", assessed_count)
    return assessed_count


def safe_download_log_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return "configured_url"

    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def get_download_backoff_seconds(attempt: int) -> int:
    return min(120, 10 * (2 ** max(0, attempt - 1)))


def ensure_core_schema(conn) -> None:
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
                geodata_status TEXT NOT NULL DEFAULT 'unassessed',
                geodata_reason TEXT NULL,
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

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stations_mimit_id ON stations(mimit_id);
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fuel_prices_station_id ON fuel_prices(station_id);
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fuel_prices_fuel_type_reported_at
            ON fuel_prices(fuel_type, reported_at DESC);
            """
        )

    ensure_station_geodata_schema(conn)


SUPPORTED_FUEL_TYPES = frozenset(
    {
        "benzina",
        "benzina_premium",
        "diesel",
        "diesel_premium",
        "hvo",
        "gpl",
        "metano",
    }
)

# The order within each tuple is also the canonical preference used when two
# observations have the same MIMIT timestamp.
MIMIT_FUEL_NAMES_BY_TYPE: dict[str, tuple[str, ...]] = {
    "benzina": (
        "benzina",
        "super",
        "super senza piombo",
    ),
    "benzina_premium": (
        "blue super",
        "benzina speciale",
        "benzina speciale 98 ottani",
        "benzina wr 100",
        "benzina shell v power",
        "benzina energy 98 ottani",
        "benzina plus 98",
        "v-power",
        "verde speciale",
        "f101",
        "f-101",
        "hiq perform b100 ottani",
        "benzina 100 ottani",
        "benzina 102 ottani",
    ),
    "diesel": (
        "gasolio",
        "diesel",
        "gasolio alpino",
        "gasolio gelo",
        "gasolio artico",
    ),
    "diesel_premium": (
        "blue diesel",
        "supreme diesel",
        "hi-q diesel",
        "hiq perform+",
        "gasolio speciale",
        "gasolio premium",
        "diesel shell v power",
        "v-power diesel",
        "excellium diesel",
        "gasolio oro diesel",
        "gasolio prestazionale",
        "gasolio plus",
        "blu diesel alpino",
        "s-diesel",
        "dieselmax",
        "e-diesel",
        "gp diesel",
        "gasolio energy d",
    ),
    "gpl": (
        "gpl",
        "lpg",
    ),
    "metano": (
        "metano",
        "gnc",
        "cng",
        "l-gnc",
        "lng",
        "gnl",
    ),
}

MIMIT_FUEL_TYPE_BY_NAME = {
    raw_name: fuel_type
    for fuel_type, raw_names in MIMIT_FUEL_NAMES_BY_TYPE.items()
    for raw_name in raw_names
}
MIMIT_CANONICAL_NAME_PRIORITY = {
    (fuel_type, raw_name): priority
    for fuel_type, raw_names in MIMIT_FUEL_NAMES_BY_TYPE.items()
    for priority, raw_name in enumerate(raw_names)
}


@dataclass(frozen=True)
class NormalizedMimitPrice:
    mimit_id: int
    raw_fuel: str
    fuel_type: str
    price: float
    is_self_service: bool
    reported_at: datetime


@dataclass
class PriceSelectionDiagnostics:
    input_rows: int = 0
    candidate_rows: int = 0
    selected_rows: int = 0
    duplicate_rows_removed: int = 0
    collision_groups: int = 0
    skipped_missing_station: int = 0
    skipped_invalid_price: int = 0
    unknown_fuel_rows: int = 0
    unknown_fuel_names: Counter[str] = field(default_factory=Counter)
    unknown_fuel_station_ids: dict[str, set[int]] = field(default_factory=dict)
    final_fuel_type_counts: Counter[str] = field(default_factory=Counter)
    oldest_reported_at: datetime | None = None
    newest_reported_at: datetime | None = None
    final_duplicate_keys: int = 0


class MimitCandidateDatasetRejected(RuntimeError):
    pass


class MimitDatabaseWriteValidationError(RuntimeError):
    pass


def normalize_mimit_fuel_name(raw_fuel: object) -> str:
    return " ".join(str(raw_fuel).strip().casefold().split())


def normalize_fuel_type(raw_fuel: str) -> str | None:
    value = normalize_mimit_fuel_name(raw_fuel)
    mapped = MIMIT_FUEL_TYPE_BY_NAME.get(value)
    if mapped is not None:
        return mapped

    # HVO is a fuel family marker, not a generic diesel marketing term.
    if "hvo" in value:
        return "hvo"

    # Unknown commercial names are deliberately excluded instead of being
    # published as standard petrol or diesel.
    return None


def read_mimit_extraction_date(path: Path) -> str | None:
    try:
        with path.open(encoding="utf-8") as source:
            first_line = source.readline().strip()
    except OSError:
        return None

    match = re.fullmatch(r"Estrazione del\s+(\d{4}-\d{2}-\d{2})", first_line)
    if match is None:
        return None
    return match.group(1)


def get_unknown_fuel_details(
    diagnostics: PriceSelectionDiagnostics,
) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    for raw_name, count in diagnostics.unknown_fuel_names.items():
        normalized_name = normalize_mimit_fuel_name(raw_name)
        current = grouped.setdefault(
            normalized_name,
            {
                "raw_name": raw_name,
                "normalized_name": normalized_name,
                "count": 0,
                "example_station_ids": set(),
                "known": normalized_name in KNOWN_UNKNOWN_MIMIT_FUEL_NAMES,
            },
        )
        if (raw_name.casefold(), raw_name) < (
            str(current["raw_name"]).casefold(),
            str(current["raw_name"]),
        ):
            current["raw_name"] = raw_name
        current["count"] = int(current["count"]) + count
        current["example_station_ids"].update(
            diagnostics.unknown_fuel_station_ids.get(normalized_name, set())
        )

    details: list[dict[str, object]] = []
    for normalized_name in sorted(grouped):
        item = grouped[normalized_name]
        details.append(
            {
                "raw_name": item["raw_name"],
                "normalized_name": normalized_name,
                "count": item["count"],
                "example_station_ids": sorted(item["example_station_ids"])[:5],
                "known": item["known"],
            }
        )
    return details


def evaluate_mimit_import_warnings(
    summary: dict[str, Any],
    *,
    minimum_final_rows: int = MIMIT_MIN_FINAL_PRICE_ROWS,
    maximum_reported_age_days: int = MIMIT_MAX_REPORTED_AGE_DAYS,
    unknown_fuel_warning_rows: int = MIMIT_UNKNOWN_FUEL_WARNING_ROWS,
) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []

    if int(summary.get("prices_csv") or 0) == 0:
        warnings.append({"code": "zero_price_rows", "value": 0})
    if int(summary.get("stations_imported") or 0) == 0:
        warnings.append({"code": "zero_imported_stations", "value": 0})

    final_rows = int(summary.get("final_rows") or 0)
    if int(summary.get("prices_csv") or 0) > 0 and final_rows < minimum_final_rows:
        warnings.append(
            {
                "code": "final_rows_below_threshold",
                "value": final_rows,
                "threshold": minimum_final_rows,
            }
        )

    extraction_date_raw = summary.get("source_extraction_date")
    newest_reported_at_raw = summary.get("newest_reported_at")
    if isinstance(extraction_date_raw, str) and isinstance(newest_reported_at_raw, str):
        try:
            extraction_date = datetime.fromisoformat(extraction_date_raw).date()
            newest_reported_date = datetime.fromisoformat(newest_reported_at_raw).date()
            age_days = (extraction_date - newest_reported_date).days
            if age_days > maximum_reported_age_days:
                warnings.append(
                    {
                        "code": "stale_reported_at",
                        "age_days": age_days,
                        "threshold_days": maximum_reported_age_days,
                    }
                )
        except ValueError:
            pass

    unknown_fuels = summary.get("unknown_fuels")
    if isinstance(unknown_fuels, list):
        new_names = sorted(
            str(item.get("normalized_name"))
            for item in unknown_fuels
            if isinstance(item, dict) and item.get("known") is False
        )
        if new_names:
            warnings.append({"code": "new_unknown_fuel_names", "names": new_names})

    unknown_rows = int(summary.get("unknown_fuel_rows") or 0)
    if unknown_rows > unknown_fuel_warning_rows:
        warnings.append(
            {
                "code": "unknown_fuel_rows_increased",
                "value": unknown_rows,
                "threshold": unknown_fuel_warning_rows,
            }
        )

    duplicate_keys = int(summary.get("final_duplicate_keys") or 0)
    if duplicate_keys > 0:
        warnings.append({"code": "final_duplicate_keys", "value": duplicate_keys})

    return warnings


def build_mimit_import_summary(
    metrics: dict[str, Any],
    *,
    run_id: int | None,
    status: str,
    duration_seconds: float,
) -> dict[str, Any]:
    summary = {
        "run_id": run_id,
        "status": status,
        "source_extraction_date": metrics.get("source_extraction_date"),
        "stations_csv": int(metrics.get("stations_csv") or 0),
        "stations_imported": int(metrics.get("stations_imported") or 0),
        "stations_excluded": int(metrics.get("stations_excluded") or 0),
        "prices_csv": int(metrics.get("prices_csv") or 0),
        "prices_for_importable_stations": int(
            metrics.get("prices_for_importable_stations") or 0
        ),
        "candidate_rows": int(metrics.get("candidate_rows") or 0),
        "final_rows": int(metrics.get("final_rows") or 0),
        "duplicates_removed": int(metrics.get("duplicates_removed") or 0),
        "final_duplicate_keys": int(metrics.get("final_duplicate_keys") or 0),
        "unknown_fuel_rows": int(metrics.get("unknown_fuel_rows") or 0),
        "unknown_fuels": metrics.get("unknown_fuels") or [],
        "fuel_type_counts": metrics.get("fuel_type_counts") or {},
        "oldest_reported_at": metrics.get("oldest_reported_at"),
        "newest_reported_at": metrics.get("newest_reported_at"),
        "duration_seconds": round(duration_seconds, 3),
    }
    if metrics.get("error_type"):
        summary["error_type"] = metrics["error_type"]
    summary["warnings"] = evaluate_mimit_import_warnings(summary)
    return summary


def log_mimit_import_summary(summary: dict[str, Any]) -> None:
    for warning in summary.get("warnings", []):
        LOGGER.warning(
            "MIMIT_IMPORT_WARNING %s",
            json.dumps(warning, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        )

    serialized = json.dumps(summary, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if summary.get("status") == "success":
        LOGGER.info("MIMIT_IMPORT_SUMMARY %s", serialized)
    else:
        LOGGER.error("MIMIT_IMPORT_SUMMARY %s", serialized)


def get_canonical_fuel_priority(raw_fuel: str, fuel_type: str) -> int:
    normalized_name = normalize_mimit_fuel_name(raw_fuel)
    if fuel_type == "hvo":
        return 0 if normalized_name == "hvo" else 100
    return MIMIT_CANONICAL_NAME_PRIORITY.get((fuel_type, normalized_name), 100)


def _candidate_wins(candidate: NormalizedMimitPrice, current: NormalizedMimitPrice) -> bool:
    if candidate.reported_at != current.reported_at:
        return candidate.reported_at > current.reported_at

    candidate_priority = get_canonical_fuel_priority(candidate.raw_fuel, candidate.fuel_type)
    current_priority = get_canonical_fuel_priority(current.raw_fuel, current.fuel_type)
    if candidate_priority != current_priority:
        return candidate_priority < current_priority

    # Stable final tie-break: normalized raw name, then price, then original name.
    candidate_tie_break = (
        normalize_mimit_fuel_name(candidate.raw_fuel),
        candidate.price,
        candidate.raw_fuel,
    )
    current_tie_break = (
        normalize_mimit_fuel_name(current.raw_fuel),
        current.price,
        current.raw_fuel,
    )
    return candidate_tie_break < current_tie_break


def select_deterministic_price_rows(
    rows: Iterable[NormalizedMimitPrice],
) -> list[NormalizedMimitPrice]:
    selected: dict[tuple[int, str, bool], NormalizedMimitPrice] = {}
    for row in rows:
        key = (row.mimit_id, row.fuel_type, row.is_self_service)
        current = selected.get(key)
        if current is None or _candidate_wins(row, current):
            selected[key] = row

    return sorted(
        selected.values(),
        key=lambda row: (
            row.mimit_id,
            row.fuel_type,
            row.is_self_service,
            row.reported_at,
            normalize_mimit_fuel_name(row.raw_fuel),
            row.price,
        ),
    )


def prepare_prices_for_import(
    df_prices: pd.DataFrame,
    station_id_map: dict[int, int],
) -> tuple[list[NormalizedMimitPrice], PriceSelectionDiagnostics]:
    diagnostics = PriceSelectionDiagnostics(input_rows=len(df_prices))
    candidates: list[NormalizedMimitPrice] = []
    group_counts: Counter[tuple[int, str, bool]] = Counter()

    for _, row in df_prices.iterrows():
        mimit_id = int(row["idImpianto"])
        if mimit_id not in station_id_map:
            diagnostics.skipped_missing_station += 1
            continue

        raw_fuel = str(row["descCarburante"]).strip()
        fuel_type = normalize_fuel_type(raw_fuel)
        if fuel_type is None:
            diagnostics.unknown_fuel_rows += 1
            diagnostics.unknown_fuel_names[raw_fuel or "<empty>"] += 1
            normalized_unknown = normalize_mimit_fuel_name(raw_fuel or "<empty>")
            diagnostics.unknown_fuel_station_ids.setdefault(normalized_unknown, set()).add(
                mimit_id
            )
            continue

        try:
            price = float(row["prezzo"])
        except (ValueError, TypeError):
            diagnostics.skipped_invalid_price += 1
            continue
        if not math.isfinite(price) or price <= 0:
            diagnostics.skipped_invalid_price += 1
            continue

        is_self_service = str(row["isSelf"]).strip() == "1"
        reported_at = pd.to_datetime(
            str(row["dtComu"]).strip(),
            format="%d/%m/%Y %H:%M:%S",
        ).to_pydatetime()
        candidate = NormalizedMimitPrice(
            mimit_id=mimit_id,
            raw_fuel=raw_fuel,
            fuel_type=fuel_type,
            price=price,
            is_self_service=is_self_service,
            reported_at=reported_at,
        )
        candidates.append(candidate)
        group_counts[(mimit_id, fuel_type, is_self_service)] += 1

    selected = select_deterministic_price_rows(candidates)
    diagnostics.candidate_rows = len(candidates)
    diagnostics.selected_rows = len(selected)
    diagnostics.duplicate_rows_removed = len(candidates) - len(selected)
    diagnostics.collision_groups = sum(count > 1 for count in group_counts.values())
    diagnostics.final_fuel_type_counts.update(item.fuel_type for item in selected)
    if selected:
        diagnostics.oldest_reported_at = min(item.reported_at for item in selected)
        diagnostics.newest_reported_at = max(item.reported_at for item in selected)
    final_key_counts = Counter(
        (item.mimit_id, item.fuel_type, item.is_self_service) for item in selected
    )
    diagnostics.final_duplicate_keys = sum(count > 1 for count in final_key_counts.values())
    return selected, diagnostics


def download_file(url: str, destination: Path) -> dict[str, object]:
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    temporary_destination = destination.with_suffix(destination.suffix + ".tmp")
    safe_url = safe_download_log_url(url)
    last_modified = None
    last_error: Exception | None = None

    for attempt in range(1, MIMIT_DOWNLOAD_MAX_ATTEMPTS + 1):
        if temporary_destination.exists():
            temporary_destination.unlink()

        print(
            f"[MIMIT] download_attempt attempt={attempt}/{MIMIT_DOWNLOAD_MAX_ATTEMPTS} "
            f"url={safe_url} timeout_seconds={MIMIT_DOWNLOAD_TIMEOUT_SECONDS}"
        )
        try:
            with urlopen(
                url,
                context=ssl_context,
                timeout=MIMIT_DOWNLOAD_TIMEOUT_SECONDS,
            ) as response, open(temporary_destination, "wb") as output_file:
                last_modified = response.headers.get("Last-Modified")
                shutil.copyfileobj(response, output_file)
        except HTTPError as exc:
            last_error = exc
            if exc.code not in MIMIT_RETRYABLE_HTTP_STATUSES:
                print(f"[MIMIT] download_failed url={safe_url} http_status={exc.code}")
                raise RuntimeError(f"MIMIT download failed with HTTP {exc.code}") from exc

            print(f"[MIMIT] temporary_http_error url={safe_url} http_status={exc.code}")
        except (TimeoutError, socket.timeout) as exc:
            last_error = exc
            print(f"[MIMIT] timeout url={safe_url} type={exc.__class__.__name__}")
        except URLError as exc:
            last_error = exc
            reason_type = exc.reason.__class__.__name__ if exc.reason is not None else "unknown"
            print(f"[MIMIT] download_failed url={safe_url} network_error={reason_type}")
        except OSError as exc:
            last_error = exc
            print(f"[MIMIT] download_failed url={safe_url} network_error={exc.__class__.__name__}")
        else:
            if not temporary_destination.exists() or temporary_destination.stat().st_size == 0:
                last_error = RuntimeError("Downloaded file is empty")
                print(f"[MIMIT] download_failed url={safe_url} reason=empty_file")
            else:
                temporary_destination.replace(destination)
                print(
                    f"[MIMIT] download_success url={safe_url} "
                    f"size_bytes={destination.stat().st_size}"
                )
                break

        if attempt >= MIMIT_DOWNLOAD_MAX_ATTEMPTS:
            if temporary_destination.exists():
                temporary_destination.unlink()
            print(f"[MIMIT] download_failed url={safe_url} attempts={attempt}")
            raise RuntimeError("MIMIT download failed after retry attempts") from last_error

        backoff_seconds = get_download_backoff_seconds(attempt)
        print(
            f"[MIMIT] download_retry url={safe_url} "
            f"next_attempt={attempt + 1} backoff_seconds={backoff_seconds}"
        )
        time.sleep(backoff_seconds)

    if not temporary_destination.exists() or temporary_destination.stat().st_size == 0:
        if not destination.exists() or destination.stat().st_size == 0:
            raise RuntimeError("MIMIT download did not produce a valid file")

    parsed_last_modified = None
    if last_modified:
        try:
            parsed_last_modified = parsedate_to_datetime(last_modified).isoformat()
        except (TypeError, ValueError):
            parsed_last_modified = last_modified

    return {
        "url": url,
        "path": str(destination),
        "size_bytes": destination.stat().st_size,
        "last_modified": parsed_last_modified,
    }


def download_latest_mimit_files() -> dict[str, object]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        anagrafica = download_file(ANAGRAFICA_URL, ANAGRAFICA_PATH)
        prezzi = download_file(PREZZI_URL, PREZZI_PATH)
    except Exception:
        anagrafica_tmp = ANAGRAFICA_PATH.with_suffix(ANAGRAFICA_PATH.suffix + ".tmp")
        prezzi_tmp = PREZZI_PATH.with_suffix(PREZZI_PATH.suffix + ".tmp")

        if anagrafica_tmp.exists():
            anagrafica_tmp.unlink()
        if prezzi_tmp.exists():
            prezzi_tmp.unlink()

        raise

    return {
        "anagrafica_path": str(ANAGRAFICA_PATH),
        "prezzi_path": str(PREZZI_PATH),
        "anagrafica": anagrafica,
        "prezzi": prezzi,
    }


def load_stations_dataframe() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    fixed_rows = 0
    skipped_rows = 0

    with open(ANAGRAFICA_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")

        next(reader, None)
        next(reader, None)

        for line_number, row in enumerate(reader, start=3):
            if not row:
                continue

            if len(row) < 10:
                skipped_rows += 1
                print(
                    f"Riga anagrafica saltata (colonne insufficienti) alla linea {line_number}: {row}"
                )
                continue

            if len(row) > 10:
                fixed_rows += 1

            head = row[:4]
            middle = row[4:-4]
            tail = row[-4:]

            if len(middle) < 2:
                skipped_rows += 1
                print(
                    f"Riga anagrafica saltata (middle non valido) alla linea {line_number}: {row}"
                )
                continue

            nome_impianto = middle[0].strip()
            indirizzo = " | ".join(part.strip() for part in middle[1:] if part.strip())

            rows.append(
                {
                    "idImpianto": head[0].strip(),
                    "Gestore": head[1].strip(),
                    "Bandiera": head[2].strip(),
                    "Tipo Impianto": head[3].strip(),
                    "Nome Impianto": nome_impianto,
                    "Indirizzo": indirizzo,
                    "Comune": tail[0].strip(),
                    "Provincia": tail[1].strip(),
                    "Latitudine": tail[2].strip(),
                    "Longitudine": tail[3].strip(),
                }
            )

    df = pd.DataFrame(rows)
    print(f"Righe anagrafiche corrette automaticamente: {fixed_rows}")
    print(f"Righe anagrafiche saltate: {skipped_rows}")
    return df


def load_prices_dataframe() -> pd.DataFrame:
    df = pd.read_csv(
        PREZZI_PATH,
        sep="|",
        skiprows=1,
        encoding="utf-8",
    )

    df.columns = df.columns.str.strip()
    return df


def get_prices_csv_max_reported_at(df_prices: pd.DataFrame):
    parsed_dates = pd.to_datetime(
        df_prices["dtComu"].astype(str).str.strip(),
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce",
    )
    max_reported_at = parsed_dates.max()

    if pd.isna(max_reported_at):
        return None

    return max_reported_at.to_pydatetime()


def geodata_distance_km(
    latitude: float,
    longitude: float,
    reference_latitude: float,
    reference_longitude: float,
) -> float:
    latitude_radians = math.radians(latitude)
    reference_latitude_radians = math.radians(reference_latitude)
    latitude_delta = math.radians(reference_latitude - latitude)
    longitude_delta = math.radians(reference_longitude - longitude)
    haversine = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(latitude_radians)
        * math.cos(reference_latitude_radians)
        * math.sin(longitude_delta / 2) ** 2
    )
    return 6371 * 2 * math.atan2(math.sqrt(haversine), math.sqrt(max(0.0, 1 - haversine)))


def assess_station_coordinate_quality(df_stations: pd.DataFrame) -> pd.DataFrame:
    assessed = df_stations.copy()
    latitudes = pd.to_numeric(assessed["Latitudine"], errors="coerce")
    longitudes = pd.to_numeric(assessed["Longitudine"], errors="coerce")
    city_keys = assessed["Comune"].fillna("").astype(str).str.strip().str.casefold()
    province_keys = assessed["Provincia"].fillna("").astype(str).str.strip().str.casefold()

    statuses = pd.Series(GEODATA_STATUS_VALID, index=assessed.index, dtype="object")
    reasons = pd.Series(None, index=assessed.index, dtype="object")
    finite_coordinates = latitudes.map(lambda value: pd.notna(value) and math.isfinite(float(value))) & longitudes.map(
        lambda value: pd.notna(value) and math.isfinite(float(value))
    )
    inside_italy_bounds = (
        finite_coordinates
        & latitudes.between(GEODATA_MIN_LATITUDE, GEODATA_MAX_LATITUDE)
        & longitudes.between(GEODATA_MIN_LONGITUDE, GEODATA_MAX_LONGITUDE)
    )

    statuses.loc[~finite_coordinates] = GEODATA_STATUS_QUARANTINED
    reasons.loc[~finite_coordinates] = "invalid_coordinate"
    statuses.loc[finite_coordinates & ~inside_italy_bounds] = GEODATA_STATUS_QUARANTINED
    reasons.loc[finite_coordinates & ~inside_italy_bounds] = "outside_italy_bounds"

    references = pd.DataFrame(
        {
            "city_key": city_keys,
            "province_key": province_keys,
            "latitude": latitudes,
            "longitude": longitudes,
        },
        index=assessed.index,
    ).loc[inside_italy_bounds & city_keys.ne("") & province_keys.ne("")]
    municipality_stats = references.groupby(["city_key", "province_key"]).agg(
        reference_count=("latitude", "size"),
        median_latitude=("latitude", "median"),
        median_longitude=("longitude", "median"),
    )

    for row_index in references.index:
        location_key = (city_keys.at[row_index], province_keys.at[row_index])
        stats = municipality_stats.loc[location_key]
        if int(stats["reference_count"]) < GEODATA_MIN_MUNICIPALITY_STATIONS:
            continue
        distance_km = geodata_distance_km(
            float(latitudes.at[row_index]),
            float(longitudes.at[row_index]),
            float(stats["median_latitude"]),
            float(stats["median_longitude"]),
        )
        if distance_km > GEODATA_MAX_MUNICIPALITY_DISTANCE_KM:
            statuses.at[row_index] = GEODATA_STATUS_QUARANTINED
            reasons.at[row_index] = "municipality_outlier"

    assessed["geodata_status"] = statuses
    assessed["geodata_reason"] = reasons
    return assessed


def import_stations(conn, df_stations: pd.DataFrame) -> dict[int, int]:
    station_id_map: dict[int, int] = {}
    skipped_missing_coords = 0
    skipped_missing_address = 0

    if "geodata_status" not in df_stations.columns:
        df_stations = assess_station_coordinate_quality(df_stations)

    with conn.cursor() as cur:
        for _, row in df_stations.iterrows():
            latitude_raw = str(row["Latitudine"]).strip() if pd.notna(row["Latitudine"]) else ""
            longitude_raw = str(row["Longitudine"]).strip() if pd.notna(row["Longitudine"]) else ""

            if not latitude_raw or not longitude_raw:
                skipped_missing_coords += 1
                continue

            mimit_id = int(row["idImpianto"])
            operator = str(row["Gestore"]).strip() if pd.notna(row["Gestore"]) else None
            brand = str(row["Bandiera"]).strip() if pd.notna(row["Bandiera"]) else None
            name = str(row["Nome Impianto"]).strip() if pd.notna(row["Nome Impianto"]) else None
            address = str(row["Indirizzo"]).strip() if pd.notna(row["Indirizzo"]) else ""
            city = str(row["Comune"]).strip() if pd.notna(row["Comune"]) else ""
            province = str(row["Provincia"]).strip() if pd.notna(row["Provincia"]) else ""

            if not address or not city or not province:
                skipped_missing_address += 1
                continue

            latitude = float(latitude_raw)
            longitude = float(longitude_raw)

            cur.execute(
                """
                INSERT INTO stations (
                    mimit_id, name, brand, operator, address, city, province,
                    latitude, longitude, is_active, geodata_status, geodata_reason
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s)
                ON CONFLICT (mimit_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    brand = EXCLUDED.brand,
                    operator = EXCLUDED.operator,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    province = EXCLUDED.province,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    is_active = TRUE,
                    geodata_status = EXCLUDED.geodata_status,
                    geodata_reason = EXCLUDED.geodata_reason,
                    updated_at = NOW()
                RETURNING id;
                """,
                (
                    mimit_id,
                    name,
                    brand,
                    operator,
                    address,
                    city,
                    province,
                    latitude,
                    longitude,
                    str(row["geodata_status"]),
                    row["geodata_reason"] if pd.notna(row["geodata_reason"]) else None,
                ),
            )

            station_db_id = cur.fetchone()[0]
            station_id_map[mimit_id] = station_db_id

    print(f"Stazioni saltate per coordinate mancanti: {skipped_missing_coords}")
    print(f"Stazioni saltate per indirizzo/comune/provincia mancanti: {skipped_missing_address}")
    return station_id_map


def clear_prices(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM fuel_prices;")


def update_price_import_metrics(
    monitoring_metrics: dict[str, Any] | None,
    diagnostics: PriceSelectionDiagnostics,
) -> None:
    if monitoring_metrics is None:
        return

    monitoring_metrics.update(
        {
            "prices_for_importable_stations": (
                diagnostics.input_rows - diagnostics.skipped_missing_station
            ),
            "candidate_rows": diagnostics.candidate_rows,
            "final_rows": diagnostics.selected_rows,
            "duplicates_removed": diagnostics.duplicate_rows_removed,
            "final_duplicate_keys": diagnostics.final_duplicate_keys,
            "unknown_fuel_rows": diagnostics.unknown_fuel_rows,
            "unknown_fuels": get_unknown_fuel_details(diagnostics),
            "fuel_type_counts": dict(sorted(diagnostics.final_fuel_type_counts.items())),
            "oldest_reported_at": (
                diagnostics.oldest_reported_at.isoformat()
                if diagnostics.oldest_reported_at
                else None
            ),
            "newest_reported_at": (
                diagnostics.newest_reported_at.isoformat()
                if diagnostics.newest_reported_at
                else None
            ),
        }
    )


def validate_candidate_price_count(
    candidate_rows: int,
    *,
    minimum_rows: int | None = None,
) -> None:
    required_rows = (
        MIMIT_MIN_FINAL_PRICE_ROWS if minimum_rows is None else minimum_rows
    )
    validation_passed = candidate_rows > 0 and candidate_rows >= required_rows
    print(
        "[MIMIT] candidate_validation "
        f"prices={candidate_rows} minimum={required_rows} "
        f"passed={str(validation_passed).lower()}"
    )
    if not validation_passed:
        print(
            "[MIMIT] candidate rejected "
            f"prices={candidate_rows} minimum={required_rows} "
            "existing dataset preserved"
        )
        raise MimitCandidateDatasetRejected(
            "Candidate dataset rejected: "
            f"final price rows {candidate_rows} below required minimum {required_rows}; "
            "existing dataset preserved"
        )


def validate_post_write_price_count(
    written_rows: int,
    candidate_rows: int,
    *,
    minimum_rows: int | None = None,
) -> None:
    required_rows = (
        MIMIT_MIN_FINAL_PRICE_ROWS if minimum_rows is None else minimum_rows
    )
    validation_passed = (
        written_rows > 0
        and written_rows >= required_rows
        and written_rows == candidate_rows
    )
    print(
        "[MIMIT] post_write_validation "
        f"written_rows={written_rows} candidate_rows={candidate_rows} "
        f"minimum={required_rows} passed={str(validation_passed).lower()}"
    )
    if not validation_passed:
        raise MimitDatabaseWriteValidationError(
            "Database write validation failed: "
            f"wrote {written_rows} price rows for {candidate_rows} candidates "
            f"with required minimum {required_rows}"
        )


def import_prices(
    conn,
    df_prices: pd.DataFrame,
    station_id_map: dict[int, int],
    monitoring_metrics: dict[str, Any] | None = None,
    *,
    prepared_prices: tuple[
        list[NormalizedMimitPrice], PriceSelectionDiagnostics
    ] | None = None,
) -> int:
    selected_prices, diagnostics = prepared_prices or prepare_prices_for_import(
        df_prices,
        station_id_map,
    )
    update_price_import_metrics(monitoring_metrics, diagnostics)

    with conn.cursor() as cur:
        for item in selected_prices:
            cur.execute(
                """
                INSERT INTO fuel_prices (
                    station_id, fuel_type, price, is_self_service, reported_at, source
                )
                VALUES (%s, %s, %s, %s, %s, 'mimit');
                """,
                (
                    station_id_map[item.mimit_id],
                    item.fuel_type,
                    item.price,
                    item.is_self_service,
                    item.reported_at,
                ),
            )

    unknown_names = ", ".join(
        f"{name}={count}"
        for name, count in sorted(
            diagnostics.unknown_fuel_names.items(),
            key=lambda item: (-item[1], item[0].casefold()),
        )
    )
    print(
        "[MIMIT] price_selection "
        f"input_rows={diagnostics.input_rows} "
        f"candidate_rows={diagnostics.candidate_rows} "
        f"selected_rows={diagnostics.selected_rows} "
        f"duplicate_rows_removed={diagnostics.duplicate_rows_removed} "
        f"collision_groups={diagnostics.collision_groups} "
        f"skipped_missing_station={diagnostics.skipped_missing_station} "
        f"skipped_invalid_price={diagnostics.skipped_invalid_price} "
        f"unknown_fuel_rows={diagnostics.unknown_fuel_rows}"
    )
    if unknown_names:
        print(f"[MIMIT] unknown_fuel_names {unknown_names}")

    return diagnostics.selected_rows


def import_local_mimit_files(
    monitoring_metrics: dict[str, Any] | None = None,
) -> dict[str, object]:
    conn = get_connection()
    metrics = monitoring_metrics if monitoring_metrics is not None else {}

    try:
        print("Caricamento anagrafica...")
        df_stations = load_stations_dataframe()
        if df_stations.empty:
            raise RuntimeError("Anagrafica CSV vuota o non valida")
        df_stations = assess_station_coordinate_quality(df_stations)
        metrics["stations_csv"] = len(df_stations)
        metrics["stations_geodata_quarantined"] = int(
            (
                (df_stations["geodata_status"] == GEODATA_STATUS_QUARANTINED)
                & (df_stations["geodata_reason"] != "invalid_coordinate")
            ).sum()
        )
        print(f"Stazioni lette dal CSV: {len(df_stations)}")
        print(
            "[MIMIT] geodata_quality "
            f"quarantined_count={metrics['stations_geodata_quarantined']}"
        )

        print("Caricamento prezzi...")
        df_prices = load_prices_dataframe()
        if df_prices.empty:
            raise RuntimeError("Prezzi CSV vuoto o non valido")
        metrics["prices_csv"] = len(df_prices)
        metrics["source_extraction_date"] = read_mimit_extraction_date(PREZZI_PATH)
        print(f"Prezzi letti dal CSV: {len(df_prices)}")
        max_reported_at_csv = get_prices_csv_max_reported_at(df_prices)
        print(f"Max dtComu nel CSV prezzi: {max_reported_at_csv.isoformat() if max_reported_at_csv else None}")

        print("Import stazioni nel database...")
        station_id_map = import_stations(conn, df_stations)
        if not station_id_map:
            raise RuntimeError("Nessuna stazione importata: import interrotto")
        metrics["stations_imported"] = len(station_id_map)
        metrics["stations_excluded"] = max(0, len(df_stations) - len(station_id_map))
        print(f"Stazioni importate/aggiornate: {len(station_id_map)}")

        print("Preparazione e validazione dataset prezzi candidato...")
        prepared_prices = prepare_prices_for_import(df_prices, station_id_map)
        candidate_final_price_rows = prepared_prices[1].selected_rows
        update_price_import_metrics(metrics, prepared_prices[1])
        validate_candidate_price_count(candidate_final_price_rows)

        print(
            "[MIMIT] replace_started "
            f"candidate_rows={candidate_final_price_rows}"
        )
        print("Pulizia prezzi esistenti...")
        clear_prices(conn)

        print("Import prezzi nel database...")
        imported_prices = import_prices(
            conn,
            df_prices,
            station_id_map,
            monitoring_metrics=metrics,
            prepared_prices=prepared_prices,
        )
        print(f"Prezzi importati: {imported_prices}")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MAX(reported_at) FROM fuel_prices;")
            post_write_count, max_reported_at_imported = cur.fetchone()
        validate_post_write_price_count(
            int(post_write_count),
            candidate_final_price_rows,
        )
        print(
            "Max reported_at importato in fuel_prices: "
            f"{max_reported_at_imported.isoformat() if max_reported_at_imported else None}"
        )

        conn.commit()
        print("Import completato con successo.")

        return {
            "stations_csv": len(df_stations),
            "prices_csv": len(df_prices),
            "stations_imported": len(station_id_map),
            "stations_geodata_quarantined": metrics["stations_geodata_quarantined"],
            "prices_imported": imported_prices,
            "max_reported_at_csv": max_reported_at_csv.isoformat() if max_reported_at_csv else None,
            "max_reported_at_imported": max_reported_at_imported.isoformat() if max_reported_at_imported else None,
        }
    except Exception as exc:
        conn.rollback()
        print(
            "[MIMIT] import_rollback "
            f"reason={exc.__class__.__name__} previous_dataset_preserved=true"
        )
        raise
    finally:
        conn.close()


def update_mimit_data(
    download: bool = True,
    run_id: int | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {}
    metrics: dict[str, Any] = {}
    started_at = time.monotonic()

    try:
        if download:
            print("Download dei file MIMIT in corso...")
            result["download"] = download_latest_mimit_files()
            prezzi_download = (
                result["download"].get("prezzi")
                if isinstance(result["download"], dict)
                else None
            )
            if isinstance(prezzi_download, dict):
                print(
                    "[MIMIT] Dataset prezzi scaricato: "
                    f"last_modified={prezzi_download.get('last_modified')} "
                    f"size_bytes={prezzi_download.get('size_bytes')} "
                    f"path={prezzi_download.get('path')}"
                )
            print("Download completato.")

        result["import"] = import_local_mimit_files(monitoring_metrics=metrics)
    except Exception as exc:
        metrics["error_type"] = exc.__class__.__name__
        summary = build_mimit_import_summary(
            metrics,
            run_id=run_id,
            status="failed",
            duration_seconds=time.monotonic() - started_at,
        )
        log_mimit_import_summary(summary)
        raise

    summary = build_mimit_import_summary(
        metrics,
        run_id=run_id,
        status="success",
        duration_seconds=time.monotonic() - started_at,
    )
    log_mimit_import_summary(summary)
    result["monitoring"] = summary
    return result


if __name__ == "__main__":
    summary = update_mimit_data(download=False)
    print(summary)
