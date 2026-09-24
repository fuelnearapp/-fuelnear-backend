"""Official road-network averages; independent of station-price ingestion."""

from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal
from html.parser import HTMLParser
import logging
import re
import ssl
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zoneinfo import ZoneInfo

import certifi
from psycopg2.extras import RealDictCursor

from app.db import get_connection
from app.import_mimit import (
    MIMIT_DOWNLOAD_MAX_ATTEMPTS,
    MIMIT_DOWNLOAD_TIMEOUT_SECONDS,
    MIMIT_RETRYABLE_HTTP_STATUSES,
    get_download_backoff_seconds,
)

SOURCE_URL = "https://www.mimit.gov.it/it/prezzi-carburanti-media-nazionale"
MAX_HTML_BYTES = 1_000_000
ROME = ZoneInfo("Europe/Rome")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Price:
    current: Decimal
    previous: Decimal
    change: Decimal


@dataclass(frozen=True)
class Snapshot:
    reference_date: date
    benzina: Price
    gasolio: Price


class _PageParser(HTMLParser):
    """Collect headings, paragraphs and table cells, not navigation links."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.blocks = []
        self.capture = None
        self.text = []
        self.rows = None
        self.row = None
        self.cell = None
        self.invalid = False
        self.table_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.table_depth += 1
            if self.rows is not None:
                self.invalid = True
                return
            self.invalid = False
            self.rows = []
            self.row = None
            self.cell = None
            self.capture = None
        elif self.table_depth > 1:
            return
        elif self.rows is not None:
            if tag == "tr":
                if self.row is not None:
                    self.invalid = True
                self.row = []
            elif tag in {"td", "th"}:
                if self.cell is not None or self.row is None:
                    self.invalid = True
                if any(k in {"rowspan", "colspan"} and v != "1" for k, v in attrs):
                    self.invalid = True
                self.cell = tag
                self.text = []
        elif tag in {"h1", "h2", "h3", "h4", "h5", "h6", "p"}:
            self.capture = tag
            self.text = []

    def handle_data(self, data):
        if self.capture or self.cell:
            self.text.append(data)

    def handle_endtag(self, tag):
        if self.table_depth > 1:
            if tag == "table":
                self.table_depth -= 1
            return
        if self.rows is not None:
            if tag in {"th", "td"}:
                if self.cell != tag or self.row is None:
                    self.invalid = True
                else:
                    self.row.append(" ".join("".join(self.text).split()))
                self.cell = None
            elif tag == "tr":
                if self.row is None or self.cell is not None:
                    self.invalid = True
                else:
                    self.rows.append(self.row)
                self.row = None
            elif tag == "table":
                if self.row is not None or self.cell is not None:
                    self.invalid = True
                self.blocks.append(("table", (self.rows, self.invalid)))
                self.rows = None
                self.table_depth = 0
        elif tag == self.capture:
            self.blocks.append((tag, " ".join("".join(self.text).split())))
            self.capture = None


def parse_snapshot(html: str) -> Snapshot:
    parser = _PageParser()
    parser.feed(html)
    parser.close()
    if parser.rows is not None:
        parser.blocks.append(("table", (parser.rows, True)))
    sections = [i for i, (tag, text) in enumerate(parser.blocks)
                if tag.startswith("h") and text.casefold() == "prezzi rete stradale"]
    if len(sections) != 1:
        raise ValueError("Missing or ambiguous road-network section")
    section = []
    for tag, value in parser.blocks[sections[0] + 1:]:
        if tag.startswith("h"):
            break
        section.append((tag, value))
    metadata = [value for tag, value in section if tag == "p" and value]
    tables = [value for tag, value in section if tag == "table"]
    if len(metadata) != 1 or len(tables) != 1:
        raise ValueError("Incomplete or ambiguous road-network data")
    match = re.fullmatch(
        r"Medie dei prezzi in modalit\u00e0 Self - Aggiornamento (\d{2}-\d{2}-\d{4})",
        metadata[0], re.IGNORECASE,
    )
    if not match:
        raise ValueError("Unsupported service mode or reference date")
    reference_date = datetime.strptime(match[1], "%d-%m-%Y").date()
    rows, invalid = tables[0]
    if invalid:
        raise ValueError("Malformed road-network table")
    if len(rows) != 3 or [s.upper() for s in rows[0]] != [
        "TIPOLOGIA", "OGGI", "IERI", "DIFFERENZA"
    ]:
        raise ValueError("Unexpected national-average columns or row count")
    prices = {}
    for row in rows[1:]:
        if len(row) != 4:
            raise ValueError("Incomplete price row")
        fuel = row[0].casefold()
        if fuel not in {"benzina", "gasolio"} or fuel in prices:
            raise ValueError("Unexpected or duplicate fuel")
        if not all(re.fullmatch(r"[+-]?\d+[.,]\d{3}", x) for x in row[1:]):
            raise ValueError("Invalid price precision or format")
        current, previous, change = (Decimal(x.replace(",", ".")) for x in row[1:])
        if not (0 < current < 100 and 0 < previous < 100):
            raise ValueError("Invalid fuel price")
        if current - previous != change:
            raise ValueError("Inconsistent published difference")
        prices[fuel] = Price(current, previous, change)
    return Snapshot(reference_date, prices["benzina"], prices["gasolio"])


def ensure_mimit_national_averages_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mimit_national_average_snapshot (
                id SMALLINT PRIMARY KEY CHECK (id = 1),
                reference_date DATE NOT NULL,
                fetch_started_at TIMESTAMPTZ NOT NULL,
                acquired_at TIMESTAMPTZ NOT NULL,
                benzina_current NUMERIC(6,3) NOT NULL CHECK (benzina_current > 0 AND benzina_current < 100),
                benzina_previous NUMERIC(6,3) NOT NULL CHECK (benzina_previous > 0 AND benzina_previous < 100),
                benzina_change NUMERIC(6,3) NOT NULL,
                gasolio_current NUMERIC(6,3) NOT NULL CHECK (gasolio_current > 0 AND gasolio_current < 100),
                gasolio_previous NUMERIC(6,3) NOT NULL CHECK (gasolio_previous > 0 AND gasolio_previous < 100),
                gasolio_change NUMERIC(6,3) NOT NULL,
                CHECK (benzina_current - benzina_previous = benzina_change),
                CHECK (gasolio_current - gasolio_previous = gasolio_change),
                CHECK (acquired_at >= fetch_started_at)
            );
        """)


def save_snapshot(conn, snapshot: Snapshot, *, fetch_started_at: datetime,
                  acquired_at: datetime) -> bool:
    # Latest-started fetch wins same-date corrections, even if responses reorder.
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO mimit_national_average_snapshot AS old
                (id, reference_date, fetch_started_at, acquired_at,
                 benzina_current, benzina_previous, benzina_change,
                 gasolio_current, gasolio_previous, gasolio_change)
            VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                reference_date = EXCLUDED.reference_date,
                fetch_started_at = EXCLUDED.fetch_started_at,
                acquired_at = EXCLUDED.acquired_at,
                benzina_current = EXCLUDED.benzina_current,
                benzina_previous = EXCLUDED.benzina_previous,
                benzina_change = EXCLUDED.benzina_change,
                gasolio_current = EXCLUDED.gasolio_current,
                gasolio_previous = EXCLUDED.gasolio_previous,
                gasolio_change = EXCLUDED.gasolio_change
            WHERE EXCLUDED.reference_date > old.reference_date
               OR (EXCLUDED.reference_date = old.reference_date
                   AND EXCLUDED.fetch_started_at > old.fetch_started_at)
            RETURNING id;
        """, (snapshot.reference_date, fetch_started_at, acquired_at,
              snapshot.benzina.current, snapshot.benzina.previous, snapshot.benzina.change,
              snapshot.gasolio.current, snapshot.gasolio.previous, snapshot.gasolio.change))
        return cur.fetchone() is not None


def fetch_snapshot() -> Snapshot:
    context = ssl.create_default_context(cafile=certifi.where())
    for attempt in range(1, MIMIT_DOWNLOAD_MAX_ATTEMPTS + 1):
        try:
            with urlopen(SOURCE_URL, timeout=MIMIT_DOWNLOAD_TIMEOUT_SECONDS,
                         context=context) as response:
                if response.headers.get_content_type() != "text/html":
                    raise ValueError("Unexpected national-average content type")
                raw = response.read(MAX_HTML_BYTES + 1)
                if not raw or len(raw) > MAX_HTML_BYTES:
                    raise ValueError("Empty or oversized national-average response")
                return parse_snapshot(raw.decode("utf-8"))
        except HTTPError as exc:
            if exc.code not in MIMIT_RETRYABLE_HTTP_STATUSES or attempt == MIMIT_DOWNLOAD_MAX_ATTEMPTS:
                raise
        except (URLError, TimeoutError, OSError):
            if attempt == MIMIT_DOWNLOAD_MAX_ATTEMPTS:
                raise
        logger.warning("MIMIT national-average fetch retry attempt=%s", attempt)
        time.sleep(get_download_backoff_seconds(attempt))
    raise RuntimeError("National-average fetch attempts exhausted")


def refresh_snapshot() -> bool:
    started_at = datetime.now(timezone.utc)
    snapshot = fetch_snapshot()
    acquired_at = datetime.now(timezone.utc)
    if snapshot.reference_date > acquired_at.astimezone(ROME).date():
        raise ValueError("Future national-average reference date")
    conn = get_connection()
    try:
        with conn:
            accepted = save_snapshot(conn, snapshot, fetch_started_at=started_at,
                                     acquired_at=acquired_at)
        logger.info("MIMIT national-average refresh accepted=%s reference_date=%s",
                    accepted, snapshot.reference_date)
        return accepted
    finally:
        conn.close()


def read_snapshot(conn, *, now: datetime | None = None) -> dict | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM mimit_national_average_snapshot WHERE id = 1;")
        row = cur.fetchone()
    if row is None:
        return None
    now = now or datetime.now(timezone.utc)
    # A DATE has no publication time: age starts at midnight in Italy, in UTC elapsed hours.
    reference_start = datetime.combine(row["reference_date"], datetime_time.min, ROME)
    stale = now.astimezone(timezone.utc) - reference_start.astimezone(timezone.utc) > timedelta(hours=48)
    return {
        "source": "MIMIT", "network": "road",
        "reference_date": row["reference_date"].isoformat(),
        "updated_at": row["acquired_at"].astimezone(timezone.utc).isoformat(),
        "stale": stale,
        "prices": [{
            "fuel_type": fuel, "service_mode": "self",
            "average_price": row[f"{fuel}_current"],
            "previous_price": row[f"{fuel}_previous"],
            "change": row[f"{fuel}_change"],
            # API convention, not an extracted HTML field.
            "unit": "EUR/L",
        } for fuel in ("benzina", "gasolio")],
    }
