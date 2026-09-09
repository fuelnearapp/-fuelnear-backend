from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import secrets
from typing import Any
from uuid import UUID

from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor


TELEMETRY_PATH = "/telemetry/admob"


class AdMobTelemetryEvent(str, Enum):
    BANNER_ELIGIBLE = "banner_eligible"
    BANNER_BLOCKED = "banner_blocked"
    BANNER_REQUEST = "banner_request"
    BANNER_RECEIVED = "banner_received"
    BANNER_IMPRESSION = "banner_impression"
    BANNER_FAILED = "banner_failed"


class AdMobTelemetryPlacement(str, Enum):
    HOME = "home"
    INFO = "info"
    FAVORITES = "favorites"
    LOGBOOK = "logbook"


class AdMobTelemetryNetworkType(str, Enum):
    WIFI = "wifi"
    CELLULAR = "cellular"
    OTHER = "other"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class AdMobTelemetryRecord:
    session_id: UUID
    event: AdMobTelemetryEvent
    placement: AdMobTelemetryPlacement
    app_version: str
    build_number: str
    ios_version: str
    network_type: AdMobTelemetryNetworkType
    ump_can_request_ads: bool | None = None
    ump_status: str | None = None
    att_status: str | None = None
    error_domain: str | None = None
    error_code: int | None = None
    response_id_present: bool | None = None
    latency_ms: int | None = None
    reason: str | None = None
    timestamp_client: datetime | None = None


class AdMobTelemetryBodyTooLarge(RuntimeError):
    pass


class AdMobTelemetryRequestGuardMiddleware:
    def __init__(self, app: Any, max_body_bytes: int):
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def _send_error(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        status_code: int,
        error_code: str,
        message: str,
    ) -> None:
        response = JSONResponse(
            status_code=status_code,
            content={
                "error_code": error_code,
                "message": message,
                "detail": message,
            },
        )
        await response(scope, receive, send)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if (
            scope.get("type") != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != TELEMETRY_PATH
        ):
            await self.app(scope, receive, send)
            return

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            await self._send_error(
                scope,
                receive,
                send,
                415,
                "UNSUPPORTED_MEDIA_TYPE",
                "Content-Type must be application/json",
            )
            return

        content_length = headers.get("content-length")
        if content_length is not None:
            try:
                declared_size = int(content_length)
            except ValueError:
                await self._send_error(
                    scope,
                    receive,
                    send,
                    400,
                    "INVALID_REQUEST",
                    "Invalid request",
                )
                return
            if declared_size < 0 or declared_size > self.max_body_bytes:
                await self._send_error(
                    scope,
                    receive,
                    send,
                    413,
                    "REQUEST_TOO_LARGE",
                    "Request body is too large",
                )
                return

        received_size = 0

        async def limited_receive() -> Any:
            nonlocal received_size
            message = await receive()
            if message.get("type") == "http.request":
                received_size += len(message.get("body", b""))
                if received_size > self.max_body_bytes:
                    raise AdMobTelemetryBodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except AdMobTelemetryBodyTooLarge:
            await self._send_error(
                scope,
                receive,
                send,
                413,
                "REQUEST_TOO_LARGE",
                "Request body is too large",
            )


def ensure_admob_telemetry_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admob_telemetry_events (
                id BIGSERIAL PRIMARY KEY,
                session_id UUID NOT NULL,
                event TEXT NOT NULL,
                placement TEXT NOT NULL,
                app_version TEXT NOT NULL,
                build_number TEXT NOT NULL,
                ios_version TEXT NOT NULL,
                network_type TEXT NOT NULL,
                ump_can_request_ads BOOLEAN NULL,
                ump_status TEXT NULL,
                att_status TEXT NULL,
                error_domain TEXT NULL,
                error_code INTEGER NULL,
                response_id_present BOOLEAN NULL,
                latency_ms INTEGER NULL,
                reason TEXT NULL,
                timestamp_client TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT admob_telemetry_event_check CHECK (
                    event IN (
                        'banner_eligible',
                        'banner_blocked',
                        'banner_request',
                        'banner_received',
                        'banner_impression',
                        'banner_failed'
                    )
                ),
                CONSTRAINT admob_telemetry_placement_check CHECK (
                    placement IN ('home', 'info', 'favorites', 'logbook')
                ),
                CONSTRAINT admob_telemetry_network_type_check CHECK (
                    network_type IN ('wifi', 'cellular', 'other', 'unknown')
                ),
                CONSTRAINT admob_telemetry_latency_check CHECK (
                    latency_ms IS NULL OR latency_ms BETWEEN 0 AND 300000
                )
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_admob_telemetry_created_at
            ON admob_telemetry_events(created_at DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_admob_telemetry_session_created_at
            ON admob_telemetry_events(session_id, created_at);
            """
        )


def insert_admob_telemetry_event(
    conn: Any,
    record: AdMobTelemetryRecord,
    *,
    retention_days: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO admob_telemetry_events (
                session_id,
                event,
                placement,
                app_version,
                build_number,
                ios_version,
                network_type,
                ump_can_request_ads,
                ump_status,
                att_status,
                error_domain,
                error_code,
                response_id_present,
                latency_ms,
                reason,
                timestamp_client
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            );
            """,
            (
                str(record.session_id),
                record.event.value,
                record.placement.value,
                record.app_version,
                record.build_number,
                record.ios_version,
                record.network_type.value,
                record.ump_can_request_ads,
                record.ump_status,
                record.att_status,
                record.error_domain,
                record.error_code,
                record.response_id_present,
                record.latency_ms,
                record.reason,
                record.timestamp_client,
            ),
        )

        if secrets.randbelow(100) == 0:
            cur.execute(
                """
                DELETE FROM admob_telemetry_events
                WHERE created_at < NOW() - (%s * INTERVAL '1 day');
                """,
                (retention_days,),
            )


def _breakdown(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return {
        str(row[key]) if row[key] is not None else "unknown": int(row["event_count"])
        for row in rows
    }


def get_admob_telemetry_summary(conn: Any, *, since: datetime) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT session_id) AS total_sessions,
                COUNT(*) AS total_events,
                COUNT(*) FILTER (WHERE event = 'banner_request') AS request_count,
                COUNT(*) FILTER (WHERE event = 'banner_received') AS received_count,
                COUNT(*) FILTER (WHERE event = 'banner_impression') AS impression_count,
                COUNT(*) FILTER (WHERE event = 'banner_failed') AS failed_count,
                COUNT(*) FILTER (WHERE event = 'banner_blocked') AS blocked_count
            FROM admob_telemetry_events
            WHERE created_at >= %s;
            """,
            (since,),
        )
        totals = dict(cur.fetchone())

        cur.execute(
            """
            SELECT error_code, COUNT(*) AS event_count
            FROM admob_telemetry_events
            WHERE created_at >= %s
              AND error_code IS NOT NULL
            GROUP BY error_code
            ORDER BY event_count DESC, error_code;
            """,
            (since,),
        )
        by_error_code = _breakdown(
            [dict(row) for row in cur.fetchall()],
            "error_code",
        )

        cur.execute(
            """
            SELECT network_type, COUNT(*) AS event_count
            FROM admob_telemetry_events
            WHERE created_at >= %s
            GROUP BY network_type
            ORDER BY event_count DESC, network_type;
            """,
            (since,),
        )
        by_network_type = _breakdown(
            [dict(row) for row in cur.fetchall()],
            "network_type",
        )

        cur.execute(
            """
            SELECT placement, COUNT(*) AS event_count
            FROM admob_telemetry_events
            WHERE created_at >= %s
            GROUP BY placement
            ORDER BY event_count DESC, placement;
            """,
            (since,),
        )
        by_placement = _breakdown(
            [dict(row) for row in cur.fetchall()],
            "placement",
        )

        cur.execute(
            """
            SELECT
                app_version,
                build_number,
                COUNT(*) AS event_count,
                COUNT(DISTINCT session_id) AS session_count
            FROM admob_telemetry_events
            WHERE created_at >= %s
            GROUP BY app_version, build_number
            ORDER BY event_count DESC, app_version, build_number;
            """,
            (since,),
        )
        by_app_version_build = [dict(row) for row in cur.fetchall()]

    request_count = int(totals["request_count"] or 0)
    received_count = int(totals["received_count"] or 0)
    impression_count = int(totals["impression_count"] or 0)
    return {
        "since": since,
        "total_sessions": int(totals["total_sessions"] or 0),
        "total_events": int(totals["total_events"] or 0),
        "request": request_count,
        "received": received_count,
        "impressions": impression_count,
        "failed": int(totals["failed_count"] or 0),
        "blocked": int(totals["blocked_count"] or 0),
        "request_to_received_percent": (
            round(received_count * 100.0 / request_count, 2)
            if request_count
            else None
        ),
        "received_to_impression_percent": (
            round(impression_count * 100.0 / received_count, 2)
            if received_count
            else None
        ),
        "by_error_code": by_error_code,
        "by_network_type": by_network_type,
        "by_placement": by_placement,
        "by_app_version_build": by_app_version_build,
    }


def get_recent_admob_telemetry_sessions(
    conn: Any,
    *,
    since: datetime,
    limit: int,
    max_events_per_session: int = 50,
) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT session_id, MAX(created_at) AS last_event_at
            FROM admob_telemetry_events
            WHERE created_at >= %s
            GROUP BY session_id
            ORDER BY last_event_at DESC
            LIMIT %s;
            """,
            (since, limit),
        )
        latest_sessions = [dict(row) for row in cur.fetchall()]
        if not latest_sessions:
            return []

        session_ids = [str(row["session_id"]) for row in latest_sessions]
        cur.execute(
            """
            WITH ranked_events AS (
                SELECT
                    session_id,
                    event,
                    placement,
                    app_version,
                    build_number,
                    ios_version,
                    network_type,
                    ump_can_request_ads,
                    ump_status,
                    att_status,
                    error_domain,
                    error_code,
                    response_id_present,
                    latency_ms,
                    reason,
                    timestamp_client,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY session_id
                        ORDER BY created_at DESC, id DESC
                    ) AS event_rank
                FROM admob_telemetry_events
                WHERE created_at >= %s
                  AND session_id = ANY(%s::uuid[])
            )
            SELECT *
            FROM ranked_events
            WHERE event_rank <= %s
            ORDER BY session_id, created_at ASC;
            """,
            (since, session_ids, max_events_per_session),
        )
        events = [dict(row) for row in cur.fetchall()]

    events_by_session: dict[str, list[dict[str, Any]]] = {
        session_id: [] for session_id in session_ids
    }
    for event in events:
        session_id = str(event.pop("session_id"))
        event.pop("event_rank", None)
        events_by_session[session_id].append(event)

    return [
        {
            "session_id": str(row["session_id"]),
            "last_event_at": row["last_event_at"],
            "events": events_by_session[str(row["session_id"])],
        }
        for row in latest_sessions
    ]
