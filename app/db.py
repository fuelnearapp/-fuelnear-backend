from __future__ import annotations

from typing import Any
import os
import threading
import time
from urllib.parse import urlparse

import psycopg2
from psycopg2 import pool


DB_NAME = os.getenv("DB_NAME", "fuelnear")
DB_USER = os.getenv("DB_USER", "matteo")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DATABASE_URL = os.getenv("DATABASE_URL")

DB_POOL_MIN_CONNECTIONS = max(1, int(os.getenv("DB_POOL_MIN_CONNECTIONS", "1")))
DB_POOL_MAX_CONNECTIONS = max(DB_POOL_MIN_CONNECTIONS, int(os.getenv("DB_POOL_MAX_CONNECTIONS", "5")))
DB_POOL_ACQUIRE_TIMEOUT_SECONDS = min(
    30.0,
    max(0.1, float(os.getenv("DB_POOL_ACQUIRE_TIMEOUT_SECONDS", "3"))),
)
DB_CONNECT_TIMEOUT_SECONDS = max(1, int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "10")))
DB_STATEMENT_TIMEOUT_MS = max(0, int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "0")))
DB_POOL_LOG_CONNECTIONS = (os.getenv("DB_POOL_LOG_CONNECTIONS") or "").strip().lower() in {"1", "true", "yes", "on"}

_connection_pool: pool.ThreadedConnectionPool | None = None
_connection_pool_lock = threading.Lock()
_connection_slots: threading.BoundedSemaphore | None = None


class DatabasePoolExhausted(RuntimeError):
    pass


def _acquire_raw_connection(
    owner_pool: pool.ThreadedConnectionPool,
    connection_slots: threading.BoundedSemaphore,
    timeout_seconds: float,
) -> Any:
    wait_started_at = time.monotonic()
    if not connection_slots.acquire(timeout=timeout_seconds):
        waited_ms = int((time.monotonic() - wait_started_at) * 1000)
        print(
            "[DB] db_pool_exhausted=true "
            f"acquire_timeout=true waited_ms={waited_ms}"
        )
        raise DatabasePoolExhausted("Database connection pool acquire timed out")

    try:
        raw_connection = owner_pool.getconn()
    except pool.PoolError as exc:
        connection_slots.release()
        print("[DB] db_pool_exhausted=true acquire_timeout=false")
        raise DatabasePoolExhausted("Database connection pool exhausted") from exc
    except Exception:
        connection_slots.release()
        raise

    waited_ms = int((time.monotonic() - wait_started_at) * 1000)
    if waited_ms >= 10:
        print(f"[DB] db_connection_waited=true waited_ms={waited_ms}")
    if DB_POOL_LOG_CONNECTIONS:
        print("[DB] db_connection_acquired=true")
    return raw_connection


class PooledConnection:
    def __init__(
        self,
        raw_connection: Any,
        owner_pool: pool.ThreadedConnectionPool,
        connection_slots: threading.BoundedSemaphore,
        acquire_timeout_seconds: float,
    ):
        self._raw_connection = raw_connection
        self._owner_pool = owner_pool
        self._connection_slots = connection_slots
        self._acquire_timeout_seconds = acquire_timeout_seconds
        self._returned = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw_connection, name)

    def __enter__(self):
        if self._returned:
            self._raw_connection = _acquire_raw_connection(
                self._owner_pool,
                self._connection_slots,
                self._acquire_timeout_seconds,
            )
            self._returned = False
        try:
            self._raw_connection.__enter__()
        except Exception:
            self.close()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            return self._raw_connection.__exit__(exc_type, exc, tb)
        finally:
            self.close()

    def close(self) -> None:
        if self._returned:
            return

        self._returned = True
        try:
            if not self._raw_connection.closed:
                try:
                    self._raw_connection.rollback()
                except Exception:
                    pass
                self._owner_pool.putconn(self._raw_connection)
                if DB_POOL_LOG_CONNECTIONS:
                    print("[DB] db_connection_returned=true")
            else:
                self._owner_pool.putconn(self._raw_connection, close=True)
                if DB_POOL_LOG_CONNECTIONS:
                    print("[DB] db_connection_returned=false closed=true")
        except Exception:
            try:
                self._raw_connection.close()
            except Exception:
                pass
            raise
        finally:
            self._connection_slots.release()


def build_connection_kwargs() -> dict[str, Any]:
    if DATABASE_URL:
        parsed = urlparse(DATABASE_URL)
        connection_kwargs: dict[str, Any] = {
            "dbname": parsed.path.lstrip("/"),
            "user": parsed.username,
            "password": parsed.password,
            "host": parsed.hostname,
            "port": parsed.port,
        }
    else:
        connection_kwargs = {
            "dbname": DB_NAME,
            "user": DB_USER,
            "host": DB_HOST,
            "port": DB_PORT,
        }
        if DB_PASSWORD:
            connection_kwargs["password"] = DB_PASSWORD

    connection_kwargs["connect_timeout"] = DB_CONNECT_TIMEOUT_SECONDS
    if DB_STATEMENT_TIMEOUT_MS > 0:
        connection_kwargs["options"] = f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS}"

    return connection_kwargs


def get_connection_pool() -> pool.ThreadedConnectionPool:
    global _connection_pool, _connection_slots
    if _connection_pool is None:
        with _connection_pool_lock:
            if _connection_pool is None:
                _connection_pool = pool.ThreadedConnectionPool(
                    DB_POOL_MIN_CONNECTIONS,
                    DB_POOL_MAX_CONNECTIONS,
                    **build_connection_kwargs(),
                )
                _connection_slots = threading.BoundedSemaphore(DB_POOL_MAX_CONNECTIONS)
                print(
                    "[DB] db_pool_initialized=true "
                    f"min={DB_POOL_MIN_CONNECTIONS} max={DB_POOL_MAX_CONNECTIONS} "
                    f"acquire_timeout_seconds={DB_POOL_ACQUIRE_TIMEOUT_SECONDS} "
                    f"connect_timeout_seconds={DB_CONNECT_TIMEOUT_SECONDS} "
                    f"statement_timeout_ms={DB_STATEMENT_TIMEOUT_MS}"
                )

    return _connection_pool


def get_connection(*, timeout_seconds: float | None = None) -> PooledConnection:
    pool_obj = get_connection_pool()
    connection_slots = _connection_slots
    if connection_slots is None:
        raise RuntimeError("Database connection pool capacity is not initialized")

    selected_timeout = (
        DB_POOL_ACQUIRE_TIMEOUT_SECONDS
        if timeout_seconds is None
        else max(0.0, float(timeout_seconds))
    )
    raw_connection = _acquire_raw_connection(
        pool_obj,
        connection_slots,
        selected_timeout,
    )
    return PooledConnection(
        raw_connection,
        pool_obj,
        connection_slots,
        selected_timeout,
    )


def close_connection_pool() -> None:
    global _connection_pool, _connection_slots
    with _connection_pool_lock:
        if _connection_pool is not None:
            _connection_pool.closeall()
            _connection_pool = None
            _connection_slots = None
            print("[DB] db_pool_closed=true")
