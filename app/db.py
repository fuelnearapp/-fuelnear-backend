from __future__ import annotations

from typing import Any
import os
import threading
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
DB_CONNECT_TIMEOUT_SECONDS = max(1, int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "10")))
DB_STATEMENT_TIMEOUT_MS = max(0, int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "0")))
DB_POOL_LOG_CONNECTIONS = (os.getenv("DB_POOL_LOG_CONNECTIONS") or "").strip().lower() in {"1", "true", "yes", "on"}

_connection_pool: pool.ThreadedConnectionPool | None = None
_connection_pool_lock = threading.Lock()


class DatabasePoolExhausted(RuntimeError):
    pass


class PooledConnection:
    def __init__(self, raw_connection: Any, owner_pool: pool.ThreadedConnectionPool):
        self._raw_connection = raw_connection
        self._owner_pool = owner_pool
        self._returned = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw_connection, name)

    def __enter__(self):
        if self._returned:
            self._raw_connection = self._owner_pool.getconn()
            self._returned = False
            if DB_POOL_LOG_CONNECTIONS:
                print("[DB] db_connection_acquired=true")
        self._raw_connection.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        suppress = self._raw_connection.__exit__(exc_type, exc, tb)
        self.close()
        return suppress

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
    global _connection_pool
    if _connection_pool is None:
        with _connection_pool_lock:
            if _connection_pool is None:
                _connection_pool = pool.ThreadedConnectionPool(
                    DB_POOL_MIN_CONNECTIONS,
                    DB_POOL_MAX_CONNECTIONS,
                    **build_connection_kwargs(),
                )
                print(
                    "[DB] db_pool_initialized=true "
                    f"min={DB_POOL_MIN_CONNECTIONS} max={DB_POOL_MAX_CONNECTIONS} "
                    f"connect_timeout_seconds={DB_CONNECT_TIMEOUT_SECONDS} "
                    f"statement_timeout_ms={DB_STATEMENT_TIMEOUT_MS}"
                )

    return _connection_pool


def get_connection() -> PooledConnection:
    pool_obj = get_connection_pool()
    try:
        raw_connection = pool_obj.getconn()
    except pool.PoolError as exc:
        print("[DB] db_pool_exhausted=true")
        raise DatabasePoolExhausted("Database connection pool exhausted") from exc

    if DB_POOL_LOG_CONNECTIONS:
        print("[DB] db_connection_acquired=true")
    return PooledConnection(raw_connection, pool_obj)


def close_connection_pool() -> None:
    global _connection_pool
    with _connection_pool_lock:
        if _connection_pool is not None:
            _connection_pool.closeall()
            _connection_pool = None
            print("[DB] db_pool_closed=true")
