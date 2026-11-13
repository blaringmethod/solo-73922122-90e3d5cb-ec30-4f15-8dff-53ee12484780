"""SQLite persistence layer.

Design points
-------------
* WAL journal + a single dedicated writer thread (the MQTT worker) means
  writes are serialised while API read connections never block on a writer.
* The business key ``(instrument_id, batch_no, sample_seq)`` has a UNIQUE
  constraint, so re-delivery of the exact same message can never create a
  second sample -- de-duplication never relies on the MQTT packet id.
* Conflicts carry their own UNIQUE constraint that includes the incoming
  values, so a re-delivered conflicting message is recorded exactly once,
  while genuinely different conflicting values are all retained. The original
  sample row is never modified.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS samples (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id TEXT NOT NULL,
    batch_no      TEXT NOT NULL,
    sample_seq    INTEGER NOT NULL,
    ts_utc        TEXT NOT NULL,
    temperature_c REAL NOT NULL,
    ph            REAL NOT NULL,
    payload       TEXT NOT NULL,
    stored_at     TEXT NOT NULL,
    UNIQUE (instrument_id, batch_no, sample_seq)
);

CREATE TABLE IF NOT EXISTS conflicts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id    TEXT NOT NULL,
    batch_no         TEXT NOT NULL,
    sample_seq       INTEGER NOT NULL,
    field_name       TEXT NOT NULL,
    stored_value     REAL NOT NULL,
    incoming_value   REAL NOT NULL,
    incoming_ts_utc  TEXT NOT NULL,
    incoming_payload TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    -- Same key + same incoming values + same field => the re-delivery of one
    -- and the same conflicting publish; ignore it. A different value gets a
    -- new row, keeping the full contradiction history.
    UNIQUE (instrument_id, batch_no, sample_seq, field_name,
            incoming_value, incoming_ts_utc),
    FOREIGN KEY (instrument_id, batch_no, sample_seq)
        REFERENCES samples (instrument_id, batch_no, sample_seq)
);

CREATE INDEX IF NOT EXISTS idx_samples_lookup
    ON samples (instrument_id, batch_no);
CREATE INDEX IF NOT EXISTS idx_conflicts_lookup
    ON conflicts (instrument_id, batch_no);
"""

# Fields whose values define "same sample content". Every field is a number in
# this lab protocol, so numeric equality is the intended comparison.
VALUE_FIELDS = ("temperature_c", "ph")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=5.0,
        check_same_thread=False,
        isolation_level=None,  # autocommit; statements below are atomic
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


class Database:
    """Thin data-access wrapper.

    One instance is shared by the writer thread (which owns the long-lived
    writer connection) and the FastAPI handlers (which open short-lived read
    connections, safe under WAL).
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._writer_lock = threading.Lock()
        self._writer = _connect(db_path)
        self._writer.executescript(SCHEMA)

    # -- write side (single worker thread, additionally guarded) -----------

    def store_measurement(self, payload: dict[str, Any]) -> str:
        """Persist one validated payload.

        Returns one of:
          ``stored``              - new sample inserted
          ``duplicate``           - same key, identical field values
          ``conflict``            - differing values; conflict row(s) added
          ``conflict_duplicate``  - that same conflict was already recorded
        """
        key = (
            payload["instrument_id"],
            payload["batch_no"],
            int(payload["sample_seq"]),
        )
        ts = payload["ts_utc"]
        incoming_temp = float(payload["temperature_c"])
        incoming_ph = float(payload["ph"])
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        now = utc_now_iso()

        with self._writer_lock:
            row = self._writer.execute(
                "SELECT id, temperature_c, ph FROM samples "
                "WHERE instrument_id=? AND batch_no=? AND sample_seq=?",
                key,
            ).fetchone()

            if row is None:
                # First sighting of this business key. The UNIQUE constraint is
                # the hard guarantee even if two writers ever raced.
                self._writer.execute(
                    "INSERT INTO samples (instrument_id, batch_no, sample_seq, "
                    "ts_utc, temperature_c, ph, payload, stored_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (*key, ts, incoming_temp, incoming_ph, raw, now),
                )
                return "stored"

            differing = []
            stored_temp, stored_ph = row["temperature_c"], row["ph"]
            if incoming_temp != stored_temp:
                differing.append(("temperature_c", stored_temp, incoming_temp))
            if incoming_ph != stored_ph:
                differing.append(("ph", stored_ph, incoming_ph))

            if not differing:
                # Same key AND identical values -> plain duplicate, ignored.
                return "duplicate"

            any_new = False
            for field_name, stored_value, incoming_value in differing:
                cursor = self._writer.execute(
                    "INSERT OR IGNORE INTO conflicts (instrument_id, batch_no, "
                    "sample_seq, field_name, stored_value, incoming_value, "
                    "incoming_ts_utc, incoming_payload, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        *key,
                        field_name,
                        stored_value,
                        incoming_value,
                        ts,
                        raw,
                        now,
                    ),
                )
                any_new = any_new or cursor.rowcount > 0

            # The original sample is intentionally left untouched.
            return "conflict" if any_new else "conflict_duplicate"

    # -- read side (FastAPI request threads; fresh read-only connections) --

    def _read_connection(self) -> sqlite3.Connection:
        conn = _connect(self.db_path)
        conn.execute("PRAGMA query_only=ON;")
        return conn

    def read_connection(self) -> sqlite3.Connection:
        """Open a strictly read-only connection (WAL safe).

        Callers own the connection and must close it. The packaging worker
        also uses this factory for its own long-lived snapshot connection so
        it never touches the writer connection and can never take a write
        lock (``PRAGMA query_only`` rejects any statement that would).
        """
        return self._read_connection()

    @staticmethod
    def _filters(
        instrument_id: Optional[str], batch_no: Optional[str]
    ) -> tuple[str, list[Any]]:
        clauses, params = [], []
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if batch_no:
            clauses.append("batch_no = ?")
            params.append(batch_no)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return where, params

    def list_samples(
        self,
        instrument_id: Optional[str],
        batch_no: Optional[str],
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where, params = self._filters(instrument_id, batch_no)
        conn = self._read_connection()
        try:
            rows = conn.execute(
                f"SELECT * FROM samples{where} "
                "ORDER BY instrument_id, batch_no, sample_seq "
                "LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        finally:
            conn.close()
        items = [dict(r) for r in rows]
        for item in items:
            item["payload"] = json.loads(item["payload"])
        return items

    def list_conflicts(
        self,
        instrument_id: Optional[str],
        batch_no: Optional[str],
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        where, params = self._filters(instrument_id, batch_no)
        conn = self._read_connection()
        try:
            rows = conn.execute(
                f"SELECT * FROM conflicts{where} "
                "ORDER BY id "
                "LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        finally:
            conn.close()
        items = [dict(r) for r in rows]
        for item in items:
            item["incoming_payload"] = json.loads(item["incoming_payload"])
        return items

    def count(
        self,
        table: str,
        instrument_id: Optional[str],
        batch_no: Optional[str],
    ) -> int:
        if table not in ("samples", "conflicts"):
            raise ValueError("unknown table")
        where, params = self._filters(instrument_id, batch_no)
        conn = self._read_connection()
        try:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM {table}{where}", params
            ).fetchone()
        finally:
            conn.close()
        return int(row["n"])

    def close(self) -> None:
        with self._writer_lock:
            self._writer.close()
