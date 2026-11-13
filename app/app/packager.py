"""Offline-delivery packaging: snapshot-consistent, streamed ZIP jobs.

Guarantees implemented here
---------------------------
* **One job per process.** A single packaging slot is held for the whole
  lifecycle; a second ``POST /packages`` is rejected until the slot is free.
* **One committed snapshot for both tables.** The worker opens *one*
  read-only SQLite connection and keeps a single deferred transaction open
  while streaming both CSVs. Every batch therefore sees the same committed
  database state -- the state at the moment the snapshot transaction started.
  Rows committed by the MQTT writer afterwards are never mixed in. The
  connection is ``query_only`` and only ever runs ``SELECT``\\s, so the
  packager never takes a write lock; MQTT ingestion and API queries proceed
  normally (WAL readers do not block the writer, the writer does not block
  the reader).
* **Batched, streamed, disk-backed.** Rows are fetched in bounded batches
  (``PACKAGE_BATCH_SIZE``) and written straight into two ordinary CSV files
  inside a ZIP on a staging file. :class:`StreamingZipWriter` emits a regular
  ZIP entry per CSV using a local header followed by streamed deflate blocks
  and a data descriptor, so neither a batch nor the whole archive is ever
  held in memory; peak memory is bounded by the configured batch size.
* **Only complete archives are published.** The staged ZIP is CRC-verified
  (``ZipFile.testzip``) and checksummed, then atomically ``os.replace``\\d
  into the published directory. Download only ever serves files from that
  directory belonging to a ``completed`` job. A failure or a cancel before
  the move leaves nothing downloadable.
* **Cancel is settled before it is reported.** Requesting cancellation flips
  a flag; the worker stops at the next checkpoint, closes the DB connection
  and the file, removes the staging data and only then transitions the job
  to ``cancelled``. Repeated cancel requests block on the same single
  settling event. A cancel accepted during the pre-publish window suppresses
  the just-written ZIP -- it is never published.
* **Client disconnects are irrelevant.** The worker runs on its own thread
  and is not tied to any HTTP request; cancelling is only possible through
  the explicit cancel endpoint.
* **Restart safety.** Job metadata is persisted as JSON on every transition.
  On startup, non-terminal jobs are marked ``failed`` and their staging files
  removed; completed packages and their metadata stay on disk and remain
  downloadable.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import struct
import threading
import uuid
import zipfile
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .config import Settings
from .db import Database
from .models import PackageStatus

log = logging.getLogger("archiver.packager")

# The two deliverable documents inside every archive.
SAMPLES_CSV = "samples.csv"
CONFLICTS_CSV = "conflicts.csv"

SAMPLE_COLUMNS = [
    "id",
    "instrument_id",
    "batch_no",
    "sample_seq",
    "ts_utc",
    "temperature_c",
    "ph",
    "payload",
    "stored_at",
]
CONFLICT_COLUMNS = [
    "id",
    "instrument_id",
    "batch_no",
    "sample_seq",
    "field_name",
    "stored_value",
    "incoming_value",
    "incoming_ts_utc",
    "incoming_payload",
    "created_at",
]

_TERMINAL = frozenset(
    {PackageStatus.COMPLETED, PackageStatus.CANCELLED, PackageStatus.FAILED}
)
_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")

# Fixed DOS timestamp (1980-01-01 00:00:00) for deterministic entries.
_DOS_TIME = 0
_DOS_DATE = (1 << 5) | 1  # day 1, month 1, year offset 0 -> 1980


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _safe_label(value: str) -> str:
    """Sanitise an instrument/batch label for use inside a file name."""
    cleaned = _UNSAFE_NAME.sub("_", value).strip("._-")
    return cleaned or "unknown"


# ----------------------------------------------------------- streaming ZIP


class StreamingZipWriter:
    """Minimal ZIP writer that streams entries straight to an open file.

    The stdlib :class:`zipfile.ZipFile` buffers each entry's full bytes before
    writing (it only learns the CRC/sizes when the entry closes), which would
    force the whole CSV into memory. This writer instead emits the standard
    "data descriptor" layout per entry:

    ``[local header, sizes=0] [streamed deflate blocks...] [data descriptor]``

    followed by a central directory at :meth:`close`. Each
    :meth:`write_stream` call appends one compressed block immediately, so
    memory use stays flat regardless of table size. The output is a normal
    ZIP: it is re-opened with the stdlib ``ZipFile`` for verification and is
    readable by every ordinary unzip implementation.
    """

    _LOCAL_FILE_HEADER = 0x04034B50
    _DATA_DESCRIPTOR = 0x08074B50
    _CENTRAL_DIR_HEADER = 0x02014B50
    _ZIP64_EXTRA_HEADER_ID = 0x0001
    _END_OF_CENTRAL_DIR = 0x06054B50
    _VERSION_BASE = 20
    _VERSION_ZIP64 = 45
    _FLAG_DATA_DESCRIPTOR = 0x0008
    _FLAG_UTF8 = 0x0800
    _DEFLATE = 8
    # Entries at or above this size carry ZIP64 extra fields / sentinels.
    _ZIP64_LIMIT = 0xFFFFFFFF

    def __init__(self, path: Path) -> None:
        self._file = open(path, mode="wb", buffering=1024 * 1024)
        self._entries: list[dict] = []
        self._open_entry: Optional[dict] = None
        self._closed = False

    # ------------------------------------------------------------- entries

    def start_entry(self, name: str) -> None:
        """Begin a new entry; subsequent write_stream blocks belong to it."""
        if self._closed:
            raise RuntimeError("zip writer is closed")
        if self._open_entry is not None:
            raise RuntimeError(f"entry {self._open_entry['name']!r} still open")
        name_bytes = name.encode("utf-8")
        offset = self._file.tell()
        # Every entry is written with a ZIP64 extra field carrying the size
        # placeholders. Streaming means the real sizes are unknown until the
        # entry closes; the extra field (with a matching data descriptor and
        # central record) makes even >4 GiB entries unambiguous and keeps the
        # hot path branch-free. Standard unzip tooling accepts this layout.
        extra = struct.pack(
            "<HHQQ", self._ZIP64_EXTRA_HEADER_ID, 16, 0, 0
        )
        header = struct.pack(
            "<IHHHHHIIIHH",
            self._LOCAL_FILE_HEADER,
            self._VERSION_ZIP64,
            self._FLAG_DATA_DESCRIPTOR | self._FLAG_UTF8,
            self._DEFLATE,
            _DOS_TIME,
            _DOS_DATE,
            0,  # crc32 -> data descriptor
            self._ZIP64_LIMIT,  # sizes -> ZIP64 extra / descriptor
            self._ZIP64_LIMIT,
            len(name_bytes),
            len(extra),
        )
        self._file.write(header)
        self._file.write(name_bytes)
        self._file.write(extra)
        self._open_entry = {
            "name": name,
            "name_bytes": name_bytes,
            "offset": offset,
            "compressor": zlib.compressobj(6, zlib.DEFLATED, -15),
            "crc": 0,
            "compressed_size": 0,
            "uncompressed_size": 0,
        }

    def write_stream(self, data: bytes) -> None:
        """Append a block of plaintext bytes to the currently open entry."""
        if self._open_entry is None:
            raise RuntimeError("no open zip entry")
        entry = self._open_entry
        block = entry["compressor"].compress(data)
        if block:
            self._file.write(block)
            entry["compressed_size"] += len(block)
        entry["crc"] = zlib.crc32(data, entry["crc"]) & 0xFFFFFFFF
        entry["uncompressed_size"] += len(data)

    def close_entry(self) -> None:
        """Flush the final deflate block and write the data descriptor."""
        if self._open_entry is None:
            raise RuntimeError("no open zip entry")
        entry = self._open_entry
        tail = entry["compressor"].flush()
        if tail:
            self._file.write(tail)
            entry["compressed_size"] += len(tail)
        # ZIP64 data descriptor: signature + crc + two 64-bit sizes.
        descriptor = struct.pack(
            "<IIQQ",
            self._DATA_DESCRIPTOR,
            entry["crc"],
            entry["compressed_size"],
            entry["uncompressed_size"],
        )
        self._file.write(descriptor)
        self._entries.append(
            {
                "name": entry["name"],
                "name_bytes": entry["name_bytes"],
                "offset": entry["offset"],
                "crc": entry["crc"],
                "compressed_size": entry["compressed_size"],
                "uncompressed_size": entry["uncompressed_size"],
            }
        )
        self._open_entry = None

    def write_complete(self, name: str, data: bytes) -> None:
        """Convenience for small, fully-materialised entries (the manifest)."""
        self.start_entry(name)
        self.write_stream(data)
        self.close_entry()

    # ------------------------------------------------------------- finalise

    def close(self) -> None:
        if self._closed:
            return
        if self._open_entry is not None:
            raise RuntimeError("entry still open at close()")
        central_start = self._file.tell()
        for entry in self._entries:
            # Central ZIP64 extra field carries the real (possibly >4 GiB)
            # sizes; the fixed header keeps the 0xFFFFFFFF sentinels.
            extra = struct.pack(
                "<HHQQ",
                self._ZIP64_EXTRA_HEADER_ID,
                16,
                entry["uncompressed_size"],
                entry["compressed_size"],
            )
            central_header = struct.pack(
                "<IHHHHHHIIIHHHHHII",
                self._CENTRAL_DIR_HEADER,
                self._VERSION_ZIP64,  # version made by
                self._VERSION_ZIP64,  # version needed
                self._FLAG_DATA_DESCRIPTOR | self._FLAG_UTF8,
                self._DEFLATE,
                _DOS_TIME,
                _DOS_DATE,
                entry["crc"],
                self._ZIP64_LIMIT,
                self._ZIP64_LIMIT,
                len(entry["name_bytes"]),
                len(extra),
                0,  # comment length
                0,  # disk number start
                0,  # internal file attributes
                0,  # external file attributes
                entry["offset"],
            )
            self._file.write(central_header)
            self._file.write(entry["name_bytes"])
            self._file.write(extra)
        central_size = self._file.tell() - central_start
        if central_start >= 0xFFFFFFFF or central_size >= 0xFFFFFFFF:
            raise RuntimeError("archive exceeds non-ZIP64 EOCD range")
        eocd = struct.pack(
            "<IHHHHIIH",
            self._END_OF_CENTRAL_DIR,
            0,
            0,
            len(self._entries),
            len(self._entries),
            central_size,
            central_start,
            0,
        )
        self._file.write(eocd)
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        self._closed = True

    def abort(self) -> None:
        if not self._closed:
            try:
                self._file.close()
            except OSError:  # pragma: no cover - defensive
                log.exception("error closing aborted zip writer")
            self._closed = True


# ------------------------------------------------------------------- hooks


@dataclass
class PackageDemoHooks:
    """Demo-only delays, applied by the packaging worker thread only.

    ``read_batch_seconds`` pauses after each streamed batch while READING;
    ``pre_publish_seconds`` holds the verified ZIP before the publish move so
    the "written, awaiting publication" state can be observed and cancelled.
    """

    read_batch_seconds: float = 0.0
    pre_publish_seconds: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> tuple[float, float]:
        with self.lock:
            return self.read_batch_seconds, self.pre_publish_seconds

    def set(self, read_batch: float, pre_publish: float) -> None:
        with self.lock:
            self.read_batch_seconds, self.pre_publish_seconds = (
                read_batch,
                pre_publish,
            )


# ------------------------------------------------------------------ record


@dataclass
class JobRecord:
    job_id: str
    instrument_id: str
    batch_no: str
    status: PackageStatus = PackageStatus.PENDING
    cancel_requested: bool = False
    created_at: str = field(default_factory=utc_now_iso)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    snapshot_at: Optional[str] = None
    rows_written: int = 0
    sample_rows: int = 0
    conflict_rows: int = 0
    bytes_written: int = 0
    error: Optional[str] = None
    file_name: Optional[str] = None
    sha256: Optional[str] = None

    # Threading/state bookkeeping (not serialised to metadata JSON).
    thread: Optional[threading.Thread] = None
    settled: Optional[threading.Event] = None
    staging_dir: Optional[Path] = None
    staging_zip: Optional[Path] = None
    published_path: Optional[Path] = None

    def to_metadata(self) -> dict:
        return {
            "job_id": self.job_id,
            "instrument_id": self.instrument_id,
            "batch_no": self.batch_no,
            "status": self.status.value,
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "snapshot_at": self.snapshot_at,
            "rows_written": self.rows_written,
            "sample_rows": self.sample_rows,
            "conflict_rows": self.conflict_rows,
            "bytes_written": self.bytes_written,
            "error": self.error,
            "file_name": self.file_name,
            "sha256": self.sha256,
        }

    @classmethod
    def from_metadata(cls, data: dict) -> "JobRecord":
        return cls(
            job_id=data["job_id"],
            instrument_id=data["instrument_id"],
            batch_no=data["batch_no"],
            status=PackageStatus(data["status"]),
            cancel_requested=bool(data.get("cancel_requested", False)),
            created_at=data["created_at"],
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            snapshot_at=data.get("snapshot_at"),
            rows_written=int(data.get("rows_written", 0)),
            sample_rows=int(data.get("sample_rows", 0)),
            conflict_rows=int(data.get("conflict_rows", 0)),
            bytes_written=int(data.get("bytes_written", 0)),
            error=data.get("error"),
            file_name=data.get("file_name"),
            sha256=data.get("sha256"),
        )


class SlotBusyError(RuntimeError):
    """Raised when a packaging job is requested while the slot is occupied."""


# ----------------------------------------------------------------- manager


class PackageManager:
    """Owns the packaging slot, job records, worker threads and disk layout."""

    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        self.hooks = PackageDemoHooks()
        self.batch_size = max(1, settings.package_batch_size)

        self.root = Path(settings.packages_dir)
        self.staging_root = self.root / "staging"
        self.published_dir = self.root / "published"
        self.metadata_dir = self.root / "jobs"
        for directory in (self.staging_root, self.published_dir,
                          self.metadata_dir):
            directory.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._jobs: dict[str, JobRecord] = {}
        self._active_job_id: Optional[str] = None

    # ---------------------------------------------------------------- startup

    def recover(self) -> None:
        """Reconcile on-disk state after a process restart.

        Completed packages stay published and downloadable. Any job that was
        still active (or merely pending) when the process died is marked
        failed and its staging directory removed. Orphan staging directories
        without metadata are removed as well.
        """
        with self._lock:
            known_staging: set[str] = set()
            for meta_path in sorted(self.metadata_dir.glob("*.json")):
                try:
                    data = json.loads(meta_path.read_text(encoding="utf-8"))
                    record = JobRecord.from_metadata(data)
                except (OSError, ValueError, KeyError):
                    log.warning("ignoring unreadable job metadata %s", meta_path)
                    continue
                if record.status not in _TERMINAL:
                    record.status = PackageStatus.FAILED
                    record.error = (
                        record.error
                        or "process restarted before the package finished"
                    )
                    record.finished_at = record.finished_at or utc_now_iso()
                    record.staging_dir = self.staging_root / record.job_id
                    self._persist_locked(record)
                    # The interrupted worker can no longer finish this file:
                    # drop its staging data just like an in-process failure.
                    self._remove_staging_locked(record)
                if record.status is PackageStatus.COMPLETED:
                    name = record.file_name or self._zip_name(record)
                    published = self.published_dir / name
                    if not published.exists():
                        record.status = PackageStatus.FAILED
                        record.error = "published archive missing after restart"
                        record.finished_at = utc_now_iso()
                        self._persist_locked(record)
                    else:
                        record.published_path = published
                record.settled = threading.Event()
                record.settled.set()
                self._jobs[record.job_id] = record
                known_staging.add(record.job_id)

            for entry in self.staging_root.glob("*"):
                if entry.is_dir() and entry.name not in known_staging:
                    shutil.rmtree(entry, ignore_errors=True)
                    log.warning("removed orphan staging dir %s", entry)
            log.info(
                "package manager recovered %d job record(s)", len(self._jobs)
            )

    # ------------------------------------------------------------- submission

    def create_job(self, instrument_id: str, batch_no: str) -> JobRecord:
        """Reserve the single packaging slot and start a worker thread."""
        with self._lock:
            if self._active_job_id is not None:
                active = self._jobs.get(self._active_job_id)
                raise SlotBusyError(
                    "another packaging job is already running: "
                    f"job_id={self._active_job_id}"
                    + (f" status={active.status.value}" if active else "")
                )
            job_id = uuid.uuid4().hex
            record = JobRecord(
                job_id=job_id,
                instrument_id=instrument_id,
                batch_no=batch_no,
            )
            record.settled = threading.Event()
            record.staging_dir = self.staging_root / job_id
            record.staging_dir.mkdir(parents=True, exist_ok=False)
            record.staging_zip = record.staging_dir / "package.zip.tmp"
            record.file_name = self._zip_name(record)
            self._jobs[job_id] = record
            self._active_job_id = job_id
            self._persist_locked(record)

            record.thread = threading.Thread(
                target=self._run_guarded,
                args=(job_id,),
                name=f"packager-{job_id[:8]}",
                daemon=True,
            )
            record.thread.start()
            log.info(
                "package job %s started for instrument=%s batch=%s",
                job_id, instrument_id, batch_no,
            )
            return record

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[JobRecord]:
        with self._lock:
            return sorted(
                self._jobs.values(), key=lambda r: r.created_at, reverse=True
            )

    # --------------------------------------------------------------- cancel

    def request_cancel(self, job_id: str) -> JobRecord:
        """Mark a job cancel-requested; the worker performs the actual stop.

        Calling this on an already terminal job is a harmless no-op.
        """
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                raise KeyError(job_id)
            if record.status in _TERMINAL:
                return record
            record.cancel_requested = True
            self._persist_locked(record)
            log.info("cancel requested for package job %s", job_id)
            return record

    def wait_settled(self, job_id: str, timeout: Optional[float] = None) -> bool:
        """Block until the worker's full teardown has completed.

        Repeated cancel requests share this one event, so every caller waits
        on the same settling process instead of racing at cleanup.
        """
        record = self.get_job(job_id)
        if record is None or record.settled is None:
            return True
        return record.settled.wait(timeout=timeout)

    # --------------------------------------------------------------- download

    def published_file(self, job_id: str) -> Optional[Path]:
        """Return the downloadable archive path only for a completed job."""
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None or record.status is not PackageStatus.COMPLETED:
                return None
            path = record.published_path
            if path is not None and path.is_file():
                return path
            candidate = self.published_dir / (
                record.file_name or self._zip_name(record)
            )
            if candidate.is_file():
                record.published_path = candidate
                return candidate
            return None

    def shutdown(self, timeout: float = 30.0) -> None:
        """Process shutdown: ask active work to stop and wait for teardown."""
        with self._lock:
            active_id = self._active_job_id
        if active_id is not None:
            try:
                self.request_cancel(active_id)
            except KeyError:  # pragma: no cover - defensive
                pass
            self.wait_settled(active_id, timeout=timeout)

    # ================================================================= worker

    def _run_guarded(self, job_id: str) -> None:
        record = self.get_job(job_id)
        assert record is not None
        try:
            self._run_job(record)
        except Exception as exc:
            log.exception("package job %s failed unexpectedly", job_id)
            self._fail(record, f"unexpected error: {exc!r}")
        finally:
            with self._lock:
                record.settled.set()
                if self._active_job_id == job_id:
                    self._active_job_id = None
            log.info(
                "package job %s settled in status=%s",
                job_id, record.status.value,
            )

    def _run_job(self, record: JobRecord) -> None:
        conn: Optional[sqlite3.Connection] = None
        writer: Optional[StreamingZipWriter] = None
        try:
            with self._lock:
                if record.cancel_requested:
                    self._cancel_locked(record)
                    return
                record.status = PackageStatus.READING
                record.started_at = utc_now_iso()
                self._persist_locked(record)

            # One read-only connection, one deferred transaction: the single
            # shared snapshot for BOTH CSVs. query_only guarantees we cannot
            # take a write lock; the MQTT writer keeps committing unblocked.
            conn = self.db.read_connection()
            # Autocommit (isolation_level=None) + explicit BEGIN opens a
            # deferred transaction that stays open until COMMIT, so every
            # later batch read sees exactly this snapshot.
            conn.execute("BEGIN")
            snapshot_row = conn.execute(
                "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS now"
            ).fetchone()
            with self._lock:
                record.snapshot_at = snapshot_row["now"]
                self._persist_locked(record)

            writer = StreamingZipWriter(record.staging_zip)
            sample_count = self._stream_table(
                record, conn, writer, SAMPLES_CSV, SAMPLE_COLUMNS, "samples"
            )
            if self._cancelled(record):
                writer.abort()
                writer = None
                self._cancel(record)
                return
            conflict_count = self._stream_table(
                record,
                conn,
                writer,
                CONFLICTS_CSV,
                CONFLICT_COLUMNS,
                "conflicts",
            )
            if self._cancelled(record):
                writer.abort()
                writer = None
                self._cancel(record)
                return

            manifest = {
                "job_id": record.job_id,
                "instrument_id": record.instrument_id,
                "batch_no": record.batch_no,
                "snapshot_at": record.snapshot_at,
                "files": [SAMPLES_CSV, CONFLICTS_CSV],
                "sample_rows": sample_count,
                "conflict_rows": conflict_count,
            }
            writer.write_complete(
                "manifest.json",
                json.dumps(manifest, indent=2, ensure_ascii=False).encode(
                    "utf-8"
                ),
            )

            # All rows are streamed: end the snapshot transaction and release
            # the read connection before verification/publish.
            conn.commit()
            conn.close()
            conn = None

            with self._lock:
                record.status = PackageStatus.VERIFYING
                record.sample_rows = sample_count
                record.conflict_rows = conflict_count
                record.rows_written = sample_count + conflict_count
                self._persist_locked(record)

            # Close flushes the central directory and fsyncs the file.
            writer.close()
            writer = None

            self._verify_archive(record)
            if self._cancelled(record):
                self._cancel(record)
                return

            # "Written but awaiting publication" window: the archive is
            # complete and verified on disk yet not downloadable. A cancel
            # here must still suppress it.
            _, pre_publish_delay = self.hooks.snapshot()
            if pre_publish_delay > 0 and self._interruptible_wait(
                record, pre_publish_delay
            ):
                self._cancel(record)
                return
            if self._cancelled(record):
                self._cancel(record)
                return

            with self._lock:
                record.status = PackageStatus.PUBLISHING
                self._persist_locked(record)

            target = self.published_dir / record.file_name
            # Atomic move inside the same volume: readers see either no file
            # or the complete published file, never a partial one.
            os.replace(record.staging_zip, target)
            record.published_path = target
            record.bytes_written = target.stat().st_size

            with self._lock:
                record.status = PackageStatus.COMPLETED
                record.finished_at = utc_now_iso()
                self._persist_locked(record)
            log.info(
                "package job %s published: %s (%d samples, %d conflicts, %d bytes)",
                record.job_id, target.name, sample_count, conflict_count,
                record.bytes_written,
            )
            self._remove_staging(record)
        finally:
            if writer is not None:
                writer.abort()
            if conn is not None:
                try:
                    conn.rollback()
                    conn.close()
                except sqlite3.Error:  # pragma: no cover - defensive
                    log.exception("error closing snapshot connection")
            if record.status not in _TERMINAL and not record.cancel_requested:
                self._fail(record, "packaging failed; see api logs")

    def _stream_table(
        self,
        record: JobRecord,
        conn: sqlite3.Connection,
        writer: StreamingZipWriter,
        arcname: str,
        columns: list[str],
        table: str,
    ) -> int:
        """Stream one table into a single CSV entry in bounded batches.

        Keyset pagination on the immutable primary key ``id`` keeps memory
        flat regardless of table size and avoids OFFSET scan costs. Reads use
        the caller's open snapshot transaction, so rows committed after the
        snapshot started never appear. Returns the number of data rows.
        """
        assert table in ("samples", "conflicts")
        query = (
            f"SELECT {', '.join(columns)} FROM {table} "
            "WHERE instrument_id = ? AND batch_no = ? AND id > ? "
            f"ORDER BY id ASC LIMIT {int(self.batch_size)}"
        )
        key_params = (record.instrument_id, record.batch_no)
        last_id = 0
        total = 0
        read_delay, _ = self.hooks.snapshot()

        writer.start_entry(arcname)
        try:
            # Header row first; only one batch's worth of rows is ever held.
            header_buffer = io.StringIO(newline="")
            csv.writer(header_buffer).writerow(columns)
            writer.write_stream(header_buffer.getvalue().encode("utf-8"))

            while True:
                if self._cancelled(record):
                    return total
                rows = conn.execute(
                    query, (*key_params, last_id)
                ).fetchall()
                if not rows:
                    break

                buffer = io.StringIO(newline="")
                csv_writer = csv.writer(buffer)
                for row in rows:
                    csv_writer.writerow([row[col] for col in columns])
                    last_id = row["id"]
                writer.write_stream(buffer.getvalue().encode("utf-8"))
                total += len(rows)

                with self._lock:
                    if table == "samples":
                        record.sample_rows = total
                    else:
                        record.conflict_rows = total
                    record.rows_written = (
                        record.sample_rows + record.conflict_rows
                    )
                    self._persist_locked(record)

                if read_delay > 0:
                    # Demo pause inside READING; interruptible by cancellation.
                    if self._interruptible_wait(record, read_delay):
                        return total
        finally:
            # On cancel the whole archive is discarded, so closing the entry
            # keeps the on-disk file structurally valid up to this point.
            if not self._cancelled(record):
                writer.close_entry()
        return total

    # ------------------------------------------------------------ verify etc

    def _verify_archive(self, record: JobRecord) -> None:
        """Full CRC/structure check plus SHA-256 of the staged archive."""
        digest = hashlib.sha256()
        with open(record.staging_zip, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        with zipfile.ZipFile(record.staging_zip, mode="r") as zf:
            bad = zf.testzip()
            if bad is not None:
                raise RuntimeError(f"corrupt entry in archive: {bad}")
            names = set(zf.namelist())
            for required in (SAMPLES_CSV, CONFLICTS_CSV, "manifest.json"):
                if required not in names:
                    raise RuntimeError(f"{required} missing from archive")
        record.sha256 = digest.hexdigest()

    def _interruptible_wait(self, record: JobRecord, seconds: float) -> bool:
        """Sleep up to ``seconds``; return True if cancellation fired."""
        if record.settled is None:  # pragma: no cover - defensive
            return record.cancel_requested
        # Poll the cancel flag in small slices so a request during a demo
        # pause is acted on promptly.
        slices = max(1, int(seconds / 0.1))
        slice_time = seconds / slices
        for _ in range(slices):
            if self._cancelled(record):
                return True
            if record.settled.wait(timeout=slice_time):
                return True
        return self._cancelled(record)

    def _cancelled(self, record: JobRecord) -> bool:
        with self._lock:
            return record.cancel_requested

    # ------------------------------------------------------------ transitions

    def _cancel(self, record: JobRecord) -> None:
        with self._lock:
            self._cancel_locked(record)

    def _cancel_locked(self, record: JobRecord) -> None:
        record.cancel_requested = True
        record.status = PackageStatus.CANCELLED
        record.finished_at = utc_now_iso()
        record.published_path = None
        self._persist_locked(record)
        self._remove_staging_locked(record)
        log.info("package job %s cancelled and cleaned up", record.job_id)

    def _fail(self, record: JobRecord, message: str) -> None:
        with self._lock:
            if record.status in _TERMINAL:
                return
            record.status = PackageStatus.FAILED
            record.error = message
            record.finished_at = utc_now_iso()
            record.published_path = None
            self._persist_locked(record)
            self._remove_staging_locked(record)
            log.warning("package job %s failed: %s", record.job_id, message)

    def _remove_staging(self, record: JobRecord) -> None:
        with self._lock:
            self._remove_staging_locked(record)

    def _remove_staging_locked(self, record: JobRecord) -> None:
        if record.staging_dir and record.staging_dir.exists():
            shutil.rmtree(record.staging_dir, ignore_errors=True)

    def _zip_name(self, record: JobRecord) -> str:
        return (
            f"{_safe_label(record.instrument_id)}_"
            f"{_safe_label(record.batch_no)}_{record.job_id[:8]}.zip"
        )

    # -------------------------------------------------------------- metadata

    def _persist_locked(self, record: JobRecord) -> None:
        """Atomic JSON metadata write (temp file + replace), never in /tmp."""
        target = self.metadata_dir / f"{record.job_id}.json"
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(record.to_metadata(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp, target)
