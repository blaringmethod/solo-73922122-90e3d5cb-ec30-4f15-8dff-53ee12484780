"""FastAPI application: query API, packaging API and demo controls."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from .config import settings
from .db import Database
from .models import (
    CancelOut,
    ConflictOut,
    HealthOut,
    PackageCreate,
    PackageDemoIn,
    PackageDemoOut,
    PackageListOut,
    PackageOut,
    PackageStatus,
    Paginated,
    PauseIn,
    PauseOut,
    ReconnectOut,
    SampleOut,
    StatsOut,
)
from .mqtt_archiver import MqttArchiver
from .packager import PackageManager, SlotBusyError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("archiver.api")

MAX_PAGE = 500
# A settling cancel must complete promptly; this is only a safety bound.
CANCEL_WAIT_SECONDS = 60


class State:
    db: Database
    archiver: MqttArchiver
    packager: PackageManager


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.db = Database(settings.db_path)
    state.archiver = MqttArchiver(settings, state.db)
    state.packager = PackageManager(settings, state.db)
    # Restart reconciliation: keep completed packages, fail+clean unfinished.
    state.packager.recover()
    state.archiver.start()
    try:
        yield
    finally:
        state.packager.shutdown()
        state.archiver.stop()
        state.db.close()


app = FastAPI(
    title="Fermentation Lab Archiver",
    version="1.1.0",
    description=(
        "Subscribes to QoS 1 instrument measurements over MQTT, stores one "
        "sample per (instrument, batch, sequence) in SQLite, retains "
        "contradicting readings as conflicts, and only PUBACKs after the "
        "write commits. A packaging API builds snapshot-consistent ZIP "
        "deliveries (samples.csv + conflicts.csv) in the background with "
        "status, cancel and download endpoints. See /docs for interactive "
        "calls."
    ),
    lifespan=lifespan,
)


def _paginate(total: int, limit: int, offset: int, items: list) -> Paginated:
    nxt = offset + limit if offset + limit < total else None
    return Paginated(
        count=total, limit=limit, offset=offset, items=items, next_offset=nxt
    )


# ---------------------------------------------------------------- samples

@app.get("/samples", response_model=Paginated, tags=["queries"])
def get_samples(
    instrument_id: Optional[str] = Query(default=None, max_length=128),
    batch_no: Optional[str] = Query(default=None, max_length=128),
    limit: int = Query(default=100, ge=1, le=MAX_PAGE),
    offset: int = Query(default=0, ge=0),
):
    """List stored samples, optionally filtered by instrument and/or batch."""
    rows = state.db.list_samples(instrument_id, batch_no, limit, offset)
    total = state.db.count("samples", instrument_id, batch_no)
    items = [SampleOut(**row) for row in rows]
    return _paginate(total, limit, offset, items)


# --------------------------------------------------------------- conflicts

@app.get("/conflicts", response_model=Paginated, tags=["queries"])
def get_conflicts(
    instrument_id: Optional[str] = Query(default=None, max_length=128),
    batch_no: Optional[str] = Query(default=None, max_length=128),
    limit: int = Query(default=100, ge=1, le=MAX_PAGE),
    offset: int = Query(default=0, ge=0),
):
    """List retained contradictory readings (originals are never altered)."""
    rows = state.db.list_conflicts(instrument_id, batch_no, limit, offset)
    total = state.db.count("conflicts", instrument_id, batch_no)
    items = [ConflictOut(**row) for row in rows]
    return _paginate(total, limit, offset, items)


# -------------------------------------------------------------- packaging

def _package_out(record) -> PackageOut:
    download_url = None
    if state.packager.published_file(record.job_id) is not None:
        download_url = f"/packages/{record.job_id}/download"
    return PackageOut(
        job_id=record.job_id,
        instrument_id=record.instrument_id,
        batch_no=record.batch_no,
        status=record.status,
        cancel_requested=record.cancel_requested,
        created_at=record.created_at,
        started_at=record.started_at,
        finished_at=record.finished_at,
        snapshot_at=record.snapshot_at,
        rows_written=record.rows_written,
        sample_rows=record.sample_rows,
        conflict_rows=record.conflict_rows,
        bytes_written=record.bytes_written,
        error=record.error,
        file_name=record.file_name,
        sha256=record.sha256,
        download_url=download_url,
    )


@app.post("/packages", response_model=PackageOut, status_code=202,
          tags=["packaging"])
def create_package(body: PackageCreate):
    """Start a background ZIP packaging job for one instrument + batch.

    Exactly one packaging job runs in this API process. The job opens a
    single read-only SQLite snapshot before reading anything and streams both
    ``samples.csv`` and ``conflicts.csv`` from that same committed snapshot
    into a staged ZIP; later measurements never enter the archive. The
    response carries the ``job_id`` used for status, cancel and download.
    """
    try:
        record = state.packager.create_job(
            body.instrument_id, body.batch_no
        )
    except SlotBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _package_out(record)


@app.get("/packages", response_model=PackageListOut, tags=["packaging"])
def list_packages():
    """List all packaging jobs (newest first), including terminal ones."""
    records = state.packager.list_jobs()
    return PackageListOut(
        count=len(records), items=[_package_out(r) for r in records]
    )


@app.get("/packages/{job_id}", response_model=PackageOut, tags=["packaging"])
def get_package(job_id: str):
    """Get the current status of one packaging job."""
    record = state.packager.get_job(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    return _package_out(record)


@app.post("/packages/{job_id}/cancel", response_model=CancelOut,
          tags=["packaging"])
def cancel_package(job_id: str):
    """Request cancellation and wait for the job's full teardown.

    The response is only sent after reading/writing has actually stopped, the
    SQLite connection and ZIP file are closed and the staging files are
    removed, so a returned ``cancelled`` guarantees nothing will be
    published. Repeated calls wait on the same single teardown. A cancel
    landing in the verified-but-unpublished window suppresses the finished
    ZIP. Disconnecting the HTTP client never cancels a job by itself.
    """
    record = state.packager.get_job(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")

    if record.status not in (
        PackageStatus.COMPLETED,
        PackageStatus.CANCELLED,
        PackageStatus.FAILED,
    ):
        state.packager.request_cancel(job_id)

    settled = state.packager.wait_settled(job_id, timeout=CANCEL_WAIT_SECONDS)
    record = state.packager.get_job(job_id)
    if not settled:
        raise HTTPException(
            status=504,
            detail=(
                "cancel accepted but the worker did not settle within "
                f"{CANCEL_WAIT_SECONDS}s; status={record.status.value}"
            ),
        )

    if record.status.value == "completed":
        message = (
            "job had already completed and published before cancellation "
            "took effect; the immutable package remains available"
        )
    elif record.status.value == "failed":
        message = f"job had already failed: {record.error or 'unknown error'}"
    else:
        message = (
            "cancelled: IO stopped, connections and files closed, staging "
            "files removed; no archive was published"
        )
    return CancelOut(
        job_id=job_id,
        status=record.status,
        cancel_requested=record.cancel_requested,
        message=message,
    )


@app.get("/packages/{job_id}/download", tags=["packaging"])
def download_package(job_id: str):
    """Download the completed ZIP.

    Only complete, verified, published archives are served. Pending,
    running, cancelled and failed jobs return 404 -- there is never a
    partial or provisional file to download.
    """
    record = state.packager.get_job(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    path = state.packager.published_file(job_id)
    if path is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "no downloadable package: job status is "
                f"{record.status.value}"
            ),
        )
    return FileResponse(
        path=str(path),
        media_type="application/zip",
        filename=record.file_name or path.name,
        headers={
            "X-Job-Id": record.job_id,
            "X-SHA256": record.sha256 or "",
        },
    )


# ------------------------------------------------- packaging demo controls

@app.post("/demo/package-pause", response_model=PackageDemoOut,
          tags=["demo"])
def set_package_pause(pause: PackageDemoIn):
    """Configure packaging demo pauses.

    ``read_batch_seconds`` pauses the worker after every streamed DB batch
    while the job is in ``reading`` (use it to observe a long read and to
    cancel mid-read). ``pre_publish_seconds`` holds a fully written and
    verified ZIP before the atomic publish move, exposing the
    "written, awaiting publication" window; cancelling in that window still
    suppresses the archive. Set both to 0 to clear.
    """
    state.packager.hooks.set(
        pause.read_batch_seconds, pause.pre_publish_seconds
    )
    read_batch, pre_publish = state.packager.hooks.snapshot()
    log.info(
        "package demo pauses: read_batch=%.2fs pre_publish=%.2fs",
        read_batch, pre_publish,
    )
    return PackageDemoOut(
        read_batch_seconds=read_batch, pre_publish_seconds=pre_publish
    )


@app.get("/demo/package-pause", response_model=PackageDemoOut, tags=["demo"])
def get_package_pause():
    """Inspect the current packaging demo pauses."""
    read_batch, pre_publish = state.packager.hooks.snapshot()
    return PackageDemoOut(
        read_batch_seconds=read_batch, pre_publish_seconds=pre_publish
    )


# ------------------------------------------------------------- demo controls

@app.post("/demo/pause", response_model=PauseOut, tags=["demo"])
def set_pause(pause: PauseIn):
    """Pause the writer thread before the DB write.

    The MQTT network loop (heartbeats, inbound traffic) and every GET endpoint
    keep responding while the writer is paused; messages simply queue up and
    are acked once the pause ends. Set ``seconds`` to 0 to clear.
    """
    state.archiver.pauses.set(pause.seconds, state.archiver.pauses.snapshot()[1])
    pre, post = state.archiver.pauses.snapshot()
    log.info("pre-write pause set to %.1fs", pre)
    return PauseOut(pre_write_seconds=pre, post_write_seconds=post)


@app.post("/demo/pause-post-commit", response_model=PauseOut, tags=["demo"])
def set_post_commit_pause(pause: PauseIn):
    """Pause the writer *after* commit but *before* PUBACK.

    This is the window in which killing the container must not lose data: on
    restart the broker redelivers the unacked message and unique constraints
    collapse the replay.
    """
    state.archiver.pauses.set(state.archiver.pauses.snapshot()[0], pause.seconds)
    pre, post = state.archiver.pauses.snapshot()
    log.info("post-commit/pre-ack pause set to %.1fs", post)
    return PauseOut(pre_write_seconds=pre, post_write_seconds=post)


@app.post("/demo/reconnect", response_model=ReconnectOut, tags=["demo"])
def reconnect():
    """Force a TCP reconnect while keeping the persistent session."""
    try:
        state.archiver.force_reconnect()
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return ReconnectOut(
        status="reconnecting",
        message=(
            "TCP link torn down; paho reconnects with the same client id and "
            "clean_session=False, the broker redelivers unacked QoS 1 messages"
        ),
    )


@app.get("/demo/stats", response_model=StatsOut, tags=["demo"])
def stats():
    c = state.archiver.counters
    pre, post = state.archiver.pauses.snapshot()
    return StatsOut(
        connected=state.archiver.connected,
        connection_epoch=state.archiver.epoch,
        queue_depth=state.archiver.queue_depth(),
        stored=c.stored,
        duplicates=c.duplicates,
        conflicts=c.conflicts,
        duplicate_conflicts=c.duplicate_conflicts,
        invalid_payloads=c.invalid_payloads,
        stale_drops=c.stale_drops,
        ack_failures=c.ack_failures,
        pre_write_seconds=pre,
        post_write_seconds=post,
    )


# -------------------------------------------------------------------- health

@app.get("/health", response_model=HealthOut, tags=["health"])
def health():
    return HealthOut(
        status="ok",
        mqtt_connected=state.archiver.connected,
        broker=f"{settings.broker_host}:{settings.broker_port}",
        topic=settings.topic,
        client_id=settings.client_id,
        persistent_session=True,
    )
