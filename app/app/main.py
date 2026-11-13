"""FastAPI application: query API plus demo controls for the MQTT archiver."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from .config import settings
from .db import Database
from .models import (
    ConflictOut,
    HealthOut,
    Paginated,
    PauseIn,
    PauseOut,
    ReconnectOut,
    SampleOut,
    StatsOut,
)
from .mqtt_archiver import MqttArchiver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("archiver.api")

MAX_PAGE = 500


class State:
    db: Database
    archiver: MqttArchiver


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.db = Database(settings.db_path)
    state.archiver = MqttArchiver(settings, state.db)
    state.archiver.start()
    try:
        yield
    finally:
        state.archiver.stop()
        state.db.close()


app = FastAPI(
    title="Fermentation Lab Archiver",
    version="1.0.0",
    description=(
        "Subscribes to QoS 1 instrument measurements over MQTT, stores one "
        "sample per (instrument, batch, sequence) in SQLite, retains "
        "contradicting readings as conflicts, and only PUBACKs after the "
        "write commits. See /docs for interactive calls."
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
