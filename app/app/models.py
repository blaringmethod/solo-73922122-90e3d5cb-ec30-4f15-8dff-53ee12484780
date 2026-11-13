"""Pydantic schemas: payload validation (in) and API responses (out)."""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class Measurement(BaseModel):
    """JSON payload pushed by an instrument.

    ``extra="ignore"`` keeps forward-compatible instruments working, while the
    business identity triple below is the only key used for de-duplication.
    """

    model_config = ConfigDict(extra="ignore")

    instrument_id: str = Field(min_length=1, max_length=128)
    batch_no: str = Field(min_length=1, max_length=128)
    sample_seq: int = Field(ge=0)
    ts_utc: str = Field(min_length=1, max_length=64)
    temperature_c: float
    ph: float


class SampleOut(BaseModel):
    instrument_id: str
    batch_no: str
    sample_seq: int
    ts_utc: str
    temperature_c: float
    ph: float
    payload: dict[str, Any]
    stored_at: str


class ConflictOut(BaseModel):
    id: int
    instrument_id: str
    batch_no: str
    sample_seq: int
    field_name: str
    stored_value: float
    incoming_value: float
    incoming_ts_utc: str
    incoming_payload: dict[str, Any]
    created_at: str


class PauseIn(BaseModel):
    # Applied only to the worker loop; the MQTT network loop and the FastAPI
    # event loop keep running during a pause.
    seconds: float = Field(ge=0, le=600)


class PauseOut(BaseModel):
    pre_write_seconds: float
    post_write_seconds: float


class ReconnectOut(BaseModel):
    status: str
    message: str


class StatsOut(BaseModel):
    connected: bool
    connection_epoch: int
    queue_depth: int
    stored: int
    duplicates: int
    conflicts: int
    duplicate_conflicts: int
    invalid_payloads: int
    stale_drops: int
    ack_failures: int
    pre_write_seconds: float
    post_write_seconds: float


class HealthOut(BaseModel):
    status: str
    mqtt_connected: bool
    broker: str
    topic: str
    client_id: str
    persistent_session: bool


class Paginated(BaseModel):
    count: int
    limit: int
    offset: int
    items: list[Any]
    next_offset: Optional[int]


# ---------------------------------------------------------------- packaging

class PackageStatus(str, Enum):
    """Lifecycle of one offline-delivery package job.

    PENDING   - accepted, worker has not opened its snapshot yet
    READING   - snapshot transaction open; rows are being streamed to the ZIP
    VERIFYING - ZIP closed; CRC/structure check and checksum in progress
    PUBLISHING- archive verified; about to be atomically moved to published/
    COMPLETED - published and downloadable; content is now immutable
    CANCELLED - cancel requested and fully settled (IO stopped, cleaned up)
    FAILED    - error before publication; nothing is downloadable
    """

    PENDING = "pending"
    READING = "reading"
    VERIFYING = "verifying"
    PUBLISHING = "publishing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


# Statuses in which a job occupies the single in-process packaging slot.
ACTIVE_STATUSES = frozenset(
    {
        PackageStatus.PENDING,
        PackageStatus.READING,
        PackageStatus.VERIFYING,
        PackageStatus.PUBLISHING,
    }
)
# Statuses in which a finished ZIP may be downloaded.
DOWNLOADABLE_STATUSES = frozenset({PackageStatus.COMPLETED})


class PackageCreate(BaseModel):
    instrument_id: str = Field(min_length=1, max_length=128)
    batch_no: str = Field(min_length=1, max_length=128)


class PackageOut(BaseModel):
    job_id: str
    instrument_id: str
    batch_no: str
    status: PackageStatus
    cancel_requested: bool
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    snapshot_at: Optional[str] = None
    rows_written: int
    sample_rows: int
    conflict_rows: int
    bytes_written: int
    error: Optional[str] = None
    file_name: Optional[str] = None
    sha256: Optional[str] = None
    download_url: Optional[str] = None


class PackageListOut(BaseModel):
    count: int
    items: list[PackageOut]


class CancelOut(BaseModel):
    job_id: str
    status: PackageStatus
    cancel_requested: bool
    message: str


class PackageDemoIn(BaseModel):
    # Seconds the packaging worker sleeps after every streamed DB batch while
    # in READING. Bounded so a demo can never hang a job forever.
    read_batch_seconds: float = Field(default=0.0, ge=0.0, le=600.0)
    # Seconds the worker holds a fully written, verified ZIP *before* the
    # atomic publish move. This is the "written but not yet published" window
    # in which a cancel must still be able to suppress the archive.
    pre_publish_seconds: float = Field(default=0.0, ge=0.0, le=600.0)


class PackageDemoOut(BaseModel):
    read_batch_seconds: float
    pre_publish_seconds: float
