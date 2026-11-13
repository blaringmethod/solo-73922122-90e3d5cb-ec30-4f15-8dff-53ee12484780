"""Pydantic schemas: payload validation (in) and API responses (out)."""
from __future__ import annotations

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
