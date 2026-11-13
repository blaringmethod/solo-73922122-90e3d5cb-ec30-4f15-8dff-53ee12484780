"""Runtime configuration for the fermentation lab archiver.

All values are overridable through environment variables so the same image can
run inside Docker Compose as well as on a developer machine.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    broker_host: str = os.getenv("MQTT_HOST", "localhost")
    broker_port: int = int(os.getenv("MQTT_PORT", "1883"))
    # Fixed client id + clean_session=False => the broker keeps a *persistent
    # session*: QoS 1 messages published while we are offline are redelivered
    # after we reconnect, with their original DUP flag set by the broker.
    client_id: str = os.getenv("MQTT_CLIENT_ID", "ferment-archiver")
    topic: str = os.getenv("MQTT_TOPIC", "lab/fermenter/measurement")
    keepalive: int = int(os.getenv("MQTT_KEEPALIVE", "30"))
    db_path: str = os.getenv("DB_PATH", "/data/archiver.db")
    # Bounded in-flight window: paho will not push more than this many
    # not-yet-acked messages at us, providing natural back-pressure instead of
    # letting the in-memory queue grow without limit.
    max_inflight: int = int(os.getenv("MQTT_MAX_INFLIGHT", "256"))

    # -- offline package delivery ------------------------------------------
    # Root directory for ZIP packaging: staging work, published archives and
    # per-job metadata all live underneath it. It sits on the same persistent
    # volume as the SQLite database so completed packages survive restarts.
    packages_dir: str = os.getenv("PACKAGES_DIR", "/data/packages")
    # Number of rows fetched from SQLite and streamed into one CSV chunk. The
    # whole batch is never loaded: peak memory is bounded by this value.
    package_batch_size: int = int(os.getenv("PACKAGE_BATCH_SIZE", "500"))


settings = Settings()
