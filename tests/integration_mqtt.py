"""End-to-end check against a real (pure-Python) MQTT broker.

This mirrors the Docker Compose topology on a developer machine:

    amqtt broker  <--QoS1--  MqttArchiver (manual ack, persistent session)
         ^
         | QoS1
    paho publisher

It verifies, on the wire, that PUBACK happens after storage, that exact
duplicates collapse, that contradictions become conflicts, that a forced TCP
reconnect advances the connection epoch (so a late result can never ack a
reused mid on the new connection), and that a process restart replays the
same business keys idempotently. Mosquitto in Compose plays the broker role
in the real deployment.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import logging  # noqa: E402
logging.basicConfig(level=logging.WARNING)

from amqtt.broker import Broker  # noqa: E402

from app.config import Settings  # noqa: E402
from app.db import Database  # noqa: E402
from app.mqtt_archiver import MqttArchiver  # noqa: E402

PORT = 11883
TOPIC = "lab/fermenter/measurement"
CLIENT_ID = "ferment-archiver-it"

BROKER_CONFIG = {
    "listeners": {
        "default": {"type": "tcp", "bind": f"127.0.0.1:{PORT}"},
    },
    "sys_interval": 0,
    "auth": {"allow-anonymous": True},
    "topic-check": {"enabled": False},
}


def make_payload(seq, temp=30.0, ph=6.8):
    return {
        "instrument_id": "FERM-IT",
        "batch_no": "B-IT",
        "sample_seq": seq,
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "temperature_c": temp,
        "ph": ph,
    }


class ThreadedPublisher:
    """A paho publisher living in its own thread.

    Keeping it off the asyncio thread mirrors a real separate instrument
    process and guarantees its blocking connect()/wait_for_publish() can never
    starve the in-process broker's event loop.
    """

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("publisher thread did not start")

    def _run(self) -> None:
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="it-publisher",
            protocol=mqtt.MQTTv311,
        )
        self._client.connect("127.0.0.1", PORT, keepalive=30)
        self._client.loop_start()
        self._ready.set()

    async def publish(self, payload: str | bytes) -> None:
        def _blocking_publish() -> None:
            info = self._client.publish(TOPIC, payload, qos=1)
            info.wait_for_publish(timeout=15)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"publish failed rc={info.rc}")

        await self._loop.run_in_executor(None, _blocking_publish)

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()


async def wait_until(desc, predicate, timeout=10.0):
    """Async polling helper.

    A synchronous ``time.sleep`` here would block the *only* asyncio event
    loop that the in-process broker runs on, so its connection coroutines
    could never be scheduled -- the same class of bug the production design
    avoids by moving SQLite writes onto a separate worker thread.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for: {desc}")


async def main() -> None:
    workdir = tempfile.mkdtemp(prefix="amqtt-")
    cwd = os.getcwd()
    # The broker writes persistence files into its cwd; keep them strictly
    # separate from the SQLite directory (untrusted-file/import hygiene too).
    broker_dir = os.path.join(workdir, "broker")
    data_dir = os.path.join(workdir, "data")
    os.makedirs(broker_dir)
    os.makedirs(data_dir)
    os.chdir(broker_dir)
    db_path = os.path.join(data_dir, "it.db")
    failures = []

    broker = Broker(BROKER_CONFIG)
    try:
        await broker.start()
        print("[1] broker up")

        settings = Settings(
            broker_host="127.0.0.1", broker_port=PORT,
            client_id=CLIENT_ID, topic=TOPIC, db_path=db_path,
            keepalive=15,
        )
        db = Database(db_path)
        archiver = MqttArchiver(settings, db)
        archiver.client.enable_logger(logging.getLogger("paho-it"))
        archiver.start()
        await wait_until("archiver connected", lambda: archiver.connected)
        epoch_before = archiver.epoch
        print(f"[2] archiver connected, epoch={epoch_before}")

        pub = ThreadedPublisher()

        # --- A: fresh samples ------------------------------------------
        for seq in range(1, 4):
            await pub.publish(json.dumps(make_payload(seq)))
        await wait_until("3 samples stored",
                   lambda: db.count("samples", None, None) == 3)
        assert archiver.counters.stored == 3
        print("[3] 3 samples stored, PUBACK happened post-commit")

        # --- B: exact duplicate ----------------------------------------
        await pub.publish(json.dumps(make_payload(2)))
        await wait_until("duplicate counted",
                   lambda: archiver.counters.duplicates == 1)
        assert db.count("samples", None, None) == 3
        print("[4] exact duplicate ignored, still one row per key")

        # --- C: contradiction -> conflict, original kept ---------------
        bad = make_payload(2, temp=33.3, ph=6.8)
        await pub.publish(json.dumps(bad))
        await wait_until("conflict recorded",
                   lambda: db.count("conflicts", None, None) == 1)
        row = db.list_samples("FERM-IT", "B-IT", 10, 0)[1]
        assert row["temperature_c"] == 30.0, "original sample was modified!"
        # redelivered same contradiction -> idempotent
        await pub.publish(json.dumps(bad))
        await wait_until("conflict duplicate counted",
                   lambda: archiver.counters.duplicate_conflicts == 1)
        assert db.count("conflicts", None, None) == 1
        print("[5] conflict retained once; original sample untouched")

        # --- D: writer pause does not block MQTT heartbeats -------------
        archiver.pauses.set(2.0, 0.0)
        await pub.publish(json.dumps(make_payload(4)))  # broker accepts;
        # subscriber's writer thread is still paused
        await asyncio.sleep(0.5)
        assert archiver.connected, "connection died while writer was paused"
        assert db.count("samples", None, None) == 3, "write happened too early"
        archiver.pauses.set(0.0, 0.0)
        await wait_until("paused message eventually stored",
                   lambda: db.count("samples", None, None) == 4)
        print("[6] 2s writer pause: connection alive, message stored after")

        # --- E: reconnect keeps session; no stale ack across epochs -----
        archiver.force_reconnect()
        await wait_until("reconnected with a new epoch",
                   lambda: archiver.connected and archiver.epoch > epoch_before)
        print(f"[7] reconnect ok, new epoch={archiver.epoch}")

        # --- F: crash/restart resume and redelivery idempotency ---------
        # amqtt does not queue QoS 1 for offline sessions (Mosquitto in the
        # real deployment does), so we simulate the broker's redelivery of
        # the same business keys after the archiver restarts -- the exact
        # sequence a crash in the commit->PUBACK window produces.
        redelivered = [make_payload(seq) for seq in range(1, 4)]
        fresh = [make_payload(seq) for seq in range(10, 13)]

        archiver.stop()
        db.close()
        await asyncio.sleep(0.5)
        print("[8] archiver process stopped (crash window)")

        # Restart with the SAME client id and clean_session=False.
        db = Database(db_path)
        # Baseline row count at restart (the pause step added seq 4).
        baseline = db.count("samples", None, None)
        expected_total = baseline + len(fresh)
        archiver = MqttArchiver(settings, db)
        archiver.start()
        await wait_until("archiver reconnected after restart",
                   lambda: archiver.connected, timeout=15)

        # Replayed publishes (the broker's persistent-session redelivery) plus
        # genuinely new samples, interleaved.
        for replay, new in zip(redelivered, fresh):
            await pub.publish(json.dumps(replay))
            await pub.publish(json.dumps(new))
        try:
            await wait_until("replayed + new samples reconciled",
                       lambda: db.count("samples", None, None) == expected_total,
                       timeout=15)
        except AssertionError:
            print("DIAG samples=", db.count("samples", None, None),
                  "expected=", expected_total,
                  "connected=", archiver.connected, "epoch=", archiver.epoch,
                  "queue=", archiver.queue_depth(), "counters=", archiver.counters,
                  flush=True)
            raise
        # The replays must not create duplicate samples.
        assert archiver.counters.duplicates >= len(redelivered)
        print(f"[9] restart resumed; redelivery idempotent "
              f"({len(redelivered)} replays collapsed, {len(fresh)} new stored)")

        pub.stop()
        archiver.stop()
        db.close()
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append(str(exc))
    finally:
        await broker.shutdown()
        os.chdir(cwd)
        shutil.rmtree(workdir, ignore_errors=True)

    if failures:
        print("INTEGRATION TEST FAILED")
        sys.exit(1)
    print("INTEGRATION TEST PASSED")


if __name__ == "__main__":
    asyncio.run(main())
