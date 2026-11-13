"""Unit tests for the archiver's core guarantees.

These tests run without a broker: the MQTT client is replaced by a tiny stub
so the tests can deterministically simulate connection generations (epochs).
Run with:  python -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import os
import tempfile
import time
import unittest

from app.config import Settings
from app.db import Database
from app.models import Measurement
from app.mqtt_archiver import MqttArchiver, _WorkItem


def payload(seq: int, temp: float = 30.0, ph: float = 6.8,
            instrument: str = "FERM-01", batch: str = "B-1") -> dict:
    return {
        "instrument_id": instrument,
        "batch_no": batch,
        "sample_seq": seq,
        "ts_utc": "2026-11-13T00:00:00.000+00:00",
        "temperature_c": temp,
        "ph": ph,
    }


class FakeAckClient:
    """Records PUBACK calls instead of touching any socket."""

    def __init__(self) -> None:
        self.acks: list[tuple[int, int]] = []
        self.raise_on_ack = False

    def ack(self, mid: int, qos: int):
        if self.raise_on_ack:
            raise OSError("socket gone")
        self.acks.append((mid, qos))
        import paho.mqtt.client as mqtt
        return mqtt.MQTT_ERR_SUCCESS

    def loop_stop(self, *a, **kw):
        return None

    def disconnect(self, *a, **kw):
        import paho.mqtt.client as mqtt
        return mqtt.MQTT_ERR_SUCCESS


def make_archiver(db_path: str) -> tuple[MqttArchiver, FakeAckClient]:
    settings = Settings(
        broker_host="localhost", broker_port=1883,
        client_id="test-archiver", topic="t", db_path=db_path,
    )
    arch = MqttArchiver(settings, Database(db_path))
    fake = FakeAckClient()
    arch.client = fake  # replace paho client entirely
    return arch, fake


class DatabaseRulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.tmp.name, "test.db"))

    def tearDown(self) -> None:
        self.db.close()
        self.tmp.cleanup()

    def test_first_sample_stored_and_duplicate_ignored(self):
        p = payload(1)
        self.assertEqual(self.db.store_measurement(p), "stored")
        # Re-delivery of the exact same bytes: no second row, no conflict.
        self.assertEqual(self.db.store_measurement(dict(p)), "duplicate")
        rows = self.db.list_samples(None, None, 100, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(self.db.count("conflicts", None, None), 0)

    def test_conflict_retained_without_touching_original(self):
        self.db.store_measurement(payload(2, temp=30.0, ph=6.8))
        # Different temperature on the same business key.
        result = self.db.store_measurement(payload(2, temp=31.5, ph=6.8))
        self.assertEqual(result, "conflict")
        # Different pH -> another conflict row.
        result = self.db.store_measurement(payload(2, temp=30.0, ph=7.1))
        self.assertEqual(result, "conflict")

        rows = self.db.list_samples(None, None, 100, 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["temperature_c"], 30.0)  # original intact
        self.assertEqual(rows[0]["ph"], 6.8)

        conflicts = self.db.list_conflicts(None, None, 100, 0)
        self.assertEqual(len(conflicts), 2)
        fields = {c["field_name"]: c for c in conflicts}
        self.assertEqual(fields["temperature_c"]["incoming_value"], 31.5)
        self.assertEqual(fields["ph"]["incoming_value"], 7.1)

    def test_redelivered_conflict_is_idempotent(self):
        self.db.store_measurement(payload(3, temp=30.0, ph=6.8))
        bad = payload(3, temp=99.0, ph=6.8)
        self.assertEqual(self.db.store_measurement(bad), "conflict")
        # Broker redelivery after an ack loss: must not duplicate the row.
        self.assertEqual(self.db.store_measurement(dict(bad)),
                         "conflict_duplicate")
        self.assertEqual(self.db.count("conflicts", None, None), 1)

    def test_two_different_conflicting_values_both_kept(self):
        self.db.store_measurement(payload(4, temp=30.0, ph=6.8))
        self.db.store_measurement(payload(4, temp=31.0, ph=6.8))
        self.db.store_measurement(payload(4, temp=32.0, ph=6.8))
        self.assertEqual(self.db.count("conflicts", None, None), 2)

    def test_filters_by_instrument_and_batch(self):
        self.db.store_measurement(payload(1, instrument="A", batch="X"))
        self.db.store_measurement(payload(1, instrument="A", batch="Y"))
        self.db.store_measurement(payload(1, instrument="B", batch="X"))
        self.assertEqual(self.db.count("samples", "A", None), 2)
        self.assertEqual(self.db.count("samples", "A", "X"), 1)
        self.assertEqual(self.db.count("samples", None, "X"), 2)


class EpochAckGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.arch, self.fake = make_archiver(
            os.path.join(self.tmp.name, "test.db"))
        self.arch.start_worker()

    def tearDown(self) -> None:
        self.arch.stop()
        self.arch.db.close()
        self.tmp.cleanup()

    def _enqueue(self, epoch: int, mid: int, raw: bytes) -> _WorkItem:
        item = _WorkItem(epoch=epoch, mid=mid, raw=raw, topic="t")
        self.arch._queue.put(item)
        return item

    def _drain(self, timeout: float = 3.0) -> None:
        # Queue.join() returns only after every dequeued item has run through
        # the worker and called task_done(), avoiding empty()-poll races.
        deadline = time.monotonic() + timeout
        while not self.arch._queue.empty() and time.monotonic() < deadline:
            try:
                self.arch._queue.join()
                break
            except RuntimeError:
                time.sleep(0.01)
        self.assertTrue(self.arch._queue.empty(), "worker did not drain queue")

    def test_ack_only_after_successful_write(self):
        with self.arch._epoch_lock:
            self.arch._epoch = 1
            self.arch._connected = True
        self._enqueue(1, 100, json.dumps(payload(1)).encode())
        self._drain()
        self.assertEqual(self.fake.acks, [(100, 1)])
        self.assertEqual(self.arch.counters.stored, 1)

    def test_stale_epoch_never_acked_even_with_reused_mid(self):
        # Message arrives on connection epoch 1 (mid 55)...
        with self.arch._epoch_lock:
            self.arch._epoch = 1
            self.arch._connected = True
        self._enqueue(1, 55, json.dumps(payload(1)).encode())

        # ...the link dies and a new connection comes up (epoch 2), which may
        # legitimately reuse mid 55 for a brand-new message.
        with self.arch._epoch_lock:
            self.arch._epoch = 2
            self.arch._connected = True
        self._enqueue(2, 55, json.dumps(payload(2)).encode())

        self._drain()

        # Exactly one PUBACK for mid 55: the one belonging to epoch 2. The
        # stale epoch-1 result was dropped, never acked on the new connection.
        self.assertEqual(self.fake.acks, [(55, 1)])
        self.assertEqual(self.arch.counters.stale_drops, 1)
        # The stale write did land; broker redelivery will reconcile it.
        self.assertEqual(self.arch.counters.stored, 2)

    def test_ack_failure_does_not_crash_and_is_counted(self):
        self.fake.raise_on_ack = True
        with self.arch._epoch_lock:
            self.arch._epoch = 1
            self.arch._connected = True
        self._enqueue(1, 7, json.dumps(payload(9)).encode())
        self._drain()
        self.assertEqual(self.arch.counters.ack_failures, 1)
        self.assertEqual(self.arch.counters.stored, 1)  # write still committed

    def test_invalid_payload_acked_but_not_stored(self):
        with self.arch._epoch_lock:
            self.arch._epoch = 1
            self.arch._connected = True
        self._enqueue(1, 9, b"{broken json")
        self._drain()
        self.assertEqual(self.fake.acks, [(9, 1)])
        self.assertEqual(self.arch.counters.invalid_payloads, 1)
        self.assertEqual(self.arch.db.count("samples", None, None), 0)

    def test_pause_delays_ack_but_queue_keeps_accepting(self):
        with self.arch._epoch_lock:
            self.arch._epoch = 1
            self.arch._connected = True
        self.arch.pauses.set(0.6, 0.0)
        self._enqueue(1, 1, json.dumps(payload(1)).encode())
        # While the writer is sleeping inside the first item, more messages
        # can still be enqueued (network thread would do exactly this).
        self._enqueue(1, 2, json.dumps(payload(2)).encode())
        self._enqueue(1, 3, json.dumps(payload(3)).encode())
        start = time.monotonic()
        self._drain(timeout=5)
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.55)
        # All three messages were accepted into the queue up-front and every
        # one is eventually acked (total delay is one pause, not three -- the
        # queue kept buffering while the writer slept on the first item).
        self.assertEqual(len(self.fake.acks), 3)
        self.assertEqual(self.arch.counters.stored, 3)


class PayloadValidationTest(unittest.TestCase):
    def test_business_key_fields_required(self):
        good = payload(1)
        Measurement.model_validate(good)  # does not raise
        for field in ("instrument_id", "batch_no", "sample_seq",
                      "ts_utc", "temperature_c", "ph"):
            bad = dict(good)
            del bad[field]
            with self.assertRaises(Exception):
                Measurement.model_validate(bad)

    def test_extra_fields_ignored(self):
        p = payload(1)
        p["firmware"] = "v42"
        m = Measurement.model_validate(p)
        self.assertFalse(hasattr(m, "firmware"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
