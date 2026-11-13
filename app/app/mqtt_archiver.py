"""MQTT ingestion: persistent session, manual QoS 1 ack, serialised writer.

Threading model
---------------
* paho-mqtt runs its own network loop thread (``loop_start``). It only parses
  packets and hands messages to ``on_message``; it never touches SQLite.
* ``on_message`` enqueues a lightweight work item and returns immediately, so
  MQTT keep-alive heartbeats and incoming traffic are never blocked by a slow
  write (or by a demo pause).
* One dedicated worker thread performs every database write and sends the
  PUBACK **after** the write has committed (manual ack mode,
  ``ManualAcks``). FastAPI queries use separate read connections and proceed
  concurrently under SQLite WAL.

Reconnection safety
-------------------
A packet id (mid) is scoped to one TCP connection and may be reused after a
reconnect. We therefore never key any state on the mid. Each connection gets a
monotonic ``epoch``; every work item remembers the epoch of the connection it
arrived on. A late result from a dead connection is *never* acked on the new
connection -- even if a new message happens to reuse the same mid. paho itself
also drops queued outgoing acks on reconnect; the epoch check is our own guard
on top of that.
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import dataclass, field
from typing import Optional

import paho.mqtt.client as mqtt

from .config import Settings
from .db import Database
from .models import Measurement

log = logging.getLogger("archiver.mqtt")


@dataclass
class _WorkItem:
    epoch: int
    mid: int
    raw: bytes
    topic: str


@dataclass
class Counters:
    stored: int = 0
    duplicates: int = 0
    conflicts: int = 0
    duplicate_conflicts: int = 0
    invalid_payloads: int = 0
    stale_drops: int = 0
    ack_failures: int = 0


@dataclass
class PauseState:
    # Delays applied by the *worker* thread only.
    pre_write_seconds: float = 0.0
    post_write_seconds: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> tuple[float, float]:
        with self.lock:
            return self.pre_write_seconds, self.post_write_seconds

    def set(self, pre: float, post: float) -> None:
        with self.lock:
            self.pre_write_seconds, self.post_write_seconds = pre, post


class MqttArchiver:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db
        self._queue: queue.Queue[_WorkItem] = queue.Queue()
        self.counters = Counters()
        self.pauses = PauseState()

        # Epoch bookkeeping. The lock also orders connect/disconnect callbacks
        # relative to the worker's ack decision.
        self._epoch_lock = threading.Lock()
        self._epoch = 0
        self._connected = False

        self._stop = threading.Event()
        self._worker: Optional[threading.Thread] = None

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=False,  # durable (persistent) session
            # PUBACK/PUBCOMP only leave when *we* call client.ack(), i.e. after
            # the database write has committed.
            manual_ack=True,
        )
        self.client.max_inflight_messages_set(settings.max_inflight)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    # -- public lifecycle --------------------------------------------------

    def start_worker(self) -> None:
        """Start only the DB writer thread (used by tests and by start())."""
        if self._worker is not None and self._worker.is_alive():
            return
        self._stop.clear()
        self._worker = threading.Thread(
            target=self._worker_loop, name="db-writer", daemon=True
        )
        self._worker.start()

    def start(self) -> None:
        self.start_worker()
        self.client.connect_async(
            self.settings.broker_host, self.settings.broker_port,
            keepalive=self.settings.keepalive,
        )
        # paho's own network thread. Reconnects are handled automatically.
        self.client.loop_start()
        log.info(
            "archiver started: broker=%s:%s topic=%s client_id=%s "
            "clean_session=False",
            self.settings.broker_host,
            self.settings.broker_port,
            self.settings.topic,
            self.settings.client_id,
        )

    def stop(self) -> None:
        """Drain in-memory work, then tear the client down.

        Note the guarantee this deliberately does *not* give: if the process
        is killed before the PUBACK of a committed message reaches the broker,
        the broker will redeliver that message after restart. The unique
        constraints make the replay harmless (duplicate/conflict_duplicate),
        which is exactly why business keys -- not packet ids -- do the
        de-duplication.
        """
        log.info("stopping archiver: draining queued writes")
        self._stop.set()
        if self._worker is not None:
            self._worker.join(timeout=30)
        self.client.loop_stop()
        try:
            self.client.disconnect()
        except Exception:  # pragma: no cover - broker may already be gone
            log.exception("error during mqtt disconnect")

    def force_reconnect(self) -> None:
        """Demo hook: tear down the TCP link and let paho reconnect.

        The broker keeps our persistent session and redelivers any unacked
        QoS 1 publish.
        """
        with self._epoch_lock:
            self._connected = False
        # disconnect() resets paho's outbound packet queue, so acks generated
        # by the old connection cannot leak out after the new one is up.
        self.client.disconnect()
        self.client.reconnect()

    # -- introspection for the API -----------------------------------------

    @property
    def connected(self) -> bool:
        with self._epoch_lock:
            return self._connected

    @property
    def epoch(self) -> int:
        with self._epoch_lock:
            return self._epoch

    def queue_depth(self) -> int:
        return self._queue.qsize()

    # -- paho callbacks (network thread; keep them short) ------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            log.error("connect failed: reason_code=%s", reason_code)
            return
        with self._epoch_lock:
            self._epoch += 1
            self._connected = True
            epoch = self._epoch
            session_present = bool(getattr(flags, "session_present", False))
        # (Re)subscribing on every connect is safe and recommended; with a
        # persistent session the broker also restores the subscription itself.
        client.subscribe(self.settings.topic, qos=1)
        log.info(
            "connected (epoch=%d, session_present=%s) subscribed to %s qos=1",
            epoch, session_present, self.settings.topic,
        )

    def _on_disconnect(self, client, userdata, *args, **kwargs):
        with self._epoch_lock:
            self._connected = False
            epoch = self._epoch
        log.warning("disconnected (epoch=%d); paho will auto-reconnect", epoch)

    def _on_message(self, client, userdata, message: mqtt.MQTTMessage):
        # Runs on the network thread: just snapshot the epoch and hand off.
        # No JSON parsing, no SQLite, no sleeping here.
        with self._epoch_lock:
            epoch = self._epoch
        self._queue.put(
            _WorkItem(
                epoch=epoch,
                mid=message.mid,
                raw=message.payload,
                topic=message.topic,
            )
        )

    # -- worker thread (the only writer) -----------------------------------

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._process(item)
            except Exception:
                # Never let a poison task kill the writer. A failure leaves the
                # message unacked, so the broker will redeliver it.
                log.exception("worker error on mid=%s", item.mid)
            finally:
                self._queue.task_done()

    def _process(self, item: _WorkItem) -> None:
        pre, post = self.pauses.snapshot()
        if pre:
            # Simulates slow storage. Only this thread sleeps; the network
            # loop keeps answering PINGREQ and accepting messages.
            self._interruptible_sleep(pre)

        try:
            payload = json.loads(item.raw.decode("utf-8"))
            measurement = Measurement.model_validate(payload)
        except Exception as exc:
            # Malformed publish: nothing idempotent we could do later, and
            # retrying forever would create a poison queue. Ack it and log.
            log.warning("invalid payload on mid=%s: %r", item.mid, exc)
            with self._epoch_lock:
                self.counters.invalid_payloads += 1
            self._ack_if_current(item)
            return

        result = self.db.store_measurement(measurement.model_dump())
        self._record_result(result)
        log.info(
            "processed key=(%s,%s,%d) result=%s mid=%d epoch=%d",
            measurement.instrument_id,
            measurement.batch_no,
            measurement.sample_seq,
            result,
            item.mid,
            item.epoch,
        )

        if post:
            # Simulates a slow fsync *after* commit but *before* PUBACK: the
            # crash window where restart must resume via redelivery.
            self._interruptible_sleep(post)

        self._ack_if_current(item)

    def _record_result(self, result: str) -> None:
        if result == "stored":
            self.counters.stored += 1
        elif result == "duplicate":
            self.counters.duplicates += 1
        elif result == "conflict":
            self.counters.conflicts += 1
        elif result == "conflict_duplicate":
            self.counters.duplicate_conflicts += 1

    def _interruptible_sleep(self, seconds: float) -> None:
        self._stop.wait(timeout=seconds)

    def _ack_if_current(self, item: _WorkItem) -> bool:
        """Ack only when the message still belongs to the live connection."""
        with self._epoch_lock:
            current_epoch = self._epoch
            connected = self._connected
        if item.epoch != current_epoch:
            # Stale result from a connection that died mid-write. Do NOT ack:
            # the mid may already identify a different message on the new
            # connection. The broker redelivers the original one.
            self.counters.stale_drops += 1
            log.warning(
                "dropping ack for stale message: item_epoch=%d current_epoch=%d "
                "mid=%d (mid may be reused on the new connection)",
                item.epoch, current_epoch, item.mid,
            )
            return False
        if not connected:
            # Gap between disconnect and the new connect callback: wait for
            # the new epoch, which will make this item stale by definition.
            self.counters.stale_drops += 1
            log.warning(
                "not connected; leaving mid=%d unacked for redelivery", item.mid
            )
            return False
        try:
            # paho 2.x: ack(mid, qos) queues the PUBACK; it is thread-safe via
            # the internal packet mutex. The epoch check above is the real
            # protection against acking a message whose mid was reused on a
            # newer connection.
            rc = self.client.ack(item.mid, 1)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"paho ack returned {rc!r}")
            return True
        except Exception as exc:
            self.counters.ack_failures += 1
            log.error(
                "PUBACK failed for mid=%d epoch=%d: %r; broker will redeliver",
                item.mid, item.epoch, exc,
            )
            return False
