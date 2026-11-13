"""Unit tests for the offline-delivery packaging feature.

Covers the end-to-end job lifecycle without an MQTT broker: snapshot
consistency, streamed ZIP contents, single-slot enforcement, cancellation
(including the verified-but-unpublished window), repeated cancels, restart
recovery, download gating and bounded-memory batch streaming.

Run with:  PYTHONPATH=app python -m unittest tests.test_packager -v
"""
from __future__ import annotations

import csv
import io
import json
import os
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path

from app.config import Settings
from app.db import Database
from app.models import PackageStatus
from app.packager import (
    CONFLICT_COLUMNS,
    CONFLICTS_CSV,
    SAMPLE_COLUMNS,
    SAMPLES_CSV,
    PackageManager,
    SlotBusyError,
)


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


def make_manager(root: str, db: Database, batch_size: int = 100) -> PackageManager:
    settings = Settings(
        broker_host="localhost",
        broker_port=1883,
        client_id="test",
        topic="t",
        db_path=os.path.join(root, "data", "test.db"),
        packages_dir=os.path.join(root, "packages"),
        package_batch_size=batch_size,
    )
    manager = PackageManager(settings, db)
    return manager


def wait_status(manager: PackageManager, job_id: str, statuses,
                timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = manager.get_job(job_id)
        if record.status in statuses:
            return record
        time.sleep(0.02)
    raise AssertionError(
        f"job {job_id} never reached {statuses}; "
        f"current={manager.get_job(job_id).status}"
    )


def read_archive(path: Path) -> dict[str, list[list[str]]]:
    with zipfile.ZipFile(path) as zf:
        assert zf.testzip() is None
        out = {}
        for name in (SAMPLES_CSV, CONFLICTS_CSV):
            with zf.open(name) as handle:
                text = io.TextIOWrapper(handle, encoding="utf-8", newline="")
                out[name] = list(csv.reader(text))
        out["manifest.json"] = json.loads(zf.read("manifest.json"))
    return out


class PackagingTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.db_path = os.path.join(self.root, "test.db")
        self.db = Database(self.db_path)
        self.manager = make_manager(self.root, self.db)
        self.manager.recover()

    def tearDown(self) -> None:
        manager = getattr(self, "manager", None)
        if manager is not None:
            manager.shutdown(timeout=10)
        self.db.close()
        self.tmp.cleanup()

    def seed(self, n: int = 25, instrument: str = "FERM-01",
             batch: str = "B-1", with_conflict: bool = True) -> None:
        for seq in range(1, n + 1):
            self.db.store_measurement(payload(seq, instrument=instrument,
                                              batch=batch))
        if with_conflict:
            # Contradicting temperature on seq 3 -> one conflict row.
            self.db.store_measurement(
                payload(3, temp=99.9, instrument=instrument, batch=batch)
            )


class HappyPathTest(PackagingTestBase):
    def test_package_contains_snapshot_samples_and_conflicts(self):
        self.seed(n=25)
        record = self.manager.create_job("FERM-01", "B-1")
        record = wait_status(
            self.manager, record.job_id, {PackageStatus.COMPLETED}
        )

        self.assertEqual(record.sample_rows, 25)
        self.assertEqual(record.conflict_rows, 1)
        self.assertEqual(record.rows_written, 26)
        self.assertIsNotNone(record.sha256)
        self.assertGreater(record.bytes_written, 0)
        self.assertIsNotNone(record.snapshot_at)
        self.assertIsNotNone(record.finished_at)

        path = self.manager.published_file(record.job_id)
        self.assertIsNotNone(path)
        self.assertTrue(path.is_file())
        contents = read_archive(path)

        sample_rows = contents[SAMPLES_CSV]
        self.assertEqual(sample_rows[0], SAMPLE_COLUMNS)
        self.assertEqual(len(sample_rows), 26)  # header + 25 data rows
        seqs = {int(r[3]) for r in sample_rows[1:]}
        self.assertEqual(seqs, set(range(1, 26)))

        conflict_rows = contents[CONFLICTS_CSV]
        self.assertEqual(conflict_rows[0], CONFLICT_COLUMNS)
        self.assertEqual(len(conflict_rows), 2)  # header + 1 conflict
        self.assertEqual(conflict_rows[1][4], "temperature_c")
        self.assertEqual(float(conflict_rows[1][6]), 99.9)

        manifest = contents["manifest.json"]
        self.assertEqual(manifest["sample_rows"], 25)
        self.assertEqual(manifest["conflict_rows"], 1)
        self.assertEqual(manifest["instrument_id"], "FERM-01")
        self.assertEqual(manifest["batch_no"], "B-1")

        # Staging directory must be gone after success.
        self.assertFalse(
            (Path(self.root) / "packages" / "staging" / record.job_id).exists()
        )

    def test_rows_committed_after_snapshot_are_never_mixed_in(self):
        self.seed(n=10)
        # Slow every batch so the snapshot opens before the extra write lands.
        self.manager.hooks.set(0.3, 0.0)
        record = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, record.job_id, {PackageStatus.READING})
        time.sleep(0.15)  # worker is inside its first paused batch

        # New measurement committed while the packaging snapshot is open.
        self.db.store_measurement(payload(999, instrument="FERM-01",
                                          batch="B-1"))
        # A second batch's data, too.
        self.db.store_measurement(payload(998, instrument="FERM-01",
                                          batch="B-1"))

        record = wait_status(
            self.manager, record.job_id, {PackageStatus.COMPLETED}, timeout=20
        )
        self.assertEqual(record.sample_rows, 10)  # seq 999/998 excluded
        path = self.manager.published_file(record.job_id)
        contents = read_archive(path)
        seqs = {int(r[3]) for r in contents[SAMPLES_CSV][1:]}
        self.assertNotIn(999, seqs)
        self.assertNotIn(998, seqs)
        # The live table, however, does contain the late rows.
        self.assertEqual(self.db.count("samples", "FERM-01", "B-1"), 12)

    def test_filters_by_instrument_and_batch(self):
        self.seed(n=5, instrument="A", batch="X")
        self.seed(n=3, instrument="A", batch="Y", with_conflict=False)
        self.seed(n=2, instrument="B", batch="X", with_conflict=False)
        record = self.manager.create_job("A", "X")
        wait_status(self.manager, record.job_id, {PackageStatus.COMPLETED})
        contents = read_archive(self.manager.published_file(record.job_id))
        self.assertEqual(len(contents[SAMPLES_CSV]) - 1, 5)
        self.assertEqual(len(contents[CONFLICTS_CSV]) - 1, 1)

    def test_empty_filter_still_produces_valid_package(self):
        self.seed(n=3)
        record = self.manager.create_job("NOPE", "NOPE")
        wait_status(self.manager, record.job_id, {PackageStatus.COMPLETED})
        contents = read_archive(self.manager.published_file(record.job_id))
        self.assertEqual(contents[SAMPLES_CSV], [SAMPLE_COLUMNS])
        self.assertEqual(contents[CONFLICTS_CSV], [CONFLICT_COLUMNS])
        self.assertEqual(contents["manifest.json"]["sample_rows"], 0)


class SingleSlotTest(PackagingTestBase):
    def test_second_job_rejected_while_first_runs_then_allowed(self):
        self.seed(n=200)
        self.manager.hooks.set(0.2, 0.0)
        first = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, first.job_id, {PackageStatus.READING})

        with self.assertRaises(SlotBusyError):
            self.manager.create_job("FERM-01", "B-1")

        wait_status(self.manager, first.job_id,
                    {PackageStatus.COMPLETED}, timeout=30)
        # Slot released: a new job is accepted immediately.
        second = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, second.job_id, {PackageStatus.COMPLETED})

    def test_failed_job_releases_slot(self):
        # Force the worker to fail after the snapshot transaction has opened
        # by making the first ZIP entry write raise. The job must settle as
        # failed, clean staging and free the slot for the next job.
        self.seed(n=2)
        from app.packager import StreamingZipWriter

        original = StreamingZipWriter.start_entry

        def boom(self, name):  # noqa: ANN001
            raise OSError("simulated disk failure")

        StreamingZipWriter.start_entry = boom  # type: ignore[method-assign]
        try:
            job = self.manager.create_job("FERM-01", "B-1")
            record = wait_status(
                self.manager, job.job_id, {PackageStatus.FAILED}
            )
            self.assertIsNotNone(record.error)
            self.assertIsNone(self.manager.published_file(job.job_id))
            # Staging directory was removed by the failure path.
            self.assertFalse(
                (
                    Path(self.root) / "packages" / "staging" / job.job_id
                ).exists()
            )
        finally:
            StreamingZipWriter.start_entry = original  # type: ignore[method-assign]

        # Slot released: a subsequent, healthy job runs and publishes.
        new_job = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, new_job.job_id, {PackageStatus.COMPLETED})
        self.assertIsNotNone(
            self.manager.published_file(new_job.job_id)
        )


class CancelTest(PackagingTestBase):
    def test_cancel_during_reading_stops_and_cleans(self):
        self.seed(n=300)
        self.manager.hooks.set(0.3, 0.0)  # long per-batch pause
        job = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, job.job_id, {PackageStatus.READING})
        time.sleep(0.2)

        self.manager.request_cancel(job.job_id)
        settled = self.manager.wait_settled(job.job_id, timeout=15)
        self.assertTrue(settled)
        record = self.manager.get_job(job.job_id)
        self.assertEqual(record.status, PackageStatus.CANCELLED)
        self.assertTrue(record.cancel_requested)
        self.assertIsNotNone(record.finished_at)

        # Nothing published, nothing downloadable, staging cleaned.
        self.assertIsNone(self.manager.published_file(job.job_id))
        staging = Path(self.root) / "packages" / "staging" / job.job_id
        self.assertFalse(staging.exists())
        published = list((Path(self.root) / "packages" / "published").glob("*"))
        self.assertEqual(published, [])

        # Slot released for a new job.
        next_job = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, next_job.job_id,
                    {PackageStatus.COMPLETED}, timeout=20)

    def test_repeated_cancel_waits_on_same_teardown(self):
        self.seed(n=300)
        self.manager.hooks.set(0.25, 0.0)
        job = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, job.job_id, {PackageStatus.READING})
        time.sleep(0.15)

        results = []

        def cancel_call() -> None:
            self.manager.request_cancel(job.job_id)
            ok = self.manager.wait_settled(job.job_id, timeout=15)
            results.append(ok)

        threads = [threading.Thread(target=cancel_call) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)
        self.assertEqual(results, [True, True, True])
        self.assertEqual(
            self.manager.get_job(job.job_id).status, PackageStatus.CANCELLED
        )

    def test_cancel_in_pre_publish_window_suppresses_finished_zip(self):
        self.seed(n=10)
        self.manager.hooks.set(0.0, 5.0)  # hold before publishing
        job = self.manager.create_job("FERM-01", "B-1")
        # Wait until the verified archive is sitting in staging, unpublished.
        record = wait_status(
            self.manager, job.job_id,
            {PackageStatus.VERIFYING, PackageStatus.PUBLISHING},
            timeout=15,
        )
        time.sleep(0.3)
        staging_zip = (
            Path(self.root) / "packages" / "staging" / job.job_id /
            "package.zip.tmp"
        )
        self.assertTrue(staging_zip.is_file())  # fully written on disk
        self.assertIsNone(self.manager.published_file(job.job_id))

        self.manager.request_cancel(job.job_id)
        self.assertTrue(self.manager.wait_settled(job.job_id, timeout=15))
        record = self.manager.get_job(job.job_id)
        self.assertEqual(record.status, PackageStatus.CANCELLED)
        self.assertIsNone(self.manager.published_file(job.job_id))
        self.assertFalse(staging_zip.exists())
        self.assertEqual(
            list((Path(self.root) / "packages" / "published").glob("*")), []
        )

    def test_cancel_after_completion_keeps_package(self):
        self.seed(n=4)
        job = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, job.job_id, {PackageStatus.COMPLETED})
        record = self.manager.request_cancel(job.job_id)
        self.assertEqual(record.status, PackageStatus.COMPLETED)
        self.assertIsNotNone(self.manager.published_file(job.job_id))

    def test_client_disconnect_does_not_cancel(self):
        # The worker is independent of HTTP requests; here we simply assert
        # that letting the creating reference go out of scope never cancels.
        self.seed(n=5)
        job = self.manager.create_job("FERM-01", "B-1")
        job_id = job.job_id
        del job
        record = wait_status(
            self.manager, job_id, {PackageStatus.COMPLETED}
        )
        self.assertEqual(record.status, PackageStatus.COMPLETED)


class RestartRecoveryTest(PackagingTestBase):
    def test_completed_package_survives_new_manager(self):
        self.seed(n=6)
        job = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, job.job_id, {PackageStatus.COMPLETED})
        job_id = job.job_id
        archive = self.manager.published_file(job_id)
        self.assertTrue(archive.is_file())

        # Simulate a full process restart: brand new manager over same dirs.
        manager2 = make_manager(self.root, self.db)
        manager2.recover()
        try:
            record = manager2.get_job(job_id)
            self.assertEqual(record.status, PackageStatus.COMPLETED)
            path = manager2.published_file(job_id)
            self.assertIsNotNone(path)
            contents = read_archive(path)
            self.assertEqual(len(contents[SAMPLES_CSV]) - 1, 6)
        finally:
            manager2.shutdown(timeout=10)

    def test_unfinished_job_marked_failed_and_staging_cleared(self):
        # Simulate a hard process crash purely from on-disk state: metadata
        # left in a non-terminal status and a half-written staging file, with
        # no live worker thread that could clean it up.
        crash_job_id = "c0ffeec0ffeec0ffeec0ffeec0ffee12"
        jobs_dir = Path(self.root) / "packages" / "jobs"
        staging = (
            Path(self.root) / "packages" / "staging" / crash_job_id
        )
        staging.mkdir(parents=True)
        (staging / "package.zip.tmp").write_bytes(b"partial zip bytes")
        metadata = {
            "job_id": crash_job_id,
            "instrument_id": "FERM-01",
            "batch_no": "B-1",
            "status": "reading",
            "cancel_requested": False,
            "created_at": "2026-11-13T00:00:00.000+00:00",
            "started_at": "2026-11-13T00:00:01.000+00:00",
            "finished_at": None,
            "snapshot_at": "2026-11-13T00:00:02.000+00:00",
            "rows_written": 4,
            "sample_rows": 4,
            "conflict_rows": 0,
            "bytes_written": 0,
            "error": None,
            "file_name": "FERM-01_B-1_c0ffeec0.zip",
            "sha256": None,
        }
        (jobs_dir / f"{crash_job_id}.json").write_text(
            json.dumps(metadata), encoding="utf-8"
        )

        # A brand-new manager over the same directories plays the restart.
        manager2 = make_manager(self.root, self.db)
        manager2.recover()
        try:
            record = manager2.get_job(crash_job_id)
            self.assertEqual(record.status, PackageStatus.FAILED)
            self.assertIn("restart", record.error)
            self.assertFalse(staging.exists())
            self.assertIsNone(manager2.published_file(crash_job_id))
            # A new job can run immediately: the slot starts free.
            new_job = manager2.create_job("FERM-01", "B-1")
            wait_status(manager2, new_job.job_id,
                        {PackageStatus.COMPLETED}, timeout=20)
        finally:
            manager2.shutdown(timeout=10)

    def test_orphan_staging_dir_removed_on_recovery(self):
        orphan = Path(self.root) / "packages" / "staging" / "deadbeef"
        orphan.mkdir(parents=True)
        (orphan / "package.zip.tmp").write_bytes(b"partial")
        manager2 = make_manager(self.root, self.db)
        manager2.recover()
        try:
            self.assertFalse(orphan.exists())
        finally:
            manager2.shutdown(timeout=10)


class StreamingMemoryTest(PackagingTestBase):
    def test_large_table_streamed_in_small_batches(self):
        # 2,000 rows with a tiny batch size exercises many streaming chunks
        # while keeping each batch to a handful of rows.
        manager = make_manager(self.root, self.db, batch_size=13)
        manager.recover()
        for seq in range(1, 2001):
            self.db.store_measurement(
                payload(seq, instrument="BIG", batch="RUN")
            )
        job = manager.create_job("BIG", "RUN")
        record = wait_status(manager, job.job_id,
                             {PackageStatus.COMPLETED}, timeout=60)
        self.assertEqual(record.sample_rows, 2000)
        contents = read_archive(manager.published_file(job.job_id))
        self.assertEqual(len(contents[SAMPLES_CSV]) - 1, 2000)
        # Bounded batch metadata persisted as the job progressed.
        self.assertGreaterEqual(record.rows_written, 2000)
        manager.shutdown(timeout=10)


class WriterLockTest(PackagingTestBase):
    def test_packaging_never_blocks_mqtt_writer(self):
        self.seed(n=50)
        self.manager.hooks.set(0.2, 0.0)
        job = self.manager.create_job("FERM-01", "B-1")
        wait_status(self.manager, job.job_id, {PackageStatus.READING})

        # While the snapshot read transaction is open, writes must keep
        # committing immediately (WAL: reader never takes a write lock).
        start = time.monotonic()
        for seq in range(500, 520):
            self.db.store_measurement(
                payload(seq, instrument="OTHER", batch="X")
            )
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 5.0)
        self.assertEqual(self.db.count("samples", "OTHER", "X"), 20)

        wait_status(self.manager, job.job_id,
                    {PackageStatus.COMPLETED}, timeout=30)


if __name__ == "__main__":
    unittest.main(verbosity=2)
