"""HTTP-level tests for the packaging API using FastAPI's TestClient.

The MQTT archiver is still constructed by the lifespan but never reaches a
broker in this environment; it retries harmlessly in the background while
the packaging endpoints are exercised against a temporary SQLite database
and package directory.

Run with:  PYTHONPATH=app python -m unittest tests.test_api_packages -v
"""
from __future__ import annotations

import io
import json
import os
import tempfile
import time
import unittest
import zipfile

# Settings are read from the environment at import time, so point the data
# directories at a throwaway location before importing the app.
_TMP = tempfile.mkdtemp(prefix="pkg-api-")
os.environ["DB_PATH"] = os.path.join(_TMP, "archiver.db")
os.environ["PACKAGES_DIR"] = os.path.join(_TMP, "packages")
os.environ["MQTT_HOST"] = "127.0.0.1"
os.environ["MQTT_PORT"] = "11999"  # nothing listening; retries stay harmless

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app, state  # noqa: E402
from app.models import PackageStatus  # noqa: E402


def payload(seq: int, temp: float = 30.0, ph: float = 6.8) -> dict:
    return {
        "instrument_id": "FERM-01",
        "batch_no": "BATCH-1",
        "sample_seq": seq,
        "ts_utc": "2026-11-13T00:00:00.000+00:00",
        "temperature_c": temp,
        "ph": ph,
    }


def wait_for(client: TestClient, job_id: str, statuses: set[str],
             timeout: float = 20.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/packages/{job_id}").json()
        if body["status"] in statuses:
            return body
        time.sleep(0.05)
    raise AssertionError(f"{job_id} never reached {statuses}")


class PackageApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cls.client.__enter__()  # runs lifespan (db + packager.recover())

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.__exit__(None, None, None)

    def setUp(self) -> None:
        for seq in range(1, 21):
            state.db.store_measurement(payload(seq))
        state.db.store_measurement(payload(3, temp=41.0))  # one conflict

    # ------------------------------------------------------------ lifecycle

    def test_full_lifecycle_create_status_download(self):
        resp = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        )
        self.assertEqual(resp.status_code, 202)
        job = resp.json()
        self.assertEqual(job["status"], PackageStatus.PENDING.value)
        self.assertFalse(job["cancel_requested"])
        self.assertIsNone(job["download_url"])
        job_id = job["job_id"]

        done = wait_for(self.client, job_id, {"completed"})
        self.assertEqual(done["sample_rows"], 20)
        self.assertEqual(done["conflict_rows"], 1)
        self.assertEqual(done["rows_written"], 21)
        self.assertIsNotNone(done["sha256"])
        self.assertEqual(done["download_url"], f"/packages/{job_id}/download")

        listing = self.client.get("/packages").json()
        self.assertGreaterEqual(listing["count"], 1)
        self.assertTrue(any(j["job_id"] == job_id for j in listing["items"]))

        dl = self.client.get(f"/packages/{job_id}/download")
        self.assertEqual(dl.status_code, 200)
        self.assertEqual(dl.headers["content-type"], "application/zip")
        self.assertEqual(dl.headers["x-job-id"], job_id)
        self.assertEqual(dl.headers["x-sha256"], done["sha256"])
        with zipfile.ZipFile(io.BytesIO(dl.content)) as zf:
            self.assertIsNone(zf.testzip())
            self.assertEqual(
                set(zf.namelist()),
                {"samples.csv", "conflicts.csv", "manifest.json"},
            )
            manifest = json.loads(zf.read("manifest.json"))
            self.assertEqual(manifest["snapshot_at"], done["snapshot_at"])

    def test_unknown_job_returns_404(self):
        self.assertEqual(
            self.client.get("/packages/nope").status_code, 404
        )
        self.assertEqual(
            self.client.post("/packages/nope/cancel").status_code, 404
        )
        self.assertEqual(
            self.client.get("/packages/nope/download").status_code, 404
        )

    def test_validation_error_on_empty_fields(self):
        resp = self.client.post(
            "/packages", json={"instrument_id": "", "batch_no": "BATCH-1"}
        )
        self.assertEqual(resp.status_code, 422)

    # ----------------------------------------------------- single-job slot

    def test_slot_conflict_returns_409(self):
        state.packager.hooks.set(0.25, 0.0)
        first = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        ).json()
        wait_for(self.client, first["job_id"], {"reading"})

        conflict = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        )
        self.assertEqual(conflict.status_code, 409)
        self.assertIn("already running", conflict.json()["detail"])

        wait_for(self.client, first["job_id"], {"completed"}, timeout=20)
        # Slot released afterwards.
        again = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        )
        self.assertEqual(again.status_code, 202)
        wait_for(self.client, again.json()["job_id"], {"completed"})

    # --------------------------------------------------------------- cancel

    def test_cancel_during_reading_reports_only_after_settling(self):
        state.packager.hooks.set(0.3, 0.0)
        job = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        ).json()
        wait_for(self.client, job["job_id"], {"reading"})
        time.sleep(0.15)

        resp = self.client.post(f"/packages/{job['job_id']}/cancel")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["status"], "cancelled")
        self.assertTrue(body["cancel_requested"])

        # Status confirms the settled terminal state; no download exists.
        status = self.client.get(f"/packages/{job['job_id']}").json()
        self.assertEqual(status["status"], "cancelled")
        self.assertEqual(
            self.client.get(f"/packages/{job['job_id']}/download").status_code,
            404,
        )

        # A replacement job is accepted immediately after teardown.
        replacement = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        )
        self.assertEqual(replacement.status_code, 202)
        wait_for(self.client, replacement.json()["job_id"], {"completed"})

    def test_cancel_in_pre_publish_window_suppresses_archive(self):
        state.packager.hooks.set(0.0, 4.0)
        job = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        ).json()
        # Wait for the fully written, verified but still unpublished archive.
        wait_for(
            self.client, job["job_id"],
            {"verifying", "publishing"}, timeout=15,
        )
        time.sleep(0.3)
        # Not downloadable while it waits in the pre-publish window.
        self.assertEqual(
            self.client.get(f"/packages/{job['job_id']}/download").status_code,
            404,
        )

        resp = self.client.post(f"/packages/{job['job_id']}/cancel")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "cancelled")
        self.assertEqual(
            self.client.get(f"/packages/{job['job_id']}/download").status_code,
            404,
        )

    def test_repeated_cancel_calls_all_settle(self):
        state.packager.hooks.set(0.3, 0.0)
        job = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        ).json()
        wait_for(self.client, job["job_id"], {"reading"})
        time.sleep(0.1)

        first = self.client.post(f"/packages/{job['job_id']}/cancel")
        second = self.client.post(f"/packages/{job['job_id']}/cancel")
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["status"], "cancelled")
        self.assertEqual(second.json()["status"], "cancelled")

    def test_cancel_completed_job_keeps_download(self):
        job = self.client.post(
            "/packages",
            json={"instrument_id": "FERM-01", "batch_no": "BATCH-1"},
        ).json()
        wait_for(self.client, job["job_id"], {"completed"})
        resp = self.client.post(f"/packages/{job['job_id']}/cancel")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "completed")
        self.assertEqual(
            self.client.get(f"/packages/{job['job_id']}/download").status_code,
            200,
        )

    # ------------------------------------------------------------ demo hooks

    def test_demo_package_pause_endpoints(self):
        resp = self.client.post(
            "/demo/package-pause",
            json={"read_batch_seconds": 0.4, "pre_publish_seconds": 1.5},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            resp.json(),
            {"read_batch_seconds": 0.4, "pre_publish_seconds": 1.5},
        )
        got = self.client.get("/demo/package-pause").json()
        self.assertEqual(got["read_batch_seconds"], 0.4)
        # Clear so later tests run without delays.
        self.client.post(
            "/demo/package-pause",
            json={"read_batch_seconds": 0.0, "pre_publish_seconds": 0.0},
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
