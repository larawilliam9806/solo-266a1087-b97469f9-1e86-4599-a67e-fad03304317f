"""Tests for the file date-partition correction API.

Upstream occasionally registers a whole batch of clicks under the previous
day. ``POST /files/{file_id}/correct-date`` moves such a file to the UTC date
of its clicks — atomically, and without ever weakening the erasure guarantees.
"""

from __future__ import annotations

import csv
import io
import threading
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import storage
from app.config import Config
from app.main import create_app


# ------------------------------------------------------------------ helpers


def make_config(tmp_path: Path, **overrides) -> Config:
    kwargs = dict(
        data_dir=tmp_path / "data",
        state_dir=tmp_path / "state",
        retry_delay=0.2,
        worker_poll=0.05,
    )
    kwargs.update(overrides)
    return Config(**kwargs)


def make_client(tmp_path: Path, **overrides) -> TestClient:
    return TestClient(create_app(make_config(tmp_path, **overrides)))


def csv_bytes(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["user_id", "page", "timestamp"])
    w.writerows(rows)
    return buf.getvalue().encode()


def upload(client: TestClient, date: str, rows):
    return client.post(
        f"/upload/{date}", files={"file": ("clicks.csv", csv_bytes(rows), "text/csv")}
    )


def upload_file_id(client: TestClient, date: str, rows) -> str:
    r = upload(client, date, rows)
    assert r.status_code == 200, r.text
    file_id = r.json()["file_id"]
    assert file_id, r.json()
    return file_id


def wait_task(client: TestClient, task_id: str, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    body = None
    while time.time() < deadline:
        r = client.get(f"/erasure/{task_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] == "completed":
            return body
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not complete in time: {body}")


def download_users(client: TestClient, date: str) -> set[str]:
    r = client.get(f"/dates/{date}/download")
    assert r.status_code == 200, r.text
    rows = list(csv.reader(io.StringIO(r.text)))
    return {row[0] for row in rows[1:]}


def part_files(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "data").rglob("part-*.parquet"))


# ------------------------------------------------------------------- tests


def test_correct_date_moves_file_and_updates_listing(tmp_path):
    with make_client(tmp_path) as client:
        # upstream registered the batch under the previous day
        fid = upload_file_id(
            client,
            "2026-11-01",
            [
                ("alice", "/a", "2026-11-02T00:10:00Z"),
                ("bob", "/b", "2026-11-02T23:59:59Z"),
            ],
        )
        # an unrelated, correctly registered file on the same source date
        upload_file_id(client, "2026-11-01", [("carol", "/c", "2026-11-01T08:00:00Z")])

        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 200, r.text
        assert r.json() == {
            "file_id": fid,
            "old_date": "2026-11-01",
            "new_date": "2026-11-02",
            "rows": 2,
            "users": 2,
        }

        # the old date no longer returns these clicks but keeps its own file
        assert download_users(client, "2026-11-01") == {"carol"}
        assert download_users(client, "2026-11-02") == {"alice", "bob"}

        dates = {d["date"]: d for d in client.get("/dates").json()}
        assert dates["2026-11-01"] == {
            "date": "2026-11-01",
            "files": 1,
            "rows": 1,
            "restricted": False,
        }
        assert dates["2026-11-02"] == {
            "date": "2026-11-02",
            "files": 1,
            "rows": 2,
            "restricted": False,
        }

        # the file physically lives under the new partition only, content intact
        new_path = tmp_path / "data" / "date=2026-11-02" / f"part-{fid}.parquet"
        assert new_path.exists()
        assert not (tmp_path / "data" / "date=2026-11-01" / f"part-{fid}.parquet").exists()
        table = pq.read_table(new_path)
        assert table.num_rows == 2
        rows = {(r["user_id"], r["page"], r["ts"].date().isoformat()) for r in table.to_pylist()}
        assert rows == {("alice", "/a", "2026-11-02"), ("bob", "/b", "2026-11-02")}


def test_correct_date_last_file_empties_old_date(tmp_path):
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("alice", "/a", "2026-11-02T00:10:00Z")]
        )
        assert client.post(f"/files/{fid}/correct-date").status_code == 200
        assert client.get("/dates/2026-11-01/download").status_code == 404
        assert download_users(client, "2026-11-02") == {"alice"}


def test_correct_date_rejects_multi_day_file_and_keeps_serving(tmp_path):
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client,
            "2026-11-01",
            [
                ("alice", "/a", "2026-11-02T00:10:00Z"),
                ("bob", "/b", "2026-11-03T00:10:00Z"),
            ],
        )
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 422
        assert "multiple UTC dates" in r.json()["detail"]
        # the original partition keeps serving the file untouched
        assert download_users(client, "2026-11-01") == {"alice", "bob"}
        assert client.get("/dates/2026-11-02/download").status_code == 404


def test_correct_date_rejects_file_already_on_correct_date(tmp_path):
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("alice", "/a", "2026-11-01T00:10:00Z")]
        )
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 422
        assert download_users(client, "2026-11-01") == {"alice"}


def test_correct_date_unknown_file(tmp_path):
    with make_client(tmp_path) as client:
        assert client.post("/files/does-not-exist/correct-date").status_code == 404


def test_correct_date_rejected_while_source_date_restricted(tmp_path):
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("bob", "/b", "2026-11-02T00:10:00Z")]
        )
        upload_file_id(client, "2026-11-01", [("alice", "/a", "2026-11-01T09:00:00Z")])

        r = client.post("/erasure", json={"user_id": "alice"})
        task_id = r.json()["task_id"]
        assert r.json()["restricted_dates"] == ["2026-11-01"]

        # the source date is restricted: correction is refused
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 409
        assert "2026-11-01" in r.json()["detail"]

        wait_task(client, task_id)
        # the file was never moved; it survived the erasure (no alice rows)
        assert download_users(client, "2026-11-01") == {"bob"}

        # restriction lifted: the correction now succeeds
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 200
        assert download_users(client, "2026-11-02") == {"bob"}
        assert client.get("/dates/2026-11-01/download").status_code == 404


def test_correct_date_rejected_while_target_date_restricted(tmp_path):
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("bob", "/b", "2026-11-03T00:10:00Z")]
        )
        upload_file_id(client, "2026-11-03", [("alice", "/a", "2026-11-03T09:00:00Z")])

        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]

        # the target date is restricted: correction is refused
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 409
        assert "2026-11-03" in r.json()["detail"]

        wait_task(client, task_id)
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 200
        assert download_users(client, "2026-11-03") == {"bob"}


def test_correction_allowed_when_unrelated_date_restricted(tmp_path):
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("bob", "/b", "2026-11-02T00:10:00Z")]
        )
        upload_file_id(client, "2026-11-05", [("alice", "/a", "2026-11-05T09:00:00Z")])

        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]
        # 2026-11-05 is restricted, but the file moves between two free dates
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 200, r.text

        wait_task(client, task_id)
        assert download_users(client, "2026-11-02") == {"bob"}
        assert client.get("/dates/2026-11-05/download").status_code == 404


def test_erasure_after_correction_finds_file_on_new_date(tmp_path):
    """The new date joins the erasure index: a later erasure covers the move."""
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client,
            "2026-11-01",
            [
                ("bob", "/b", "2026-11-02T00:10:00Z"),
                ("carol", "/c", "2026-11-02T01:00:00Z"),
            ],
        )
        assert client.post(f"/files/{fid}/correct-date").status_code == 200

        r = client.post("/erasure", json={"user_id": "bob"})
        task_id = r.json()["task_id"]
        # the new date joined the erasure index; the old date entry is kept as
        # a conservative superset, so both are restricted while the task runs
        assert r.json()["restricted_dates"] == ["2026-11-01", "2026-11-02"]

        status = wait_task(client, task_id)
        assert status["files_total"] == 1
        assert download_users(client, "2026-11-02") == {"carol"}
        assert client.get("/dates/2026-11-01/download").status_code == 404


def test_erasure_accepted_during_correction_wins(tmp_path):
    """An erasure slipping in between validation and commit still wins: the
    correction is refused and the erasure processes the file where it is."""
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client,
            "2026-11-01",
            [
                ("bob", "/b", "2026-11-02T00:10:00Z"),
                ("carol", "/c", "2026-11-02T01:00:00Z"),
            ],
        )
        service = client.app.state.service

        def accept_erasure_mid_correction():
            service.submit_erasure("bob")

        service.hooks.before_correct_commit = accept_erasure_mid_correction
        try:
            r = client.post(f"/files/{fid}/correct-date")
        finally:
            service.hooks.before_correct_commit = None
        assert r.status_code == 409, r.text

        # the file stayed on the old date and the erasure still finds it there
        task = client.get("/erasure", params={"user_id": "bob"}).json()[0]
        wait_task(client, task["task_id"])
        assert download_users(client, "2026-11-01") == {"carol"}
        # bob never surfaced from the new partition
        assert client.get("/dates/2026-11-02/download").status_code == 404


def test_write_failure_keeps_old_partition_serving(tmp_path, monkeypatch):
    config = make_config(tmp_path)
    with TestClient(create_app(config), raise_server_exceptions=False) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("alice", "/a", "2026-11-02T00:10:00Z")]
        )

        def boom(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(storage, "write_table_atomic", boom)
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 500

        # the old partition still serves everything; nothing leaked to the new one
        assert download_users(client, "2026-11-01") == {"alice"}
        assert client.get("/dates/2026-11-02/download").status_code == 404

        monkeypatch.undo()
        # a healthy disk lets the very same request succeed
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 200
        assert download_users(client, "2026-11-02") == {"alice"}
        assert client.get("/dates/2026-11-01/download").status_code == 404


def test_registration_failure_keeps_old_partition_and_recovers_on_restart(
    tmp_path, monkeypatch
):
    config = make_config(tmp_path)
    app1 = create_app(config)
    with TestClient(app1, raise_server_exceptions=False) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("alice", "/a", "2026-11-02T00:10:00Z")]
        )
        service = client.app.state.service

        def boom(*args, **kwargs):
            raise RuntimeError("db down")

        monkeypatch.setattr(service.store, "relocate_file", boom)
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 500

        # the new file was written but never registered: the old partition
        # remains the single source of truth
        assert download_users(client, "2026-11-01") == {"alice"}
        assert client.get("/dates/2026-11-02/download").status_code == 404

    # restart: reconciliation removes the unregistered copy; still no loss,
    # still no double serving
    app2 = create_app(config)
    with TestClient(app2) as client2:
        assert download_users(client2, "2026-11-01") == {"alice"}
        assert client2.get("/dates/2026-11-02/download").status_code == 404
        parts = part_files(tmp_path)
        assert [p.parent.name for p in parts] == ["date=2026-11-01"]

        r = client2.post(f"/files/{fid}/correct-date")
        assert r.status_code == 200
        assert download_users(client2, "2026-11-02") == {"alice"}
        assert client2.get("/dates/2026-11-01/download").status_code == 404


def test_restart_with_leftover_old_copy_never_serves_both_days(tmp_path, monkeypatch):
    """Simulate a crash right after the DB commit: the old copy is never
    removed. Reconciliation must clean it up — one partition serves, always."""
    config = make_config(tmp_path)
    app1 = create_app(config)
    with TestClient(app1) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("alice", "/a", "2026-11-02T00:10:00Z")]
        )
        monkeypatch.setattr(storage, "delete_file", lambda *a, **k: None)
        r = client.post(f"/files/{fid}/correct-date")
        assert r.status_code == 200
        # both copies are on disk, but only the new one is registered
        assert (tmp_path / "data" / "date=2026-11-01" / f"part-{fid}.parquet").exists()
        assert (tmp_path / "data" / "date=2026-11-02" / f"part-{fid}.parquet").exists()
        assert client.get("/dates/2026-11-01/download").status_code == 404
        assert download_users(client, "2026-11-02") == {"alice"}

    monkeypatch.undo()
    app2 = create_app(config)
    with TestClient(app2) as client2:
        assert client2.get("/dates/2026-11-01/download").status_code == 404
        assert download_users(client2, "2026-11-02") == {"alice"}
        parts = part_files(tmp_path)
        assert [p.parent.name for p in parts] == ["date=2026-11-02"]


def test_download_in_progress_finishes_before_old_copy_is_removed(tmp_path, monkeypatch):
    with make_client(tmp_path) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("alice", "/a", "2026-11-02T00:10:00Z")]
        )

        real_read = storage.read_table
        entered_gate = threading.Event()
        release = threading.Event()
        first_read = {"done": False}

        def slow_read(data_dir, rel_path, columns=None):
            if not first_read["done"] and rel_path.endswith(f"part-{fid}.parquet"):
                first_read["done"] = True
                entered_gate.set()
                release.wait(10)
            return real_read(data_dir, rel_path, columns=columns)

        monkeypatch.setattr(storage, "read_table", slow_read)

        results: dict[str, object] = {}

        def do_download():
            results["download"] = client.get("/dates/2026-11-01/download")

        def do_correct():
            results["correct"] = client.post(f"/files/{fid}/correct-date")

        dl = threading.Thread(target=do_download)
        dl.start()
        # the download is now inside read_table, holding the date lock
        assert entered_gate.wait(5)

        corr = threading.Thread(target=do_correct)
        corr.start()
        time.sleep(0.3)  # let the correction reach the date lock and wait
        release.set()
        dl.join(10)
        corr.join(10)
        assert not dl.is_alive() and not corr.is_alive()

        # the started download finished with the full old-partition data
        resp = results["download"]
        assert resp.status_code == 200
        assert "alice" in resp.text
        # the correction then completed
        assert results["correct"].status_code == 200
        assert client.get("/dates/2026-11-01/download").status_code == 404
        assert download_users(client, "2026-11-02") == {"alice"}


def test_corrected_file_is_tracked_across_restart(tmp_path):
    """A correction followed by a restart keeps exactly one registered copy."""
    config = make_config(tmp_path)
    app1 = create_app(config)
    with TestClient(app1) as client:
        fid = upload_file_id(
            client, "2026-11-01", [("alice", "/a", "2026-11-02T00:10:00Z")]
        )
        assert client.post(f"/files/{fid}/correct-date").status_code == 200

    app2 = create_app(config)
    with TestClient(app2) as client2:
        assert client2.get("/dates/2026-11-01/download").status_code == 404
        assert download_users(client2, "2026-11-02") == {"alice"}
        parts = part_files(tmp_path)
        assert [p.parent.name for p in parts] == ["date=2026-11-02"]
        # a second correction attempt is now rejected: already on the right date
        assert client2.post(f"/files/{fid}/correct-date").status_code == 422
