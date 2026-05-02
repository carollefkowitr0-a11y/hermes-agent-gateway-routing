import json
import os
import sqlite3
import stat
import threading
from pathlib import Path

import pytest

from gateway.spool import GatewaySpool


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / "hermes-home"
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


def test_default_path_uses_isolated_hermes_home(hermes_home):
    spool = GatewaySpool()

    assert spool.db_path == hermes_home / "gateway_spool.db"


def test_initialize_creates_db_with_wal_journal_mode(hermes_home):
    spool = GatewaySpool()

    spool.initialize()

    assert spool.db_path.exists()
    with sqlite3.connect(spool.db_path) as conn:
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert journal_mode.lower() == "wal"


def test_enqueue_creates_queued_row_and_preserves_naval_route_profile_payload(hermes_home):
    spool = GatewaySpool()
    spool.initialize()
    payload = {"update_id": 123, "message": {"text": "hello"}}

    result = spool.enqueue_update(
        platform="telegram",
        route="naval",
        target_profile="naval",
        update_id=123,
        payload=payload,
        chat_id="chat-1",
        thread_id="thread-1",
        user_id="user-1",
        event_type="message",
    )

    assert result.inserted is True
    assert result.duplicate is False
    row = spool.get_next_queued(target_profile="naval")
    assert row is not None
    assert row["id"] == result.id
    assert row["state"] == "queued"
    assert row["platform"] == "telegram"
    assert row["route"] == "naval"
    assert row["target_profile"] == "naval"
    assert row["update_id"] == "123"
    assert row["chat_id"] == "chat-1"
    assert row["thread_id"] == "thread-1"
    assert row["user_id"] == "user-1"
    assert row["event_type"] == "message"
    assert json.loads(row["payload_json"]) == payload


def test_duplicate_update_id_for_same_platform_route_does_not_create_second_queued_row(hermes_home):
    spool = GatewaySpool()
    spool.initialize()

    first = spool.enqueue_update("telegram", "naval", "naval", 123, {"n": 1})
    second = spool.enqueue_update("telegram", "naval", "naval", 123, {"n": 2})

    assert first.inserted is True
    assert second.inserted is False
    assert second.duplicate is True
    assert second.id == first.id
    assert spool.count_by_state() == {"queued": 1}


def test_same_update_id_on_different_route_can_enqueue_separately(hermes_home):
    spool = GatewaySpool()
    spool.initialize()

    naval = spool.enqueue_update("telegram", "naval", "naval", 123, {"route": "naval"})
    other = spool.enqueue_update("telegram", "other", "naval", 123, {"route": "other"})

    assert naval.inserted is True
    assert other.inserted is True
    assert other.id != naval.id
    assert spool.count_by_state() == {"queued": 2}


def test_get_next_queued_filters_by_target_profile_and_returns_oldest(hermes_home):
    spool = GatewaySpool()
    spool.initialize()

    spool.enqueue_update("telegram", "naval", "other", 1, {"profile": "other"})
    oldest_naval = spool.enqueue_update("telegram", "naval", "naval", 2, {"order": "oldest"})
    spool.enqueue_update("telegram", "naval", "naval", 3, {"order": "newest"})

    row = spool.get_next_queued(target_profile="naval")

    assert row is not None
    assert row["id"] == oldest_naval.id
    assert json.loads(row["payload_json"]) == {"order": "oldest"}


def test_claim_next_queued_atomically_marks_one_row_processing(hermes_home):
    spool = GatewaySpool()
    spool.initialize()
    first = spool.enqueue_update("telegram", "naval", "naval", 1, {"order": "first"})
    second = spool.enqueue_update("telegram", "naval", "naval", 2, {"order": "second"})

    claimed = spool.claim_next_queued(target_profile="naval")

    assert claimed is not None
    assert claimed["id"] == first.id
    assert claimed["state"] == "processing"
    assert claimed["attempts"] == 1
    assert spool.get_row(first.id)["state"] == "processing"
    assert spool.get_row(second.id)["state"] == "queued"


def test_claim_next_queued_does_not_double_claim_under_concurrency(hermes_home):
    spool = GatewaySpool()
    spool.initialize()
    inserted = spool.enqueue_update("telegram", "naval", "naval", 1, {"order": "only"})
    barrier = threading.Barrier(2)
    results = []

    def claim_once():
        barrier.wait(timeout=5)
        other = GatewaySpool(spool.db_path)
        other.initialize()
        results.append(other.claim_next_queued(target_profile="naval"))

    threads = [threading.Thread(target=claim_once) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    claimed = [row for row in results if row is not None]
    assert len(claimed) == 1
    assert claimed[0]["id"] == inserted.id
    assert spool.get_row(inserted.id)["attempts"] == 1


def test_mark_state_transitions_queued_item_to_done_or_failed_and_stores_error(hermes_home):
    spool = GatewaySpool()
    spool.initialize()
    done = spool.enqueue_update("telegram", "naval", "naval", 1, {"ok": True})
    failed = spool.enqueue_update("telegram", "naval", "naval", 2, {"ok": False})

    spool.mark_state(done.id, "done")
    spool.mark_state(failed.id, "failed", error="boom")

    assert spool.count_by_state() == {"done": 1, "failed": 1}
    with sqlite3.connect(spool.db_path) as conn:
        error = conn.execute("SELECT error FROM gateway_spool WHERE id = ?", (failed.id,)).fetchone()[0]
    assert error == "boom"


def test_new_db_and_wal_sidecar_permissions_are_private_when_supported(hermes_home):
    spool = GatewaySpool()

    spool.initialize()
    spool.enqueue_update("telegram", "naval", "naval", 1, {"message": "sensitive"})

    paths = [spool.db_path, Path(str(spool.db_path) + "-wal"), Path(str(spool.db_path) + "-shm")]
    for path in paths:
        assert path.exists()
        mode = stat.S_IMODE(os.stat(path).st_mode)
        assert mode & 0o077 == 0
