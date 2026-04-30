import sqlite3

from gateway.dispatcher import dispatch_one
from gateway.spool import GatewaySpool


def _spool(tmp_path):
    spool = GatewaySpool(tmp_path / "spool.db")
    spool.initialize()
    return spool


def _state(spool, row_id):
    with sqlite3.connect(spool.db_path) as conn:
        return conn.execute("SELECT state, error FROM gateway_spool WHERE id = ?", (row_id,)).fetchone()


def test_dispatch_one_passes_oldest_naval_row_to_consumer_and_marks_dispatched(tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update("telegram", "naval", "naval", 123, {"update_id": 123})
    seen = []

    result = dispatch_one(spool, target_profile="naval", consumer=seen.append)

    assert result.dispatched is True
    assert result.row_id == inserted.id
    assert seen and seen[0]["id"] == inserted.id
    assert _state(spool, inserted.id) == ("dispatched", None)


def test_dispatch_one_returns_idle_when_no_queued_naval_row(tmp_path):
    spool = _spool(tmp_path)

    result = dispatch_one(spool, target_profile="naval", consumer=lambda row: None)

    assert result.dispatched is False
    assert result.row_id is None
    assert result.state == "idle"


def test_dispatch_one_marks_failed_when_consumer_raises_below_max_attempts(tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update("telegram", "naval", "naval", 123, {"update_id": 123})

    result = dispatch_one(
        spool,
        target_profile="naval",
        consumer=lambda row: (_ for _ in ()).throw(RuntimeError("boom secret-token")),
        max_attempts=3,
    )

    assert result.dispatched is False
    assert result.row_id == inserted.id
    assert result.state == "failed"
    state, error = _state(spool, inserted.id)
    assert state == "failed"
    assert "boom" in error
    assert "secret-token" not in error


def test_dispatch_one_marks_dead_letter_when_current_attempts_reaches_limit(tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update("telegram", "naval", "naval", 123, {"update_id": 123})
    with sqlite3.connect(spool.db_path) as conn:
        conn.execute("UPDATE gateway_spool SET attempts = 3 WHERE id = ?", (inserted.id,))

    result = dispatch_one(
        spool,
        target_profile="naval",
        consumer=lambda row: (_ for _ in ()).throw(RuntimeError("boom")),
        max_attempts=3,
    )

    assert result.dispatched is False
    assert result.state == "dead_letter"
    assert _state(spool, inserted.id)[0] == "dead_letter"
