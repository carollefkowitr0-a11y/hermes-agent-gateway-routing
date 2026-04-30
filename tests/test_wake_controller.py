import sqlite3

from gateway.spool import GatewaySpool
from gateway.supervisor_boundary import DryRunSupervisor
from gateway.wake_controller import WakeController


def _spool(tmp_path):
    spool = GatewaySpool(tmp_path / "spool.db")
    spool.initialize()
    return spool


def _state(spool, row_id):
    with sqlite3.connect(spool.db_path) as conn:
        return conn.execute("SELECT state, error FROM gateway_spool WHERE id = ?", (row_id,)).fetchone()


def test_plan_once_empty_queue_is_idle_and_side_effect_free(tmp_path):
    spool = _spool(tmp_path)
    before = spool.count_by_state()

    plan = WakeController(spool=spool).plan_once()

    assert spool.count_by_state() == before
    assert plan.profile == "naval"
    assert plan.dry_run is True
    assert plan.safe_to_execute is True
    assert [command.action for command in plan.commands] == ["inspect_profile", "noop"]


def test_plan_once_queued_row_contains_wake_and_dispatch_without_mutating_payload(tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update(
        "telegram",
        "naval",
        "naval",
        123,
        {"message": {"text": "PRIVATE MESSAGE SENTINEL"}, "token": "dummy-token-sentinel"},
    )

    plan = WakeController(spool=spool).plan_once()

    assert _state(spool, inserted.id) == ("queued", None)
    actions = [command.action for command in plan.commands]
    assert actions == ["inspect_profile", "probe_health", "wake_worker", "dispatch_spool_item"]
    assert all(command.dry_run is True for command in plan.commands)
    wake = next(command for command in plan.commands if command.action == "wake_worker")
    assert wake.requires_approval is True
    rendered = str(plan.to_dict())
    assert "PRIVATE MESSAGE SENTINEL" not in rendered
    assert "dummy-token-sentinel" not in rendered
    assert "payload_json" not in rendered


def test_run_once_dispatches_one_row_with_injected_consumer_and_records_dry_run_events(tmp_path):
    spool = _spool(tmp_path)
    supervisor = DryRunSupervisor()
    inserted = spool.enqueue_update("telegram", "naval", "naval", 123, {"update_id": 123})
    seen = []

    result = WakeController(spool=spool, supervisor=supervisor, consumer=seen.append).run_once()

    assert result.dispatch_result is not None
    assert result.dispatch_result.dispatched is True
    assert result.dispatch_result.row_id == inserted.id
    assert _state(spool, inserted.id) == ("dispatched", None)
    assert seen and seen[0]["id"] == inserted.id
    assert result.supervisor_events == supervisor.events
    assert [event["action"] for event in supervisor.events] == [
        "inspect_profile",
        "probe_health",
        "wake_worker",
        "dispatch_spool_item",
    ]
    assert all(event["dry_run"] is True and event["executed"] is False for event in supervisor.events)
    wake_event = next(event for event in supervisor.events if event["action"] == "wake_worker")
    assert wake_event["requires_approval"] is True


def test_run_once_consumer_failure_uses_dispatcher_failure_semantics(tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update("telegram", "naval", "naval", 123, {"update_id": 123})

    result = WakeController(
        spool=spool,
        consumer=lambda row: (_ for _ in ()).throw(RuntimeError("boom secret-token")),
        max_attempts=3,
    ).run_once()

    assert result.dispatch_result is not None
    assert result.dispatch_result.dispatched is False
    assert result.dispatch_result.state == "failed"
    state, error = _state(spool, inserted.id)
    assert state == "failed"
    assert "boom" in error
    assert "secret-token" not in error


def test_run_once_does_not_process_non_naval_rows_by_default(tmp_path):
    spool = _spool(tmp_path)
    other = spool.enqueue_update("telegram", "other", "other", 123, {"update_id": 123})

    result = WakeController(spool=spool, profile="other", consumer=lambda row: None).run_once()

    assert result.plan.safe_to_execute is False
    assert result.dispatch_result is None
    assert _state(spool, other.id) == ("queued", None)
    assert result.supervisor_events[0]["action"] == "noop"


def test_explicit_non_naval_override_can_process_only_that_profile(tmp_path):
    spool = _spool(tmp_path)
    other = spool.enqueue_update("telegram", "other", "other", 123, {"update_id": 123})
    naval = spool.enqueue_update("telegram", "naval", "naval", 124, {"update_id": 124})

    result = WakeController(spool=spool, profile="other", allow_non_naval=True).run_once()

    assert result.dispatch_result is not None
    assert result.dispatch_result.row_id == other.id
    assert _state(spool, other.id) == ("dispatched", None)
    assert _state(spool, naval.id) == ("queued", None)
