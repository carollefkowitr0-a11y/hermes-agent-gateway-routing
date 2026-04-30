import io
import json
import sqlite3

from gateway.spool import GatewaySpool
from gateway.spool_worker import default_spool_db_path, main, run_worker


def _spool(tmp_path):
    spool = GatewaySpool(tmp_path / "spool.db")
    spool.initialize()
    return spool


def _state(spool, row_id):
    with sqlite3.connect(spool.db_path) as conn:
        return conn.execute("SELECT state, error FROM gateway_spool WHERE id = ?", (row_id,)).fetchone()


def test_empty_queue_returns_idle_noop_and_does_not_error(tmp_path):
    spool = _spool(tmp_path)

    summary = run_worker(spool_db=spool.db_path)

    assert summary["ok"] is True
    assert summary["status"] == "idle"
    assert summary["profile"] == "naval"
    assert summary["dry_run"] is True
    assert summary["once"] is True
    assert summary["dispatch"] is None
    assert summary["plan"]["actions"] == ["inspect_profile", "noop"]
    assert summary["supervisor"]["executed"] is False


def test_queued_naval_row_is_processed_by_noop_path_and_marked_dispatched(tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update(
        "telegram",
        "naval",
        "naval",
        123,
        {"message": {"text": "PRIVATE MESSAGE SENTINEL"}},
        chat_id="private-chat-sentinel",
        thread_id="private-thread-sentinel",
        user_id="private-user-sentinel",
    )
    spool.enqueue_update("telegram", "naval", "naval", 124, {"message": "second"})

    summary = run_worker(spool_db=spool.db_path)

    assert summary["ok"] is True
    assert summary["status"] == "processed"
    assert summary["dispatch"] == {"dispatched": True, "row_id": inserted.id, "state": "dispatched"}
    assert _state(spool, inserted.id) == ("dispatched", None)
    assert spool.count_by_state() == {"dispatched": 1, "queued": 1}


def test_non_naval_profile_is_refused_by_default(tmp_path):
    spool = _spool(tmp_path)
    other = spool.enqueue_update("telegram", "other", "other", 123, {"message": "do not process"})

    summary = run_worker(profile="other", spool_db=spool.db_path)

    assert summary["ok"] is False
    assert summary["status"] == "refused"
    assert summary["reason"] == "non_naval_profile_refused"
    assert _state(spool, other.id) == ("queued", None)


def test_output_excludes_payload_private_fields_and_ids(tmp_path):
    spool = _spool(tmp_path)
    spool.enqueue_update(
        "telegram",
        "naval",
        "naval",
        "private-update-sentinel",
        {"message": {"text": "PRIVATE PAYLOAD SENTINEL"}, "token": "secret-token-sentinel"},
        chat_id="private-chat-sentinel",
        thread_id="private-thread-sentinel",
        user_id="private-user-sentinel",
        event_type="message",
    )

    stdout = io.StringIO()
    exit_code = main(["--spool-db", str(spool.db_path), "--once", "--dry-run"], stdout=stdout)
    rendered = stdout.getvalue()
    summary = json.loads(rendered)

    assert exit_code == 0
    assert summary["ok"] is True
    for forbidden in (
        "PRIVATE PAYLOAD SENTINEL",
        "secret-token-sentinel",
        "private-chat-sentinel",
        "private-thread-sentinel",
        "private-user-sentinel",
        "payload_json",
        "chat_id",
        "thread_id",
        "user_id",
        "argv",
        "env",
        "logs",
    ):
        assert forbidden not in rendered


def test_non_dry_run_flag_processes_through_direct_mode_or_fails_safely(monkeypatch, tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update("telegram", "naval", "naval", 123, {"message": "do not process"})

    import gateway.polling_watcher as polling_watcher
    import hermes_cli.env_loader as env_loader

    class FakeConsumer:
        def __call__(self, row):
            raise RuntimeError("conversion failed safely")

    monkeypatch.setattr(env_loader, "load_hermes_dotenv", lambda **kwargs: [])
    monkeypatch.setattr(polling_watcher, "DirectGatewayConsumer", lambda: FakeConsumer())

    stdout = io.StringIO()
    exit_code = main(["--spool-db", str(spool.db_path), "--no-dry-run"], stdout=stdout)
    summary = json.loads(stdout.getvalue())

    assert exit_code == 2
    assert summary["ok"] is True
    assert summary["status"] == "failed"
    assert summary["dispatch"] == {"dispatched": False, "row_id": inserted.id, "state": "failed"}
    assert _state(spool, inserted.id)[0] == "failed"


def test_default_spool_db_resolves_profile_naval_db(monkeypatch, tmp_path):
    hermes_root = tmp_path / "hermes-root"
    monkeypatch.setenv("HERMES_HOME", str(hermes_root))

    assert default_spool_db_path("naval") == hermes_root / "profiles" / "naval" / "gateway_spool.db"


def test_worker_module_does_not_expose_forbidden_control_path():
    import gateway.spool_worker as spool_worker

    source = spool_worker.__loader__.get_source(spool_worker.__name__)
    assert source is not None
    forbidden = (
        "get" + "Updates",
        "set" + "Webhook",
        "delete" + "Webhook",
        "get" + "Webhook" + "Info",
        "system" + "ctl",
        "serv" + "ice ",
        "sub" + "process",
        "P" + "open",
        "os." + "system",
        "ki" + "ll(",
        "termi" + "nate(",
        "send" + "_signal",
    )
    for token in forbidden:
        assert token not in source



def test_non_dry_run_direct_mode_invokes_gateway_consumer(monkeypatch, tmp_path):
    spool = _spool(tmp_path)
    inserted = spool.enqueue_update(
        "telegram",
        "naval",
        "naval",
        321,
        {
            "update_id": 321,
            "message": {
                "message_id": 99,
                "text": "hello",
                "chat": {"id": 456, "type": "private", "first_name": "F"},
                "from": {"id": 789, "first_name": "F", "username": "f"},
            },
        },
        chat_id="456",
        user_id="789",
        event_type="message",
    )
    calls = []

    class FakeConsumer:
        def __call__(self, row):
            calls.append(row)

    import gateway.polling_watcher as polling_watcher
    import hermes_cli.env_loader as env_loader

    monkeypatch.setattr(env_loader, "load_hermes_dotenv", lambda **kwargs: [])
    monkeypatch.setattr(polling_watcher, "DirectGatewayConsumer", lambda: FakeConsumer())

    summary = run_worker(profile="naval", spool_db=spool.db_path, dry_run=False)

    assert summary["ok"] is True
    assert summary["dry_run"] is False
    assert summary["status"] == "processed"
    assert summary["dispatch"] == {"dispatched": True, "row_id": inserted.id, "state": "dispatched"}
    assert len(calls) == 1
    assert _state(spool, inserted.id) == ("dispatched", None)
