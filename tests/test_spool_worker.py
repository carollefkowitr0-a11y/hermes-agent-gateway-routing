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


def test_queued_naval_row_is_processed_by_noop_path_and_marked_done(tmp_path):
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
    assert summary["dispatch"] == {"dispatched": True, "row_id": inserted.id, "state": "done"}
    assert _state(spool, inserted.id) == ("done", None)
    assert spool.count_by_state() == {"done": 1, "queued": 1}


def test_non_naval_profile_is_processed_when_explicitly_targeted(tmp_path):
    spool = _spool(tmp_path)
    other = spool.enqueue_update("telegram", "franklin", "franklin", 123, {"message": "process"})

    summary = run_worker(profile="franklin", spool_db=spool.db_path)

    assert summary["ok"] is True
    assert summary["status"] == "processed"
    assert summary["profile"] == "franklin"
    assert summary["dispatch"] == {"dispatched": True, "row_id": other.id, "state": "done"}
    assert _state(spool, other.id) == ("done", None)


def test_output_excludes_payload_private_fields_and_ids(tmp_path):
    spool = _spool(tmp_path)
    spool.enqueue_update(
        "telegram",
        "naval",
        "naval",
        "private-update-sentinel",
        {"message": {"text": "PRIVATE PAYLOAD SENTINEL"}, "token": "secret...inel"},
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


def test_default_spool_db_resolves_profile_db(monkeypatch, tmp_path):
    hermes_root = tmp_path / "hermes-root"
    monkeypatch.setenv("HERMES_HOME", str(hermes_root))

    assert default_spool_db_path("naval") == hermes_root / "profiles" / "naval" / "gateway_spool.db"
    assert default_spool_db_path("franklin") == hermes_root / "profiles" / "franklin" / "gateway_spool.db"


def test_default_spool_db_rejects_unsafe_profile_path(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-root"))

    try:
        default_spool_db_path("../evil")
    except ValueError as exc:
        assert "unsafe profile name" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("unsafe profile path was accepted")


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



def test_non_dry_run_direct_mode_invokes_gateway_consumer_with_profile_home(monkeypatch, tmp_path):
    profile_home = tmp_path / "profiles" / "franklin"
    spool = _spool(profile_home)
    inserted = spool.enqueue_update(
        "telegram",
        "franklin",
        "franklin",
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
    observed_env_loads = []
    observed_consumer_home = []
    observed_call_home = []
    original_home = __import__("os").environ.get("HERMES_HOME")

    class FakeConsumer:
        def __init__(self):
            observed_consumer_home.append(__import__("os").environ.get("HERMES_HOME"))

        def __call__(self, row):
            observed_call_home.append(__import__("os").environ.get("HERMES_HOME"))
            calls.append(row)

    import gateway.polling_watcher as polling_watcher
    import hermes_cli.env_loader as env_loader

    def fake_load_hermes_dotenv(**kwargs):
        observed_env_loads.append(kwargs)
        return []

    monkeypatch.setattr(env_loader, "load_hermes_dotenv", fake_load_hermes_dotenv)
    monkeypatch.setattr(polling_watcher, "DirectGatewayConsumer", lambda: FakeConsumer())

    summary = run_worker(profile="franklin", spool_db=spool.db_path, dry_run=False)

    assert summary["ok"] is True
    assert summary["dry_run"] is False
    assert summary["status"] == "processed"
    assert summary["dispatch"] == {"dispatched": True, "row_id": inserted.id, "state": "done"}
    assert len(calls) == 1
    assert observed_env_loads and observed_env_loads[0]["hermes_home"] == profile_home
    assert observed_consumer_home == [str(profile_home)]
    assert observed_call_home == [str(profile_home)]
    assert __import__("os").environ.get("HERMES_HOME") == original_home
    assert _state(spool, inserted.id) == ("done", None)


def test_non_dry_run_worker_maps_profile_token_env_to_direct_gateway_token(monkeypatch, tmp_path):
    profile_home = tmp_path / "franklin"
    spool = _spool(profile_home)
    spool.enqueue_update(
        "telegram",
        "franklin",
        "franklin",
        987,
        {
            "update_id": 987,
            "message": {
                "message_id": 1,
                "text": "hello",
                "chat": {"id": 1, "type": "private"},
                "from": {"id": 2},
            },
        },
        event_type="message",
    )
    observed = []
    original_home = __import__("os").environ.get("HERMES_HOME")

    import gateway.polling_watcher as polling_watcher
    import hermes_cli.env_loader as env_loader

    def fake_load_hermes_dotenv(**kwargs):
        __import__("os").environ["FRANKLIN_TELEGRAM_BOT_TOKEN"] = "profile-specific-token"
        __import__("os").environ["TELEGRAM_BOT_TOKEN"] = "stale-generic-token"
        return []

    class FakeConsumer:
        def __init__(self):
            observed.append(
                {
                    "home": __import__("os").environ.get("HERMES_HOME"),
                    "direct": __import__("os").environ.get("TELEGRAM_BOT_TOKEN"),
                    "profile": __import__("os").environ.get("FRANKLIN_TELEGRAM_BOT_TOKEN"),
                }
            )

        def __call__(self, row):
            pass

    monkeypatch.setenv("HERMES_WATCHER_PROFILE_TOKEN_ENV_FRANKLIN", "FRANKLIN_TELEGRAM_BOT_TOKEN")
    monkeypatch.setattr(env_loader, "load_hermes_dotenv", fake_load_hermes_dotenv)
    monkeypatch.setattr(polling_watcher, "DirectGatewayConsumer", lambda: FakeConsumer())

    summary = run_worker(profile="franklin", spool_db=spool.db_path, dry_run=False)

    assert summary["status"] == "processed"
    assert observed == [
        {
            "home": str(profile_home),
            "direct": "profile-specific-token",
            "profile": "profile-specific-token",
        }
    ]
    assert __import__("os").environ.get("HERMES_HOME") == original_home
    assert __import__("os").environ.get("TELEGRAM_BOT_TOKEN") != "profile-specific-token"


def test_dispatcher_marks_row_failed_before_reraising_base_exception(tmp_path):
    from gateway.dispatcher import dispatch_one

    spool = _spool(tmp_path)
    inserted = spool.enqueue_update("telegram", "franklin", "franklin", 654, {"message": "interrupt"})

    class ShutdownSignal(BaseException):
        pass

    def consumer(_row):
        raise ShutdownSignal("interrupted")

    try:
        dispatch_one(spool, target_profile="franklin", consumer=consumer)
    except ShutdownSignal:
        pass
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("ShutdownSignal was not re-raised")

    state, error = _state(spool, inserted.id)
    assert state == "failed"
    assert "interrupted" in error


def test_legacy_dispatched_rows_are_not_treated_as_worker_completed_queue_items(tmp_path):
    spool = _spool(tmp_path)
    legacy = spool.enqueue_update("telegram", "naval", "naval", 777, {"message": "legacy"})
    spool.mark_state(legacy.id, "dispatched")

    summary = run_worker(profile="naval", spool_db=spool.db_path)

    assert summary["ok"] is True
    assert summary["status"] == "idle"
    assert summary["dispatch"] is None
    assert spool.get_row(legacy.id)["state"] == "dispatched"
