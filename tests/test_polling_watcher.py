import json
import os
import sqlite3
import stat

import pytest

from gateway.polling_watcher import (
    DryRunWakeRecorder,
    ProfileWatchConfig,
    WatcherConfig,
    main,
    poll_once,
    read_offset,
    route_update,
)


class MockTelegramClient:
    def __init__(self, updates_by_token):
        self.updates_by_token = updates_by_token
        self.calls = []

    def get_updates(self, token, offset, timeout, allowed_updates):
        self.calls.append(
            {"token": token, "offset": offset, "timeout": timeout, "allowed_updates": allowed_updates}
        )
        return list(self.updates_by_token.get(token, []))


class FailingSpoolClient(MockTelegramClient):
    pass


def token_provider(profile):
    return f"token-for-{profile.profile}"


def message_update(update_id=10, text="PRIVATE TEXT SENTINEL", chat=123, user=456, thread=789):
    return {
        "update_id": update_id,
        "message": {
            "message_id": 1,
            "message_thread_id": thread,
            "chat": {"id": chat, "type": "private"},
            "from": {"id": user, "is_bot": False},
            "text": text,
        },
    }


def rows(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM gateway_spool ORDER BY id")]


def test_multi_profile_config_validates_unique_token_ownership(tmp_path):
    with pytest.raises(ValueError, match="one owner"):
        WatcherConfig(
            profiles=[
                ProfileWatchConfig("alpha", token_label="shared", spool_db=tmp_path / "a.db"),
                ProfileWatchConfig("beta", token_label="shared", spool_db=tmp_path / "b.db"),
            ],
            manager_spool_db=tmp_path / "manager.db",
        )

    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig("alpha", token_label="alpha", spool_db=tmp_path / "a.db"),
            ProfileWatchConfig("beta", token_env="BETA_TOKEN", spool_db=tmp_path / "b.db"),
        ],
        manager_spool_db=tmp_path / "manager.db",
    )
    assert [profile.profile for profile in config.profiles] == ["alpha", "beta"]


def test_route_update_owned_profile_and_disabled_unowned_ambiguous_fallback(tmp_path):
    manager_db = tmp_path / "manager.db"
    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig("owned", bot_name="owned-bot", token_label="owned", spool_db=tmp_path / "owned.db"),
            ProfileWatchConfig("disabled", enabled=False, token_label="disabled", spool_db=tmp_path / "disabled.db"),
            ProfileWatchConfig("unowned", owned=False, token_label="unowned", spool_db=tmp_path / "unowned.db"),
            ProfileWatchConfig("ambiguous", ambiguous=True, token_label="ambiguous", spool_db=tmp_path / "ambiguous.db"),
        ],
        manager_spool_db=manager_db,
    )

    owned = route_update(message_update(update_id=11), config.profiles[0], config)
    assert owned.target_profile == "owned"
    assert owned.source_bot == "owned-bot"
    assert owned.spool_db == tmp_path / "owned.db"
    assert owned.fallback is False

    for profile, reason in zip(
        config.profiles[1:],
        ["disabled_profile", "unowned_profile", "ambiguous_profile"],
        strict=True,
    ):
        decision = route_update(message_update(update_id=12), profile, config)
        assert decision.target_profile == "default"
        assert decision.route == "default"
        assert decision.spool_db == manager_db
        assert decision.fallback is True
        assert decision.reason == reason


def test_disabled_profile_is_not_polled(tmp_path):
    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig(
                "enabled", token_label="enabled", spool_db=tmp_path / "enabled.db", offset_path=tmp_path / "enabled.offset"
            ),
            ProfileWatchConfig(
                "disabled", enabled=False, token_label="disabled", spool_db=tmp_path / "disabled.db", offset_path=tmp_path / "disabled.offset"
            ),
        ],
        manager_spool_db=tmp_path / "manager.db",
        default_timeout=0,
    )
    client = MockTelegramClient({"token-for-enabled": []})

    summary = poll_once(config, client, token_provider)

    assert [call["token"] for call in client.calls] == ["token-for-enabled"]
    assert summary["profiles"]["disabled"] == {"enabled": False, "polled": False}
    assert not (tmp_path / "disabled.db").exists()


def test_profile_update_writes_to_profile_spool_and_advances_offset_after_write(tmp_path):
    spool_db = tmp_path / "expert.db"
    offset_path = tmp_path / "expert.offset.json"
    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig("expert", token_label="expert", spool_db=spool_db, offset_path=offset_path)
        ],
        manager_spool_db=tmp_path / "manager.db",
        default_timeout=0,
    )
    client = MockTelegramClient({"token-for-expert": [message_update(update_id=41)]})

    summary = poll_once(config, client, token_provider)

    db_rows = rows(spool_db)
    assert len(db_rows) == 1
    assert db_rows[0]["route"] == "expert"
    assert db_rows[0]["target_profile"] == "expert"
    assert db_rows[0]["update_id"] == "41"
    assert json.loads(db_rows[0]["payload_json"])["message"]["text"] == "PRIVATE TEXT SENTINEL"
    assert read_offset(offset_path) == 42
    mode = stat.S_IMODE(os.stat(offset_path).st_mode)
    assert mode & 0o077 == 0
    assert summary["profiles"]["expert"]["next_offset"] == 42
    assert summary["profiles"]["expert"]["accepted"][0]["row_id"] == db_rows[0]["id"]


def test_if_write_fails_offset_does_not_advance(tmp_path, monkeypatch):
    offset_path = tmp_path / "expert.offset.json"
    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig("expert", token_label="expert", spool_db=tmp_path / "expert.db", offset_path=offset_path)
        ],
        manager_spool_db=tmp_path / "manager.db",
        default_timeout=0,
    )
    client = MockTelegramClient({"token-for-expert": [message_update(update_id=50)]})

    def boom(self, *args, **kwargs):
        raise RuntimeError("spool write failed")

    monkeypatch.setattr("gateway.spool.GatewaySpool.enqueue_update", boom)

    with pytest.raises(RuntimeError, match="spool write failed"):
        poll_once(config, client, token_provider)

    assert read_offset(offset_path) is None
    assert not offset_path.exists()


def test_unknown_unowned_update_goes_to_manager_default_route(tmp_path):
    manager_db = tmp_path / "manager.db"
    expert_db = tmp_path / "expert.db"
    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig(
                "unowned", token_label="unowned", route_kind="manager_fallback", spool_db=expert_db, offset_path=tmp_path / "unowned.offset"
            )
        ],
        manager_profile="default",
        manager_spool_db=manager_db,
        default_timeout=0,
    )
    client = MockTelegramClient({"token-for-unowned": [message_update(update_id=60)]})

    summary = poll_once(config, client, token_provider)

    assert not expert_db.exists()
    db_rows = rows(manager_db)
    assert len(db_rows) == 1
    assert db_rows[0]["route"] == "default"
    assert db_rows[0]["target_profile"] == "default"
    accepted = summary["profiles"]["unowned"]["accepted"][0]
    assert accepted["target_profile"] == "default"
    assert accepted["route"] == "default"


def test_wake_planning_records_profile_but_does_not_execute_commands(tmp_path):
    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig(
                "expert",
                token_label="expert",
                spool_db=tmp_path / "expert.db",
                offset_path=tmp_path / "expert.offset",
                wake_command=("PRIVATE_ARGV_SENTINEL", "do-not-run"),
            )
        ],
        manager_spool_db=tmp_path / "manager.db",
        default_timeout=0,
    )
    client = MockTelegramClient({"token-for-expert": [message_update(update_id=70)]})

    recorder = DryRunWakeRecorder()
    summary = poll_once(config, client, token_provider, wake_runner=recorder)

    assert len(recorder.plans) == 1
    assert recorder.plans[0].profile == "expert"
    assert summary["wake_planned_profiles"] == ["expert"]
    assert summary["wake_plans"][0]["profile"] == "expert"
    assert summary["wake_plans"][0]["dry_run"] is True
    assert summary["wake_plans"][0]["action"] == "wake_profile"


def test_summaries_exclude_payload_text_private_ids_token_env_and_argv(tmp_path):
    config = WatcherConfig(
        profiles=[
            ProfileWatchConfig(
                "expert",
                token_env="PRIVATE_ENV_SENTINEL",
                spool_db=tmp_path / "expert.db",
                offset_path=tmp_path / "expert.offset",
                wake_command=("PRIVATE_ARGV_SENTINEL",),
            )
        ],
        manager_spool_db=tmp_path / "manager.db",
        default_timeout=0,
    )
    client = MockTelegramClient(
        {
            "safe-test-token": [
                message_update(
                    update_id=80,
                    text="PRIVATE TEXT SENTINEL",
                    chat="PRIVATE_CHAT_SENTINEL",
                    user="PRIVATE_USER_SENTINEL",
                    thread="PRIVATE_THREAD_SENTINEL",
                )
            ]
        }
    )

    summary = poll_once(config, client, lambda profile: "safe-test-token")
    rendered = json.dumps(summary, sort_keys=True)

    for forbidden in (
        "PRIVATE TEXT SENTINEL",
        "PRIVATE_CHAT_SENTINEL",
        "PRIVATE_USER_SENTINEL",
        "PRIVATE_THREAD_SENTINEL",
        "safe-test-token",
        "PRIVATE_ENV_SENTINEL",
        "PRIVATE_ARGV_SENTINEL",
        "payload_json",
        "chat_id",
        "user_id",
        "thread_id",
        "argv",
        "env",
    ):
        assert forbidden not in rendered


def test_no_real_telegram_calls_required(tmp_path):
    config = WatcherConfig(
        profiles=[ProfileWatchConfig("expert", token_label="expert", spool_db=tmp_path / "expert.db")],
        manager_spool_db=tmp_path / "manager.db",
        default_timeout=0,
    )
    client = MockTelegramClient({"token-for-expert": []})

    summary = poll_once(config, client, token_provider)

    assert len(client.calls) == 1
    assert client.calls[0]["offset"] is None
    assert client.calls[0]["timeout"] == 0
    assert client.calls[0]["allowed_updates"] is None
    assert summary["counts"]["updates_seen"] == 0


def test_cli_defaults_to_dry_run_and_refuses_non_live_non_dry_run(capsys):
    assert main(["--once"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["dry_run"] is True
    assert output["once"] is True

    with pytest.raises(SystemExit):
        main(["--no-dry-run"])
    err = capsys.readouterr().err
    assert "requires --live" in err



def test_telegram_update_to_message_event_converts_private_text_message():
    from gateway.config import Platform
    from gateway.polling_watcher import telegram_update_to_message_event

    event = telegram_update_to_message_event(
        {
            "update_id": 1,
            "message": {
                "message_id": 22,
                "text": "hello naval",
                "chat": {"id": 333, "type": "private", "first_name": "F"},
                "from": {"id": 444, "username": "f_user"},
            },
        }
    )

    assert event is not None
    assert event.text == "hello naval"
    assert event.message_id == "22"
    assert event.source.platform == Platform.TELEGRAM
    assert event.source.chat_id == "333"
    assert event.source.chat_type == "dm"
    assert event.source.user_id == "444"
    assert event.source.user_name == "f_user"


def test_subprocess_wake_runner_uses_non_dry_run_worker(monkeypatch, tmp_path):
    from gateway.polling_watcher import SubprocessWakeRunner

    calls = []

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            calls.append((cmd, kwargs))

    import subprocess

    monkeypatch.setattr(subprocess, "Popen", FakePopen)
    runner = SubprocessWakeRunner(dry_run=False)

    plan = runner.request_wake("naval", tmp_path / "spool.db", [1])

    assert plan.dry_run is False
    assert calls
    cmd = calls[0][0]
    assert "gateway.spool_worker" in cmd
    assert "--no-dry-run" in cmd
