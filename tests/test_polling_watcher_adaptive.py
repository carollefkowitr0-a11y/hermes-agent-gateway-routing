import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway.polling_watcher import AdaptivePollingPolicy, ProfileWatchConfig, WatcherConfig
import json


def test_load_watcher_config_file_json_builds_multi_profile_config(tmp_path):
    from gateway.polling_watcher import load_watcher_config_file

    config_path = tmp_path / "watcher.json"
    config_path.write_text(
        json.dumps(
            {
                "manager_profile": "default",
                "manager_spool_db": str(tmp_path / "manager.db"),
                "default_timeout": 25,
                "allowed_updates": ["message", "edited_message", "callback_query"],
                "profiles": [
                    {
                        "profile": "naval",
                        "enabled": True,
                        "token_env": "TELEGRAM_BOT_TOKEN",
                        "bot_name": "naval",
                        "owned": True,
                        "ambiguous": False,
                        "spool_db": str(tmp_path / "naval.db"),
                        "offset_path": str(tmp_path / "naval.offset.json"),
                        "route_kind": "profile",
                        "route": "naval",
                        "wake_profile": "naval",
                        "allowed_updates": ["message", "edited_message", "callback_query"],
                        "timeout": 25,
                    },
                    {
                        "profile": "future-persona",
                        "enabled": False,
                        "token_env": "FUTURE_PERSONA_TELEGRAM_BOT_TOKEN",
                        "bot_name": "future_persona_placeholder",
                        "owned": True,
                        "ambiguous": False,
                        "spool_db": str(tmp_path / "future.db"),
                        "offset_path": str(tmp_path / "future.offset.json"),
                        "route_kind": "profile",
                        "route": "future-persona",
                        "wake_profile": "future-persona",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    config = load_watcher_config_file(config_path)

    assert isinstance(config, WatcherConfig)
    assert config.manager_profile == "default"
    assert config.manager_spool_db == tmp_path / "manager.db"
    assert config.default_timeout == 25
    assert config.allowed_updates == ["message", "edited_message", "callback_query"]
    assert [profile.profile for profile in config.profiles] == ["naval", "future-persona"]
    assert all(isinstance(profile, ProfileWatchConfig) for profile in config.profiles)
    assert config.profiles[0].token_env == "TELEGRAM_BOT_TOKEN"
    assert config.profiles[0].spool_db == tmp_path / "naval.db"
    assert config.profiles[0].offset_path == tmp_path / "naval.offset.json"
    assert config.profiles[1].enabled is False
    assert config.profiles[1].timeout is None


def test_load_watcher_config_uses_file_when_path_supplied_and_naval_fallback_without_path(tmp_path, monkeypatch):
    from gateway.polling_watcher import load_watcher_config

    config_path = tmp_path / "watcher.json"
    config_path.write_text(
        json.dumps(
            {
                "manager_profile": "default",
                "manager_spool_db": str(tmp_path / "manager.db"),
                "default_timeout": 12,
                "profiles": [
                    {
                        "profile": "naval-from-file",
                        "token_env": "TELEGRAM_BOT_TOKEN",
                        "spool_db": str(tmp_path / "file.db"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("HERMES_WATCH_PROFILE", "naval-env")
    monkeypatch.setenv("HERMES_WATCHER_POLL_TIMEOUT", "25")

    file_config = load_watcher_config(config_path)
    fallback_config = load_watcher_config(None)

    assert file_config.profiles[0].profile == "naval-from-file"
    assert file_config.default_timeout == 12
    assert fallback_config.profiles[0].profile == "naval-env"
    assert fallback_config.profiles[0].route == "naval-env"
    assert fallback_config.profiles[0].spool_db == tmp_path / "home" / "gateway_spool.db"


def test_main_passes_config_path_to_live_loop(monkeypatch, tmp_path):
    import gateway.polling_watcher as polling_watcher

    config_path = tmp_path / "watcher.json"
    config_path.write_text('{"profiles": [{"profile": "naval", "token_env": "TELEGRAM_BOT_TOKEN"}]}', encoding="utf-8")
    observed = {}

    def fake_run_live_loop(*, once=False, dry_run=False, config_path=None):
        observed["once"] = once
        observed["dry_run"] = dry_run
        observed["config_path"] = config_path
        return 0

    monkeypatch.setattr(polling_watcher, "run_live_loop", fake_run_live_loop)

    assert polling_watcher.main(["--live", "--once", "--config", str(config_path)]) == 0
    assert observed == {"once": True, "dry_run": True, "config_path": str(config_path)}



def test_polling_watcher_import_does_not_load_heavy_gateway_runtime():
    script = """
import sys
import gateway.polling_watcher  # noqa: F401

heavy_modules = [
    "gateway.run",
    "gateway.platforms.telegram",
    "gateway.config",
    "gateway.session",
    "gateway.spool",
]
loaded = [module for module in heavy_modules if module in sys.modules]
if loaded:
    raise AssertionError(f"heavy gateway modules loaded by polling_watcher import: {loaded}")
"""

    subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )



def test_telegram_update_to_message_event_converts_text_message_without_name_error():
    from gateway.polling_watcher import telegram_update_to_message_event
    from gateway.platforms.base import MessageType

    event = telegram_update_to_message_event(
        {
            "update_id": 1001,
            "message": {
                "message_id": 2002,
                "chat": {"id": 3003, "type": "private", "first_name": "Chat"},
                "from": {"id": 4004, "username": "alice"},
                "text": "hello watcher",
            },
        }
    )

    assert event is not None
    assert event.text == "hello watcher"
    assert event.message_type is MessageType.TEXT
    assert event.source.platform.value == "telegram"
    assert event.source.chat_id == "3003"
    assert event.source.chat_type == "dm"
    assert event.source.user_id == "4004"
    assert event.message_id == "2002"
    assert event.platform_update_id == 1001



def test_adaptive_polling_uses_min_sleep_after_activity():
    policy = AdaptivePollingPolicy(min_sleep=5, base_sleep=25, max_sleep=120, active_window=300, idle_backoff_after=10)

    assert policy.next_sleep(updates_seen=1, now=1000.0) == 5
    assert policy.next_sleep(updates_seen=0, now=1100.0) == 5


def test_adaptive_polling_returns_base_after_active_window_without_long_idle():
    policy = AdaptivePollingPolicy(min_sleep=5, base_sleep=25, max_sleep=120, active_window=300, idle_backoff_after=10)
    policy.next_sleep(updates_seen=1, now=1000.0)

    assert policy.next_sleep(updates_seen=0, now=1401.0) == 25


def test_adaptive_polling_backs_off_after_repeated_empty_polls_and_resets_on_activity():
    policy = AdaptivePollingPolicy(min_sleep=5, base_sleep=25, max_sleep=120, active_window=300, idle_backoff_after=3)

    assert policy.next_sleep(updates_seen=0, now=1.0) == 25
    assert policy.next_sleep(updates_seen=0, now=2.0) == 25
    assert policy.next_sleep(updates_seen=0, now=3.0) == 50
    assert policy.next_sleep(updates_seen=0, now=4.0) == 100
    assert policy.next_sleep(updates_seen=0, now=5.0) == 120
    assert policy.next_sleep(updates_seen=1, now=6.0) == 5
    assert policy.next_sleep(updates_seen=0, now=7.0) == 5
