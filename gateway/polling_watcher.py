"""Local multi-profile Telegram polling watcher framework.

This module intentionally provides a testable local framework only. It does not
read real environment variables by default, does not execute wake commands, and
contains no default live Telegram API calls. Callers must inject a Telegram
client and a token provider.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from gateway.spool import GatewaySpool
from gateway.config import Platform, load_gateway_config
from gateway.platforms.base import MessageEvent, MessageType
from gateway.platforms.telegram import TelegramAdapter
from gateway.run import GatewayRunner
from gateway.session import SessionSource


TokenProvider = Callable[["ProfileWatchConfig"], str]


class TelegramPollingClient(Protocol):
    """Injected Telegram polling client boundary."""

    def get_updates(
        self,
        token: str,
        offset: int | None,
        timeout: int,
        allowed_updates: list[str] | None,
    ) -> list[dict[str, Any]]:
        """Return update dictionaries for one long-poll request."""


@dataclass(frozen=True)
class WakePlan:

    """Privacy-safe local wake plan; never executes process commands."""

    profile: str
    spool_db: Path
    row_ids: tuple[int, ...]
    dry_run: bool = True
    action: str = "wake_profile"

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "spool_db": str(self.spool_db),
            "row_ids": list(self.row_ids),
            "dry_run": self.dry_run,
            "action": self.action,
        }


class WakeRunner(Protocol):
    """Injected dry-run wake boundary."""

    def request_wake(self, profile: str, spool_db: Path, row_ids: Sequence[int]) -> WakePlan:
        """Record a local wake request and return a privacy-safe plan."""


class DryRunWakeRecorder:
    """Default wake runner: records dry-run wake plans only."""

    def __init__(self) -> None:
        self.plans: list[WakePlan] = []

    def request_wake(self, profile: str, spool_db: Path, row_ids: Sequence[int]) -> WakePlan:
        plan = WakePlan(profile=profile, spool_db=Path(spool_db), row_ids=tuple(int(row_id) for row_id in row_ids))
        self.plans.append(plan)
        return plan


@dataclass(frozen=True)
class ProfileWatchConfig:

    """Configuration for one Telegram bot/profile owner."""

    profile: str
    enabled: bool = True
    token_env: str | None = None
    token_label: str | None = None
    bot_name: str | None = None
    owned: bool = True
    ambiguous: bool = False
    spool_db: Path | str | None = None
    offset_path: Path | str | None = None
    route_kind: str = "profile"  # "profile" or "manager_fallback"
    route: str | None = None
    wake_command: tuple[str, ...] = field(default_factory=tuple)
    wake_profile: str | None = None
    allowed_updates: list[str] | None = None
    timeout: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.profile, str) or not self.profile.strip():
            raise ValueError("profile must be a non-empty string")
        if self.token_env and self.token_label:
            raise ValueError("configure only one of token_env or token_label")
        if not (self.token_env or self.token_label):
            raise ValueError("token_env or token_label is required")
        if self.route_kind not in {"profile", "manager_fallback"}:
            raise ValueError("route_kind must be 'profile' or 'manager_fallback'")
        if self.timeout is not None and self.timeout < 0:
            raise ValueError("timeout must be non-negative")
        object.__setattr__(self, "profile", self.profile.strip())
        if self.bot_name is not None:
            if not isinstance(self.bot_name, str) or not self.bot_name.strip():
                raise ValueError("bot_name must be a non-empty string when provided")
            object.__setattr__(self, "bot_name", self.bot_name.strip())
        if self.spool_db is not None:
            object.__setattr__(self, "spool_db", Path(self.spool_db))
        if self.offset_path is not None:
            object.__setattr__(self, "offset_path", Path(self.offset_path))
        object.__setattr__(self, "wake_command", tuple(self.wake_command))

    @property
    def token_ref(self) -> str:
        if self.token_env:
            return f"env:{self.token_env}"
        return f"label:{self.token_label}"


@dataclass(frozen=True)
class WatcherConfig:
    """Multi-profile polling watcher configuration."""

    profiles: list[ProfileWatchConfig]
    manager_profile: str = "default"
    manager_spool_db: Path | str | None = None
    default_timeout: int = 30
    allowed_updates: list[str] | None = None

    def __post_init__(self) -> None:
        if not self.profiles:
            raise ValueError("at least one profile is required")
        if not isinstance(self.manager_profile, str) or not self.manager_profile.strip():
            raise ValueError("manager_profile must be a non-empty string")
        if self.default_timeout < 0:
            raise ValueError("default_timeout must be non-negative")
        names: set[str] = set()
        token_refs: set[str] = set()
        for profile in self.profiles:
            if profile.profile in names:
                raise ValueError(f"duplicate profile name: {profile.profile}")
            names.add(profile.profile)
            if profile.token_ref in token_refs:
                raise ValueError("each configured token must have exactly one owner")
            token_refs.add(profile.token_ref)
        if self.manager_spool_db is not None:
            object.__setattr__(self, "manager_spool_db", Path(self.manager_spool_db))


@dataclass(frozen=True)
class RoutingDecision:
    """Privacy-safe routing result for one update."""

    source_profile: str
    source_bot: str
    target_profile: str
    route: str
    spool_db: Path
    fallback: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_profile": self.source_profile,
            "source_bot": self.source_bot,
            "target_profile": self.target_profile,
            "route": self.route,
            "spool_db": str(self.spool_db),
            "fallback": self.fallback,
            "reason": self.reason,
        }


def read_offset(path: Path | str | None) -> int | None:
    """Read a durable offset JSON file; missing/null means no offset."""

    if path is None:
        return None
    offset_file = Path(path)
    if not offset_file.exists():
        return None
    with offset_file.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    value = data.get("offset") if isinstance(data, dict) else data
    if value is None:
        return None
    return int(value)


def write_offset(path: Path | str | None, offset: int | None) -> None:
    """Atomically write a durable offset JSON file with private permissions."""

    if path is None or offset is None:
        return
    offset_file = Path(path)
    offset_file.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{offset_file.name}.", suffix=".tmp", dir=str(offset_file.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"offset": int(offset)}, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        os.chmod(tmp_name, 0o600)
        os.replace(tmp_name, offset_file)
        os.chmod(offset_file, 0o600)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def poll_once(
    config: WatcherConfig,
    client: TelegramPollingClient,
    tokens_provider: TokenProvider | Mapping[str, str],
    wake_runner: WakeRunner | None = None,
) -> dict[str, Any]:
    """Poll enabled profiles once and enqueue accepted updates.

    The returned summary is deliberately privacy-safe: it includes counts,
    profile/route labels, local row ids/states, next offsets, and dry-run wake
    plans only. It never includes payload bodies, message text, chat/user/thread
    ids, tokens, token env names, token labels, process argv, or environment.
    """

    runner = wake_runner if wake_runner is not None else DryRunWakeRecorder()
    summary: dict[str, Any] = {
        "ok": True,
        "dry_run": True,
        "profiles": {},
        "counts": {"polled_profiles": 0, "updates_seen": 0, "updates_accepted": 0, "wake_plans": 0},
        "wake_planned_profiles": [],
        "wake_plans": [],
    }
    planned_profiles: set[str] = set()

    for profile in config.profiles:
        if not profile.enabled:
            summary["profiles"][profile.profile] = {"enabled": False, "polled": False}
            continue

        token = _resolve_token(tokens_provider, profile)
        current_offset = read_offset(profile.offset_path)
        timeout = profile.timeout if profile.timeout is not None else config.default_timeout
        allowed_updates = profile.allowed_updates if profile.allowed_updates is not None else config.allowed_updates
        updates = client.get_updates(token, current_offset, timeout, allowed_updates)

        profile_summary: dict[str, Any] = {
            "enabled": True,
            "polled": True,
            "updates_seen": len(updates),
            "accepted": [],
            "next_offset": current_offset,
        }
        summary["counts"]["polled_profiles"] += 1
        summary["counts"]["updates_seen"] += len(updates)

        next_offset = current_offset
        for update in updates:
            update_id = _update_id(update)
            route_target = route_update(update, profile, config)
            spool = GatewaySpool(route_target.spool_db)
            spool.initialize()
            enqueue = spool.enqueue_update(
                platform="telegram",
                route=route_target.route,
                target_profile=route_target.target_profile,
                update_id=update_id,
                payload=update,
                chat_id=_chat_id(update),
                thread_id=_thread_id(update),
                user_id=_user_id(update),
                event_type=_event_type(update),
            )
            # Advance only after the update has been accepted by the spool layer.
            next_offset = update_id + 1
            write_offset(profile.offset_path, next_offset)
            accepted = {
                "row_id": enqueue.id,
                "state": enqueue.state,
                "inserted": enqueue.inserted,
                "duplicate": enqueue.duplicate,
                "target_profile": route_target.target_profile,
                "route": route_target.route,
                "source_profile": route_target.source_profile,
                "source_bot": route_target.source_bot,
                "fallback": route_target.fallback,
                "reason": route_target.reason,
            }
            profile_summary["accepted"].append(accepted)
            summary["counts"]["updates_accepted"] += 1
            wake_profile = route_target.target_profile
            planned_profiles.add(wake_profile)
            plan = runner.request_wake(wake_profile, route_target.spool_db, (enqueue.id,))
            summary["wake_plans"].append(plan.to_dict())

        profile_summary["next_offset"] = next_offset
        summary["profiles"][profile.profile] = profile_summary

    summary["wake_planned_profiles"] = sorted(planned_profiles)
    summary["counts"]["wake_plans"] = len(summary["wake_plans"])
    return summary


def _resolve_token(tokens_provider: TokenProvider | Mapping[str, str], profile: ProfileWatchConfig) -> str:
    if callable(tokens_provider):
        token = tokens_provider(profile)
    else:
        token = tokens_provider[profile.token_ref]
    if not isinstance(token, str) or not token:
        raise ValueError("token provider returned an empty token")
    return token


def route_update(
    update: Mapping[str, Any],
    source_profile: ProfileWatchConfig,
    config: WatcherConfig,
) -> RoutingDecision:
    """Route an update from a configured bot/profile.

    Polling normally routes each owned, enabled, unambiguous bot to its profile
    spool. Disabled, unowned, ambiguous, or explicit manager-fallback routes go
    to the default manager spool; this function never guesses an arbitrary
    expert from message content.
    """

    # Ensure callers can unit-test validation without needing a Telegram client.
    _update_id(update)
    source_bot = source_profile.bot_name or source_profile.route or source_profile.profile
    if not source_profile.enabled:
        return _manager_route(config, source_profile, source_bot, "disabled_profile")
    if not source_profile.owned:
        return _manager_route(config, source_profile, source_bot, "unowned_profile")
    if source_profile.ambiguous:
        return _manager_route(config, source_profile, source_bot, "ambiguous_profile")
    if source_profile.route_kind == "manager_fallback":
        return _manager_route(config, source_profile, source_bot, "configured_manager_fallback")
    if source_profile.spool_db is None:
        return _manager_route(config, source_profile, source_bot, "missing_profile_spool")
    return RoutingDecision(
        source_profile=source_profile.profile,
        source_bot=source_bot,
        target_profile=source_profile.wake_profile or source_profile.profile,
        route=source_profile.route or source_profile.profile,
        spool_db=Path(source_profile.spool_db),
        fallback=False,
        reason="owned_profile",
    )


def _manager_route(
    config: WatcherConfig,
    source_profile: ProfileWatchConfig,
    source_bot: str,
    reason: str,
) -> RoutingDecision:
    if config.manager_spool_db is None:
        raise ValueError("manager_spool_db is required for manager fallback routing")
    return RoutingDecision(
        source_profile=source_profile.profile,
        source_bot=source_bot,
        target_profile=config.manager_profile,
        route=config.manager_profile,
        spool_db=Path(config.manager_spool_db),
        fallback=True,
        reason=reason,
    )


def _update_id(update: Mapping[str, Any]) -> int:
    if "update_id" not in update:
        raise ValueError("telegram update missing update_id")
    return int(update["update_id"])


def _event(update: Mapping[str, Any]) -> tuple[str | None, Mapping[str, Any] | None]:
    for key in (
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "callback_query",
        "my_chat_member",
        "chat_member",
    ):
        value = update.get(key)
        if isinstance(value, Mapping):
            return key, value
    return None, None


def _event_type(update: Mapping[str, Any]) -> str | None:
    event_type, _ = _event(update)
    return event_type


def _chat_id(update: Mapping[str, Any]) -> str | int | None:
    event_type, event = _event(update)
    if event is None:
        return None
    if event_type == "callback_query":
        message = event.get("message")
        if isinstance(message, Mapping):
            chat = message.get("chat")
            if isinstance(chat, Mapping):
                return chat.get("id")
        return None
    chat = event.get("chat")
    if isinstance(chat, Mapping):
        return chat.get("id")
    return None


def _thread_id(update: Mapping[str, Any]) -> str | int | None:
    _, event = _event(update)
    if event is None:
        return None
    return event.get("message_thread_id")


def _user_id(update: Mapping[str, Any]) -> str | int | None:
    event_type, event = _event(update)
    if event is None:
        return None
    if event_type == "callback_query":
        user = event.get("from")
    else:
        user = event.get("from") or event.get("new_chat_member") or event.get("old_chat_member")
    if isinstance(user, Mapping):
        return user.get("id")
    return None



def _message_payload(update: Mapping[str, Any]) -> Mapping[str, Any] | None:
    event_type, event = _event(update)
    if event is None:
        return None
    if event_type == "callback_query":
        message = event.get("message")
        return message if isinstance(message, Mapping) else None
    return event


def _message_text(message: Mapping[str, Any]) -> tuple[str, MessageType]:
    if isinstance(message.get("text"), str):
        return message["text"], MessageType.TEXT
    if isinstance(message.get("caption"), str):
        return message["caption"], MessageType.TEXT
    return "", MessageType.TEXT


def telegram_update_to_message_event(update: Mapping[str, Any]) -> MessageEvent | None:
    """Convert a raw Telegram update dict into the gateway MessageEvent seam.

    This supports text/caption messages for the polling watcher live path. Other
    update shapes remain queued/failed instead of being silently treated as text.
    """

    message = _message_payload(update)
    if message is None:
        return None
    chat = message.get("chat")
    if not isinstance(chat, Mapping) or chat.get("id") is None:
        return None
    user = message.get("from") if isinstance(message.get("from"), Mapping) else {}
    text, message_type = _message_text(message)
    message_id = message.get("message_id")
    thread_id = message.get("message_thread_id")
    chat_type = str(chat.get("type") or "dm")
    if chat_type == "private":
        chat_type = "dm"
    chat_name = chat.get("title") or chat.get("username")
    if not chat_name:
        first = chat.get("first_name")
        last = chat.get("last_name")
        chat_name = " ".join(str(v) for v in (first, last) if v).strip() or None
    user_name = None
    if isinstance(user, Mapping):
        user_name = user.get("username")
        if not user_name:
            user_name = " ".join(str(v) for v in (user.get("first_name"), user.get("last_name")) if v).strip() or None
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id=str(chat["id"]),
        chat_name=str(chat_name) if chat_name else None,
        chat_type=chat_type,
        user_id=str(user.get("id")) if isinstance(user, Mapping) and user.get("id") is not None else None,
        user_name=str(user_name) if user_name else None,
        thread_id=str(thread_id) if thread_id is not None else None,
        message_id=str(message_id) if message_id is not None else None,
    )
    return MessageEvent(
        text=text,
        message_type=message_type,
        source=source,
        raw_message=dict(update),
        message_id=str(message_id) if message_id is not None else None,
        platform_update_id=_update_id(update),
    )


class DirectGatewayConsumer:
    """Consume one spooled Telegram row by invoking the real gateway handler.

    The consumer does not start Telegram polling/webhooks. It only creates a
    Telegram adapter with a Bot object for outbound sends, wires it to
    GatewayRunner._handle_message, and calls adapter.handle_message(event).
    """

    def __init__(self) -> None:
        config = load_gateway_config()
        telegram_config = config.platforms.get(Platform.TELEGRAM)
        if telegram_config is None or not telegram_config.enabled or not telegram_config.token:
            raise RuntimeError("telegram platform is not configured for direct gateway delivery")
        self.runner = GatewayRunner(config)
        self.adapter = TelegramAdapter(telegram_config)
        self.adapter.set_message_handler(self.runner._handle_message)
        self.adapter.set_session_store(self.runner.session_store)
        self.adapter.set_busy_session_handler(self.runner._handle_active_session_busy_message)
        self.adapter._bot = self.adapter._create_bot_for_send_only(telegram_config.token)
        self.runner.adapters[Platform.TELEGRAM] = self.adapter

    async def _handle_and_drain(self, event: MessageEvent) -> None:
        await self.adapter.handle_message(event)
        deadline = asyncio.get_running_loop().time() + float(os.getenv("HERMES_WATCHER_DIRECT_TIMEOUT", "180"))
        while True:
            tasks = [task for task in getattr(self.adapter, "_background_tasks", set()) if not task.done()]
            if not tasks:
                return
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                for task in tasks:
                    task.cancel()
                raise RuntimeError("direct gateway delivery timed out")
            done, _ = await asyncio.wait(tasks, timeout=min(1.0, remaining), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc

    def __call__(self, row: dict[str, Any]) -> None:
        payload_raw = row.get("payload_json")
        if not isinstance(payload_raw, str):
            raise RuntimeError("spool row missing payload_json")
        payload = json.loads(payload_raw)
        if not isinstance(payload, Mapping):
            raise RuntimeError("spool row payload is not an object")
        event = telegram_update_to_message_event(payload)
        if event is None:
            raise RuntimeError("spool row payload cannot be converted to MessageEvent")
        asyncio.run(self._handle_and_drain(event))


class LiveTelegramPollingClient:
    """Minimal live Telegram getUpdates client using stdlib only.

    The client is intentionally small and token-safe: it never returns URLs,
    never logs tokens, and raises redacted errors. Proxy support is delegated to
    urllib via *_proxy environment variables.
    """

    api_base = "https://api.telegram.org"

    def get_updates(
        self,
        token: str,
        offset: int | None,
        timeout: int,
        allowed_updates: list[str] | None,
    ) -> list[dict[str, Any]]:
        import urllib.error
        import urllib.parse
        import urllib.request

        params: dict[str, str] = {"timeout": str(int(timeout))}
        if offset is not None:
            params["offset"] = str(int(offset))
        if allowed_updates is not None:
            params["allowed_updates"] = json.dumps(allowed_updates, separators=(",", ":"))
        body = urllib.parse.urlencode(params).encode("utf-8")
        req = urllib.request.Request(
            f"{self.api_base}/bot{token}/getUpdates",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=max(int(timeout) + 10, 15)) as resp:  # noqa: S310 - Telegram API URL is fixed.
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:  # pragma: no cover - live boundary
            raise RuntimeError(f"telegram getUpdates HTTP {exc.code}") from exc
        except Exception as exc:  # pragma: no cover - live boundary
            raise RuntimeError(f"telegram getUpdates failed: {exc.__class__.__name__}") from exc
        if not isinstance(data, dict) or data.get("ok") is not True:
            description = str(data.get("description", "unknown")) if isinstance(data, dict) else "invalid_response"
            raise RuntimeError(f"telegram getUpdates returned not ok: {description[:120]}")
        result = data.get("result", [])
        if not isinstance(result, list):
            raise RuntimeError("telegram getUpdates result was not a list")
        return [item for item in result if isinstance(item, dict)]


class SubprocessWakeRunner:
    """Wake runner that executes a fixed command template for accepted rows."""

    def __init__(self, *, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.plans: list[WakePlan] = []

    def request_wake(self, profile: str, spool_db: Path, row_ids: Sequence[int]) -> WakePlan:
        import subprocess

        plan = WakePlan(
            profile=profile,
            spool_db=Path(spool_db),
            row_ids=tuple(int(row_id) for row_id in row_ids),
            dry_run=self.dry_run,
            action="subprocess_wake_profile",
        )
        self.plans.append(plan)
        if not self.dry_run:
            cmd = [
                os.environ.get("PYTHON", "python"),
                "-m",
                "gateway.spool_worker",
                "--profile",
                profile,
                "--spool-db",
                str(spool_db),
                "--once",
                "--no-dry-run",
            ]
            subprocess.Popen(  # noqa: S603 - fixed argv, no shell.
                cmd,
                cwd=str(Path(__file__).resolve().parents[1]),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        return plan


def _load_live_naval_config() -> WatcherConfig:
    home = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()
    profile = os.environ.get("HERMES_WATCH_PROFILE", "naval").strip() or "naval"
    return WatcherConfig(
        profiles=[
            ProfileWatchConfig(
                profile,
                token_env="TELEGRAM_BOT_TOKEN",
                bot_name=os.environ.get("HERMES_WATCH_BOT_NAME") or profile,
                spool_db=home / "gateway_spool.db",
                offset_path=home / "telegram_polling_watcher.offset.json",
                wake_profile=profile,
                route=profile,
                allowed_updates=["message", "edited_message", "callback_query"],
                timeout=int(os.environ.get("HERMES_WATCHER_POLL_TIMEOUT", "25")),
            )
        ],
        manager_profile="default",
        manager_spool_db=Path(os.environ.get("HERMES_MANAGER_SPOOL_DB", str(Path.home() / ".hermes" / "gateway_spool.db"))).expanduser(),
        default_timeout=int(os.environ.get("HERMES_WATCHER_POLL_TIMEOUT", "25")),
        allowed_updates=["message", "edited_message", "callback_query"],
    )


def _token_from_env(profile: ProfileWatchConfig) -> str:
    if not profile.token_env:
        raise ValueError("live mode requires token_env")
    token = os.environ.get(profile.token_env, "")
    if not token:
        raise ValueError("configured Telegram token environment variable is empty")
    return token


def run_live_loop(*, once: bool = False, dry_run: bool = False) -> int:
    from hermes_cli.env_loader import load_hermes_dotenv

    hermes_home = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()
    load_hermes_dotenv(hermes_home=hermes_home, project_env=Path(__file__).resolve().parents[1] / ".env")
    config = _load_live_naval_config()
    client = LiveTelegramPollingClient()
    runner: WakeRunner = DryRunWakeRecorder() if dry_run else SubprocessWakeRunner(dry_run=False)
    while True:
        summary = poll_once(config, client, _token_from_env, wake_runner=runner)
        safe = {
            "ok": summary.get("ok"),
            "dry_run": dry_run,
            "counts": summary.get("counts"),
            "wake_planned_profiles": summary.get("wake_planned_profiles"),
        }
        print(json.dumps(safe, ensure_ascii=False, sort_keys=True), flush=True)
        if once:
            return 0


def main(argv: Sequence[str] | None = None) -> int:
    """CLI for dry-run planning and explicit live Naval watcher mode."""

    parser = argparse.ArgumentParser(description="Hermes Telegram polling watcher")
    parser.add_argument("--config", help="planned watcher config path", default=None)
    parser.add_argument("--once", action="store_true", help="run one polling iteration")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--live", action="store_true", help="explicitly enable live Telegram getUpdates")
    args = parser.parse_args(argv)
    if args.live:
        return run_live_loop(once=bool(args.once), dry_run=bool(args.dry_run))
    if not args.dry_run:
        parser.error("live/non-dry-run polling requires --live")
    summary = {
        "ok": True,
        "dry_run": True,
        "once": bool(args.once),
        "config": str(args.config) if args.config else None,
        "live_refused": False,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


__all__ = [
    "DryRunWakeRecorder",
    "ProfileWatchConfig",
    "RoutingDecision",
    "TelegramPollingClient",
    "WakePlan",
    "WakeRunner",
    "WatcherConfig",
    "LiveTelegramPollingClient",
    "SubprocessWakeRunner",
    "main",
    "poll_once",
    "read_offset",
    "route_update",
    "write_offset",
]


if __name__ == "__main__":
    raise SystemExit(main())
