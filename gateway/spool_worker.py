"""Local one-shot dry-run worker for the gateway spool.

This module intentionally stays inside the local SQLite spool + dry-run wake
controller boundary. It does not contact platform APIs and does not manage any
external worker process.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence, TextIO
import os

from hermes_constants import get_default_hermes_root
from gateway.polling_watcher import safe_profile_name

from gateway.spool import GatewaySpool
from gateway.supervisor_boundary import DryRunSupervisor
from gateway.wake_controller import WakeController, WakeControllerResult


DEFAULT_PROFILE = "naval"


class SpoolWorkerError(ValueError):
    """Raised when the local one-shot worker request is outside the safe boundary."""


def default_spool_db_path(profile: str = DEFAULT_PROFILE) -> Path:
    """Return the default profile-local gateway spool database path."""

    return get_default_hermes_root() / "profiles" / safe_profile_name(profile) / "gateway_spool.db"


def _safe_dispatch_summary(result: WakeControllerResult) -> dict[str, Any] | None:
    if result.dispatch_result is None:
        return None
    return {
        "dispatched": result.dispatch_result.dispatched,
        "row_id": result.dispatch_result.row_id,
        "state": result.dispatch_result.state,
    }


def _safe_worker_summary(
    *,
    profile: str,
    spool_db: Path,
    result: WakeControllerResult,
    dry_run: bool = True,
    requeued_stale_processing: int = 0,
) -> dict[str, Any]:
    dispatch = _safe_dispatch_summary(result)
    if dispatch is None:
        status = "idle" if result.plan.safe_to_execute else "refused"
    elif dispatch["dispatched"]:
        status = "processed"
    else:
        status = dispatch["state"]

    spool = GatewaySpool(spool_db)
    spool.initialize()
    return {
        "ok": result.plan.safe_to_execute,
        "status": status,
        "profile": profile,
        "dry_run": dry_run,
        "once": True,
        "spool_db": str(spool_db),
        "counts": spool.count_by_state_for_profile(profile),
        "requeued_stale_processing": int(requeued_stale_processing),
        "plan": {
            "safe_to_execute": result.plan.safe_to_execute,
            "dry_run": result.plan.dry_run,
            "actions": [command.action for command in result.plan.commands],
            "notes": list(result.plan.notes),
        },
        "dispatch": dispatch,
        "supervisor": {
            "dry_run": True,
            "executed": False,
            "actions": [str(event.get("action")) for event in result.supervisor_events],
            "event_count": len(result.supervisor_events),
        },
    }


def _refusal_summary(*, profile: str, spool_db: Path | None, reason: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "ok": False,
        "status": "refused",
        "profile": profile,
        "dry_run": True,
        "once": True,
        "reason": reason,
    }
    if spool_db is not None:
        summary["spool_db"] = str(spool_db)
    return summary


def _profile_token_env_override_name(profile: str) -> str:
    safe_profile = "".join(ch if ch.isalnum() else "_" for ch in profile.strip().upper())
    return f"HERMES_WATCHER_PROFILE_TOKEN_ENV_{safe_profile}"


def _build_direct_gateway_env(profile: str) -> dict[str, str | None]:
    env_name = os.environ.get(_profile_token_env_override_name(profile), "").strip()
    if not env_name or env_name == "TELEGRAM_BOT_TOKEN":
        return {}
    token = os.environ.get(env_name)
    if not token:
        raise RuntimeError("configured profile Telegram token environment variable is empty")
    return {"TELEGRAM_BOT_TOKEN": token}


def _restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def run_worker(
    *,
    profile: str = DEFAULT_PROFILE,
    spool_db: str | Path | None = None,
    once: bool = True,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Run one local spool worker iteration and return a safe summary."""

    profile = safe_profile_name((profile or "").strip() or DEFAULT_PROFILE)
    resolved_db = Path(spool_db) if spool_db is not None else None
    if not once:
        return _refusal_summary(profile=profile, spool_db=resolved_db, reason="only_once_supported")

    db_path = resolved_db if resolved_db is not None else default_spool_db_path(profile)
    spool = GatewaySpool(db_path)
    spool.initialize()
    requeued = spool.requeue_stale_processing(
        older_than_seconds=int(__import__("os").environ.get("HERMES_SPOOL_STALE_PROCESSING_SECONDS", "900")),
        target_profile=profile,
    )
    if dry_run:
        consumer = WakeController._noop_consumer
    else:
        from hermes_cli.env_loader import load_hermes_dotenv
        from gateway.polling_watcher import DirectGatewayConsumer

        profile_home = db_path.parent
        scoped_env: dict[str, str | None] = {"HERMES_HOME": os.environ.get("HERMES_HOME")}
        os.environ["HERMES_HOME"] = str(profile_home)
        load_hermes_dotenv(hermes_home=profile_home, project_env=Path(__file__).resolve().parents[1] / ".env")
        direct_env = _build_direct_gateway_env(profile)
        scoped_env.update({key: os.environ.get(key) for key in direct_env})
        os.environ.update({key: value for key, value in direct_env.items() if value is not None})
        consumer = DirectGatewayConsumer()
    try:
        controller = WakeController(
            spool=spool,
            profile=profile,
            supervisor=DryRunSupervisor(),
            consumer=consumer,
            allow_non_naval=True,
        )
        result = controller.run_once()
        return _safe_worker_summary(
            profile=profile,
            spool_db=db_path,
            result=result,
            dry_run=dry_run,
            requeued_stale_processing=requeued,
        )
    finally:
        if not dry_run:
            _restore_env(scoped_env)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one local gateway spool worker iteration.")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="Target profile to consume from the local spool.")
    parser.add_argument("--spool-db", type=Path, default=None, help="Explicit local spool SQLite database path.")
    parser.add_argument("--once", action="store_true", default=True, help="Run at most one local spool item.")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Dry-run mode; this is always required.")
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false", help="Refused: non-dry-run is disabled.")
    return parser


def main(argv: Sequence[str] | None = None, stdout: TextIO | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output = stdout if stdout is not None else sys.stdout
    summary = run_worker(profile=args.profile, spool_db=args.spool_db, once=args.once, dry_run=args.dry_run)
    json.dump(summary, output, ensure_ascii=False, sort_keys=True)
    output.write("\n")
    return 0 if summary.get("ok") is True and summary.get("status") not in {"failed", "dead_letter"} else 2


if __name__ == "__main__":  # pragma: no cover - exercised through main() tests.
    raise SystemExit(main())
