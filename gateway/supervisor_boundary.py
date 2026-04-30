"""Dry-run supervisor wakeup boundary for gateway spool tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_dry_run_wakeup_event(*, target_profile: str, reason: str, queued_count: int = 0) -> dict[str, object]:
    """Build an inert event describing a supervisor wakeup that would be requested."""
    return {
        "dry_run": True,
        "target_profile": target_profile,
        "reason": reason,
        "queued_count": queued_count,
        "created_at": _utc_now(),
    }


def dry_run_wakeup_events(target_profiles: Iterable[str], *, reason: str, queued_count: int = 0) -> list[dict[str, object]]:
    """Return dry-run wakeup events without invoking external managers."""
    return [
        build_dry_run_wakeup_event(target_profile=target_profile, reason=reason, queued_count=queued_count)
        for target_profile in target_profiles
    ]


class DryRunSupervisor:
    """No-op supervisor backend that only records safe command-plan events."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def record_command(self, command: object) -> dict[str, object]:
        if not hasattr(command, "to_dict"):
            raise TypeError("record_command expects an object with to_dict()")
        data = command.to_dict()  # type: ignore[attr-defined]
        event: dict[str, object] = {
            "dry_run": True,
            "executed": False,
            "action": data.get("action"),
            "target_profile": data.get("profile"),
            "reason": data.get("reason"),
            "requires_approval": data.get("requires_approval", False),
            "created_at": _utc_now(),
        }
        for key in ("item_id", "route", "target", "metadata"):
            if key in data:
                event[key] = data[key]
        self.events.append(event)
        return event

    def execute_plan(self, plan: object) -> list[dict[str, object]]:
        if not getattr(plan, "dry_run", True):
            raise ValueError("DryRunSupervisor refuses non-dry-run plans")
        commands = getattr(plan, "commands", None)
        if commands is None:
            raise TypeError("execute_plan expects an object with commands")
        return [self.record_command(command) for command in commands]
