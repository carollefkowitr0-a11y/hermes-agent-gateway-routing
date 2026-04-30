"""Secret-safe dry-run command plan objects for gateway worker boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

HIGH_RISK_ACTIONS = frozenset(
    {
        "wake_worker",
        "start_worker",
        "stop_worker",
        "restart_worker",
        "probe_health",
        "wake_profile",
        "start_profile",
        "stop_profile",
        "restart_profile",
    }
)
_ALLOWED_METADATA_KEYS = frozenset(
    {
        "platform",
        "route",
        "target_profile",
        "update_id",
        "attempts",
        "state",
        "queued_count",
    }
)


def _safe_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Return allowlisted, scalar metadata only; never payload text or secrets."""
    safe: dict[str, Any] = {}
    for key, value in metadata.items():
        if key not in _ALLOWED_METADATA_KEYS:
            continue
        if value is None or isinstance(value, (str, int, float, bool)):
            safe[key] = value
    return safe


@dataclass(frozen=True)
class GatewayCommand:
    """A planned gateway boundary action.

    M4 commands are descriptive and dry-run by default. They intentionally do
    not carry raw payload bodies, shell snippets, credentials, or process args.
    """

    action: str
    profile: str = "naval"
    reason: str = "unspecified"
    target: str | None = None
    item_id: int | None = None
    route: str | None = None
    dry_run: bool = True
    requires_approval: bool | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.action, str) or not self.action.strip():
            raise ValueError("GatewayCommand.action must be a non-empty string")
        if not isinstance(self.profile, str) or not self.profile.strip():
            raise ValueError("GatewayCommand.profile must be a non-empty string")
        if self.requires_approval is None:
            object.__setattr__(self, "requires_approval", self.action in HIGH_RISK_ACTIONS)
        if self.action in HIGH_RISK_ACTIONS:
            if self.dry_run is not True:
                raise ValueError(f"High-risk gateway action must remain dry-run in M4: {self.action}")
            if self.requires_approval is not True:
                raise ValueError(f"High-risk gateway action requires approval: {self.action}")
        object.__setattr__(self, "metadata", _safe_metadata(dict(self.metadata)))

    def to_dict(self) -> dict[str, Any]:
        """Serialize only safe planning fields."""
        data: dict[str, Any] = {
            "action": self.action,
            "profile": self.profile,
            "reason": self.reason,
            "dry_run": self.dry_run,
            "requires_approval": bool(self.requires_approval),
        }
        if self.target is not None:
            data["target"] = self.target
        if self.item_id is not None:
            data["item_id"] = self.item_id
        if self.route is not None:
            data["route"] = self.route
        if self.metadata:
            data["metadata"] = dict(self.metadata)
        return data


@dataclass(frozen=True)
class GatewayCommandPlan:
    """A side-effect-free command plan for one controller iteration."""

    profile: str = "naval"
    commands: list[GatewayCommand] = field(default_factory=list)
    safe_to_execute: bool = True
    dry_run: bool = True
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "commands": [command.to_dict() for command in self.commands],
            "safe_to_execute": self.safe_to_execute,
            "dry_run": self.dry_run,
            "notes": list(self.notes),
        }
