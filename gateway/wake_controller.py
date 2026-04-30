"""Dry-run wake controller boundary for local gateway spool work.

M4 intentionally provides a bounded, no-op control seam only. It can plan a
Naval wake/dispatch iteration and can dispatch one local spool row through an
injected consumer, but it cannot start, stop, restart, signal, or probe any real
process or external service.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from gateway.commands import GatewayCommand, GatewayCommandPlan
from gateway.dispatcher import DispatchResult, dispatch_one
from gateway.spool import GatewaySpool
from gateway.supervisor_boundary import DryRunSupervisor


@dataclass(frozen=True)
class WakeControllerResult:
    plan: GatewayCommandPlan
    dispatch_result: DispatchResult | None
    supervisor_events: list[dict[str, object]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan": self.plan.to_dict(),
            "dispatch_result": None
            if self.dispatch_result is None
            else {
                "dispatched": self.dispatch_result.dispatched,
                "row_id": self.dispatch_result.row_id,
                "state": self.dispatch_result.state,
            },
            "supervisor_events": list(self.supervisor_events),
        }


class WakeController:
    """Plan and run one dry-run local wake/dispatch iteration."""

    def __init__(
        self,
        *,
        spool: GatewaySpool,
        profile: str = "naval",
        supervisor: DryRunSupervisor | None = None,
        dispatcher: Callable[..., DispatchResult] = dispatch_one,
        consumer: Callable[[dict[str, Any]], Any] | None = None,
        max_attempts: int = 3,
        allow_non_naval: bool = False,
    ) -> None:
        self.spool = spool
        self.profile = profile
        self.supervisor = supervisor if supervisor is not None else DryRunSupervisor()
        self.dispatcher = dispatcher
        self.consumer = consumer if consumer is not None else self._noop_consumer
        self.max_attempts = max_attempts
        self.allow_non_naval = allow_non_naval

    def plan_once(self) -> GatewayCommandPlan:
        """Return a command plan without mutating spool state or external state."""
        if self.profile != "naval" and not self.allow_non_naval:
            return GatewayCommandPlan(
                profile=self.profile,
                commands=[
                    GatewayCommand(
                        action="noop",
                        profile=self.profile,
                        reason="non_naval_target_refused",
                        dry_run=True,
                        requires_approval=False,
                    )
                ],
                safe_to_execute=False,
                dry_run=True,
                notes=["non-naval target refused by default"],
            )

        row = self.spool.get_next_queued(target_profile=self.profile)
        if row is None:
            return GatewayCommandPlan(
                profile=self.profile,
                commands=[
                    GatewayCommand(
                        action="inspect_profile",
                        profile=self.profile,
                        reason="no_queued_spool_item",
                        dry_run=True,
                        requires_approval=False,
                    ),
                    GatewayCommand(
                        action="noop",
                        profile=self.profile,
                        reason="idle",
                        dry_run=True,
                        requires_approval=False,
                    ),
                ],
                safe_to_execute=True,
                dry_run=True,
                notes=["no queued spool item"],
            )

        row_id = int(row["id"])
        metadata = {
            "platform": row.get("platform"),
            "route": row.get("route"),
            "target_profile": row.get("target_profile"),
            "update_id": row.get("update_id"),
            "attempts": row.get("attempts"),
            "state": row.get("state"),
        }
        return GatewayCommandPlan(
            profile=self.profile,
            commands=[
                GatewayCommand(
                    action="inspect_profile",
                    profile=self.profile,
                    reason="queued_spool_item",
                    item_id=row_id,
                    route=row.get("route"),
                    dry_run=True,
                    requires_approval=False,
                    metadata=metadata,
                ),
                GatewayCommand(
                    action="probe_health",
                    profile=self.profile,
                    reason="queued_spool_item",
                    item_id=row_id,
                    route=row.get("route"),
                    dry_run=True,
                    metadata=metadata,
                ),
                GatewayCommand(
                    action="wake_worker",
                    profile=self.profile,
                    reason="queued_spool_item",
                    target=self.profile,
                    item_id=row_id,
                    route=row.get("route"),
                    dry_run=True,
                    metadata=metadata,
                ),
                GatewayCommand(
                    action="dispatch_spool_item",
                    profile=self.profile,
                    reason="queued_spool_item",
                    item_id=row_id,
                    route=row.get("route"),
                    dry_run=True,
                    requires_approval=False,
                    metadata=metadata,
                ),
            ],
            safe_to_execute=True,
            dry_run=True,
            notes=["dry-run wake only; dispatch uses injected consumer"],
        )

    def run_once(self) -> WakeControllerResult:
        """Record dry-run commands and dispatch at most one local spool row."""
        plan = self.plan_once()
        events = self.supervisor.execute_plan(plan)
        dispatch_result: DispatchResult | None = None
        should_dispatch = plan.safe_to_execute and any(
            command.action == "dispatch_spool_item" for command in plan.commands
        )
        if should_dispatch:
            dispatch_result = self.dispatcher(
                self.spool,
                target_profile=self.profile,
                consumer=self.consumer,
                max_attempts=self.max_attempts,
            )
        return WakeControllerResult(
            plan=plan,
            dispatch_result=dispatch_result,
            supervisor_events=events,
        )

    @staticmethod
    def _noop_consumer(row: dict[str, Any]) -> None:
        """Default consumer is deliberately inert and local-only."""
        return None
