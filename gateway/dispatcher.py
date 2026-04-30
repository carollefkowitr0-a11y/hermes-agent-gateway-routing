"""Single-iteration gateway spool dispatcher boundary for tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from gateway.ingress_auth import redact_secret
from gateway.spool import GatewaySpool


@dataclass(frozen=True)
class DispatchResult:
    dispatched: bool
    row_id: int | None
    state: str


def dispatch_one(
    spool: GatewaySpool,
    *,
    target_profile: str = "naval",
    consumer: Callable[[dict[str, Any]], Any],
    max_attempts: int = 3,
) -> DispatchResult:
    """Dispatch at most one queued row to an injected consumer."""
    row = spool.get_next_queued(target_profile=target_profile)
    if row is None:
        return DispatchResult(dispatched=False, row_id=None, state="idle")

    row_id = int(row["id"])
    try:
        consumer(row)
    except Exception as exc:  # noqa: BLE001 - boundary converts injected consumer failures to spool state.
        state = "dead_letter" if int(row.get("attempts") or 0) >= max_attempts else "failed"
        spool.mark_state(row_id, state, error=redact_secret(exc))
        return DispatchResult(dispatched=False, row_id=row_id, state=state)

    spool.mark_state(row_id, "dispatched")
    return DispatchResult(dispatched=True, row_id=row_id, state="dispatched")
