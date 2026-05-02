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
    """Process at most one queued/failed row through an injected consumer.

    Terminology note for live-gate evidence: ``DispatchResult.dispatched`` is a
    boolean meaning the row reached the consumer and the consumer returned
    successfully. The durable terminal spool state for that success is ``done``;
    the legacy ``dispatched`` spool state is not written by this dispatcher.
    Rows in durable state ``dispatched`` are historical/legacy residues, not new
    rows waiting for this worker.
    """
    row = spool.claim_next_queued(target_profile=target_profile)
    if row is None:
        return DispatchResult(dispatched=False, row_id=None, state="idle")

    row_id = int(row["id"])

    def _failure_state() -> str:
        # Re-read the row after marking processing so max_attempts counts this delivery attempt.
        updated = spool.get_row(row_id) or row
        return "dead_letter" if int(updated.get("attempts") or 0) >= max_attempts else "failed"

    try:
        consumer(row)
    except Exception as exc:  # noqa: BLE001 - boundary converts injected consumer failures to spool state.
        state = _failure_state()
        spool.mark_state(row_id, state, error=redact_secret(exc))
        return DispatchResult(dispatched=False, row_id=row_id, state=state)
    except BaseException as exc:
        # Best-effort safety net for cancellation/system-exit paths after the
        # row has already been claimed. Re-raise so process semantics remain
        # unchanged, but avoid leaving the spool row permanently in processing
        # when the interpreter still has a chance to clean up.
        try:
            spool.mark_state(row_id, _failure_state(), error=redact_secret(exc))
        finally:
            raise

    spool.mark_state(row_id, "done")
    return DispatchResult(dispatched=True, row_id=row_id, state="done")
