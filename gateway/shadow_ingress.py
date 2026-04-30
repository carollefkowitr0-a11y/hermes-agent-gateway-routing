"""Offline shadow webhook ingress helpers for local gateway spool testing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gateway.spool import GatewaySpool

DEFAULT_ALLOWED_ROUTES = frozenset({"naval"})


@dataclass(frozen=True)
class ShadowIngressResult:
    status: int
    body: dict[str, Any]


def enqueue_shadow_webhook(
    payload: dict,
    *,
    route: str = "naval",
    target_profile: str = "naval",
    platform: str = "telegram",
    spool: GatewaySpool | None = None,
    allowed_routes: set[str] | None = None,
) -> ShadowIngressResult:
    """Validate a local shadow webhook payload and enqueue it into GatewaySpool."""
    allowed = DEFAULT_ALLOWED_ROUTES if allowed_routes is None else allowed_routes
    if route not in allowed:
        return _rejected(403, "route_not_allowed")

    if target_profile != "naval":
        return _rejected(403, "target_profile_not_allowed")

    if "target_profile" in payload or "profile" in payload:
        return _rejected(403, "payload_profile_not_allowed")

    if "update_id" not in payload:
        return _rejected(400, "missing_update_id")

    message = payload.get("message")
    event_type = "message" if isinstance(message, dict) else payload.get("event_type", "unknown")
    if spool is None:
        spool = GatewaySpool()
        spool.initialize()
    result = spool.enqueue_update(
        platform=platform,
        route=route,
        target_profile=target_profile,
        update_id=payload["update_id"],
        payload=payload,
        chat_id=_nested_get(message, "chat", "id"),
        thread_id=_nested_get(message, "message_thread_id"),
        user_id=_nested_get(message, "from", "id"),
        event_type=event_type,
    )

    return ShadowIngressResult(
        status=202,
        body={
            "accepted": True,
            "duplicate": result.duplicate,
            "id": result.id,
            "state": result.state,
            "route": route,
            "target_profile": target_profile,
            "update_id": str(payload["update_id"]),
        },
    )


def _rejected(status: int, reason: str) -> ShadowIngressResult:
    return ShadowIngressResult(status=status, body={"accepted": False, "reason": reason})


def _nested_get(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
