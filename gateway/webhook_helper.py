"""Secret-safe Telegram webhook helper primitives.

This module is intentionally dry-run by default. Live Telegram API calls only
happen when an explicit client object is passed to the high-level helpers (or a
caller explicitly instantiates and passes TelegramHTTPClient).
"""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

REDACTED = "[REDACTED]"
_TOKEN_RE = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b")
_BOT_PATH_RE = re.compile(r"/bot[^/?#]+")
_SECRET_WORD_RE = re.compile(
    r"(?i)(token|secret|api[_-]?key|authorization|password|passwd|bearer|signature|hash)"
)
_SENSITIVE_WEBHOOK_KEYS = {
    "url",
    "last_error_message",
    "ip_address",
    "secret_token",
    "token",
    "authorization",
    "api_key",
}


class WebhookClient(Protocol):
    """Minimal injectable Telegram webhook client interface."""

    def get_webhook_info(self, bot_token: str) -> Mapping[str, Any]: ...

    def set_webhook(self, bot_token: str, **params: Any) -> Mapping[str, Any]: ...

    def delete_webhook(self, bot_token: str, **params: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class WebhookPlan:
    """Safe operation summary returned by helper functions."""

    operation: str
    dry_run: bool
    params: dict[str, Any]
    result: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"operation": self.operation, "dry_run": self.dry_run, "params": self.params}
        if self.result is not None:
            data["result"] = self.result
        return data


class TelegramHTTPClient:
    """Small urllib client for Telegram webhook methods.

    The token is only used to construct the request URL in memory and is never
    returned by this class. Prefer injecting a fake client in tests.
    """

    api_base = "https://api.telegram.org"

    def get_webhook_info(self, bot_token: str) -> Mapping[str, Any]:
        return self._request(bot_token, "getWebhookInfo", {})

    def set_webhook(self, bot_token: str, **params: Any) -> Mapping[str, Any]:
        return self._request(bot_token, "setWebhook", params)

    def delete_webhook(self, bot_token: str, **params: Any) -> Mapping[str, Any]:
        return self._request(bot_token, "deleteWebhook", params)

    def _request(self, bot_token: str, method: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        url = f"{self.api_base}/bot{bot_token}/{method}"
        body = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}).encode("utf-8")
        request = urllib.request.Request(url, data=body, method="POST")
        with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310 - explicit opt-in client
            payload = response.read().decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("Telegram API response was not a JSON object")
        return data


def capture_webhook_info(bot_token: str, *, client: WebhookClient | None = None) -> WebhookPlan:
    """Capture getWebhookInfo safely; dry-run unless client is supplied."""

    params: dict[str, Any] = {}
    if client is None:
        return WebhookPlan("getWebhookInfo", True, params)
    result = sanitize_webhook_response(client.get_webhook_info(bot_token))
    return WebhookPlan("getWebhookInfo", False, params, result=result)


def set_webhook(
    bot_token: str,
    *,
    url: str,
    client: WebhookClient | None = None,
    secret_token: str | None = None,
    drop_pending_updates: bool | None = None,
    allowed_updates: list[str] | None = None,
) -> WebhookPlan:
    """Plan or execute setWebhook without exposing token or raw secrets."""

    safe_params = {
        "url": redact_sensitive(url),
        "secret_token": REDACTED if secret_token else None,
        "drop_pending_updates": drop_pending_updates,
        "allowed_updates": list(allowed_updates) if allowed_updates is not None else None,
    }
    safe_params = {k: v for k, v in safe_params.items() if v is not None}
    if client is None:
        return WebhookPlan("setWebhook", True, safe_params)
    raw_params = {
        "url": url,
        "secret_token": secret_token,
        "drop_pending_updates": drop_pending_updates,
        "allowed_updates": json.dumps(allowed_updates) if allowed_updates is not None else None,
    }
    result = sanitize_webhook_response(client.set_webhook(bot_token, **raw_params))
    return WebhookPlan("setWebhook", False, safe_params, result=result)


def delete_webhook(
    bot_token: str,
    *,
    client: WebhookClient | None = None,
    drop_pending_updates: bool = False,
) -> WebhookPlan:
    """Plan or execute deleteWebhook safely."""

    params = {"drop_pending_updates": bool(drop_pending_updates)}
    if client is None:
        return WebhookPlan("deleteWebhook", True, params)
    result = sanitize_webhook_response(client.delete_webhook(bot_token, **params))
    return WebhookPlan("deleteWebhook", False, params, result=result)


def restore_webhook(
    bot_token: str,
    *,
    url: str,
    client: WebhookClient | None = None,
    secret_token: str | None = None,
    drop_pending_updates: bool | None = None,
    allowed_updates: list[str] | None = None,
) -> WebhookPlan:
    """Plan or execute a setWebhook restoration using caller-supplied values."""

    plan = set_webhook(
        bot_token,
        url=url,
        client=client,
        secret_token=secret_token,
        drop_pending_updates=drop_pending_updates,
        allowed_updates=allowed_updates,
    )
    return WebhookPlan("restoreWebhook", plan.dry_run, plan.params, result=plan.result)


def sanitize_webhook_response(value: Any) -> dict[str, Any]:
    """Return a privacy-preserving JSON-serializable response summary."""

    sanitized = _sanitize(value)
    if isinstance(sanitized, dict):
        return sanitized
    return {"value": sanitized}


def redact_sensitive(value: object) -> str:
    """Redact token-like strings and sensitive URL material."""

    if value is None:
        return ""
    text = _TOKEN_RE.sub(REDACTED, str(value))
    text = _BOT_PATH_RE.sub(f"/bot{REDACTED}", text)
    parsed = urllib.parse.urlsplit(text)
    if parsed.scheme and parsed.netloc:
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        safe_query = []
        for key, val in query:
            safe_query.append((key, REDACTED if _SECRET_WORD_RE.search(key) or val else val))
        text = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, _BOT_PATH_RE.sub(f"/bot{REDACTED}", parsed.path), urllib.parse.urlencode(safe_query), "")
        )
        text = text.replace(urllib.parse.quote(REDACTED), REDACTED)
    return text


def _sanitize(value: Any, key: str | None = None) -> Any:
    if key is not None and (_SECRET_WORD_RE.search(key) or key in _SENSITIVE_WEBHOOK_KEYS):
        if key == "url":
            return redact_sensitive(value)
        return REDACTED if value not in (None, "") else value
    if isinstance(value, Mapping):
        return {str(k): _sanitize(v, str(k)) for k, v in value.items() if not _drop_key(str(k))}
    if isinstance(value, list):
        return [_sanitize(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize(v) for v in value]
    if isinstance(value, str):
        return redact_sensitive(value)
    return value


def _drop_key(key: str) -> bool:
    lowered = key.lower()
    return lowered in {"env", "argv", "chat_id", "user_id", "thread_id", "payload_json", "text"}
