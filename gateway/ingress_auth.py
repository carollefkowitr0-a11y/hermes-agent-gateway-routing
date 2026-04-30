"""Dummy secret validation helpers for local shadow webhook ingress tests."""

from __future__ import annotations

import hmac
import re

DUMMY_WEBHOOK_SECRET = "dummy-m3-shadow-webhook-secret"
REDACTED = "[REDACTED]"
_SECRET_LIKE_RE = re.compile(r"(?i)\b(?:secret|token)[-_A-Za-z0-9.:]*")


def validate_dummy_secret(secret: str | None) -> bool:
    """Return True only for the fixed dummy/test webhook secret."""
    if not isinstance(secret, str):
        return False
    return hmac.compare_digest(secret, DUMMY_WEBHOOK_SECRET)


def redact_secret(value: object) -> str:
    """Redact dummy secrets and obvious secret/token fragments from safe text."""
    if value is None:
        return ""
    text = str(value).replace(DUMMY_WEBHOOK_SECRET, REDACTED)
    return _SECRET_LIKE_RE.sub(REDACTED, text)
