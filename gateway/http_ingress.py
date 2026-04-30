"""Local WSGI HTTP ingress for shadow Telegram webhook tests."""

from __future__ import annotations

import json
import sqlite3
from http import HTTPStatus
from typing import Callable, Iterable

from gateway.ingress_auth import validate_dummy_secret
from gateway.shadow_ingress import enqueue_shadow_webhook
from gateway.spool import GatewaySpool

HEADER_SECRET = "HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN"
DEFAULT_MAX_BODY_BYTES = 1024 * 1024

StartResponse = Callable[[str, list[tuple[str, str]]], None]
WsgiApp = Callable[[dict, StartResponse], Iterable[bytes]]


def create_app(*, spool: GatewaySpool | None = None, max_body_bytes: int = DEFAULT_MAX_BODY_BYTES) -> WsgiApp:
    """Create a testable local WSGI app; no external network or Telegram calls."""
    if spool is None:
        spool = GatewaySpool()
        spool.initialize()

    def app(environ: dict, start_response: StartResponse) -> Iterable[bytes]:
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "")

        if method == "GET" and path == "/healthz":
            return _json(start_response, HTTPStatus.OK, {"ok": True})

        if method == "GET" and path == "/readyz":
            return _readyz(start_response, spool)

        if method == "POST" and path == "/telegram/naval/shadow":
            return _handle_shadow_post(environ, start_response, spool, max_body_bytes)

        return _json(start_response, HTTPStatus.NOT_FOUND, {"ok": False, "reason": "not_found"})

    return app


def _readyz(start_response: StartResponse, spool: GatewaySpool) -> Iterable[bytes]:
    try:
        spool.initialize()
        with sqlite3.connect(spool.db_path) as conn:
            conn.execute("SELECT 1 FROM gateway_spool LIMIT 1").fetchone()
    except Exception:
        return _json(start_response, HTTPStatus.SERVICE_UNAVAILABLE, {"ready": False})
    return _json(start_response, HTTPStatus.OK, {"ready": True})


def _handle_shadow_post(
    environ: dict,
    start_response: StartResponse,
    spool: GatewaySpool,
    max_body_bytes: int,
) -> Iterable[bytes]:
    if not validate_dummy_secret(environ.get(HEADER_SECRET)):
        return _json(start_response, HTTPStatus.UNAUTHORIZED, {"accepted": False, "reason": "unauthorized"})

    content_type = environ.get("CONTENT_TYPE", "")
    if content_type.split(";", 1)[0].strip().lower() != "application/json":
        return _json(start_response, HTTPStatus.UNSUPPORTED_MEDIA_TYPE, {"accepted": False, "reason": "unsupported_media_type"})

    try:
        content_length = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError:
        return _json(start_response, HTTPStatus.BAD_REQUEST, {"accepted": False, "reason": "invalid_content_length"})

    if content_length > max_body_bytes:
        return _json(start_response, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"accepted": False, "reason": "body_too_large"})

    body = environ["wsgi.input"].read(content_length)
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _json(start_response, HTTPStatus.BAD_REQUEST, {"accepted": False, "reason": "invalid_json"})

    if not isinstance(payload, dict):
        return _json(start_response, HTTPStatus.BAD_REQUEST, {"accepted": False, "reason": "invalid_json"})

    result = enqueue_shadow_webhook(payload, route="naval", target_profile="naval", spool=spool)
    return _json(start_response, HTTPStatus(result.status), result.body)


def _json(start_response: StartResponse, status: HTTPStatus, body: dict) -> Iterable[bytes]:
    data = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    start_response(
        f"{status.value} {status.phrase}",
        [("Content-Type", "application/json"), ("Content-Length", str(len(data)))],
    )
    return [data]
