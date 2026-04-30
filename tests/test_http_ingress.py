import json
import urllib.error
import urllib.request
from threading import Thread
from wsgiref.simple_server import make_server

import pytest

from gateway.http_ingress import create_app
from gateway.ingress_auth import DUMMY_WEBHOOK_SECRET
from gateway.spool import GatewaySpool


@pytest.fixture
def spool(tmp_path):
    spool = GatewaySpool(tmp_path / "spool.db")
    spool.initialize()
    return spool


def _call_app(app, method, path, body=b"", headers=None):
    captured = {}

    def start_response(status, response_headers):
        captured["status"] = status
        captured["headers"] = response_headers

    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "SERVER_NAME": "127.0.0.1",
        "SERVER_PORT": "0",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": __import__("io").BytesIO(body),
        "wsgi.errors": __import__("io").StringIO(),
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "CONTENT_LENGTH": str(len(body)),
    }
    for key, value in (headers or {}).items():
        normalized = key.upper().replace("-", "_")
        if normalized == "CONTENT_TYPE":
            environ["CONTENT_TYPE"] = value
        else:
            environ[f"HTTP_{normalized}"] = value
    response_body = b"".join(app(environ, start_response))
    return int(captured["status"].split()[0]), dict(captured["headers"]), response_body


def _json_response(*args, **kwargs):
    status, headers, body = _call_app(*args, **kwargs)
    return status, headers, json.loads(body.decode("utf-8"))


def _secret_headers():
    return {
        "Content-Type": "application/json",
        "X-Telegram-Bot-Api-Secret-Token": DUMMY_WEBHOOK_SECRET,
    }


def test_healthz_returns_ok_without_external_network(spool):
    app = create_app(spool=spool)

    status, headers, body = _json_response(app, "GET", "/healthz")

    assert status == 200
    assert body == {"ok": True}


def test_readyz_confirms_local_spool_database_is_openable(spool):
    app = create_app(spool=spool)

    status, headers, body = _json_response(app, "GET", "/readyz")

    assert status == 200
    assert body["ready"] is True


def test_shadow_post_validates_secret_and_enqueues_without_echoing_sensitive_content(spool):
    app = create_app(spool=spool)
    payload = {"update_id": 123, "message": {"text": "do not echo"}}

    status, headers, body = _json_response(
        app,
        "POST",
        "/telegram/naval/shadow",
        body=json.dumps(payload).encode("utf-8"),
        headers=_secret_headers(),
    )

    assert status == 202
    assert body["accepted"] is True
    assert "do not echo" not in json.dumps(body)
    assert DUMMY_WEBHOOK_SECRET not in json.dumps(body)
    row = spool.get_next_queued(target_profile="naval")
    assert row is not None
    assert row["route"] == "naval"
    assert row["target_profile"] == "naval"


@pytest.mark.parametrize("headers", [{"Content-Type": "application/json"}, {"Content-Type": "application/json", "X-Telegram-Bot-Api-Secret-Token": "wrong"}])
def test_shadow_post_rejects_missing_or_wrong_secret_without_enqueue(spool, headers):
    app = create_app(spool=spool)

    status, _, body = _json_response(
        app,
        "POST",
        "/telegram/naval/shadow",
        body=b'{"update_id": 123}',
        headers=headers,
    )

    assert status == 401
    assert body["accepted"] is False
    assert spool.count_by_state() == {}


def test_shadow_post_rejects_wrong_content_type_body_size_and_invalid_json(spool):
    app = create_app(spool=spool, max_body_bytes=10)

    wrong_type = _json_response(app, "POST", "/telegram/naval/shadow", body=b"{}", headers={"X-Telegram-Bot-Api-Secret-Token": DUMMY_WEBHOOK_SECRET, "Content-Type": "text/plain"})
    too_large = _json_response(app, "POST", "/telegram/naval/shadow", body=b'{"update_id": 1}', headers=_secret_headers())
    invalid_json = _json_response(create_app(spool=spool), "POST", "/telegram/naval/shadow", body=b"{", headers=_secret_headers())

    assert wrong_type[0] == 415
    assert too_large[0] == 413
    assert invalid_json[0] == 400
    assert spool.count_by_state() == {}


def test_shadow_post_over_real_loopback_socket_when_needed(spool):
    app = create_app(spool=spool)
    httpd = make_server("127.0.0.1", 0, app)
    thread = Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{httpd.server_port}/telegram/naval/shadow"
        request = urllib.request.Request(
            url,
            data=b'{"update_id": 999}',
            method="POST",
            headers=_secret_headers(),
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 202
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)

    assert spool.get_next_queued(target_profile="naval")["update_id"] == "999"
