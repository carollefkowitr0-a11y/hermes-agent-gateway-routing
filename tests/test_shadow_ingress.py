import sqlite3

import pytest

from gateway.shadow_ingress import enqueue_shadow_webhook
from gateway.spool import GatewaySpool


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / "hermes-home"
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


@pytest.fixture
def spool(hermes_home):
    spool = GatewaySpool()
    spool.initialize()
    return spool


def _row_count(spool):
    with sqlite3.connect(spool.db_path) as conn:
        return conn.execute("SELECT COUNT(*) FROM gateway_spool").fetchone()[0]


def test_valid_telegram_like_payload_enqueues_naval_row(spool):
    payload = {"update_id": 123, "message": {"text": "hello naval"}}

    result = enqueue_shadow_webhook(payload, spool=spool)

    assert result.status == 202
    assert result.body["accepted"] is True
    assert result.body["duplicate"] is False
    row = spool.get_next_queued(target_profile="naval")
    assert row is not None
    assert row["route"] == "naval"
    assert row["target_profile"] == "naval"
    assert row["update_id"] == "123"
    assert spool.count_by_state() == {"queued": 1}


def test_missing_update_id_is_rejected_without_enqueue(spool):
    result = enqueue_shadow_webhook({"message": {"text": "missing"}}, spool=spool)

    assert result.status == 400
    assert result.body["accepted"] is False
    assert _row_count(spool) == 0


def test_duplicate_payload_returns_duplicate_without_second_row(spool):
    payload = {"update_id": 123, "message": {"text": "hello"}}

    first = enqueue_shadow_webhook(payload, spool=spool)
    duplicate = enqueue_shadow_webhook(payload, spool=spool)

    assert first.status == 202
    assert duplicate.status == 202
    assert duplicate.body["accepted"] is True
    assert duplicate.body["duplicate"] is True
    assert spool.count_by_state() == {"queued": 1}


@pytest.mark.parametrize("profile_key", ["target_profile", "profile"])
def test_payload_profile_override_is_rejected_without_enqueue(spool, profile_key):
    payload = {"update_id": 123, "message": {"text": "bad"}, profile_key: "ops"}

    result = enqueue_shadow_webhook(payload, spool=spool)

    assert result.status == 403
    assert result.body["accepted"] is False
    assert _row_count(spool) == 0


def test_non_allowed_route_is_rejected_without_enqueue(spool):
    payload = {"update_id": 123, "message": {"text": "bad route"}}

    result = enqueue_shadow_webhook(payload, route="ops", spool=spool)

    assert result.status == 403
    assert result.body["accepted"] is False
    assert _row_count(spool) == 0


def test_non_naval_target_profile_parameter_is_rejected_without_enqueue(spool):
    payload = {"update_id": 123, "message": {"text": "bad target"}}

    result = enqueue_shadow_webhook(payload, target_profile="ops", spool=spool)

    assert result.status == 403
    assert result.body["accepted"] is False
    assert result.body["reason"] == "target_profile_not_allowed"
    assert _row_count(spool) == 0


def test_default_spool_initializes_gateway_spool_db(hermes_home):
    payload = {"update_id": 123, "message": {"text": "default spool"}}

    result = enqueue_shadow_webhook(payload)

    assert result.status == 202
    spool = GatewaySpool()
    row = spool.get_next_queued(target_profile="naval")
    assert row is not None
    assert row["route"] == "naval"
    assert row["target_profile"] == "naval"


def test_response_body_does_not_echo_message_text_or_secret_like_fields(spool):
    payload = {
        "update_id": 123,
        "message": {"text": "do not echo this text"},
        "authorization": "secret-token",
        "x-telegram-bot-api-secret-token": "secret-header",
    }

    result = enqueue_shadow_webhook(payload, spool=spool)

    body_text = str(result.body)
    assert "do not echo this text" not in body_text
    assert "secret-token" not in body_text
    assert "secret-header" not in body_text
    assert "authorization" not in body_text.lower()


def test_extracts_telegram_metadata_and_event_type(spool):
    payload = {
        "update_id": 456,
        "message": {
            "message_thread_id": 789,
            "chat": {"id": -100123},
            "from": {"id": 42},
            "text": "metadata",
        },
    }

    enqueue_shadow_webhook(payload, spool=spool)

    row = spool.get_next_queued(target_profile="naval")
    assert row is not None
    assert row["chat_id"] == "-100123"
    assert row["thread_id"] == "789"
    assert row["user_id"] == "42"
    assert row["event_type"] == "message"
