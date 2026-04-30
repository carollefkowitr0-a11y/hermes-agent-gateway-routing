from gateway import webhook_helper as wh


BOT_TOKEN = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
SECRET = "super-secret-value"
WEBHOOK_URL = f"https://example.test/hook?token={BOT_TOKEN}&secret={SECRET}"


class FakeClient:
    def __init__(self):
        self.calls = []

    def get_webhook_info(self, bot_token):
        self.calls.append(("get", bot_token))
        return {"ok": True, "result": {"url": WEBHOOK_URL, "chat_id": "123", "last_error_message": f"bad {BOT_TOKEN}"}}

    def set_webhook(self, bot_token, **params):
        self.calls.append(("set", bot_token, params))
        return {"ok": True, "description": f"set {BOT_TOKEN}", "result": True}

    def delete_webhook(self, bot_token, **params):
        self.calls.append(("delete", bot_token, params))
        return {"ok": True, "result": True, "secret_token": SECRET}


def _dump(plan):
    return str(plan.as_dict())


def test_redacts_token_like_values_and_sensitive_url_material():
    redacted = wh.redact_sensitive(WEBHOOK_URL)

    assert BOT_TOKEN not in redacted
    assert SECRET not in redacted
    assert wh.REDACTED in redacted


def test_default_helpers_are_dry_run_and_do_not_call_network_or_expose_raw_secret():
    set_plan = wh.set_webhook(BOT_TOKEN, url=WEBHOOK_URL, secret_token=SECRET)
    delete_plan = wh.delete_webhook(BOT_TOKEN)
    capture_plan = wh.capture_webhook_info(BOT_TOKEN)
    restore_plan = wh.restore_webhook(BOT_TOKEN, url=WEBHOOK_URL, secret_token=SECRET)

    for plan in (set_plan, delete_plan, capture_plan, restore_plan):
        assert plan.dry_run is True
        text = _dump(plan)
        assert BOT_TOKEN not in text
        assert SECRET not in text


def test_mock_client_supports_capture_set_delete_restore_without_leaking_summary():
    client = FakeClient()

    capture = wh.capture_webhook_info(BOT_TOKEN, client=client)
    set_plan = wh.set_webhook(BOT_TOKEN, url=WEBHOOK_URL, client=client, secret_token=SECRET)
    delete_plan = wh.delete_webhook(BOT_TOKEN, client=client, drop_pending_updates=True)
    restore = wh.restore_webhook(BOT_TOKEN, url=WEBHOOK_URL, client=client, secret_token=SECRET)

    assert [call[0] for call in client.calls] == ["get", "set", "delete", "set"]
    assert all(plan.dry_run is False for plan in (capture, set_plan, delete_plan, restore))
    assert restore.operation == "restoreWebhook"
    for plan in (capture, set_plan, delete_plan, restore):
        text = _dump(plan)
        assert BOT_TOKEN not in text
        assert SECRET not in text
        assert "chat_id" not in text


def test_sanitize_response_drops_private_ids_payload_text_env_and_argv():
    summary = wh.sanitize_webhook_response(
        {
            "payload_json": "raw",
            "text": "hello",
            "env": {"TOKEN": BOT_TOKEN},
            "argv": [BOT_TOKEN],
            "result": {"user_id": 42, "thread_id": 99, "url": WEBHOOK_URL, "pending_update_count": 1},
        }
    )

    text = str(summary)
    assert "payload_json" not in text
    assert "hello" not in text
    assert "env" not in text
    assert "argv" not in text
    assert "user_id" not in text
    assert "thread_id" not in text
    assert BOT_TOKEN not in text
    assert SECRET not in text
    assert summary["result"]["pending_update_count"] == 1
