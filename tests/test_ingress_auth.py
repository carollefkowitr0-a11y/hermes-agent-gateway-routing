from gateway.ingress_auth import DUMMY_WEBHOOK_SECRET, redact_secret, validate_dummy_secret


def test_validate_dummy_secret_accepts_only_exact_dummy_value():
    assert validate_dummy_secret(DUMMY_WEBHOOK_SECRET) is True
    assert validate_dummy_secret(None) is False
    assert validate_dummy_secret("") is False
    assert validate_dummy_secret("wrong-secret") is False
    assert validate_dummy_secret(DUMMY_WEBHOOK_SECRET + "x") is False


def test_redact_secret_removes_secret_value_without_echoing_it():
    text = f"prefix {DUMMY_WEBHOOK_SECRET} suffix"

    redacted = redact_secret(text)

    assert DUMMY_WEBHOOK_SECRET not in redacted
    assert "[REDACTED]" in redacted
    assert redact_secret(None) == ""
