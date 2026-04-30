import pytest

from gateway import ingress_runner as runner


def test_parser_defaults_to_loopback_bind_and_expected_options():
    config = runner.parse_args([])

    assert config.host == "127.0.0.1"
    assert config.port == runner.DEFAULT_PORT
    assert config.profile == "naval"
    assert config.spool_db is None
    assert config.allow_public_bind is False


def test_public_bind_is_refused_unless_explicitly_allowed():
    config = runner.parse_args(["--host", "0.0.0.0"])

    with pytest.raises(ValueError, match="public bind"):
        runner.validate_config(config)

    allowed = runner.parse_args(["--host", "0.0.0.0", "--allow-public-bind"])
    runner.validate_config(allowed)


def test_non_loopback_host_is_treated_as_public():
    assert runner.is_public_bind("127.0.0.1") is False
    assert runner.is_public_bind("localhost") is False
    assert runner.is_public_bind("::1") is False
    assert runner.is_public_bind("0.0.0.0") is True
    assert runner.is_public_bind("192.168.1.10") is True


def test_build_app_initializes_configured_spool_without_starting_daemon(tmp_path):
    db = tmp_path / "spool.db"
    config = runner.parse_args(["--spool-db", str(db), "--port", "8888"])

    app = runner.build_app(config)

    assert callable(app)
    assert db.exists()


def test_invalid_port_is_rejected():
    with pytest.raises(ValueError, match="port"):
        runner.validate_config(runner.parse_args(["--port", "0"]))
