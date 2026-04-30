import pytest

from gateway.commands import GatewayCommand, GatewayCommandPlan


def test_gateway_command_defaults_to_dry_run():
    command = GatewayCommand(action="dispatch_spool_item", profile="naval")

    assert command.dry_run is True
    assert command.requires_approval is False


@pytest.mark.parametrize("action", ["wake_worker", "start_worker", "stop_worker", "restart_worker", "probe_health"])
def test_high_risk_actions_require_approval(action):
    command = GatewayCommand(action=action, profile="naval")

    assert command.dry_run is True
    assert command.requires_approval is True


@pytest.mark.parametrize("action", ["wake_worker", "start_worker", "stop_worker"])
def test_high_risk_actions_reject_disabled_approval(action):
    with pytest.raises(ValueError):
        GatewayCommand(action=action, profile="naval", requires_approval=False)


@pytest.mark.parametrize("action", ["wake_worker", "start_worker", "stop_worker", "restart_worker", "probe_health"])
def test_high_risk_actions_reject_non_dry_run(action):
    with pytest.raises(ValueError):
        GatewayCommand(action=action, profile="naval", dry_run=False)


def test_gateway_command_to_dict_excludes_raw_payload_and_secret_metadata():
    command = GatewayCommand(
        action="dispatch_spool_item",
        profile="naval",
        item_id=7,
        route="naval",
        metadata={
            "platform": "telegram",
            "update_id": "123",
            "payload_json": {"message": {"text": "PRIVATE MESSAGE SENTINEL"}},
            "raw_payload": "PRIVATE MESSAGE SENTINEL",
            "token": "dummy-token-sentinel",
            "secret": "dummy-secret-sentinel",
        },
    )

    data = command.to_dict()
    rendered = str(data)
    assert data["dry_run"] is True
    assert data["item_id"] == 7
    assert "payload_json" not in rendered
    assert "raw_payload" not in rendered
    assert "PRIVATE MESSAGE SENTINEL" not in rendered
    assert "dummy-token-sentinel" not in rendered
    assert "dummy-secret-sentinel" not in rendered


def test_gateway_command_plan_to_dict_is_stable_and_dry_run():
    plan = GatewayCommandPlan(
        profile="naval",
        commands=[GatewayCommand(action="noop", profile="naval", requires_approval=False)],
        notes=["idle"],
    )

    assert plan.to_dict() == {
        "profile": "naval",
        "commands": [
            {
                "action": "noop",
                "profile": "naval",
                "reason": "unspecified",
                "dry_run": True,
                "requires_approval": False,
            }
        ],
        "safe_to_execute": True,
        "dry_run": True,
        "notes": ["idle"],
    }
