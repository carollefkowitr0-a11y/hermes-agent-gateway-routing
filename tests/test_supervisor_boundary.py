from gateway.supervisor_boundary import build_dry_run_wakeup_event, dry_run_wakeup_events


def test_build_dry_run_wakeup_event_describes_action_without_side_effect_command():
    event = build_dry_run_wakeup_event(target_profile="naval", reason="queued_update", queued_count=2)

    assert event["dry_run"] is True
    assert event["target_profile"] == "naval"
    assert event["reason"] == "queued_update"
    assert event["queued_count"] == 2
    assert "command" not in event
    assert "systemctl" not in str(event).lower()


def test_dry_run_wakeup_events_generates_one_event_per_profile():
    events = dry_run_wakeup_events(["naval", "ops"], reason="manual_test")

    assert [event["target_profile"] for event in events] == ["naval", "ops"]
    assert all(event["dry_run"] is True for event in events)
