from gas_calibrator.v1_5.parameters.governance import (
    ParameterChangeRequest,
    build_parameter_audit_event,
    build_parameter_surface,
    classify_parameter,
    validate_parameter_change,
)


def test_parameter_surface_hides_high_risk_by_default():
    surface = build_parameter_surface()
    names = {row["name"] for row in surface["parameters"]}

    assert surface["sidecar_only"] is True
    assert surface["device_write_enabled"] is False
    assert surface["high_risk_parameters_hidden_by_default"] is True
    assert "SENCO9" not in names
    assert classify_parameter("SENCO9").level == "E"
    assert classify_parameter("co2_stability_slope_max").level == "B"


def test_average_parameter_labels_match_analyzer_manual_channels():
    assert classify_parameter("AVERAGE1").label == "H2O 平均/滤波参数"
    assert classify_parameter("AVERAGE2").label == "CO2 平均/滤波参数"


def test_operator_can_change_run_parameter_when_not_running_with_audit():
    request = ParameterChangeRequest(
        name="sample_window_s",
        old_value=30,
        new_value=60,
        actor="operator-a",
        role="operator",
        reason="increase sample window for formal plan",
        run_state="planning",
    )
    decision = validate_parameter_change(request)
    event = build_parameter_audit_event(request, decision)

    assert decision.status == "pass"
    assert decision.level == "A"
    assert event["audit_hash"]
    assert event["old_value"] == 30
    assert event["new_value"] == 60


def test_qc_parameter_is_locked_during_sampling_and_requires_approval():
    request = ParameterChangeRequest(
        name="pressure_delta_hpa_max",
        old_value=1.0,
        new_value=2.0,
        actor="engineer-a",
        role="engineer",
        reason="temporary relaxation",
        run_state="sample_window",
    )
    decision = validate_parameter_change(request)

    assert decision.status == "fail"
    assert decision.level == "B"
    assert "critical_parameter_locked_during_run" in decision.reasons
    assert "approval_required" in decision.reasons


def test_high_risk_parameter_is_blocked_even_for_admin_in_v0():
    request = ParameterChangeRequest(
        name="SENCO9",
        old_value="old",
        new_value="new",
        actor="admin-a",
        role="admin",
        reason="pressure calibration trial",
        run_state="planning",
        approved_by="reviewer-a",
        readback_value="new",
        rollback_plan="restore old SENCO9",
    )
    decision = validate_parameter_change(request)

    assert decision.status == "fail"
    assert decision.level == "E"
    assert decision.high_risk is True
    assert "high_risk_parameter_hidden_by_default" in decision.reasons
    assert "device_write_not_enabled_in_v1_5_parameter_ui_v0" in decision.reasons
