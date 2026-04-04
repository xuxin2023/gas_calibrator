from gas_calibrator.workflow.tuning import (
    WORKFLOW_TUNABLE_PARAMETERS,
    get_workflow_tunable_parameters,
    workflow_param,
)


def test_workflow_param_reads_nested_value_and_falls_back_default() -> None:
    cfg = {"workflow": {"pressure": {"stabilize_timeout_s": 180}}}

    assert workflow_param(cfg, "workflow.pressure.stabilize_timeout_s", 120) == 180
    assert workflow_param(cfg, "workflow.pressure.restabilize_retries", 2) == 2


def test_workflow_tunable_parameters_have_unique_paths() -> None:
    paths = [spec.path for spec in WORKFLOW_TUNABLE_PARAMETERS]

    assert len(paths) == len(set(paths))
    assert "workflow.pressure.stabilize_timeout_s" in paths
    assert "workflow.pressure.co2_post_h2o_vent_off_wait_s" in paths
    assert "workflow.stability.co2_route.preseal_soak_s" in paths
    assert "workflow.stability.co2_route.first_point_preseal_soak_s" in paths
    assert "workflow.stability.co2_route.post_h2o_zero_ppm_soak_s" in paths
    assert "workflow.sensor_read_retry.retries" in paths


def test_get_workflow_tunable_parameters_returns_catalog_tuple() -> None:
    specs = get_workflow_tunable_parameters()

    assert isinstance(specs, tuple)
    assert specs == WORKFLOW_TUNABLE_PARAMETERS
    assert any(spec.group == "pressure" for spec in specs)
