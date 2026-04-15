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
    assert "workflow.pressure.superambient_precharge_enabled" in paths
    assert "workflow.pressure.superambient_trigger_margin_hpa" in paths
    assert "workflow.pressure.superambient_precharge_margin_hpa" in paths
    assert "workflow.pressure.superambient_precharge_timeout_s" in paths
    assert "workflow.pressure.co2_post_isolation_diagnostic_enabled" in paths
    assert "workflow.pressure.co2_post_isolation_window_s" in paths
    assert "workflow.pressure.postseal_same_gas_dead_volume_purge_enabled" in paths
    assert "workflow.pressure.post_isolation_fast_capture_enabled" in paths
    assert "workflow.pressure.post_isolation_fast_capture_allow_early_sample" in paths
    assert "workflow.pressure.post_isolation_fast_capture_min_s" in paths
    assert "workflow.pressure.post_isolation_fast_capture_fallback_to_extended_diag" in paths
    assert "workflow.pressure.post_isolation_extended_diag_window_s" in paths
    assert "workflow.stability.co2_route.preseal_soak_s" in paths
    assert "workflow.stability.co2_route.first_point_preseal_soak_s" in paths
    assert "workflow.stability.co2_route.post_h2o_zero_ppm_soak_s" in paths
    assert "workflow.sensor_read_retry.retries" in paths


def test_get_workflow_tunable_parameters_returns_catalog_tuple() -> None:
    specs = get_workflow_tunable_parameters()

    assert isinstance(specs, tuple)
    assert specs == WORKFLOW_TUNABLE_PARAMETERS
    assert any(spec.group == "pressure" for spec in specs)


def test_post_h2o_zero_flush_tuning_default_matches_runtime_default() -> None:
    spec = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.stability.co2_route.post_h2o_zero_ppm_soak_s"
    )

    assert spec.default == 900.0


def test_post_stable_sample_delay_tuning_defaults_match_runtime_defaults() -> None:
    general = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.post_stable_sample_delay_s"
    )
    co2 = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.co2_post_stable_sample_delay_s"
    )

    assert general.default == 10.0
    assert co2.default == 10.0


def test_superambient_precharge_tuning_defaults_match_runtime_defaults() -> None:
    enabled = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.superambient_precharge_enabled"
    )
    trigger_margin = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.superambient_trigger_margin_hpa"
    )
    precharge_margin = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.superambient_precharge_margin_hpa"
    )
    timeout_s = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.superambient_precharge_timeout_s"
    )

    assert enabled.default is True
    assert trigger_margin.default == 5.0
    assert precharge_margin.default == 8.0
    assert timeout_s.default == 30.0


def test_post_isolation_diagnostic_tuning_defaults_match_runtime_defaults() -> None:
    enabled = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.co2_post_isolation_diagnostic_enabled"
    )
    window_s = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.co2_post_isolation_window_s"
    )
    purge = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.postseal_same_gas_dead_volume_purge_enabled"
    )

    assert enabled.default is True
    assert window_s.default == 10.0
    assert purge.default is False


def test_fast_capture_tuning_defaults_match_runtime_defaults() -> None:
    enabled = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.post_isolation_fast_capture_enabled"
    )
    allow_early = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.post_isolation_fast_capture_allow_early_sample"
    )
    minimum = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.post_isolation_fast_capture_min_s"
    )
    extended = next(
        item
        for item in WORKFLOW_TUNABLE_PARAMETERS
        if item.path == "workflow.pressure.post_isolation_extended_diag_window_s"
    )

    assert enabled.default is True
    assert allow_early.default is False
    assert minimum.default == 5.0
    assert extended.default == 20.0
