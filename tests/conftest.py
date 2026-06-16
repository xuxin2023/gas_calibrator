from pathlib import Path
import importlib
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


V1_5_FORMAL_GATE_FILES = {
    "test_v1_5_advanced_qc_exporter.py",
    "test_v1_5_calibration_reports.py",
    "test_v1_5_canonical_evidence_package.py",
    "test_v1_5_evidence_registry.py",
    "test_v1_5_formal_archive_closure.py",
    "test_v1_5_formal_calibration_package.py",
    "test_v1_5_formal_contracts_preflight.py",
    "test_v1_5_formal_evidence_run.py",
    "test_v1_5_formal_offline_review_chain.py",
    "test_v1_5_formal_open_flow.py",
    "test_v1_5_formal_open_flow_artifacts.py",
    "test_v1_5_formal_readiness.py",
    "test_v1_5_formal_run_package.py",
    "test_v1_5_formal_workbench.py",
    "test_v1_5_no_write_guard.py",
    "test_v1_5_operation_console.py",
    "test_v1_5_parameter_governance.py",
    "test_v1_5_pressure_channel_validation.py",
    "test_v1_5_qc_advanced.py",
    "test_v1_5_review_surface.py",
}

V1_5_DIAGNOSTIC_GATE_FILES = {
    "test_v1_5_controlled_outp_seal_transition.py",
    "test_v1_5_dewpoint_gate_extended_hold_diagnostic.py",
    "test_v1_5_no_outp_engineering_config.py",
    "test_v1_5_no_outp_plus_sealed_sweep.py",
    "test_v1_5_no_outp_preseal_probe.py",
    "test_v1_5_no_outp_skip_tempwait_engineering_config.py",
    "test_v1_5_no_outp_transition.py",
    "test_v1_5_open_flow_dynamic_pressure_diagnostic.py",
    "test_v1_5_pace_audit_guards.py",
    "test_v1_5_pace_mode_ingress_diagnostic.py",
    "test_v1_5_pace_output_prearm.py",
    "test_v1_5_pressure_only_tuning_harness.py",
}


V1_5_FAST_SERIAL_WAIT_TEST_FILES = {
    "test_v1_5_co2_senco1_controlled_write.py",
    "test_v1_5_co2_senco13_controlled_write.py",
    "test_v1_5_co2_senco13_controlled_rollback.py",
    "test_v1_5_co2_senco5_linear_controlled_write.py",
    "test_v1_5_co2_senco5_neutral_controlled_write.py",
    "test_v1_5_h2o_senco24_controlled_write.py",
    "test_v1_5_h2o_senco6_linear_controlled_write.py",
    "test_v1_5_h2o_senco6_neutral_controlled_write.py",
    "test_v1_5_pressure_senco9_controlled_write.py",
    "test_v1_5_pressure_senco9_clear_controlled_write.py",
    "test_v1_5_temperature_senco78_neutral_controlled_write.py",
    "test_v1_5_getco9_protocol_probe.py",
    "test_v1_5_getco_component_snapshot.py",
}

V1_5_SERIAL_WAIT_MODULES = (
    "gas_calibrator.tools.run_v1_5_co2_senco1_controlled_write",
    "gas_calibrator.tools.run_v1_5_pressure_senco9_controlled_write",
    "gas_calibrator.tools.probe_v1_5_getco9_protocol",
    "gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
)


@pytest.fixture(autouse=True)
def _fast_v1_5_controlled_writer_serial_waits(request, monkeypatch):
    """Keep fragile-serial production pacing, but do not make fake unit tests sleep."""
    path = getattr(request.node, "path", None) or getattr(request.node, "fspath", None)
    filename = Path(str(path)).name
    if filename not in V1_5_FAST_SERIAL_WAIT_TEST_FILES:
        return

    for module_name in V1_5_SERIAL_WAIT_MODULES:
        module = importlib.import_module(module_name)
        monkeypatch.setattr(module, "_sleep_gap", lambda *_args, **_kwargs: None, raising=False)
        module_time = getattr(module, "time", None)
        if module_time is not None:
            monkeypatch.setattr(module_time, "sleep", lambda *_args, **_kwargs: None, raising=False)


def pytest_collection_modifyitems(config, items):
    for item in items:
        path = getattr(item, "path", None) or getattr(item, "fspath", None)
        filename = Path(str(path)).name
        if filename in V1_5_FORMAL_GATE_FILES:
            item.add_marker(pytest.mark.v1_5_formal_gate)
        if filename in V1_5_DIAGNOSTIC_GATE_FILES:
            item.add_marker(pytest.mark.v1_5_diagnostic_gate)
            item.add_marker(pytest.mark.v1_5_legacy_pressure_diagnostic)
