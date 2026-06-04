from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_entrypoint_inventory import main as export_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
    discover_v1_5_entrypoints,
    summarize_entrypoints,
)


pytestmark = pytest.mark.v1_5_formal_gate


def test_entrypoint_classifier_separates_formal_runner_diagnostic_and_write(tmp_path: Path) -> None:
    root = tmp_path
    formal = root / "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"
    diagnostic = root / "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    write = root / "src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py"
    for path in (formal, diagnostic, write):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    formal_entry = classify_v1_5_entrypoint(formal, root=root)
    diagnostic_entry = classify_v1_5_entrypoint(diagnostic, root=root)
    write_entry = classify_v1_5_entrypoint(write, root=root)

    assert formal_entry.category == "formal_runner"
    assert formal_entry.controls_routes is True
    assert diagnostic_entry.category == "diagnostic_only"
    assert diagnostic_entry.formal_status == "diagnostic_only"
    assert write_entry.category == "controlled_write"
    assert write_entry.writes_coefficients is True


def test_entrypoint_classifier_keeps_offline_sidecars_out_of_real_com_risk(tmp_path: Path) -> None:
    root = tmp_path
    sidecar = root / "src/gas_calibrator/tools/run_v1_5_formal_evidence_sidecar.py"
    offline_chain = root / "src/gas_calibrator/tools/run_v1_5_formal_offline_review_chain.py"
    full_chain = root / "src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py"
    for path in (sidecar, offline_chain, full_chain):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    sidecar_entry = classify_v1_5_entrypoint(sidecar, root=root)
    offline_entry = classify_v1_5_entrypoint(offline_chain, root=root)
    full_chain_entry = classify_v1_5_entrypoint(full_chain, root=root)

    assert sidecar_entry.category == "formal_review_evidence"
    assert sidecar_entry.opens_com_ports is False
    assert offline_entry.category == "formal_review_evidence"
    assert offline_entry.risk_level == "offline"
    assert full_chain_entry.category == "full_flow_orchestration"
    assert full_chain_entry.controls_routes is False


def test_entrypoint_classifier_treats_getco_snapshot_as_formal_precheck(tmp_path: Path) -> None:
    root = tmp_path
    getco = root / "src/gas_calibrator/tools/probe_v1_5_getco_component_snapshot.py"
    dynamic_probe = root / "src/gas_calibrator/tools/probe_v1_5_open_flow_dynamic_pressure.py"
    for path in (getco, dynamic_probe):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    getco_entry = classify_v1_5_entrypoint(getco, root=root)
    dynamic_entry = classify_v1_5_entrypoint(dynamic_probe, root=root)

    assert getco_entry.category == "formal_review_evidence"
    assert getco_entry.formal_status == "formal_support"
    assert getco_entry.opens_com_ports is True
    assert dynamic_entry.category == "diagnostic_only"


def test_entrypoint_discovery_finds_v1_5_tools_libraries_and_tests(tmp_path: Path) -> None:
    paths = [
        "src/gas_calibrator/tools/export_v1_5_formal_readiness.py",
        "src/gas_calibrator/tools/run_v1_5_h2o_senco24_controlled_write.py",
        "src/gas_calibrator/v1_5/orchestration/full_flow.py",
        "src/gas_calibrator/storage/v1_5_evidence/repository.py",
        "tests/test_v1_5_formal_readiness.py",
        "tests/test_unrelated.py",
    ]
    for item in paths:
        path = tmp_path / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    entries = discover_v1_5_entrypoints(tmp_path)
    names = {entry.name for entry in entries}
    summary = summarize_entrypoints(entries)

    assert "export_v1_5_formal_readiness" in names
    assert "run_v1_5_h2o_senco24_controlled_write" in names
    assert "full_flow" in names
    assert "repository" in names
    assert "test_v1_5_formal_readiness" in names
    assert "test_unrelated" not in names
    assert summary["controlled_write"] == 1
    assert summary["test_gate"] == 1


def test_export_entrypoint_inventory_writes_review_artifacts(tmp_path: Path) -> None:
    tool = tmp_path / "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
    test_file = tmp_path / "tests/test_v1_5_formal_open_flow_sampling_runner.py"
    for path in (tool, test_file):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    out = tmp_path / "out"
    rc = export_main(["--repo-root", str(tmp_path), "--output-dir", str(out)])

    assert rc == 0
    assert (out / "v1_5_entrypoint_inventory.json").exists()
    assert (out / "v1_5_entrypoint_inventory.csv").exists()
    md = (out / "v1_5_formal_entrypoints.md").read_text(encoding="utf-8")
    assert "V1.5 formal entrypoint inventory" in md
    assert "`formal_runner`" in md
