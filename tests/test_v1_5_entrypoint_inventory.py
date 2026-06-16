from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_entrypoint_inventory import main as export_main
from gas_calibrator.validation import v1_5_entrypoint_inventory as inventory_validation
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    CANONICAL_FORMAL_PATH,
    audit_v1_5_isolated_reference_integrity,
    build_v1_5_workspace_surface_rows,
    classify_v1_5_entrypoint,
    discover_v1_5_entrypoints,
    guardrailed_entrypoint_rows,
    summarize_entrypoints,
    validate_v1_5_active_surface_policy,
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


def test_entrypoint_classifier_marks_open_flow_sampling_as_canonical_worker(tmp_path: Path) -> None:
    root = tmp_path
    co2_worker = root / "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
    h2o_worker = root / "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py"
    for path in (co2_worker, h2o_worker):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    co2_entry = classify_v1_5_entrypoint(co2_worker, root=root)
    h2o_entry = classify_v1_5_entrypoint(h2o_worker, root=root)

    for entry in (co2_entry, h2o_entry):
        assert entry.category == "formal_sampling_worker"
        assert entry.formal_status == "canonical_queue_worker"
        assert entry.opens_com_ports is True
        assert entry.controls_routes is True
        assert entry.writes_coefficients is False
        assert "canonical per-point sampling worker" in entry.notes[0]


def test_entrypoint_classifier_marks_pressure_runner_and_legacy_v1_reference(tmp_path: Path) -> None:
    root = tmp_path
    pressure_runner = root / "src/gas_calibrator/tools/validate_pressure_only.py"
    legacy = root / "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"
    for path in (pressure_runner, legacy):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    pressure_entry = classify_v1_5_entrypoint(pressure_runner, root=root)
    legacy_entry = classify_v1_5_entrypoint(legacy, root=root)

    assert pressure_entry.category == "formal_pressure_no_write_runner"
    assert pressure_entry.formal_status == "formal_pressure_no_write_when_authorized"
    assert pressure_entry.opens_com_ports is True
    assert pressure_entry.writes_coefficients is False
    assert legacy_entry.category == "legacy_v1_reference"
    assert legacy_entry.formal_status == "legacy_v1_reference_only"
    assert legacy_entry.writes_coefficients is True
    assert "legacy V1 reference only" in legacy_entry.notes[0]


def test_entrypoint_classifier_keeps_offline_sidecars_out_of_real_com_risk(tmp_path: Path) -> None:
    root = tmp_path
    sidecar = root / "src/gas_calibrator/tools/run_v1_5_formal_evidence_sidecar.py"
    offline_chain = root / "src/gas_calibrator/tools/run_v1_5_formal_offline_review_chain.py"
    full_chain = root / "src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py"
    archive_closure = root / "src/gas_calibrator/tools/run_v1_5_formal_archive_closure.py"
    for path in (sidecar, offline_chain, full_chain, archive_closure):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    sidecar_entry = classify_v1_5_entrypoint(sidecar, root=root)
    offline_entry = classify_v1_5_entrypoint(offline_chain, root=root)
    full_chain_entry = classify_v1_5_entrypoint(full_chain, root=root)
    archive_entry = classify_v1_5_entrypoint(archive_closure, root=root)

    assert sidecar_entry.category == "formal_review_evidence"
    assert sidecar_entry.opens_com_ports is False
    assert offline_entry.category == "formal_review_evidence"
    assert offline_entry.risk_level == "offline"
    assert full_chain_entry.category == "full_flow_orchestration"
    assert full_chain_entry.controls_routes is False
    assert archive_entry.category == "formal_review_evidence"
    assert archive_entry.risk_level == "offline"
    assert archive_entry.opens_com_ports is False
    assert archive_entry.controls_routes is False
    assert archive_entry.notes == ("offline archive closure; does not open COM ports or control routes",)


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
    assert "subordinate initialization evidence tool" in getco_entry.notes[0]
    assert dynamic_entry.category == "diagnostic_only"


def test_entrypoint_classifier_promotes_formal_initialization_runner_as_single_owner(tmp_path: Path) -> None:
    root = tmp_path
    initializer = root / "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py"
    getco = root / "src/gas_calibrator/tools/probe_v1_5_getco_component_snapshot.py"
    for path in (initializer, getco):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    init_entry = classify_v1_5_entrypoint(initializer, root=root)
    getco_entry = classify_v1_5_entrypoint(getco, root=root)
    canonical = {row["stage"]: row["entrypoint"] for row in CANONICAL_FORMAL_PATH}

    assert canonical["01_formal_initialization"] == (
        "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py"
    )
    assert "probe_v1_5_getco_component_snapshot.py" not in canonical.values()
    assert init_entry.category == "full_flow_orchestration"
    assert init_entry.formal_status == "canonical_initialization_planner"
    assert init_entry.opens_com_ports is False
    assert init_entry.controls_routes is False
    assert init_entry.writes_coefficients is False
    assert "canonical initialization owner" in init_entry.notes[0]
    assert getco_entry.category == "formal_review_evidence"
    assert getco_entry.opens_com_ports is True


def test_entrypoint_discovery_finds_v1_5_tools_libraries_and_tests(tmp_path: Path) -> None:
    paths = [
        "src/gas_calibrator/tools/export_v1_5_formal_readiness.py",
        "src/gas_calibrator/tools/run_v1_5_h2o_senco24_controlled_write.py",
        "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py",
        "src/gas_calibrator/tools/verify_v1_5_evidence_bundle.py",
        "src/gas_calibrator/v1_5/orchestration/full_flow.py",
        "src/gas_calibrator/storage/v1_5_evidence/repository.py",
        "src/gas_calibrator/tools/validate_pressure_only.py",
        "src/gas_calibrator/tools/run_v1_online_acceptance.py",
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
    assert "run_v1_5_formal_initialization_runner" in names
    assert "verify_v1_5_evidence_bundle" in names
    assert "full_flow" in names
    assert "repository" in names
    assert "validate_pressure_only" in names
    assert "run_v1_online_acceptance" in names
    assert "test_v1_5_formal_readiness" in names
    assert "test_unrelated" not in names
    assert summary["controlled_write"] == 1
    assert summary["formal_review_evidence"] == 2
    assert summary["formal_pressure_no_write_runner"] == 1
    assert summary["full_flow_orchestration"] == 2
    assert summary["legacy_v1_reference"] == 1
    assert summary["test_gate"] == 1


def test_export_entrypoint_inventory_writes_review_artifacts(tmp_path: Path) -> None:
    tool = tmp_path / "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
    test_file = tmp_path / "tests/test_v1_5_formal_open_flow_sampling_runner.py"
    v2_file = tmp_path / "src/gas_calibrator/v2/legacy_runner.py"
    observed_config = tmp_path / "configs/site_v1_5_current_observed_6ch.json"
    for path in (tool, test_file, v2_file, observed_config):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    out = tmp_path / "out"
    rc = export_main(["--repo-root", str(tmp_path), "--output-dir", str(out)])

    assert rc == 0
    assert (out / "v1_5_entrypoint_inventory.json").exists()
    assert (out / "v1_5_entrypoint_inventory.csv").exists()
    assert (out / "v1_5_file_convergence_report.md").exists()
    assert (out / "v1_5_active_surface_report.md").exists()
    assert (out / "v1_5_isolation_reference_audit.md").exists()
    payload = (out / "v1_5_entrypoint_inventory.json").read_text(encoding="utf-8")
    assert "active_surface_boundaries" in payload
    assert "active_surface_policy" in payload
    assert "isolation_reference_audit" in payload
    assert "canonical_entrypoint_missing" in payload
    active_surface = (out / "v1_5_active_surface_report.md").read_text(encoding="utf-8-sig")
    assert "V1.5 活跃工作面与隔离清单" in active_surface
    assert "默认入口策略校验" in active_surface
    assert "legacy_v2_source_tree" in active_surface
    assert "temporary_or_observed_v1_5_configs" in active_surface
    md = (out / "v1_5_formal_entrypoints.md").read_text(encoding="utf-8")
    assert "V1.5 formal entrypoint inventory" in md
    assert "Canonical V1.5 Formal Path" in md
    assert "Completion Matrix" in md
    assert "Do Not Start Here" in md
    assert "`formal_runner`" in md
    assert "`diagnostic_only` entries must not be used as formal acceptance inputs by default." in md
    convergence = (out / "v1_5_file_convergence_report.md").read_text(encoding="utf-8")
    assert "V1.5 文件收敛报告" in convergence
    assert "它不是第二套入口清单" in convergence
    assert "不要从这里启动正式流程" in convergence
    assert "CO2 零气锚点与 H2O 干气低水锚点不是同一个物理概念" in convergence
    assert (out / "v1_5_file_convergence_report.md").read_bytes().startswith(b"\xef\xbb\xbf")
    isolation_reference = (out / "v1_5_isolation_reference_audit.md").read_text(encoding="utf-8")
    assert "V1.5 isolation reference audit" in isolation_reference
    assert "offline source audit only" in isolation_reference


def test_workspace_surface_marks_legacy_and_temporary_surfaces(tmp_path: Path) -> None:
    paths = [
        "src/gas_calibrator/v2/runner.py",
        "src/gas_calibrator/v2/__pycache__/runner.cpython-313.pyc",
        "tests/v2/test_runner.py",
        "docs/architecture/v2_cutover_checklist.md",
        "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py",
        "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
        "configs/site_v1_5_formal_current_observed_6ch.json",
        "root_probe.stdout.log",
    ]
    for item in paths:
        path = tmp_path / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    rows = build_v1_5_workspace_surface_rows(tmp_path)
    by_surface = {row.surface: row for row in rows}

    assert by_surface["legacy_v2_source_tree"].status == "legacy_reference_only"
    assert by_surface["legacy_v2_source_tree"].action == "exclude_from_v1_5_active_surface"
    assert by_surface["legacy_v2_tests"].action == "exclude_from_v1_5_active_surface"
    assert by_surface["legacy_v2_docs"].action == "exclude_from_v1_5_active_surface"
    assert by_surface["legacy_v1_reference_tools"].action == "do_not_start_v1_5_here"
    assert by_surface["v1_5_diagnostic_tools"].action == "guarded_engineering_use_only"
    assert by_surface["temporary_or_observed_v1_5_configs"].action == "do_not_use_as_default_config"
    assert by_surface["root_temporary_run_artifacts"].action == "move_to_logs_or_archive_after_review"
    assert "src/gas_calibrator/v2/runner.py" in by_surface["legacy_v2_source_tree"].examples
    assert "src/gas_calibrator/v2/__pycache__/runner.cpython-313.pyc" not in by_surface[
        "legacy_v2_source_tree"
    ].examples
    assert by_surface["legacy_v2_source_tree"].file_count == 1
    assert by_surface["temporary_or_observed_v1_5_configs"].file_count == 1


def test_active_surface_policy_has_no_repository_blockers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    issues = validate_v1_5_active_surface_policy(repo_root)

    assert [issue.to_json() for issue in issues if issue.severity == "blocker"] == []


def test_isolation_reference_audit_blocks_formal_runtime_reference_to_diagnostic(tmp_path: Path) -> None:
    root = tmp_path
    formal_runner = root / "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"
    diagnostic = root / "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    for path in (formal_runner, diagnostic):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    formal_runner.write_text(
        "from gas_calibrator.tools.run_v1_5_open_flow_dynamic_pressure_diagnostic import main\n",
        encoding="utf-8",
    )

    issues = audit_v1_5_isolated_reference_integrity(root)

    assert [
        issue.to_json()
        for issue in issues
        if issue.severity == "blocker"
        and issue.isolated_path == "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    ] != []


def test_isolation_reference_audit_reviews_validation_reference_to_legacy_v1(tmp_path: Path) -> None:
    root = tmp_path
    legacy = root / "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"
    audit = root / "src/gas_calibrator/validation/v1_ratio_poly_algorithm_audit.py"
    for path in (legacy, audit):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    audit.write_text(
        "LEGACY_SOURCE = 'gas_calibrator.tools.run_v1_corrected_autodelivery'\n",
        encoding="utf-8",
    )

    issues = audit_v1_5_isolated_reference_integrity(root)
    legacy_issues = [
        issue for issue in issues if issue.isolated_path == "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"
    ]

    assert legacy_issues
    assert {issue.severity for issue in legacy_issues} == {"review"}
    assert {issue.reference_category for issue in legacy_issues} == {"validation_or_audit_support"}


def test_isolation_reference_audit_has_no_repository_blockers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    issues = audit_v1_5_isolated_reference_integrity(repo_root)

    assert [issue.to_json() for issue in issues if issue.severity == "blocker"] == []


def test_active_surface_policy_blocks_diagnostic_as_canonical_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    diagnostic = tmp_path / "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    diagnostic.parent.mkdir(parents=True, exist_ok=True)
    diagnostic.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        inventory_validation,
        "CANONICAL_FORMAL_PATH",
        (
            {
                "stage": "bad_diagnostic_as_formal",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
                "category": "formal_runner",
                "status": "bad",
                "physical_meaning": "bad",
                "safety_boundary": "bad",
            },
        ),
    )

    issues = inventory_validation.validate_v1_5_active_surface_policy(tmp_path)
    blocker_rules = {issue.rule for issue in issues if issue.severity == "blocker"}

    assert "canonical_category_mismatch" in blocker_rules
    assert "canonical_entrypoint_blocked_category" in blocker_rules


def test_canonical_formal_path_entries_exist_in_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    entries = discover_v1_5_entrypoints(repo_root)
    discovered_paths = {entry.path for entry in entries}

    missing = [
        item["entrypoint"]
        for item in CANONICAL_FORMAL_PATH
        if item["entrypoint"] not in discovered_paths
    ]

    assert missing == []


def test_canonical_formal_path_flags_writes_and_route_runners() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    by_path = {entry.path: entry for entry in discover_v1_5_entrypoints(repo_root)}

    co2_runner = by_path["src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"]
    h2o_runner = by_path["src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py"]
    write_tool = by_path["src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py"]
    full_flow = by_path["src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py"]

    assert co2_runner.category == "formal_runner"
    assert co2_runner.controls_routes is True
    assert h2o_runner.category == "formal_runner"
    assert h2o_runner.controls_routes is True
    co2_worker = by_path["src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"]
    h2o_worker = by_path["src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py"]
    assert co2_worker.category == "formal_sampling_worker"
    assert co2_worker.formal_status == "canonical_queue_worker"
    assert h2o_worker.category == "formal_sampling_worker"
    assert h2o_worker.formal_status == "canonical_queue_worker"
    assert write_tool.category == "controlled_write"
    assert write_tool.writes_coefficients is True
    assert full_flow.category == "full_flow_orchestration"
    assert full_flow.opens_com_ports is False


def test_guardrailed_entrypoints_collect_diagnostics_writes_and_queue_workers(tmp_path: Path) -> None:
    root = tmp_path
    paths = [
        "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
        "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
        "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
        "src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py",
        "src/gas_calibrator/tools/archive_v1_5_current_stage.py",
    ]
    for item in paths:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    rows = guardrailed_entrypoint_rows(discover_v1_5_entrypoints(root))
    by_path = {row["path"]: row for row in rows}

    assert "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py" not in by_path
    assert (
        by_path["src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"]["guardrail"]
        == "use_via_canonical_queue_only"
    )
    assert by_path["src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"]["guardrail"] == "diagnostic_not_acceptance"
    assert by_path["src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py"]["guardrail"] == "authorized_write_only"
    assert by_path["src/gas_calibrator/tools/archive_v1_5_current_stage.py"]["guardrail"] == "archive_housekeeping_only"


def test_active_surface_policy_does_not_review_canonical_queue_workers(tmp_path: Path) -> None:
    root = tmp_path
    paths = [
        "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
        "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
    ]
    for item in paths:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    issues = validate_v1_5_active_surface_policy(root, entries=discover_v1_5_entrypoints(root))

    reviewed_paths = {issue.path for issue in issues if issue.rule == "noncanonical_formal_runner"}
    assert reviewed_paths.isdisjoint(paths)


def test_guardrailed_entrypoints_collect_pressure_runner_and_legacy_v1(tmp_path: Path) -> None:
    root = tmp_path
    paths = [
        "src/gas_calibrator/tools/validate_pressure_only.py",
        "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py",
    ]
    for item in paths:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    rows = guardrailed_entrypoint_rows(discover_v1_5_entrypoints(root))
    by_path = {row["path"]: row for row in rows}

    assert by_path["src/gas_calibrator/tools/validate_pressure_only.py"]["guardrail"] == "pressure_no_write_only"
    assert by_path["src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"]["guardrail"] == "legacy_v1_reference_only"
