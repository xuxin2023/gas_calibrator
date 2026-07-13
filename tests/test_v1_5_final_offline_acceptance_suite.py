from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from gas_calibrator.tools.run_v1_5_final_offline_acceptance_suite import (
    main as cli_main,
    run_v1_5_final_offline_acceptance_suite,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_final_offline_acceptance_suite import (
    ARTIFACT_CONTRACTS,
    PASS_STATUS,
    PLAN_READY_STATUS,
    REVIEW_STATUS,
    SCHEMA,
    SUITE_TEST_FILES,
    build_v1_5_final_offline_acceptance_suite,
    write_v1_5_final_offline_acceptance_suite,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE_MAIN = "9125fcda192cd9b02938a92fae267e7993d37833"


def _passing_execution() -> dict[str, object]:
    return {
        "executed": True,
        "returncode": 0,
        "command_test_files": list(SUITE_TEST_FILES),
        "not_real_acceptance_evidence": True,
    }


def test_suite_plan_binds_current_offline_contracts_and_stays_locked() -> None:
    model = build_v1_5_final_offline_acceptance_suite(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == PLAN_READY_STATUS
    assert model["artifact_contracts_ready"] is True
    assert model["artifact_contract_count"] == len(ARTIFACT_CONTRACTS)
    assert model["artifact_contract_pass_count"] == len(ARTIFACT_CONTRACTS)
    assert model["allowlisted_test_file_count"] == len(SUITE_TEST_FILES)
    assert model["offline_program_acceptance_ready"] is False
    assert model["production_acceptance_ready"] is False
    assert model["review_reasons"] == []
    for key in (
        "full_production_auto_allowed",
        "live_queue_execution_allowed",
        "formal_release_allowed",
        "database_import_allowed",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
        "database_written",
    ):
        assert model[key] is False
    assert model["not_real_acceptance_evidence"] is True


def test_passing_allowlisted_execution_closes_only_offline_program_layer() -> None:
    model = build_v1_5_final_offline_acceptance_suite(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        test_execution=_passing_execution(),
    )

    assert model["overall_status"] == PASS_STATUS
    assert model["offline_suite_tests_passed"] is True
    assert model["offline_program_acceptance_ready"] is True
    assert model["production_acceptance_ready"] is False
    assert model["production_gap_status"]["final_offline_acceptance_suite"] == "offline_program_layer_complete"
    assert model["production_gap_status"]["real_batch_acceptance_when_hardware_available"] == "hardware_deferred"
    assert model["database_import_allowed"] is False


def test_failed_or_mismatched_execution_is_review_required() -> None:
    failed = build_v1_5_final_offline_acceptance_suite(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        test_execution={"executed": True, "returncode": 5, "command_test_files": list(SUITE_TEST_FILES)},
    )
    mismatched = build_v1_5_final_offline_acceptance_suite(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        test_execution={"executed": True, "returncode": 0, "command_test_files": ["tests/other.py"]},
    )

    assert failed["overall_status"] == REVIEW_STATUS
    assert any(reason.startswith("offline_pytest_failed") for reason in failed["review_reasons"])
    assert mismatched["overall_status"] == REVIEW_STATUS
    assert "offline_pytest_allowlist_mismatch" in mismatched["review_reasons"]


def test_missing_or_invalid_artifact_is_a_blocker(tmp_path: Path) -> None:
    model = build_v1_5_final_offline_acceptance_suite(
        repository_root=tmp_path,
        source_origin_main_commit="invalid",
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert model["artifact_contracts_ready"] is False
    assert "source_origin_main_commit_invalid" in model["review_reasons"]
    assert any(reason.startswith("artifact_contract_failed:") for reason in model["review_reasons"])


def test_writer_exports_manifest_markdown_and_csv(tmp_path: Path) -> None:
    paths = write_v1_5_final_offline_acceptance_suite(
        output_dir=tmp_path,
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        test_execution=_passing_execution(),
    )
    assert all(path.is_file() for path in paths.values())
    payload = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert payload["overall_status"] == PASS_STATUS
    assert "program-level offline evidence only" in paths["markdown"].read_text(encoding="utf-8")


def test_runner_uses_exact_allowlist_and_writes_test_output(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_runner(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout="203 passed\n", stderr="")

    result = run_v1_5_final_offline_acceptance_suite(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        output_dir=tmp_path,
        subprocess_runner=fake_runner,
    )

    command = captured["command"]
    assert isinstance(command, list)
    assert command[:5] == [command[0], "-m", "pytest", "-q", "-p"]
    assert command[-len(SUITE_TEST_FILES) :] == list(SUITE_TEST_FILES)
    assert captured["cwd"] == str(ROOT)
    assert "shell" not in captured
    assert result["model"]["overall_status"] == PASS_STATUS
    assert (tmp_path / "pytest_stdout.txt").read_text(encoding="utf-8") == "203 passed\n"
    assert result["model"]["opens_com_ports"] is False


def test_cli_rejects_live_flags_before_creating_artifacts(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "--repository-root",
                str(ROOT),
                "--source-origin-main-commit",
                SOURCE_MAIN,
                "--output-dir",
                str(tmp_path),
                "--execute-read-only-real-com",
            ]
        )
    assert exc.value.code == 2
    assert not list(tmp_path.glob("*"))


def test_entrypoint_is_offline_formal_review_support() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/run_v1_5_final_offline_acceptance_suite.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline_subprocess_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("final offline acceptance suite" in note for note in entry.notes)
