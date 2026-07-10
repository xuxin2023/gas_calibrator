import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_mature_route_continuity_gate import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_mature_route_continuity_gate import (
    build_v1_5_mature_route_continuity_gate,
    write_v1_5_mature_route_continuity_gate,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _rows(count: int, *, status: str = "ok", prefix: str = "p") -> list[dict[str, object]]:
    return [
        {
            "point_id": f"{prefix}{idx:03d}",
            "point_run_id": f"{prefix}{idx:03d}_formal",
            "status": status,
        }
        for idx in range(1, count + 1)
    ]


def _root_cause(path: Path, *, status: str = "pass", blockers: int = 0, reviews: int = 0) -> Path:
    return _write_json(
        path,
        {
            "schema": "v1_5_route_run_failure_root_cause_audit_v1",
            "manifest": {
                "status": status,
                "blocker_count": blockers,
                "review_required_count": reviews,
                "category_counts": {},
                "opens_com_ports": False,
                "controls_pressure": False,
                "controls_water_or_gas_routes": False,
                "connects_postgresql": False,
                "writes_coefficients": False,
                "writes_sn_or_device_code": False,
            },
            "findings": [],
        },
    )


def test_complete_co2_45_with_clear_root_cause_is_fit_eligible(tmp_path: Path) -> None:
    manifest = _write_csv(tmp_path / "co2_6old_0620clean_mature45_g_clean" / "queue_manifest.csv", _rows(45))
    audit = _root_cause(tmp_path / "root_cause.json")

    model = build_v1_5_mature_route_continuity_gate(
        route_kind="co2",
        queue_manifest_path=manifest,
        root_cause_audit_path=audit,
    )

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["expected_point_count"] == 45
    assert model["manifest"]["observed_point_count"] == 45
    assert model["manifest"]["continuous_route_run_fit_eligible"] is True
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["controls_water_or_gas_routes"] is False


def test_complete_h2o_13_with_clear_root_cause_is_fit_eligible(tmp_path: Path) -> None:
    manifest = _write_csv(tmp_path / "h2o_6old_0620clean_mature13_g_clean" / "queue_manifest.csv", _rows(13, prefix="h"))
    audit = _root_cause(tmp_path / "root_cause.json")

    model = build_v1_5_mature_route_continuity_gate(
        route_kind="h2o",
        queue_manifest_path=manifest,
        root_cause_audit_path=audit,
    )

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["expected_point_count"] == 13
    assert model["manifest"]["continuous_route_run_fit_eligible"] is True


def test_missing_root_cause_requires_review_even_when_manifest_rows_pass(tmp_path: Path) -> None:
    manifest = _write_csv(tmp_path / "co2_6old_0620clean_mature45_g_clean" / "queue_manifest.csv", _rows(45))

    model = build_v1_5_mature_route_continuity_gate(route_kind="co2", queue_manifest_path=manifest)

    assert model["manifest"]["status"] == "review_required"
    assert model["manifest"]["continuous_route_run_fit_eligible"] is False
    assert any(row["requirement"] == "root_cause_audit_attached" for row in model["findings"])


def test_empty_or_incomplete_manifest_is_blocked(tmp_path: Path) -> None:
    empty = _write_csv(tmp_path / "co2_6old_0620clean_mature45_empty" / "queue_manifest.csv", [])
    incomplete = _write_csv(tmp_path / "co2_6old_0620clean_mature45_short" / "queue_manifest.csv", _rows(44))
    audit = _root_cause(tmp_path / "root_cause.json")

    empty_model = build_v1_5_mature_route_continuity_gate(
        route_kind="co2",
        queue_manifest_path=empty,
        root_cause_audit_path=audit,
    )
    incomplete_model = build_v1_5_mature_route_continuity_gate(
        route_kind="co2",
        queue_manifest_path=incomplete,
        root_cause_audit_path=audit,
    )

    assert empty_model["manifest"]["status"] == "blocked"
    assert any(row["requirement"] == "queue_manifest_non_empty" for row in empty_model["findings"])
    assert incomplete_model["manifest"]["status"] == "blocked"
    assert any(row["requirement"] == "expected_point_count" for row in incomplete_model["findings"])


def test_failed_running_or_retry_direct_rows_are_blocked(tmp_path: Path) -> None:
    rows = _rows(45)
    rows[2]["status"] = "failed"
    rows[3]["status"] = "running"
    rows[4]["point_run_id"] = "p005_T20_500ppm_fit_retry1"
    rows[5]["point_run_id"] = "p006_T20_1000ppm_direct_recovery"
    manifest = _write_csv(tmp_path / "co2_6old_0620clean_mature45_g_split" / "queue_manifest.csv", rows)
    audit = _root_cause(tmp_path / "root_cause.json")

    model = build_v1_5_mature_route_continuity_gate(
        route_kind="co2",
        queue_manifest_path=manifest,
        root_cause_audit_path=audit,
    )
    requirements = {row["requirement"] for row in model["findings"] if row["severity"] == "blocker"}

    assert model["manifest"]["status"] == "blocked"
    assert "all_points_completed" in requirements
    assert "no_segmented_retry_direct_rows" in requirements
    assert model["manifest"]["continuous_route_run_fit_eligible"] is False


def test_forbidden_0624_or_handoff_manifest_path_is_blocked(tmp_path: Path) -> None:
    manifest = _write_csv(tmp_path / "_handoff" / "co2_0624_migration" / "queue_manifest.csv", _rows(45))
    audit = _root_cause(tmp_path / "root_cause.json")

    model = build_v1_5_mature_route_continuity_gate(
        route_kind="co2",
        queue_manifest_path=manifest,
        root_cause_audit_path=audit,
    )

    assert model["manifest"]["status"] == "blocked"
    assert any(row["requirement"] == "canonical_manifest_path" for row in model["findings"])


def test_root_cause_blocker_or_review_keeps_manifest_out_of_fitting(tmp_path: Path) -> None:
    manifest = _write_csv(tmp_path / "co2_6old_0620clean_mature45_g_clean" / "queue_manifest.csv", _rows(45))
    blocked = _root_cause(tmp_path / "blocked.json", status="blocked", blockers=1)
    review = _root_cause(tmp_path / "review.json", status="review_required", reviews=1)

    blocked_model = build_v1_5_mature_route_continuity_gate(
        route_kind="co2",
        queue_manifest_path=manifest,
        root_cause_audit_path=blocked,
    )
    review_model = build_v1_5_mature_route_continuity_gate(
        route_kind="co2",
        queue_manifest_path=manifest,
        root_cause_audit_path=review,
    )

    assert blocked_model["manifest"]["status"] == "blocked"
    assert review_model["manifest"]["status"] == "review_required"
    assert blocked_model["manifest"]["continuous_route_run_fit_eligible"] is False
    assert review_model["manifest"]["continuous_route_run_fit_eligible"] is False


def test_writer_cli_and_entrypoint_classification(tmp_path: Path) -> None:
    manifest = _write_csv(tmp_path / "h2o_6old_0620clean_mature13_g_clean" / "queue_manifest.csv", _rows(13, prefix="h"))
    audit = _root_cause(tmp_path / "root_cause.json")

    outputs = write_v1_5_mature_route_continuity_gate(
        route_kind="h2o",
        queue_manifest_path=manifest,
        root_cause_audit_path=audit,
        output_dir=tmp_path / "out",
    )
    model = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    markdown = outputs["markdown"].read_text(encoding="utf-8")
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_mature_route_continuity_gate.py",
        root=ROOT,
    )

    assert model["manifest"]["status"] == "pass"
    assert "V1.5 Mature Route Continuity Gate" in markdown
    assert (
        cli_main(
            [
                "--route-kind",
                "h2o",
                "--queue-manifest-path",
                str(manifest),
                "--root-cause-audit-path",
                str(audit),
                "--output-dir",
                str(tmp_path / "cli"),
            ]
        )
        == 0
    )
    assert (
        cli_main(
            [
                "--route-kind",
                "co2",
                "--queue-manifest-path",
                str(tmp_path / "missing.csv"),
                "--output-dir",
                str(tmp_path / "cli_block"),
                "--fail-on-blocker",
            ]
        )
        == 2
    )
    assert entry.category == "formal_review_evidence"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
