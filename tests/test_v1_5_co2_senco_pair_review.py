import csv
import json
from pathlib import Path

from gas_calibrator.storage.v1_5_evidence.bundle import (
    _artifact_role,
    _build_sidecar_candidates,
    _build_sidecar_qc_rows,
    _build_sidecar_write_events,
    _load_database_sidecar_rows,
)
from gas_calibrator.tools.export_v1_5_co2_senco_pair_review import main as cli_main
from gas_calibrator.validation.co2_senco_pair_review import (
    build_co2_senco_pair_review_tables,
    write_co2_senco_pair_review_report,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _make_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    candidate_dir = tmp_path / "candidate_no_write"
    mapping_dir = tmp_path / "candidate_write_review"
    verify_dir = tmp_path / "post_write_900"
    _write_csv(
        candidate_dir / "candidate_coefficients.csv",
        [
            {
                "component": "co2",
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "candidate_status": "verification_passed",
                "term": term,
                "coefficient": value,
            }
            for term, value in [
                ("intercept", 1.0),
                ("R", 2.0),
                ("R2", 3.0),
                ("R3", 4.0),
            ]
        ],
    )
    _write_csv(
        mapping_dir / "candidate_senco_mapping_review.csv",
        [
            {
                "component": "co2",
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "primary_senco": "SENCO1",
                "secondary_senco": "SENCO3",
                "secondary_action": "preserve_existing_requires_old_snapshot_and_manual_mapping_review",
            }
        ],
    )
    _write_csv(
        verify_dir / "post_write_900ppm_primary_only_vs_device_output_summary.csv",
        [
            {
                "analyzer": "ga01",
                "device_id": "023",
                "certificate_co2_ppm": 897.04,
                "mean_device_co2_ppm": 3000.0,
                "device_error_ppm": 2102.96,
                "mean_ratio_f": 1.27929,
                "primary_only_pred_ppm": 893.6,
                "primary_only_error_ppm": -3.44,
                "diagnosis": "device_output_failed_but_primary_ratio_model_predicts_near_certificate",
                "likely_cause": "SENCO3_secondary_terms_still_active_or_internal_mapping_requires_full_SENCO1_SENCO3_pair",
            }
        ],
    )
    return candidate_dir, mapping_dir, verify_dir


def test_co2_senco_pair_review_blocks_after_senco1_only_device_output_failure(tmp_path):
    candidate_dir, mapping_dir, verify_dir = _make_inputs(tmp_path)

    tables = build_co2_senco_pair_review_tables(
        candidate_dir=candidate_dir,
        mapping_review_dir=mapping_dir,
        post_write_verification_dir=verify_dir,
    )

    summary = tables["co2_senco_pair_review_summary"][0]
    assert summary["review_status"] == "blocked_single_senco1_write_failed_pair_review_required"
    assert summary["writes_coefficients"] is False
    assert summary["controls_water_or_gas_routes"] is False
    diagnostic = tables["co2_senco_pair_device_diagnostics"][0]
    assert diagnostic["device_output_qc"] == "fail"
    assert diagnostic["primary_ratio_model_qc"] == "pass"
    assert abs(float(diagnostic["primary_only_error_pct"])) < 1.0
    assert abs(float(diagnostic["device_error_pct"])) > 1.0
    assert diagnostic["device_output_relative_error_limit_pct"] == 1.0
    assert diagnostic["paired_review_need"] == "senco1_senco3_pair_required"
    options = {row["option_id"]: row for row in tables["co2_senco_pair_candidate_options"]}
    assert options["preserve_existing_senco3"]["status"] == "blocked_by_post_senco1_verification_failure"
    assert options["full_pair_multitemp_multigas_a0_a8"]["write_allowed"] is False
    db_tables = {row["db_table"] for row in tables["co2_senco_pair_database_index"]}
    assert {"coefficient_candidates", "coefficient_write_events", "qc_results", "reports"} <= db_tables


def test_co2_senco_pair_review_writer_creates_database_sidecar(tmp_path):
    candidate_dir, mapping_dir, verify_dir = _make_inputs(tmp_path)
    output_dir = tmp_path / "review"

    outputs = write_co2_senco_pair_review_report(
        candidate_dir=candidate_dir,
        mapping_review_dir=mapping_dir,
        post_write_verification_dir=verify_dir,
        output_dir=output_dir,
    )

    assert outputs["workbook"].exists()
    assert outputs["co2_senco_pair_database_index_csv"].exists()
    sidecar = json.loads(outputs["database_sidecar"].read_text(encoding="utf-8"))
    assert sidecar["no_write"] is True
    assert sidecar["opens_com_ports"] is False
    assert "coefficient_candidates" in sidecar["database_target_tables"]
    assert any(row["db_table"] == "coefficient_write_events" for row in sidecar["suggested_rows"])


def test_co2_senco_pair_cli_and_evidence_roles(tmp_path):
    candidate_dir, mapping_dir, verify_dir = _make_inputs(tmp_path)
    output_dir = tmp_path / "review"

    rc = cli_main(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--mapping-review-dir",
            str(mapping_dir),
            "--post-write-verification-dir",
            str(verify_dir),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 0
    assert (output_dir / "co2_senco_pair_review_summary.csv").exists()
    assert (
        _artifact_role(
            output_dir / "co2_senco_pair_review_summary.csv",
            plan_path=None,
            pressure_reference_path=None,
        )
        == "candidate_coefficient_review"
    )
    assert (
        _artifact_role(
            output_dir / "co2_senco1_controlled_write_post_senco1_write.csv",
            plan_path=None,
            pressure_reference_path=None,
        )
        == "coefficient_write_log"
    )


def test_co2_senco_pair_database_sidecar_expands_to_registry_rows(tmp_path):
    sidecar_path = tmp_path / "co2_senco_pair_review_database_sidecar.json"
    sidecar_path.write_text(
        json.dumps(
            {
                "suggested_rows": [
                    {
                        "db_table": "coefficient_candidates",
                        "record_key": "co2_senco1_senco3_pair_review",
                        "component": "co2",
                        "candidate_status": "blocked_single_senco1_write_failed_pair_review_required",
                        "auto_write_allowed": False,
                    },
                    {
                        "db_table": "coefficient_write_events",
                        "record_key": "co2_senco1_single_write_post_verification_failed",
                        "component": "co2",
                        "analyzer_device_id": "all",
                        "candidate_status": "review_required",
                    },
                    {
                        "db_table": "qc_results",
                        "record_key": "co2_post_senco1_output_qc_023",
                        "component": "co2",
                        "analyzer_device_id": "023",
                        "candidate_status": "blocked_not_real_acceptance",
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    sidecar_rows = _load_database_sidecar_rows(
        [{"id": "artifact-1", "artifact_role": "candidate_coefficient_review", "path": str(sidecar_path)}]
    )
    candidates = _build_sidecar_candidates(run_db_id="run-db", sidecar_rows=sidecar_rows)
    write_events = _build_sidecar_write_events(
        run_db_id="run-db",
        analyzer_id="all",
        sidecar_rows=sidecar_rows,
    )
    qc_rows = _build_sidecar_qc_rows(run_db_id="run-db", sidecar_rows=sidecar_rows)

    assert candidates[0]["candidate_status"] == "blocked_single_senco1_write_failed_pair_review_required"
    assert candidates[0]["auto_write_allowed"] is False
    assert write_events[0]["status"] == "review_required"
    assert write_events[0]["event_type"] == "co2_senco1_single_write_post_verification"
    assert qc_rows[0]["subject_id"] == "023"
    assert qc_rows[0]["status"] == "fail"
