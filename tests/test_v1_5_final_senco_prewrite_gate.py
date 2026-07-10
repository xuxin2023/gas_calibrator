import csv
import json

from gas_calibrator.validation.v1_5_final_senco_prewrite_gate import validate_final_senco_prewrite_gate


def _write_csv(path, rows):
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _pack(tmp_path, *, global_status="pass", device_status="pass", blockers=""):
    root = tmp_path / "precheck"
    root.mkdir()
    (root / "main_senco_write_precheck_meta.json").write_text(
        json.dumps(
            {
                "no_write": True,
                "writes_senco": False,
                "fit_input_traceability_required": True,
                "fit_input_traceability_status": global_status,
            }
        ),
        encoding="utf-8",
    )
    _write_csv(
        root / "candidate_write_review_checks.csv",
        [
            {
                "check": "fit_input_traceability_required_before_final_senco_review",
                "status": global_status,
            },
            {"check": "fit_input_traceability_bound:co2:091", "status": device_status},
        ],
    )
    _write_csv(
        root / "main_senco_write_precheck_summary.csv",
        [
            {
                "analyzer_device_id": "091",
                "co2_fit_input_traceability_status": device_status,
                "co2_fit_input_traceability_blockers": blockers,
            }
        ],
    )
    return root


def test_final_senco_prewrite_gate_accepts_bound_device(tmp_path):
    root = _pack(tmp_path)

    ok, reasons, detail = validate_final_senco_prewrite_gate(root, component="co2", device_ids=["91"])

    assert ok is True
    assert reasons == []
    assert detail["device_ids"] == ["091"]
    assert detail["fit_input_traceability_status"] == "pass"


def test_final_senco_prewrite_gate_rejects_global_failure(tmp_path):
    root = _pack(tmp_path, global_status="block_write")

    ok, reasons, _ = validate_final_senco_prewrite_gate(root, component="co2", device_ids=["091"])

    assert ok is False
    assert "fit_input_traceability_global_status_not_pass" in reasons


def test_final_senco_prewrite_gate_rejects_missing_device_check(tmp_path):
    root = _pack(tmp_path)

    ok, reasons, _ = validate_final_senco_prewrite_gate(root, component="co2", device_ids=["092"])

    assert ok is False
    assert "fit_input_traceability_bound:co2:092:missing" in reasons
    assert "main_senco_precheck_summary_device_missing:092" in reasons


def test_final_senco_prewrite_gate_rejects_device_blockers(tmp_path):
    root = _pack(tmp_path, blockers="historical_candidate_without_live_fit_input")

    ok, reasons, _ = validate_final_senco_prewrite_gate(root, component="co2", device_ids=["091"])

    assert ok is False
    assert any(reason.startswith("co2_fit_input_traceability_blockers:091:") for reason in reasons)
