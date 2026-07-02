import csv
import json

import pytest

from gas_calibrator.tools.export_v1_5_post_write_reverification import main as export_main
from gas_calibrator.validation.v1_5_post_write_reverification import (
    VerificationLimits,
    build_post_write_reverification_review,
    write_post_write_reverification_outputs,
)


pytestmark = pytest.mark.v1_5_formal_gate


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_post_write_reverification_reviews_co2_and_h2o_points(tmp_path):
    co2_csv = tmp_path / "co2_post_write.csv"
    h2o_csv = tmp_path / "h2o_post_write.csv"
    _write_csv(
        co2_csv,
        [
            {
                "component": "co2",
                "device_id": "22",
                "point_id": "T20_900",
                "certificate_co2_ppm": "897.04",
                "measured_co2_ppm": "900.00",
            },
            {
                "component": "co2",
                "device_id": "30",
                "point_id": "T20_900",
                "certificate_co2_ppm": "897.04",
                "measured_co2_ppm": "930.00",
            },
        ],
    )
    _write_csv(
        h2o_csv,
        [
            {
                "component": "h2o",
                "device_id": "022",
                "point_id": "T20_RH50",
                "reference_h2o_mmol_mol": "10.0",
                "measured_h2o_mmol_mol": "10.1",
            }
        ],
    )

    review = build_post_write_reverification_review(
        verification_csvs=[co2_csv, h2o_csv],
        limits=VerificationLimits(co2_relative_pct=1.5, h2o_relative_pct=2.0),
        coefficient_epoch="epoch_3",
    )

    assert review.overall_status == "fail"
    rows = {f"{row.device_id}:{row.component}": row for row in review.device_component_summary}
    assert rows["022:co2"].status == "pass"
    assert rows["030:co2"].status == "fail"
    assert rows["022:h2o"].status == "pass"
    assert next(row for row in review.point_results if row.device_id == "022").coefficient_epoch == "epoch_3"


def test_post_write_reverification_writes_utf8_chinese_artifacts(tmp_path):
    csv_path = tmp_path / "co2_post_write.csv"
    _write_csv(
        csv_path,
        [
            {
                "component": "co2",
                "Analyzer": "GA100",
                "point_id": "T20_800",
                "ppm_CO2_Tank": "800.59",
                "ppm_CO2": "801.00",
            }
        ],
    )

    review = build_post_write_reverification_review(verification_csvs=[csv_path])
    outputs = write_post_write_reverification_outputs(review, tmp_path / "out")

    payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert payload["overall_status"] == "pass"
    assert outputs["points_csv"].exists()
    text = outputs["markdown"].read_text(encoding="utf-8")
    assert "V1.5 写后复验评审" in text
    assert "设备ID" in text
    assert "100" in text


def test_post_write_reverification_cli_returns_nonzero_on_failure_when_requested(tmp_path):
    csv_path = tmp_path / "co2_post_write.csv"
    _write_csv(
        csv_path,
        [
            {
                "component": "co2",
                "device_id": "100",
                "point_id": "T20_100",
                "certificate_co2_ppm": "99.94",
                "measured_co2_ppm": "120.00",
            }
        ],
    )

    rc = export_main(
        [
            "--verification-csv",
            str(csv_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--fail-on-review-fail",
        ]
    )

    assert rc == 2
    assert (tmp_path / "out" / "post_write_reverification_review.md").exists()
