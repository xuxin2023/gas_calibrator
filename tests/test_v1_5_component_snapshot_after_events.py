import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_component_snapshot_after_events import main


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


def test_component_snapshot_overlay_uses_verified_senco5_senco6_events(tmp_path):
    base = tmp_path / "old_component_coefficients_snapshot.json"
    base.write_text(
        json.dumps(
            {
                "097": {
                    "analyzer_prefix": "ga06",
                    "port": "COM41",
                    "GETCO5_before": [85.0, 0.84],
                    "GETCO6_before": [6.0, 1.0],
                    "GETCO7_before": [0.0, 1.0, 0.0, 0.0],
                },
                "058": {
                    "analyzer_prefix": "ga02",
                    "port": "COM36",
                    "GETCO5_before": [0.0, 1.0],
                    "GETCO6_before": [0.0, 1.0],
                },
            }
        ),
        encoding="utf-8",
    )
    s5 = tmp_path / "senco5_neutral_write_events.csv"
    s6 = tmp_path / "senco6_neutral_write_events.csv"
    _write_csv(
        s5,
        [
            {
                "device_id": "097",
                "status": "written_readback_verified",
                "final_senco5": "[0.0, 1.0]",
            }
        ],
    )
    _write_csv(
        s6,
        [
            {
                "device_id": "097",
                "status": "written_readback_verified",
                "final_senco6": "[0.0, 1.0]",
            }
        ],
    )
    output = tmp_path / "overlay"

    rc = main(
        [
            "--base-snapshot-json",
            str(base),
            "--event-csv",
            str(s5),
            "--event-csv",
            str(s6),
            "--output-dir",
            str(output),
        ]
    )

    assert rc == 0
    snapshot = json.loads((output / "current_component_coefficients_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot["097"]["GETCO5_before"] == [0.0, 1.0]
    assert snapshot["097"]["GETCO6_before"] == [0.0, 1.0]
    assert snapshot["097"]["GETCO7_before"] == [0.0, 1.0, 0.0, 0.0]
    audit_rows = list(csv.DictReader((output / "component_snapshot_overlay_audit.csv").open(encoding="utf-8-sig")))
    assert [row["overlay_applied"] for row in audit_rows] == ["True", "True"]
