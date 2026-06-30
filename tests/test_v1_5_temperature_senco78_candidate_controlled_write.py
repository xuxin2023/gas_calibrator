import csv
import json

from gas_calibrator.tools import run_v1_5_temperature_senco78_candidate_controlled_write as writer


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_csv(path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        fields = list(rows[0])
        out = csv.DictWriter(handle, fieldnames=fields)
        out.writeheader()
        out.writerows(rows)


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga70",
                    "enabled": True,
                    "port": "COM35",
                    "baud": 115200,
                    "device_id": "070",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
            ],
        },
        "paths": {"output_dir": str(tmp_path / "logs")},
    }


def _review_rows():
    return [
        {
            "device_id": "070",
            "port": "COM99",
            "channel": "SENCO7",
            "candidate_command": "SENCO7,YGAS,FFF,-3.41840e-01,9.62582e-01,5.88812e-04,1.41486e-05",
            "decision": "write_recommended",
        },
        {
            "device_id": "070",
            "port": "COM99",
            "channel": "SENCO8",
            "candidate_command": "SENCO8,YGAS,FFF,-4.01808e-01,9.85897e-01,1.20627e-03,-8.90963e-06",
            "decision": "write_recommended",
        },
        {
            "device_id": "083",
            "port": "COM38",
            "channel": "SENCO8",
            "candidate_command": "SENCO8,YGAS,FFF,-8.03072e-02,1.01500e00,2.99195e-04,-1.51360e-05",
            "decision": "archive_pass_no_write_required",
        },
    ]


def test_senco78_candidate_writer_is_policy_blocked_without_unlock(tmp_path, capsys):
    cfg_path = tmp_path / "cfg.json"
    review_csv = tmp_path / "review.csv"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_csv, _review_rows())

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-csv",
            str(review_csv),
            "--output-dir",
            str(out_dir),
            "--write-all-recommended",
        ]
    )

    captured = capsys.readouterr()
    assert rc == 2
    assert "neutral-temperature policy" in captured.err
    summary = json.loads((out_dir / "senco78_candidate_write_summary.json").read_text(encoding="utf-8"))
    assert summary["blocked"] is True
    assert summary["opens_com_ports"] is False
    assert summary["writes_coefficients"] is False


def test_senco78_candidate_writer_blocks_even_with_legacy_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    review_csv = tmp_path / "review.csv"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))
    _write_csv(review_csv, _review_rows())

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--review-csv",
            str(review_csv),
            "--output-dir",
            str(out_dir),
            "--write-all-recommended",
            "--enable-senco78-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
            "--pre-device-cooldown-s",
            "0",
            "--inter-device-delay-s",
            "0",
            "--post-write-settle-s",
            "1",
            "--readback-retry-delay-s",
            "1",
            "--restore-command-gap-s",
            "1",
            "--config-ack-retry-delay-s",
            "1",
        ]
    )

    assert rc == 2
    events = list(csv.DictReader((out_dir / "senco78_candidate_write_events.csv").open(encoding="utf-8-sig")))
    assert [row["device_id"] for row in events] == ["070", "070"]
    assert {row["channel"] for row in events} == {"SENCO7", "SENCO8"}
    assert all(row["status"] == "blocked_no_write" for row in events)
    assert all(row["opens_com_ports"] == "False" for row in events)
    assert all(row["writes_coefficients"] == "False" for row in events)
    summary = json.loads((out_dir / "senco78_candidate_write_summary.json").read_text(encoding="utf-8"))
    assert summary["confirmation_text_seen"] is True
    assert summary["selected_device_ids"] == ["070"]
