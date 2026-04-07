from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools import validate_verification_doc as module


def test_load_targets_keeps_explicit_targets_json_ids(tmp_path: Path) -> None:
    payload = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01", "port": "COM35", "baud": 115200, "device_id": "021", "enabled": True},
                {"name": "ga02", "port": "COM36", "baud": 115200, "device_id": "007", "enabled": True},
            ]
        }
    }
    path = tmp_path / "targets.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    rows = module._load_targets({"devices": {}}, str(path))

    assert [row["device_id"] for row in rows] == ["021", "007"]
