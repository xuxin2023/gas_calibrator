from hashlib import sha256
from importlib import import_module

import pytest

from gas_calibrator.utils.file_io import sha256_file, write_json


@pytest.mark.parametrize(
    "module_name",
    [
        "gas_calibrator.validation.certificate_evidence_census",
        "gas_calibrator.validation.certificate_operational_admission",
        "gas_calibrator.v2.scripts.compare_v1_v2_control_flow",
        "gas_calibrator.v2.sim.protocol",
        "gas_calibrator.v2.sim.ec_dynamic",
        "gas_calibrator.v2.sim.ec_system_identification",
        "gas_calibrator.v2.sim.gas_analyzer_asset_dossier",
        "gas_calibrator.v2.sim.gas_analyzer_bench_readiness",
        "gas_calibrator.v2.sim.gas_analyzer_dynamic_uncertainty",
        "gas_calibrator.v2.sim.gas_analyzer_operating_envelope",
    ],
)
def test_offline_artifacts_share_one_json_writer(module_name: str) -> None:
    assert import_module(module_name)._write_json is write_json


def test_file_io_preserves_json_and_digest_contract(tmp_path) -> None:
    path = write_json(tmp_path / "nested" / "artifact.json", {"气体": "CO2"})
    assert path.read_text(encoding="utf-8") == '{\n  "气体": "CO2"\n}\n'
    assert sha256_file(path) == sha256(path.read_bytes()).hexdigest()

    compact_path = write_json(
        tmp_path / "status.json", {"ok": True}, trailing_newline=False
    )
    assert not compact_path.read_text(encoding="utf-8").endswith("\n")
