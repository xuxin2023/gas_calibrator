from pathlib import Path

from gas_calibrator.v2.ui_v2.packaging.preflight_checks import run_preflight_checks
from gas_calibrator.v2.ui_v2.utils.runtime_paths import RuntimePaths


def test_preflight_checks_report_local_runtime_state(tmp_path: Path) -> None:
    runtime_paths = RuntimePaths.from_base_dir(tmp_path / "client_state").ensure_dirs()
    ui_root = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "ui_v2"

    report = run_preflight_checks(runtime_paths, ui_root=ui_root)

    assert report["overall_status"] in {"ok", "warn"}
    assert report["checks"]
