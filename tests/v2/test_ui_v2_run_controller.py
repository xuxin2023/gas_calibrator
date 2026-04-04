from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.controllers.run_controller import RunController
from gas_calibrator.v2.ui_v2.i18n import t

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_run_controller_forwards_commands_to_facade(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    controller = RunController(facade)

    preview = controller.preview_points()
    original_edit = facade.edit_points_file
    facade.edit_points_file = lambda points_path=None, *, points_source="use_points_file": (
        True,
        f"edit {points_source} {points_path or ''}".strip(),
    )
    ok_start, _ = controller.start("points.json", run_mode="co2_measurement")
    ok_edit, _ = controller.edit_points_file("points.json")
    ok_pause, _ = controller.pause()
    ok_resume, _ = controller.resume()
    ok_stop, _ = controller.stop()
    facade.edit_points_file = original_edit

    assert preview["ok"] is True
    assert ok_start is True
    assert ok_edit is True
    assert ok_pause is True
    assert ok_resume is True
    assert ok_stop is True
    assert facade.service.start_calls == ["points.json"]
    assert facade.service.config.workflow.run_mode == "co2_measurement"
    assert facade.service.config.workflow.route_mode == "co2_only"
    assert facade.service.pause_calls == 1
    assert facade.service.resume_calls == 1
    assert facade.service.stop_calls == 1


def test_run_controller_can_start_from_default_profile(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    gateway = facade.get_plan_gateway()
    gateway.save_profile(
        {
            "name": "default_run",
            "is_default": True,
            "temperatures": [{"temperature_c": 25.0, "enabled": True}],
            "humidities": [{"hgen_temp_c": 25.0, "hgen_rh_pct": 35.0, "enabled": True}],
            "gas_points": [{"co2_ppm": 400.0, "enabled": True}],
            "pressures": [{"pressure_hpa": 1000.0, "enabled": True}],
            "ordering": {"skip_co2_ppm": [0]},
        }
    )
    controller = RunController(facade)

    preview = controller.preview_points(points_source="use_default_profile")
    ok_start, _ = controller.start(points_source="use_default_profile")

    assert preview["ok"] is True
    assert preview["summary"].startswith(
        t("facade.default_profile_summary", profile="default_run", summary="").rstrip()
    )
    assert ok_start is True
    assert preview["run_mode"] == "auto_calibration"
    assert facade.service.start_calls
    assert Path(facade.service.start_calls[-1]).exists()
