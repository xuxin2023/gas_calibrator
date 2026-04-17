import json
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.controlled_state_machine_profile import compile_controlled_state_machine_profile
from gas_calibrator.v2.core.plan_compiler import PlanCompiler
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.domain.plan_models import (
    AnalyzerSetupSpec,
    CalibrationPlanProfile,
    GasPointSpec,
    HumiditySpec,
    PlanOrderingOptions,
    PressureSpec,
    TemperatureSpec,
)
from gas_calibrator.v2.domain.mode_models import ModeProfile, RunMode
from gas_calibrator.v2.domain.pressure_selection import (
    AMBIENT_PRESSURE_LABEL,
    AMBIENT_PRESSURE_TOKEN,
)


def test_plan_compiler_builds_runtime_rows_that_point_parser_can_consume(tmp_path: Path) -> None:
    compiler = PlanCompiler()
    profile = CalibrationPlanProfile(
        name="ui_profile",
        profile_version="3.2",
        analyzer_setup=AnalyzerSetupSpec(
            software_version="pre_v5",
            device_id_assignment_mode="automatic",
            start_device_id="5",
        ),
        temperatures=[TemperatureSpec(temperature_c=25.0)],
        humidities=[HumiditySpec(hgen_temp_c=25.0, hgen_rh_pct=50.0)],
        gas_points=[GasPointSpec(co2_ppm=400.0, co2_group="B", cylinder_nominal_ppm=405.0)],
        pressures=[PressureSpec(pressure_hpa=1100.0), PressureSpec(pressure_hpa=900.0)],
        ordering=PlanOrderingOptions(water_first=True, water_first_temp_gte=15.0, temperature_descending=True),
    )

    compiled = compiler.compile(profile)
    payload_path = tmp_path / "compiled_points.json"
    payload_path.write_text(json.dumps(compiled.to_runtime_payload()), encoding="utf-8")

    parsed_points = PointParser().parse(payload_path)

    assert len(compiled.runtime_rows) == 4
    assert len(parsed_points) == 4
    assert any(point.route == "h2o" and point.humidity_pct == 50.0 for point in parsed_points)
    assert any(point.route == "co2" and point.co2_ppm == 400.0 for point in parsed_points)
    assert any(point.route == "co2" and point.co2_group == "B" for point in parsed_points)
    assert any(point.route == "co2" and point.cylinder_nominal_ppm == 405.0 for point in parsed_points)
    assert any(row.get("route") == "co2" and row.get("co2_group") == "B" for row in compiled.runtime_rows)
    assert any(row.get("route") == "co2" and row.get("cylinder_nominal_ppm") == 405.0 for row in compiled.runtime_rows)
    assert compiled.metadata["water_first_all_temps"] is True
    assert compiled.metadata["water_first_temp_gte"] == 15.0
    assert compiled.metadata["profile_version"] == "3.2"
    assert compiled.metadata["run_mode"] == "auto_calibration"
    assert compiled.metadata["formal_calibration_report"] is True
    assert compiled.metadata["report_family"] == "v2_product_report_family"
    assert compiled.metadata["analyzer_setup"]["software_version"] == "pre_v5"
    assert compiled.metadata["analyzer_setup"]["start_device_id"] == "005"
    assert compiled.to_runtime_payload()["profile_version"] == "3.2"
    assert compiled.to_runtime_payload()["report_family"] == "v2_product_report_family"
    assert compiled.to_runtime_payload()["analyzer_setup"]["start_device_id"] == "005"
    assert compiled.preview_points[0].route == "h2o"


def test_plan_compiler_applies_selected_temps_skip0_and_temperature_ordering() -> None:
    compiler = PlanCompiler(
        AppConfig.from_dict(
            {
                "workflow": {
                    "water_first_temp_gte": 30.0,
                }
            }
        )
    )
    profile = CalibrationPlanProfile(
        name="replacement_profile",
        temperatures=[
            TemperatureSpec(temperature_c=30.0, order=2),
            TemperatureSpec(temperature_c=10.0, order=1),
        ],
        humidities=[HumiditySpec(hgen_temp_c=10.0, hgen_rh_pct=45.0)],
        gas_points=[
            GasPointSpec(co2_ppm=0.0, order=1),
            GasPointSpec(co2_ppm=400.0, order=2),
        ],
        pressures=[PressureSpec(pressure_hpa=1000.0)],
        ordering=PlanOrderingOptions(
            selected_temps_c=[10.0, 30.0],
            skip_co2_ppm=[0],
            temperature_descending=False,
        ),
    )

    compiled = compiler.compile(profile)

    assert [point.temp_chamber_c for point in compiled.points] == [10.0, 10.0, 30.0, 30.0]
    assert all(point.co2_ppm != 0.0 for point in compiled.points if point.co2_ppm is not None)
    assert compiled.metadata["skip_co2_ppm"] == [0]
    assert compiled.metadata["temperature_descending"] is False
    assert compiled.preview_points[0].route == "co2"


def test_plan_compiler_expands_h2o_carry_forward_source_rows_into_runtime_rows() -> None:
    compiler = PlanCompiler(
        AppConfig.from_dict(
            {
                "workflow": {
                    "h2o_carry_forward": True,
                }
            }
        )
    )
    profile = CalibrationPlanProfile(
        name="carry_forward_profile",
        temperatures=[TemperatureSpec(temperature_c=20.0)],
        humidities=[HumiditySpec(hgen_temp_c=20.0, hgen_rh_pct=60.0, dewpoint_c=11.0)],
        pressures=[PressureSpec(pressure_hpa=1100.0), PressureSpec(pressure_hpa=900.0)],
    )

    compiled = compiler.compile(profile)

    assert compiled.source_rows[0]["humidity_pct"] == 60.0
    assert "humidity_pct" not in compiled.source_rows[1]
    assert compiled.runtime_rows[1]["humidity_pct"] == 60.0
    assert compiled.runtime_rows[1]["dewpoint_c"] == 11.0
    assert [point.target_pressure_hpa for point in compiled.points] == [1100.0, 900.0]


def test_plan_compiler_applies_profile_water_first_temp_threshold_override() -> None:
    compiler = PlanCompiler(
        AppConfig.from_dict(
            {
                "workflow": {
                    "water_first_temp_gte": 30.0,
                }
            }
        )
    )
    profile = CalibrationPlanProfile(
        name="threshold_override",
        temperatures=[TemperatureSpec(temperature_c=10.0)],
        humidities=[HumiditySpec(hgen_temp_c=10.0, hgen_rh_pct=45.0)],
        gas_points=[GasPointSpec(co2_ppm=400.0)],
        ordering=PlanOrderingOptions(
            water_first=False,
            water_first_temp_gte=10.0,
        ),
    )

    compiled = compiler.compile(profile)

    assert compiled.metadata["water_first_all_temps"] is False
    assert compiled.metadata["water_first_temp_gte"] == 10.0
    assert compiled.preview_points[0].route == "h2o"


def test_plan_compiler_applies_co2_measurement_mode_to_preview_order() -> None:
    compiler = PlanCompiler()
    profile = CalibrationPlanProfile(
        name="co2_measurement_profile",
        mode_profile=ModeProfile(run_mode=RunMode.CO2_MEASUREMENT),
        temperatures=[TemperatureSpec(temperature_c=25.0)],
        humidities=[HumiditySpec(hgen_temp_c=25.0, hgen_rh_pct=45.0)],
        gas_points=[GasPointSpec(co2_ppm=400.0, co2_group="B")],
        pressures=[PressureSpec(pressure_hpa=1000.0)],
    )

    compiled = compiler.compile(profile)

    assert compiled.metadata["run_mode"] == "co2_measurement"
    assert compiled.metadata["route_mode"] == "co2_only"
    assert compiled.metadata["formal_calibration_report"] is False
    assert compiled.preview_points
    assert all(point.route == "co2" for point in compiled.preview_points)


def test_plan_compiler_synthesizes_ambient_pressure_points_from_selected_pressure_points(tmp_path: Path) -> None:
    compiler = PlanCompiler()
    profile = CalibrationPlanProfile(
        name="ambient_profile",
        temperatures=[TemperatureSpec(temperature_c=25.0)],
        humidities=[HumiditySpec(hgen_temp_c=25.0, hgen_rh_pct=45.0)],
        gas_points=[GasPointSpec(co2_ppm=400.0, co2_group="B")],
        pressures=[PressureSpec(pressure_hpa=1100.0), PressureSpec(pressure_hpa=900.0)],
        ordering=PlanOrderingOptions(selected_pressure_points=[AMBIENT_PRESSURE_TOKEN, 900.0]),
    )

    compiled = compiler.compile(profile)
    payload_path = tmp_path / "compiled_ambient_points.json"
    payload_path.write_text(json.dumps(compiled.to_runtime_payload()), encoding="utf-8")

    parsed_points = PointParser().parse(payload_path)
    ambient_rows = [
        row
        for row in compiled.runtime_rows
        if row.get("pressure_selection_token") == AMBIENT_PRESSURE_TOKEN
    ]
    ambient_points = [point for point in parsed_points if point.is_ambient_pressure_point]

    assert compiled.metadata["selected_pressure_points"] == [AMBIENT_PRESSURE_TOKEN, 900.0]
    assert len(ambient_rows) == 2
    assert all(row["pressure_mode"] == "ambient_open" for row in ambient_rows)
    assert all(row["pressure_target_label"] == AMBIENT_PRESSURE_LABEL for row in ambient_rows)
    assert all(row["pressure_hpa"] is None for row in ambient_rows)
    assert len(ambient_points) == 2
    assert all(point.target_pressure_hpa is None for point in ambient_points)
    assert all(point.pressure_selection_token_value == AMBIENT_PRESSURE_TOKEN for point in ambient_points)
    assert all(point.pressure_display_label == AMBIENT_PRESSURE_LABEL for point in ambient_points)
    assert {
        row.get("pressure_hpa")
        for row in compiled.runtime_rows
        if row.get("pressure_selection_token") != AMBIENT_PRESSURE_TOKEN
    } == {900.0}


def test_plan_compiler_exposes_controlled_state_machine_profile_ready_shape() -> None:
    compiled = PlanCompiler().compile(
        CalibrationPlanProfile(
            name="measurement_core_bridge",
            temperatures=[TemperatureSpec(temperature_c=25.0)],
            humidities=[HumiditySpec(hgen_temp_c=25.0, hgen_rh_pct=50.0)],
            gas_points=[GasPointSpec(co2_ppm=400.0)],
            pressures=[PressureSpec(pressure_hpa=1000.0)],
            ordering=PlanOrderingOptions(selected_pressure_points=[AMBIENT_PRESSURE_TOKEN, 1000.0]),
        )
    )

    profile = compile_controlled_state_machine_profile(compiled)

    assert profile["profile_version"] == "controlled_flex_v2"
    assert "PRESEAL_STABILITY" in profile["enabled_states"]
    assert "PRESSURE_STABLE" in profile["enabled_states"]
    assert "RUN_COMPLETE" in profile["enabled_states"]
    assert set(profile["route_families"]) >= {"water", "gas", "ambient"}
    assert profile["metadata"]["preview_point_count"] == len(compiled.preview_points)
    assert profile["transition_policy_profile"]["feature_set_version"] == "controlled_state_machine.step2_offline_v2"
