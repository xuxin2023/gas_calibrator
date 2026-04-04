import json

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


def test_calibration_plan_profile_round_trip() -> None:
    profile = CalibrationPlanProfile(
        name="bench_default",
        profile_version="2.3",
        description="Plan profile edited in UI",
        is_default=True,
        mode_profile=ModeProfile(run_mode=RunMode.CO2_MEASUREMENT),
        analyzer_setup=AnalyzerSetupSpec(
            software_version="pre_v5",
            device_id_assignment_mode="manual",
            start_device_id="007",
            manual_device_ids=["011", "012"],
        ),
        temperatures=[
            TemperatureSpec(temperature_c=20.0, order=2),
            TemperatureSpec(temperature_c=0.0, enabled=False, order=1),
        ],
        humidities=[
            HumiditySpec(hgen_temp_c=35.0, hgen_rh_pct=60.0, dewpoint_c=12.5, order=1),
        ],
        gas_points=[
            GasPointSpec(co2_ppm=0.0, co2_group="A", cylinder_nominal_ppm=1.0, enabled=False, order=1),
            GasPointSpec(co2_ppm=400.0, co2_group="B", cylinder_nominal_ppm=405.0, order=2),
        ],
        pressures=[
            PressureSpec(pressure_hpa=1013.25, order=1),
        ],
        ordering=PlanOrderingOptions(
            water_first=True,
            water_first_temp_gte=15.0,
            selected_temps_c=[20.0, 0.0],
            skip_co2_ppm=[0],
            temperature_descending=False,
        ),
    )

    payload = profile.to_dict()
    restored = CalibrationPlanProfile.from_dict(payload)

    assert restored == profile
    assert json.loads(json.dumps(payload))["ordering"]["skip_co2_ppm"] == [0]
    assert payload["profile_version"] == "2.3"
    assert payload["run_mode"] == "co2_measurement"
    assert payload["analyzer_setup"]["software_version"] == "pre_v5"
    assert payload["analyzer_setup"]["manual_device_ids"] == ["011", "012"]
    assert payload["gas_points"][1]["cylinder_nominal_ppm"] == 405.0


def test_calibration_plan_profile_from_dict_accepts_alias_fields() -> None:
    payload = {
        "name": "alias_profile",
        "plan_version": "7.1",
        "default": True,
        "run_mode": "h2o_measurement",
        "analyzer_setup": {
            "analyzer_version": "legacy",
            "id_assignment_mode": "manual",
            "starting_device_id": "8",
            "manual_ids": ["9", "010"],
        },
        "temperature_points": [{"value_c": 30.0, "order": 1}],
        "humidity_points": [{"generator_temp_c": 25.0, "rh_pct": 55.0}],
        "co2_points": [{"ppm": 1200, "group": "B", "nominal_ppm": 1205, "enabled": False}],
        "pressure_points": [{"value_hpa": 980.0}],
        "water_first": True,
        "water_first_temp_gte": 30.0,
        "selected_temps": [30.0],
        "skip_co2_ppm": [0, 50],
        "temperature_descending": False,
    }

    profile = CalibrationPlanProfile.from_dict(payload)

    assert profile.is_default is True
    assert profile.profile_version == "7.1"
    assert profile.mode_profile.run_mode == RunMode.H2O_MEASUREMENT
    assert profile.analyzer_setup.software_version == "pre_v5"
    assert profile.analyzer_setup.device_id_assignment_mode == "manual"
    assert profile.analyzer_setup.start_device_id == "008"
    assert profile.analyzer_setup.manual_device_ids == ["009", "010"]
    assert profile.temperatures[0].temperature_c == 30.0
    assert profile.humidities[0].hgen_rh_pct == 55.0
    assert profile.gas_points[0].co2_ppm == 1200.0
    assert profile.gas_points[0].co2_group == "B"
    assert profile.gas_points[0].cylinder_nominal_ppm == 1205.0
    assert profile.gas_points[0].enabled is False
    assert profile.pressures[0].pressure_hpa == 980.0
    assert profile.ordering.water_first is True
    assert profile.ordering.water_first_temp_gte == 30.0
    assert profile.ordering.selected_temps_c == [30.0]
    assert profile.ordering.skip_co2_ppm == [0, 50]
    assert profile.ordering.temperature_descending is False


def test_plan_ordering_options_defaults_are_safe() -> None:
    ordering = PlanOrderingOptions.from_dict(None)
    profile = CalibrationPlanProfile.from_dict({"name": "empty"})

    assert ordering.water_first is False
    assert ordering.water_first_temp_gte is None
    assert ordering.selected_temps_c == []
    assert ordering.skip_co2_ppm == []
    assert ordering.temperature_descending is True
    assert profile.profile_version == "1.0"
    assert profile.temperatures == []
    assert profile.humidities == []
    assert profile.gas_points == []
    assert profile.pressures == []
    assert profile.mode_profile.run_mode == RunMode.AUTO_CALIBRATION
    assert profile.analyzer_setup.software_version == "v5_plus"
    assert profile.analyzer_setup.device_id_assignment_mode == "automatic"
