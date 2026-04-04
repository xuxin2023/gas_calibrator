from pathlib import Path

from gas_calibrator.v2.domain.plan_models import (
    AnalyzerSetupSpec,
    CalibrationPlanProfile,
    GasPointSpec,
    HumiditySpec,
    PlanOrderingOptions,
    PressureSpec,
    TemperatureSpec,
)
from gas_calibrator.v2.storage import ProfileStore


def _build_profile(name: str, *, is_default: bool = False) -> CalibrationPlanProfile:
    return CalibrationPlanProfile(
        name=name,
        profile_version="2.0",
        description=f"profile:{name}",
        is_default=is_default,
        analyzer_setup=AnalyzerSetupSpec(
            software_version="pre_v5",
            device_id_assignment_mode="manual",
            start_device_id="5",
            manual_device_ids=["011", "012"],
        ),
        temperatures=[TemperatureSpec(temperature_c=20.0)],
        humidities=[HumiditySpec(hgen_temp_c=20.0, hgen_rh_pct=50.0)],
        gas_points=[GasPointSpec(co2_ppm=400.0, cylinder_nominal_ppm=405.0)],
        pressures=[PressureSpec(pressure_hpa=1000.0)],
        ordering=PlanOrderingOptions(skip_co2_ppm=[0]),
    )


def test_profile_store_saves_loads_and_lists_profiles(tmp_path: Path) -> None:
    store = ProfileStore(tmp_path / "profile_store")
    saved = store.save_profile(_build_profile("bench_profile"))
    summaries = store.list_profiles()
    loaded = store.load_profile("bench_profile")

    assert saved.name == "bench_profile"
    assert loaded is not None
    assert loaded.description == "profile:bench_profile"
    assert loaded.profile_version == "2.0"
    assert loaded.analyzer_setup.software_version == "pre_v5"
    assert loaded.analyzer_setup.manual_device_ids == ["011", "012"]
    assert loaded.ordering.skip_co2_ppm == [0]
    assert loaded.gas_points[0].cylinder_nominal_ppm == 405.0
    assert len(summaries) == 1
    assert summaries[0].name == "bench_profile"
    assert summaries[0].profile_version == "2.0"
    assert summaries[0].is_default is False


def test_profile_store_persists_default_profile_across_reload(tmp_path: Path) -> None:
    store = ProfileStore(tmp_path / "profile_store")
    store.save_profile(_build_profile("alpha"))
    store.save_profile(_build_profile("beta"))
    store.set_default_profile("beta")

    reloaded = ProfileStore(tmp_path / "profile_store")
    default_profile = reloaded.get_default_profile()
    summaries = reloaded.list_profiles()

    assert default_profile is not None
    assert default_profile.name == "beta"
    assert default_profile.is_default is True
    assert [item.name for item in summaries] == ["beta", "alpha"]
    assert summaries[0].is_default is True


def test_profile_store_delete_clears_default_when_needed(tmp_path: Path) -> None:
    store = ProfileStore(tmp_path / "profile_store")
    store.save_profile(_build_profile("default_one", is_default=True))

    deleted = store.delete_profile("default_one")

    assert deleted is True
    assert store.get_default_profile() is None
    assert store.list_profiles() == []


def test_profile_store_imports_and_exports_profile_files(tmp_path: Path) -> None:
    source_store = ProfileStore(tmp_path / "source_store")
    source_store.save_profile(_build_profile("portable", is_default=True))
    export_path = source_store.export_profile("portable", tmp_path / "exports" / "portable.json")

    target_store = ProfileStore(tmp_path / "target_store")
    imported = target_store.import_profile(export_path)

    assert imported.name == "portable"
    assert imported.is_default is True
    assert target_store.get_default_profile() is not None
    assert target_store.get_default_profile().name == "portable"
