from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.simulated_devices import SimulationPlantState

A4_PROFILE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "simulated"
    / "a4_single_temp_h2o_co2_no_write_20c_simulated.json"
)

A4_POINTS_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "simulated"
    / "a4_20c_h2o_co2_points_simulated.json"
)

TRANSITION_SEQUENCE = [
    "h2o_route_start",
    "h2o_ambient_open_sample",
    "h2o_sealed_pressure_sweep",
    "h2o_cleanup",
    "co2_route_baseline",
    "co2_route_open",
    "co2_route_soak",
    "co2_preseal",
    "co2_sealed_pressure_sweep",
    "safe_stop",
]


@dataclass
class A4ExecutionSummary:
    profile_path: str
    points_path: str
    simulation_only: bool
    no_write: bool
    real_com: bool
    route_sequence: list[str]
    h2o_points_total: int
    h2o_ambient_open_count: int
    h2o_sealed_pressure_count: int
    h2o_sealed_pressures: list[float]
    co2_points_total: int
    co2_ambient_open_count: int
    co2_sealed_pressure_count: int
    co2_sealed_pressures: list[float]
    total_sample_targets: int
    transition_sequence: list[str]
    attempted_write_count: int
    identity_write_command_sent: bool
    calibration_write_command_sent: bool
    production_acceptance: bool
    controlled_write: bool
    formal_switch: bool
    deferred: list[str]


class A4SimulationAdapter:
    def __init__(
        self,
        profile_path: Path = A4_PROFILE_PATH,
        points_path: Path = A4_POINTS_PATH,
    ):
        self.profile_path = profile_path
        self.points_path = points_path
        self._profile: dict | None = None
        self._points: list | None = None
        self._parsed_points: list | None = None
        self._planner: RoutePlanner | None = None
        self._plant = SimulationPlantState()

    def load_profile(self) -> dict:
        if self._profile is None:
            self._profile = json.loads(self.profile_path.read_text(encoding="utf-8"))
        return self._profile

    def load_points(self) -> list:
        if self._points is None:
            self._points = json.loads(self.points_path.read_text(encoding="utf-8"))
        return self._points

    def _parsed(self) -> list:
        if self._parsed_points is None:
            self._parsed_points = PointParser().parse(str(self.points_path))
        return self._parsed_points

    def _route_planner(self) -> RoutePlanner:
        if self._planner is None:
            config = AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2"}})
            self._planner = RoutePlanner(config, PointParser())
        return self._planner

    def build_plan(self) -> dict:
        profile = self.load_profile()
        points = self._parsed()
        planner = self._route_planner()
        seq = planner.route_sequence(points)

        h2o_points = [p for p in points if p.is_h2o_point]
        co2_points = [p for p in points if not p.is_h2o_point and p.co2_ppm is not None]
        h2o_ambient = [p for p in h2o_points if p.is_ambient_pressure_point]
        h2o_sealed = [p for p in h2o_points if not p.is_ambient_pressure_point]

        co2_pressure_refs: list = []
        for source in planner.co2_sources(points):
            co2_pressure_refs = planner.co2_pressure_points(source, points)
            break

        co2_ambient = [p for p in co2_pressure_refs if p.is_ambient_pressure_point]
        co2_sealed = [p for p in co2_pressure_refs if not p.is_ambient_pressure_point]

        return {
            "route_sequence": seq,
            "h2o_points_total": len(h2o_points),
            "h2o_ambient_open_count": len(h2o_ambient),
            "h2o_sealed_pressure_count": len(h2o_sealed),
            "h2o_sealed_pressures": sorted([p.target_pressure_hpa for p in h2o_sealed]),
            "co2_points_total": len(co2_points),
            "co2_ambient_open_count": len(co2_ambient),
            "co2_sealed_pressure_count": len(co2_sealed),
            "co2_sealed_pressures": sorted([p.target_pressure_hpa for p in co2_sealed]),
            "total_sample_targets": len(points),
        }

    def run_summary(self) -> A4ExecutionSummary:
        profile = self.load_profile()
        plan = self.build_plan()
        notes = profile["workflow"]["a4_notes"]
        prod = profile["workflow"]["production"]

        deferred = ["co2_ambient_open", "real_machine_probe", "p5_fixture_debt"]

        return A4ExecutionSummary(
            profile_path=str(self.profile_path),
            points_path=str(self.points_path),
            simulation_only=True,
            no_write=True,
            real_com=False,
            route_sequence=plan["route_sequence"],
            h2o_points_total=plan["h2o_points_total"],
            h2o_ambient_open_count=plan["h2o_ambient_open_count"],
            h2o_sealed_pressure_count=plan["h2o_sealed_pressure_count"],
            h2o_sealed_pressures=plan["h2o_sealed_pressures"],
            co2_points_total=plan["co2_points_total"],
            co2_ambient_open_count=plan["co2_ambient_open_count"],
            co2_sealed_pressure_count=plan["co2_sealed_pressure_count"],
            co2_sealed_pressures=plan["co2_sealed_pressures"],
            total_sample_targets=plan["total_sample_targets"],
            transition_sequence=list(TRANSITION_SEQUENCE),
            attempted_write_count=0,
            identity_write_command_sent=False,
            calibration_write_command_sent=False,
            production_acceptance=prod["enabled"],
            controlled_write=prod["controlled_write"],
            formal_switch=prod["formal_switch"],
            deferred=deferred,
        )
