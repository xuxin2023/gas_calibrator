from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

from ..models import CalibrationPoint
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState


class ValveRoutingService:
    """Valve routing, baseline, and cleanup helpers."""

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def managed_valves(self) -> list[int]:
        valves_cfg = self.host._cfg_get("valves", {})
        if not isinstance(valves_cfg, dict):
            return []
        managed: set[int] = set()
        for key in ("co2_path", "co2_path_group2", "gas_main", "h2o_path", "hold", "flow_switch"):
            value = self.host._as_int(valves_cfg.get(key))
            if value is not None:
                managed.add(value)
        for map_name in ("co2_map", "co2_map_group2"):
            value_map = valves_cfg.get(map_name, {})
            if isinstance(value_map, dict):
                for value in value_map.values():
                    iv = self.host._as_int(value)
                    if iv is not None:
                        managed.add(iv)
        relay_map = valves_cfg.get("relay_map", {})
        if isinstance(relay_map, dict):
            for key in relay_map:
                iv = self.host._as_int(key)
                if iv is not None:
                    managed.add(iv)
        return sorted(managed)

    def resolve_valve_target(self, logical_valve: int) -> tuple[str, int]:
        relay_map = self.host._cfg_get("valves.relay_map", {})
        entry = relay_map.get(str(logical_valve)) if isinstance(relay_map, dict) else None
        relay_name = "relay_a"
        channel = logical_valve
        if isinstance(entry, dict):
            device_name = str(entry.get("device", "relay") or "relay").strip().lower()
            relay_name = "relay_b" if device_name == "relay_8" else "relay_a"
            mapped_channel = self.host._as_int(entry.get("channel"))
            if mapped_channel is not None and mapped_channel > 0:
                channel = mapped_channel
        return relay_name, channel

    def desired_valve_state(self, valve: int, open_set: set[int]) -> bool:
        desired = valve in open_set
        flow_switch = self.host._as_int(self.host._cfg_get("valves.flow_switch"))
        h2o_path = self.host._as_int(self.host._cfg_get("valves.h2o_path"))
        hold = self.host._as_int(self.host._cfg_get("valves.hold"))
        if flow_switch is not None and h2o_path is not None and hold is not None and valve == flow_switch:
            return h2o_path in open_set and hold in open_set
        return desired

    def _logical_target_state(
        self,
        open_valves: Iterable[int],
    ) -> tuple[set[int], dict[tuple[str, int], bool], list[tuple[str, int, int, bool]]]:
        open_set = {value for value in (self.host._as_int(item) for item in open_valves) if value is not None}
        physical_states: dict[tuple[str, int], bool] = {}
        logical_targets: list[tuple[str, int, int, bool]] = []
        for valve in self.managed_valves():
            relay_name, channel = self.resolve_valve_target(valve)
            desired = self.desired_valve_state(valve, open_set)
            key = (relay_name, channel)
            physical_states[key] = physical_states.get(key, False) or desired
            logical_targets.append((relay_name, valve, channel, desired))
        return open_set, physical_states, logical_targets

    def apply_valve_states(self, open_valves: Iterable[int]) -> dict[str, dict[str, bool]]:
        managed = self.managed_valves()
        if not managed:
            return {}
        _, physical_states, logical_targets = self._logical_target_state(open_valves)
        for (relay_name, channel), state in sorted(physical_states.items()):
            relay = self.host._device(relay_name)
            if relay is None:
                self.host._log(f"Relay {relay_name} unavailable, skip channel {channel}")
                continue
            if self.host._call_first(relay, ("set_valve",), channel, state):
                continue
            if self.host._call_first(
                relay,
                ("select_route", "set_route", "switch_route"),
                f"channel={channel},state={'on' if state else 'off'}",
            ):
                continue
            self.host._log(f"Relay {relay_name} has no writable valve method, skip channel {channel}")
        for relay_name, logical_valve, channel, desired in logical_targets:
            relay = self.host._device(relay_name)
            updater = getattr(relay, "set_logical_valve_state", None) if relay is not None else None
            if callable(updater):
                try:
                    updater(logical_valve, desired, physical_channel=channel)
                except Exception:
                    pass
        return self._relay_state_payload(physical_states)

    def _physical_route_evidence(
        self,
        open_valves: Iterable[int],
        actual_relay_state: dict[str, dict[str, bool]],
    ) -> dict[str, Any]:
        open_set, physical_states, logical_targets = self._logical_target_state(open_valves)
        target_relay_state = {
            relay_name: {str(channel): bool(state) for (device, channel), state in sorted(physical_states.items()) if device == relay_name}
            for relay_name in sorted({name for name, _ in physical_states})
        }
        actual_open_valves: list[int] = []
        mismatched_valves: list[int] = []
        mismatched_channels: list[dict[str, Any]] = []
        for relay_name, logical_valve, channel, desired in logical_targets:
            actual = bool((actual_relay_state.get(str(relay_name)) or {}).get(str(channel), False))
            if actual:
                actual_open_valves.append(int(logical_valve))
            if actual != bool(desired):
                mismatched_valves.append(int(logical_valve))
                mismatched_channels.append(
                    {
                        "logical_valve": int(logical_valve),
                        "relay": str(relay_name),
                        "channel": int(channel),
                        "target": bool(desired),
                        "actual": bool(actual),
                    }
                )
        return {
            "target_open_valves": sorted(int(value) for value in open_set),
            "actual_open_valves": sorted(set(actual_open_valves)),
            "target_relay_state": target_relay_state,
            "actual_relay_state": dict(actual_relay_state),
            "route_physical_state_match": not mismatched_channels,
            "relay_physical_mismatch": bool(mismatched_channels),
            "mismatched_valves": sorted(set(mismatched_valves)),
            "mismatched_channels": mismatched_channels,
        }

    def apply_route_baseline_valves(self) -> None:
        self.apply_valve_states([])

    def set_h2o_path(self, is_open: bool, point: Optional[CalibrationPoint] = None) -> None:
        open_list: list[int] = []
        if is_open:
            for key in ("h2o_path", "hold"):
                value = self.host._as_int(self.host._cfg_get(f"valves.{key}"))
                if value is not None:
                    open_list.append(value)
        relay_state = self.apply_valve_states(open_list)
        physical = self._physical_route_evidence(open_list, relay_state)
        self._record_route_trace(
            action="set_h2o_path",
            route="h2o",
            point=point,
            target={
                "is_open": bool(is_open),
                "open_valves": open_list,
                "target_open_valves": physical["target_open_valves"],
                "target_relay_state": physical["target_relay_state"],
            },
            actual={
                "actual_open_valves": physical["actual_open_valves"],
                "actual_relay_state": physical["actual_relay_state"],
                "route_physical_state_match": physical["route_physical_state_match"],
                "relay_physical_mismatch": physical["relay_physical_mismatch"],
                "mismatched_valves": physical["mismatched_valves"],
                "mismatched_channels": physical["mismatched_channels"],
            },
            relay_state=relay_state,
            result="ok",
            message="H2O route path set",
        )

    def co2_maps_for_point(self, point: CalibrationPoint) -> list[dict[str, Any]]:
        map_a = self.host._cfg_get("valves.co2_map", {})
        map_b = self.host._cfg_get("valves.co2_map_group2", {})
        group = str(point.co2_group or "").strip().upper()
        prefer_b = group in {"B", "2", "G2", "GROUP2", "SECOND"}
        maps: list[dict[str, Any]] = []
        if prefer_b:
            if isinstance(map_b, dict):
                maps.append(map_b)
            if isinstance(map_a, dict):
                maps.append(map_a)
        else:
            if isinstance(map_a, dict):
                maps.append(map_a)
            if isinstance(map_b, dict):
                maps.append(map_b)
        return maps

    def co2_path_for_point(self, point: CalibrationPoint) -> Optional[int]:
        ppm_key = str(self.host._as_int(point.co2_ppm) or "")
        map_a = self.host._cfg_get("valves.co2_map", {})
        map_b = self.host._cfg_get("valves.co2_map_group2", {})
        in_a = isinstance(map_a, dict) and ppm_key in map_a
        in_b = isinstance(map_b, dict) and ppm_key in map_b
        group = str(point.co2_group or "").strip().upper()
        prefer_b = group in {"B", "2", "G2", "GROUP2", "SECOND"}
        if prefer_b and in_b:
            return self.host._as_int(self.host._cfg_get("valves.co2_path_group2", self.host._cfg_get("valves.co2_path")))
        if in_a:
            return self.host._as_int(self.host._cfg_get("valves.co2_path"))
        if in_b:
            return self.host._as_int(self.host._cfg_get("valves.co2_path_group2", self.host._cfg_get("valves.co2_path")))
        return self.host._as_int(self.host._cfg_get("valves.co2_path"))

    def source_valve_for_point(self, point: CalibrationPoint) -> Optional[int]:
        ppm_key = str(self.host._as_int(point.co2_ppm) or "")
        for mapping in self.co2_maps_for_point(point):
            value = mapping.get(ppm_key) if isinstance(mapping, dict) else None
            iv = self.host._as_int(value)
            if iv is not None:
                return iv
        return None

    def co2_open_valves(self, point: CalibrationPoint, *, include_total_valve: bool) -> list[int]:
        open_list: list[int] = []
        if include_total_valve:
            for key in ("h2o_path", "gas_main"):
                value = self.host._as_int(self.host._cfg_get(f"valves.{key}"))
                if value is not None:
                    open_list.append(value)
        co2_path = self.co2_path_for_point(point)
        if co2_path is not None:
            open_list.append(co2_path)
        source = self.source_valve_for_point(point)
        if source is not None:
            open_list.append(source)
        return open_list

    def set_valves_for_co2(self, point: CalibrationPoint) -> None:
        open_valves = self.co2_open_valves(point, include_total_valve=True)
        self._sync_simulated_co2_target(point)
        relay_state = self.apply_valve_states(open_valves)
        physical = self._physical_route_evidence(open_valves, relay_state)
        self._record_route_trace(
            action="set_co2_valves",
            route="co2",
            point=point,
            target={
                "co2_ppm": self.host._as_float(point.co2_ppm),
                "pressure_hpa": self.host._as_float(point.target_pressure_hpa),
                "open_valves": open_valves,
                "target_open_valves": physical["target_open_valves"],
                "target_relay_state": physical["target_relay_state"],
            },
            actual={
                "actual_open_valves": physical["actual_open_valves"],
                "actual_relay_state": physical["actual_relay_state"],
                "route_physical_state_match": physical["route_physical_state_match"],
                "relay_physical_mismatch": physical["relay_physical_mismatch"],
                "mismatched_valves": physical["mismatched_valves"],
                "mismatched_channels": physical["mismatched_channels"],
            },
            relay_state=relay_state,
            result="ok",
            message="CO2 route valves set",
        )

    def _sync_simulated_co2_target(self, point: CalibrationPoint) -> None:
        target_ppm = self.host._as_float(point.co2_ppm)
        if target_ppm is None:
            return
        for device_name in (
            "gas_analyzer",
            "gas_analyzer_0",
            "pressure_controller",
            "pressure_meter",
            "pressure_gauge",
            "relay_a",
            "relay_b",
        ):
            device = self.host._device(device_name)
            plant_state = getattr(device, "plant_state", None) if device is not None else None
            if plant_state is None or not hasattr(plant_state, "analyzer_co2_ppm"):
                continue
            try:
                setattr(plant_state, "analyzer_co2_ppm", float(target_ppm))
                sync = getattr(plant_state, "sync", None)
                if callable(sync):
                    sync()
            except Exception:
                continue

    def set_co2_route_baseline(self, *, reason: str = "") -> None:
        relay_state = self.apply_valve_states([])
        physical = self._physical_route_evidence([], relay_state)
        self._record_route_trace(
            action="route_baseline",
            route="co2",
            target={
                "open_valves": [],
                "target_open_valves": physical["target_open_valves"],
                "target_relay_state": physical["target_relay_state"],
            },
            actual={
                "actual_open_valves": physical["actual_open_valves"],
                "actual_relay_state": physical["actual_relay_state"],
                "route_physical_state_match": physical["route_physical_state_match"],
                "relay_physical_mismatch": physical["relay_physical_mismatch"],
                "mismatched_valves": physical["mismatched_valves"],
                "mismatched_channels": physical["mismatched_channels"],
            },
            relay_state=relay_state,
            result="ok",
            message=reason or "CO2 route baseline applied",
        )
        self.host._log("CO2 route baseline applied: gas_main=OFF flow_switch=ON h2o_path=OFF hold=OFF")
        self.host._set_pressure_controller_vent(True, reason=reason or "before CO2 route conditioning")

    def restore_baseline_after_run(self, *, reason: str = "") -> dict[str, Any]:
        relay_state = self.apply_valve_states([])
        physical = self._physical_route_evidence([], relay_state)
        summary = {"relay_state": relay_state}
        self._record_route_trace(
            action="restore_baseline",
            target={
                "open_valves": [],
                "target_open_valves": physical["target_open_valves"],
                "target_relay_state": physical["target_relay_state"],
            },
            actual={
                "actual_open_valves": physical["actual_open_valves"],
                "actual_relay_state": physical["actual_relay_state"],
                "route_physical_state_match": physical["route_physical_state_match"],
                "relay_physical_mismatch": physical["relay_physical_mismatch"],
                "mismatched_valves": physical["mismatched_valves"],
                "mismatched_channels": physical["mismatched_channels"],
            },
            relay_state=relay_state,
            result="ok",
            message=reason or "restore baseline on finish",
        )
        self.host._log(
            f"Final route baseline applied ({reason})" if reason else "Final route baseline applied"
        )
        return summary

    def _final_chamber_stop_authorized(self) -> bool:
        no_write_active = False
        service = getattr(self.host, "service", None)
        for owner in (self.host, service):
            if owner is None:
                continue
            guard = getattr(owner, "no_write_guard", None)
            if guard is not None and bool(getattr(guard, "enabled", True)):
                no_write_active = True
        workflow_guard = getattr(self.host, "_workflow_no_write_guard_active", None)
        if callable(workflow_guard):
            try:
                no_write_active = no_write_active or bool(workflow_guard())
            except Exception:
                pass
        raw_cfg = getattr(service, "_raw_cfg", None)
        if isinstance(raw_cfg, Mapping):
            for section in ("run001_a2", "a2_co2_7_pressure_no_write_probe", "run001_a1r", "run001_r1"):
                policy = raw_cfg.get(section)
                if isinstance(policy, Mapping) and bool(policy.get("no_write")):
                    no_write_active = True
        if not no_write_active:
            return True
        cfg_getter = getattr(self.host, "_cfg_get", None)
        chamber_stop_paths = (
            "workflow.safety.allow_final_chamber_stop",
            "workflow.safety.final_safe_stop_chamber_stop_enabled",
            "chamber_stop_enabled",
            "run001_a2.chamber_stop_enabled",
            "a2_co2_7_pressure_no_write_probe.chamber_stop_enabled",
            "run001_a1r.chamber_stop_enabled",
            "r1_conditioning_only.chamber_stop_enabled",
            "run001_r1.chamber_stop_enabled",
        )
        for path in chamber_stop_paths:
            value = cfg_getter(path, None) if callable(cfg_getter) else None
            if value is True or (isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "on"}):
                return True
        return False

    def safe_stop_after_run(self, *, baseline_already_restored: bool = False, reason: str = "") -> dict[str, Any]:
        summary: dict[str, Any] = {
            "final_safe_stop_warning_count": 0,
            "final_safe_stop_warnings": [],
            "final_safe_stop_chamber_stop_warning": "",
            "final_safe_stop_chamber_stop_attempted": False,
            "final_safe_stop_chamber_stop_command_sent": False,
            "final_safe_stop_chamber_stop_result": "not_attempted",
            "final_safe_stop_chamber_stop_blocked_by_no_write": False,
        }
        safe_stop_warnings: list[str] = []
        relay_state = {} if baseline_already_restored else self.apply_valve_states([])
        physical = self._physical_route_evidence([], relay_state) if relay_state else None
        if relay_state:
            summary["relay_state"] = relay_state
        self._set_preseal_dewpoint_snapshot(None)
        self._set_post_h2o_co2_zero_flush_pending(False)
        self._set_initial_co2_zero_flush_pending(False)
        self._set_last_hgen_target((None, None))
        self._set_last_hgen_ready(False)

        chamber = self.host._device("temperature_chamber")
        if chamber is not None:
            summary["final_safe_stop_chamber_stop_attempted"] = True
            if not self._final_chamber_stop_authorized():
                warning = "chamber stop blocked by A2 no-write final safe stop policy"
                summary["final_safe_stop_chamber_stop_warning"] = warning
                summary["final_safe_stop_chamber_stop_command_sent"] = False
                summary["final_safe_stop_chamber_stop_blocked_by_no_write"] = True
                summary["final_safe_stop_chamber_stop_result"] = "blocked_by_no_write"
                safe_stop_warnings.append(warning)
                self.host._log(f"Final safe stop warning: {warning}")
            else:
                try:
                    summary["final_safe_stop_chamber_stop_command_sent"] = True
                    self.host._call_first(chamber, ("stop",))
                    summary["final_safe_stop_chamber_stop_result"] = "success"
                except Exception as exc:
                    warning = f"chamber stop failed: {exc}"
                    summary["final_safe_stop_chamber_stop_warning"] = warning
                    summary["final_safe_stop_chamber_stop_result"] = "failed"
                    safe_stop_warnings.append(warning)
                    self.host._log(f"Final safe stop warning: {warning}")
            chamber_state = self._chamber_state(chamber)
            if chamber_state:
                summary["chamber"] = chamber_state

        generator = self.host._device("humidity_generator")
        if generator is not None:
            try:
                stopper = getattr(generator, "safe_stop", None)
                if callable(stopper):
                    stopper()
                else:
                    stopper = getattr(generator, "stop", None)
                    if callable(stopper):
                        stopper()
                    else:
                        self.host._call_first(generator, ("disable_control",), False)
            except Exception as exc:
                warning = f"humidity generator stop failed: {exc}"
                safe_stop_warnings.append(warning)
                self.host._log(f"Final safe stop warning: {warning}")
            stop_check = self._humidity_generator_stop_check(generator)
            if stop_check:
                summary["hgen_stop_check"] = stop_check
                if not bool(stop_check.get("ok", True)) and stop_check.get("error"):
                    safe_stop_warnings.append(f"humidity generator verify failed: {stop_check.get('error')}")

        summary["final_safe_stop_warnings"] = list(dict.fromkeys(safe_stop_warnings))
        summary["final_safe_stop_warning_count"] = len(summary["final_safe_stop_warnings"])

        self._record_route_trace(
            action="final_safe_stop_routes",
            target={"baseline_already_restored": bool(baseline_already_restored)},
            actual={
                **summary,
                **(
                    {
                        "actual_open_valves": physical["actual_open_valves"],
                        "actual_relay_state": physical["actual_relay_state"],
                        "route_physical_state_match": physical["route_physical_state_match"],
                        "relay_physical_mismatch": physical["relay_physical_mismatch"],
                        "mismatched_valves": physical["mismatched_valves"],
                        "mismatched_channels": physical["mismatched_channels"],
                    }
                    if physical is not None
                    else {}
                ),
            },
            relay_state=relay_state if relay_state else None,
            result="ok",
            message=reason or "Routes and auxiliary devices returned to safe state",
        )
        self.host._log(
            f"Final route safe stop complete ({reason})" if reason else "Final route safe stop complete"
        )
        return summary

    def cleanup_co2_route(self, *, reason: str = "") -> None:
        self.set_co2_route_baseline(reason=reason or "after CO2 route")
        self._record_route_trace(
            action="cleanup",
            route="co2",
            result="ok",
            message=reason or "after CO2 route",
        )

    def cleanup_h2o_route(self, point: CalibrationPoint, *, reason: str = "") -> None:
        self.host._set_pressure_controller_vent(True, reason=reason or "after H2O route")
        relay_state = self.apply_valve_states([])
        physical = self._physical_route_evidence([], relay_state)
        self._set_preseal_dewpoint_snapshot(None)
        self._record_route_trace(
            action="cleanup",
            route="h2o",
            point=point,
            target={
                "open_valves": [],
                "target_open_valves": physical["target_open_valves"],
                "target_relay_state": physical["target_relay_state"],
            },
            actual={
                "actual_open_valves": physical["actual_open_valves"],
                "actual_relay_state": physical["actual_relay_state"],
                "route_physical_state_match": physical["route_physical_state_match"],
                "relay_physical_mismatch": physical["relay_physical_mismatch"],
                "mismatched_valves": physical["mismatched_valves"],
                "mismatched_channels": physical["mismatched_channels"],
            },
            relay_state=relay_state,
            result="ok",
            message=reason or "after H2O route",
        )

    def mark_post_h2o_co2_zero_flush_pending(self) -> None:
        if self.host._route_mode() == "h2o_then_co2":
            self._set_post_h2o_co2_zero_flush_pending(True)

    def _set_preseal_dewpoint_snapshot(self, value: Optional[dict[str, Any]]) -> None:
        self.run_state.humidity.preseal_dewpoint_snapshot = value
        setattr(self.host, "_preseal_dewpoint_snapshot", value)

    def _set_post_h2o_co2_zero_flush_pending(self, value: bool) -> None:
        flag = bool(value)
        self.run_state.humidity.post_h2o_co2_zero_flush_pending = flag
        setattr(self.host, "_post_h2o_co2_zero_flush_pending", flag)

    def _set_initial_co2_zero_flush_pending(self, value: bool) -> None:
        flag = bool(value)
        self.run_state.humidity.initial_co2_zero_flush_pending = flag
        setattr(self.host, "_initial_co2_zero_flush_pending", flag)

    def _set_last_hgen_target(self, target: tuple[Optional[float], Optional[float]]) -> None:
        self.run_state.humidity.last_hgen_target = target
        setattr(self.host, "_last_hgen_target", target)

    def _set_last_hgen_ready(self, ready: bool) -> None:
        flag = bool(ready)
        self.run_state.humidity.last_hgen_setpoint_ready = flag
        setattr(self.host, "_last_hgen_setpoint_ready", flag)

    @staticmethod
    def _chamber_state(chamber: Any) -> dict[str, Any]:
        state: dict[str, Any] = {}
        for key, method_name in (
            ("temp_c", "read_temp_c"),
            ("rh_pct", "read_rh_pct"),
            ("run_state", "read_run_state"),
        ):
            method = getattr(chamber, method_name, None)
            if not callable(method):
                continue
            try:
                state[key] = method()
            except Exception as exc:
                state[f"{key}_error"] = str(exc)
        return state

    def _humidity_generator_stop_check(self, generator: Any) -> Optional[dict[str, Any]]:
        waiter = getattr(generator, "wait_stopped", None)
        if not callable(waiter):
            return None
        try:
            return waiter(
                max_flow_lpm=float(self.host._cfg_get("workflow.humidity_generator.safe_stop_max_flow_lpm", 0.05)),
                timeout_s=float(self.host._cfg_get("workflow.humidity_generator.safe_stop_timeout_s", 15.0)),
                poll_s=float(self.host._cfg_get("workflow.humidity_generator.safe_stop_poll_s", 0.5)),
            )
        except Exception as exc:
            self.host._log(f"Final safe stop warning: humidity generator verify failed: {exc}")
            return {"ok": False, "error": str(exc)}

    def _relay_state_payload(self, physical_states: dict[tuple[str, int], bool]) -> dict[str, dict[str, bool]]:
        payload: dict[str, dict[str, bool]] = {}
        for (relay_name, channel), desired in sorted(physical_states.items()):
            actual = bool(desired)
            relay = self.host._device(relay_name)
            if relay is not None:
                method_finder = getattr(self.host, "_first_method", None)
                reader = (
                    method_finder(relay, ("read_coils",))
                    if callable(method_finder)
                    else getattr(relay, "read_coils", None)
                )
                if reader is not None:
                    try:
                        response = reader(max(0, int(channel) - 1), 1)
                        bits = getattr(response, "bits", None)
                        if isinstance(bits, list) and bits:
                            actual = bool(bits[0])
                    except Exception:
                        actual = bool(desired)
                else:
                    status = getattr(relay, "status", None)
                    if callable(status):
                        try:
                            snapshot = status()
                        except Exception:
                            snapshot = None
                        if isinstance(snapshot, dict):
                            coils = snapshot.get("coils")
                            if isinstance(coils, dict) and str(channel) in coils:
                                actual = bool(coils.get(str(channel)))
            payload.setdefault(str(relay_name), {})[str(channel)] = actual
        return payload

    def _record_route_trace(self, **kwargs: Any) -> None:
        status_service = getattr(self.host, "status_service", None)
        recorder = getattr(status_service, "record_route_trace", None)
        if callable(recorder):
            recorder(**kwargs)
