"""V1.5 open-flow dynamic pressure diagnostic.

This sidecar is intentionally separate from the formal V1.5 workflow. It keeps
the gas route open with 0 ppm gas by default, records PACE telemetry, and tests
whether dynamic pressure anchoring can avoid sealed-route moisture ingress.
It never writes calibration coefficients, sensor IDs, SENCO, zero, or span.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from ..config import load_config


DEFAULT_GAS_PPM = 0
DEFAULT_AMBIENT_HPA = 1006.0
DEFAULT_TARGETS_HPA = (1000.0, 900.0, 800.0, 700.0, 600.0, 500.0)
DEFAULT_SAMPLE_COUNT = 10
DEFAULT_SAMPLE_INTERVAL_S = 0.2
DEFAULT_MAX_CONTROL_S = 45.0
DEFAULT_CANDIDATE_WINDOW_LOW_HPA = 0.2
DEFAULT_CANDIDATE_WINDOW_HIGH_HPA = 1.0
DEFAULT_DRY_DEWPOINT_MAX_C = -30.0
DEFAULT_DEWPOINT_STABILITY_SPAN_C = 0.7
DEFAULT_POSITIVE_EFFORT_A_MAX_PCT = 0.3
DEFAULT_POSITIVE_EFFORT_A_INTEGRAL_PCT_S = 0.5
DEFAULT_POSITIVE_EFFORT_FAIL_PCT = 3.0
DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA = 1050.0
DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA = 20.0
DEFAULT_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S = 1.0
DEFAULT_OPEN_FLOW_TRANSIENT_GRACE_S = 3.0
DEFAULT_OPEN_FLOW_TRANSIENT_LIMIT_HPA = 1150.0
DEFAULT_RICH_TELEMETRY_INTERVAL_S = 5.0
DEFAULT_RICH_TELEMETRY_INITIAL_DELAY_S = 5.0

FORBIDDEN_WRITE_PATTERNS = (
    re.compile(r"(?<!\*)\bID\b(?!N\?)", re.IGNORECASE),
    re.compile(r"\bSENCO\b", re.IGNORECASE),
    re.compile(r"\bZERO\b", re.IGNORECASE),
    re.compile(r"\bSPAN\b", re.IGNORECASE),
    re.compile(r"\bCOEFF(?:ICIENT)?\b", re.IGNORECASE),
    re.compile(r"\bCAL(?:IBRATION|IBRATE)?\b", re.IGNORECASE),
)

PACE_VENT_WRITE_RE = re.compile(r":SOUR(?::PRES)?(?::LEV)?(?::IMM)?(?::AMPL)?:VENT\s+[01]\b", re.IGNORECASE)

TELEMETRY_FIELDS = (
    "ts",
    "trial_id",
    "mode_requested",
    "target_hpa",
    "gas_ppm",
    "phase",
    "open_flow_route_active",
    "route_sealed",
    "outp_state",
    "outp_mode",
    "vent_status",
    "isolation_state",
    "setpoint_hpa",
    "pace_pressure_hpa",
    "pace_pressure_source",
    "com22_pressure_hpa",
    "sour_pres_eff_pct",
    "sour_pres_comp1_hpa",
    "sour_pres_comp2_hpa",
    "source_pressure_range",
    "sense_pressure_range",
    "sour_pres_slew_hpa_per_s",
    "sens_pressure_slew_hpa_per_s",
    "slew_mode",
    "slew_over",
    "slew_value_max_requested",
    "control_fastest_profile_requested",
    "vent_rate",
    "vent_rate_unit",
    "dewpoint_c",
    "dewpoint_ts",
    "dewpoint_age_ms",
    "dewpoint_source",
    "analyzer_co2_ppm",
    "analyzer_h2o_mmol",
    "analyzer_ts",
    "analyzer_age_ms",
    "analyzer_source",
    "actual_open_valves",
    "syst_err",
    "pressure_safety_abort",
    "pressure_safety_abort_reason",
    "precheck_abort_phase",
    "source_pressure_rise_abort",
    "source_pressure_rise_abort_reason",
    "pressure_transient_allowed",
    "pressure_transient_elapsed_s",
    "pressure_soft_limit_hpa",
    "pressure_transient_limit_hpa",
    "control_command_confirmed",
    "control_outp_state_after_command",
    "control_setpoint_after_command_hpa",
    "control_vent_status_after_command",
    "control_pressure_after_command_hpa",
    "control_eff_after_command_pct",
    "control_syst_err_after_command",
    "control_keepalive_checked",
    "control_output_dropout_seen",
    "control_output_reasserted",
    "control_output_reassert_count",
    "rich_telemetry_collected",
    "rich_telemetry_reason",
    "rich_telemetry_interval_s",
    "rich_telemetry_initial_delay_s",
    "fast_pressure_loop_interval_s",
    "fast_pressure_sample_index",
    "runaway_detected_elapsed_s",
    "pace_vent_hold_during_outp1_allowed",
    "open_flow_atmosphere_hold_active",
    "open_flow_atmosphere_hold_strategy",
    "atmosphere_hold_stopped_before_control",
)

RESULT_FIELDS = (
    "trial_id",
    "mode_requested",
    "mode_confirmed",
    "target_hpa",
    "gas_ppm",
    "gas_source",
    "ambient_hpa",
    "target_below_ambient",
    "open_flow_route_active",
    "route_sealed",
    "candidate_detected",
    "candidate_ts",
    "candidate_pressure_hpa",
    "outp1_to_candidate_s",
    "sample_count",
    "sample_window_s",
    "pressure_start_hpa",
    "pressure_end_hpa",
    "pressure_span_hpa",
    "pressure_mean_hpa",
    "pressure_stable_for_calibration",
    "dewpoint_start_c",
    "dewpoint_end_c",
    "dewpoint_min_c",
    "dewpoint_max_c",
    "dewpoint_delta_c",
    "dewpoint_span_c",
    "dewpoint_low_and_stable",
    "dewpoint_evidence_missing",
    "analyzer_evidence_missing",
    "eff_positive_seen",
    "eff_positive_max_pct",
    "eff_positive_duration_s",
    "eff_positive_integral_pct_s",
    "eff_negative_integral_pct_s",
    "positive_effort_class",
    "possible_supply_involvement",
    "pace_vent_used_for_control",
    "vent_violation",
    "target_crossing_count",
    "target_crossing_severity_hpa",
    "pressure_chatter_detected",
    "pressure_safety_abort",
    "pressure_safety_abort_reason",
    "backdiffusion_risk",
    "candidate_row_possible",
    "candidate_row_quality_grade",
    "sample_can_enter_calibration_fit",
    "sample_can_enter_diagnostic_model",
    "rejection_reasons",
    "no_write_clean",
    "diagnostic_only",
    "not_real_acceptance_evidence",
)


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def _as_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _as_int(value: Any) -> int | None:
    numeric = _as_float(value)
    if numeric is None:
        return None
    try:
        return int(round(numeric))
    except Exception:
        return None


def _parse_first_float(text: Any) -> float | None:
    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", str(text or ""))
    if not match:
        return None
    return _as_float(match.group(0))


def _parse_scpi_value_float(text: Any) -> float | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    parts = raw.split(maxsplit=1)
    tail = parts[1] if len(parts) > 1 else raw
    value = _parse_first_float(tail)
    return value if value is not None else _parse_first_float(raw)


def _read_json(path: str | Path | None) -> Mapping[str, Any]:
    if not path:
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _append_csv_row(path: Path, fieldnames: Sequence[str], row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in fieldnames})


@dataclass(frozen=True)
class DynamicTrialPlan:
    trial_id: str
    label: str
    mode_requested: str
    target_hpa: float | None
    gas_ppm: int = DEFAULT_GAS_PPM
    slew_mode: str | None = "MAX"
    overshoot_allowed: bool | None = False
    slew_value_max: bool = False
    outp1_sent: bool = True
    route_sealed: bool = False
    open_flow_route_active: bool = True
    diagnostic_only: bool = True
    not_real_acceptance_evidence: bool = True
    use_pace_vent_for_control: bool = False

    @property
    def mode_label(self) -> str:
        parts = [self.mode_requested]
        if self.overshoot_allowed is not None:
            parts.append("OVER1" if self.overshoot_allowed else "OVER0")
        if self.slew_mode:
            parts.append(self.slew_mode)
        return "_".join(parts)


@dataclass
class DynamicTrialResult:
    trial_id: str
    mode_requested: str
    mode_confirmed: str = ""
    target_hpa: float | None = None
    gas_ppm: int = DEFAULT_GAS_PPM
    gas_source: str = "0ppm"
    ambient_hpa: float = DEFAULT_AMBIENT_HPA
    target_below_ambient: bool = True
    open_flow_route_active: bool = True
    route_sealed: bool = False
    candidate_detected: bool = False
    candidate_ts: float | None = None
    candidate_pressure_hpa: float | None = None
    outp1_to_candidate_s: float | None = None
    sample_count: int = 0
    sample_window_s: float = 0.0
    pressure_start_hpa: float | None = None
    pressure_end_hpa: float | None = None
    pressure_span_hpa: float | None = None
    pressure_mean_hpa: float | None = None
    pressure_stable_for_calibration: bool = False
    dewpoint_start_c: float | None = None
    dewpoint_end_c: float | None = None
    dewpoint_min_c: float | None = None
    dewpoint_max_c: float | None = None
    dewpoint_delta_c: float | None = None
    dewpoint_span_c: float | None = None
    dewpoint_low_and_stable: bool = False
    dewpoint_evidence_missing: bool = True
    analyzer_evidence_missing: bool = True
    eff_positive_seen: bool = False
    eff_positive_max_pct: float = 0.0
    eff_positive_duration_s: float = 0.0
    eff_positive_integral_pct_s: float = 0.0
    eff_negative_integral_pct_s: float = 0.0
    positive_effort_class: str = "none"
    possible_supply_involvement: bool = False
    pace_vent_used_for_control: bool = False
    vent_violation: bool = False
    target_crossing_count: int = 0
    target_crossing_severity_hpa: float = 0.0
    pressure_chatter_detected: bool = False
    pressure_safety_abort: bool = False
    pressure_safety_abort_reason: str = ""
    backdiffusion_risk: bool = False
    candidate_row_possible: bool = False
    candidate_row_quality_grade: str = "C"
    sample_can_enter_calibration_fit: bool = False
    sample_can_enter_diagnostic_model: bool = True
    rejection_reasons: list[str] = field(default_factory=list)
    no_write_clean: bool = True
    diagnostic_only: bool = True
    not_real_acceptance_evidence: bool = True


def validate_dynamic_targets(
    targets: Iterable[float | str],
    *,
    ambient_hpa: float = DEFAULT_AMBIENT_HPA,
    allow_above_ambient: bool = False,
) -> tuple[float, ...]:
    out: list[float] = []
    ambient = float(ambient_hpa)
    for raw in targets:
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError(f"target is not finite: {raw!r}")
        if abs(value - 1100.0) < 0.001 and not allow_above_ambient:
            raise ValueError("1100 hPa is excluded from default open-flow dynamic diagnostic")
        if value >= ambient and not allow_above_ambient:
            raise ValueError(f"{value:g} >= ambient {ambient:g}; dynamic open-flow defaults to below-ambient points")
        out.append(value)
    if not out:
        raise ValueError("at least one pressure target is required")
    return tuple(out)


def build_default_trial_plan(
    targets_hpa: Iterable[float | str] = DEFAULT_TARGETS_HPA,
    *,
    ambient_hpa: float = DEFAULT_AMBIENT_HPA,
    gas_ppm: int = DEFAULT_GAS_PPM,
    include_pass: bool = False,
    include_gaug: bool = False,
    include_over1: bool = False,
    only_over1: bool = False,
    only_gaug: bool = False,
    set_slew_value_max: bool = False,
    allow_above_ambient: bool = False,
    include_outp0_baseline: bool = True,
) -> list[DynamicTrialPlan]:
    targets = validate_dynamic_targets(
        targets_hpa,
        ambient_hpa=ambient_hpa,
        allow_above_ambient=allow_above_ambient,
    )
    plans: list[DynamicTrialPlan] = []
    if include_outp0_baseline:
        plans.append(
            DynamicTrialPlan(
                trial_id="open_flow_outp0_observe",
                label="open-flow OUTP0 observe-only baseline",
                mode_requested="OUTP0",
                target_hpa=None,
                gas_ppm=int(gas_ppm),
                slew_mode=None,
                overshoot_allowed=None,
                outp1_sent=False,
            )
        )
    for target in targets:
        if not only_over1 and not only_gaug:
            plans.append(
                DynamicTrialPlan(
                    trial_id=f"open_flow_act_over0_max_{target:g}",
                    label=f"open-flow ACT + OVER0 + MAX at {target:g} hPa",
                    mode_requested="ACT",
                    target_hpa=float(target),
                    gas_ppm=int(gas_ppm),
                    slew_value_max=bool(set_slew_value_max),
                )
            )
        if include_over1 or only_over1:
            plans.append(
                DynamicTrialPlan(
                    trial_id=f"open_flow_act_over1_max_{target:g}",
                    label=f"open-flow ACT + OVER1 + MAX fastest diagnostic at {target:g} hPa",
                    mode_requested="ACT",
                    target_hpa=float(target),
                    gas_ppm=int(gas_ppm),
                    overshoot_allowed=True,
                    slew_value_max=bool(set_slew_value_max),
                )
            )
        if include_gaug or only_gaug:
            plans.append(
                DynamicTrialPlan(
                    trial_id=f"open_flow_gaug_over0_max_{target:g}",
                    label=f"open-flow GAUG + OVER0 + MAX diagnostic at {target:g} hPa",
                    mode_requested="GAUG",
                    target_hpa=float(target),
                    gas_ppm=int(gas_ppm),
                    slew_value_max=bool(set_slew_value_max),
                )
            )
        if include_pass:
            plans.append(
                DynamicTrialPlan(
                    trial_id=f"open_flow_pass_over0_max_{target:g}",
                    label=f"open-flow PASS + OVER0 + MAX diagnostic at {target:g} hPa",
                    mode_requested="PASS",
                    target_hpa=float(target),
                    gas_ppm=int(gas_ppm),
                    slew_value_max=bool(set_slew_value_max),
                )
            )
    return plans


def command_is_forbidden_write(command: str) -> bool:
    text = str(command or "").strip()
    if not text or "?" in text:
        return False
    upper = text.upper()
    if PACE_VENT_WRITE_RE.search(upper):
        return True
    return any(pattern.search(upper) for pattern in FORBIDDEN_WRITE_PATTERNS)


def assert_no_forbidden_writes(commands: Iterable[str]) -> None:
    blocked = [cmd for cmd in commands if command_is_forbidden_write(cmd)]
    if blocked:
        raise ValueError(f"forbidden write commands planned: {blocked}")


def planned_commands_for_trial(plan: DynamicTrialPlan) -> list[str]:
    commands = [
        ":OUTP:STAT?",
        ":OUTP:MODE?",
        ":OUTP:ISOL:STAT?",
        ":SOUR:PRES:LEV:IMM:AMPL:VENT?",
        ":SOUR:PRES:LEV:IMM:AMPL:VENT:RATE?",
        ":SOUR:PRES:LEV:IMM:AMPL:VENT:UNIT?",
    ]
    if plan.mode_requested in {"ACT", "PASS", "GAUG"}:
        commands.append(f":OUTP:MODE {plan.mode_requested}")
        commands.append(":OUTP:MODE?")
    if plan.overshoot_allowed is not None:
        commands.append(f":SOUR:PRES:SLEW:OVER {1 if plan.overshoot_allowed else 0}")
        commands.append(":SOUR:PRES:SLEW:OVER?")
    if plan.slew_mode:
        if plan.slew_value_max:
            commands.append(":SOUR:PRES:SLEW max")
            commands.append(":SOUR:PRES:SLEW?")
        commands.append(f":SOUR:PRES:SLEW:MODE {plan.slew_mode}")
        commands.append(":SOUR:PRES:SLEW:MODE?")
    if plan.target_hpa is not None:
        commands.append(f":SOUR:PRES:LEV:IMM:AMPL {float(plan.target_hpa):g}")
    if plan.outp1_sent:
        commands.append(":OUTP:STAT 1")
    commands.extend(
        [
            ":SENS:PRES:INL?",
            ":SENS:PRES:CONT?",
            ":SOUR:PRES:RANG?",
            ":SENS:PRES:RANG?",
            ":SOUR:PRES:SLEW?",
            ":SENS:PRES:SLEW?",
            ":SOUR:PRES:EFF?",
            ":SOUR:PRES:COMP1?",
            ":SOUR:PRES:COMP2?",
            ":SOUR:PRES:SLEW:MODE?",
            ":SOUR:PRES:SLEW:OVER?",
            ":SYST:ERR?",
            ":OUTP:STAT 0",
        ]
    )
    assert_no_forbidden_writes(commands)
    return commands


def summarize_samples(
    samples: Sequence[Mapping[str, Any]],
    *,
    plan: DynamicTrialPlan,
    ambient_hpa: float = DEFAULT_AMBIENT_HPA,
    candidate_ts: float | None = None,
    candidate_pressure_hpa: float | None = None,
    outp1_ts: float | None = None,
    mode_confirmed: str = "",
    dry_dewpoint_max_c: float = DEFAULT_DRY_DEWPOINT_MAX_C,
    dewpoint_stability_span_c: float = DEFAULT_DEWPOINT_STABILITY_SPAN_C,
    positive_effort_a_max_pct: float = DEFAULT_POSITIVE_EFFORT_A_MAX_PCT,
    positive_effort_a_integral_pct_s: float = DEFAULT_POSITIVE_EFFORT_A_INTEGRAL_PCT_S,
    positive_effort_fail_pct: float = DEFAULT_POSITIVE_EFFORT_FAIL_PCT,
) -> DynamicTrialResult:
    if not samples:
        return DynamicTrialResult(
            trial_id=plan.trial_id,
            mode_requested=plan.mode_requested,
            mode_confirmed=mode_confirmed,
            target_hpa=plan.target_hpa,
            gas_ppm=plan.gas_ppm,
            ambient_hpa=float(ambient_hpa),
            target_below_ambient=bool(plan.target_hpa is None or float(plan.target_hpa) < float(ambient_hpa)),
            rejection_reasons=["no_samples"],
        )

    times = [_as_float(row.get("ts")) for row in samples]
    times = [value for value in times if value is not None]
    pressures = [_as_float(row.get("pace_pressure_hpa")) for row in samples]
    pressures = [value for value in pressures if value is not None]
    dewpoints = [_as_float(row.get("dewpoint_c")) for row in samples]
    dewpoints = [value for value in dewpoints if value is not None]
    analyzer_rows = [row for row in samples if _as_float(row.get("analyzer_co2_ppm")) is not None]
    efforts = [_as_float(row.get("sour_pres_eff_pct")) for row in samples]
    efforts = [value for value in efforts if value is not None]
    positives = [value for value in efforts if value > 0.0]
    negatives = [value for value in efforts if value < 0.0]
    interval_s = _median_sample_interval(times) or DEFAULT_SAMPLE_INTERVAL_S
    pressure_span = (max(pressures) - min(pressures)) if pressures else None
    dewpoint_span = (max(dewpoints) - min(dewpoints)) if dewpoints else None
    target = plan.target_hpa
    crossing_severity = 0.0
    crossing_count = 0
    if target is not None:
        for value in pressures:
            if value < float(target):
                crossing_count += 1
                crossing_severity = max(crossing_severity, float(target) - value)
    vent_values = {_as_int(row.get("vent_status")) for row in samples}
    vent_values.discard(None)
    pace_vent_used = bool(plan.use_pace_vent_for_control)
    vent_violation = bool(1 in vent_values or 3 in vent_values or pace_vent_used)
    positive_max = max(positives) if positives else 0.0
    positive_integral = sum(positives) * interval_s
    positive_duration = len(positives) * interval_s
    if positive_max >= positive_effort_fail_pct:
        positive_class = "fail"
    elif positive_max <= 0.0:
        positive_class = "none"
    elif positive_max <= positive_effort_a_max_pct and positive_integral <= positive_effort_a_integral_pct_s:
        positive_class = "micro_ok"
    else:
        positive_class = "diagnostic_only"

    dewpoint_missing = not dewpoints
    dewpoint_low_and_stable = bool(
        dewpoints
        and max(dewpoints) <= float(dry_dewpoint_max_c)
        and (dewpoint_span is not None and dewpoint_span <= float(dewpoint_stability_span_c))
    )
    pressure_stable = bool(
        pressures
        and target is not None
        and abs((sum(pressures) / len(pressures)) - float(target)) <= DEFAULT_CANDIDATE_WINDOW_HIGH_HPA
        and (pressure_span is None or pressure_span <= 1.5)
    )
    candidate_possible = bool(candidate_pressure_hpa is not None)
    backdiffusion_risk = bool(
        plan.open_flow_route_active
        and target is not None
        and float(target) < float(ambient_hpa) - 20.0
        and positive_class in {"none", "micro_ok"}
        and not negatives
    )
    rejection_reasons: list[str] = []
    if plan.route_sealed:
        rejection_reasons.append("route_sealed_not_open_flow")
    if vent_violation:
        rejection_reasons.append("pace_vent_active_or_used_for_control")
    if not candidate_possible and plan.outp1_sent:
        rejection_reasons.append("candidate_not_detected")
    if dewpoint_missing:
        rejection_reasons.append("dewpoint_evidence_missing")
    elif not dewpoint_low_and_stable:
        rejection_reasons.append("dewpoint_not_low_or_stable")
    if not analyzer_rows:
        rejection_reasons.append("analyzer_evidence_missing")
    if positive_class == "fail":
        rejection_reasons.append("positive_effort_fail")
    elif positive_class == "diagnostic_only":
        rejection_reasons.append("positive_effort_diagnostic_only")
    if not pressure_stable and plan.outp1_sent:
        rejection_reasons.append("pressure_not_stable_for_fit")
    if crossing_severity > 0.05:
        rejection_reasons.append("target_crossing_nontrivial")
    if backdiffusion_risk:
        rejection_reasons.append("below_ambient_open_flow_backdiffusion_risk")

    fit_ready = bool(
        candidate_possible
        and not plan.route_sealed
        and not vent_violation
        and not dewpoint_missing
        and dewpoint_low_and_stable
        and analyzer_rows
        and positive_class in {"none", "micro_ok"}
        and pressure_stable
        and crossing_severity <= 0.05
        and not backdiffusion_risk
    )
    if fit_ready:
        grade = "A_calibration_eligible"
    elif candidate_possible and not vent_violation:
        grade = "B_diagnostic_model_only"
    else:
        grade = "C_reject"
    return DynamicTrialResult(
        trial_id=plan.trial_id,
        mode_requested=plan.mode_requested,
        mode_confirmed=mode_confirmed,
        target_hpa=plan.target_hpa,
        gas_ppm=plan.gas_ppm,
        gas_source=f"{plan.gas_ppm}ppm",
        ambient_hpa=float(ambient_hpa),
        target_below_ambient=bool(plan.target_hpa is None or float(plan.target_hpa) < float(ambient_hpa)),
        open_flow_route_active=plan.open_flow_route_active,
        route_sealed=plan.route_sealed,
        candidate_detected=candidate_possible,
        candidate_ts=candidate_ts,
        candidate_pressure_hpa=candidate_pressure_hpa,
        outp1_to_candidate_s=(
            max(0.0, float(candidate_ts) - float(outp1_ts))
            if candidate_ts is not None and outp1_ts is not None
            else None
        ),
        sample_count=len(samples),
        sample_window_s=(max(times) - min(times)) if len(times) >= 2 else 0.0,
        pressure_start_hpa=pressures[0] if pressures else None,
        pressure_end_hpa=pressures[-1] if pressures else None,
        pressure_span_hpa=pressure_span,
        pressure_mean_hpa=(sum(pressures) / len(pressures)) if pressures else None,
        pressure_stable_for_calibration=pressure_stable,
        dewpoint_start_c=dewpoints[0] if dewpoints else None,
        dewpoint_end_c=dewpoints[-1] if dewpoints else None,
        dewpoint_min_c=min(dewpoints) if dewpoints else None,
        dewpoint_max_c=max(dewpoints) if dewpoints else None,
        dewpoint_delta_c=(dewpoints[-1] - dewpoints[0]) if len(dewpoints) >= 2 else None,
        dewpoint_span_c=dewpoint_span,
        dewpoint_low_and_stable=dewpoint_low_and_stable,
        dewpoint_evidence_missing=dewpoint_missing,
        analyzer_evidence_missing=not bool(analyzer_rows),
        eff_positive_seen=bool(positives),
        eff_positive_max_pct=positive_max,
        eff_positive_duration_s=positive_duration,
        eff_positive_integral_pct_s=positive_integral,
        eff_negative_integral_pct_s=sum(abs(value) for value in negatives) * interval_s,
        positive_effort_class=positive_class,
        possible_supply_involvement=positive_class in {"fail", "diagnostic_only"},
        pace_vent_used_for_control=pace_vent_used,
        vent_violation=vent_violation,
        target_crossing_count=crossing_count,
        target_crossing_severity_hpa=round(crossing_severity, 6),
        pressure_chatter_detected=bool(pressure_span is not None and pressure_span > 2.0),
        pressure_safety_abort=False,
        pressure_safety_abort_reason="",
        backdiffusion_risk=backdiffusion_risk,
        candidate_row_possible=candidate_possible,
        candidate_row_quality_grade=grade,
        sample_can_enter_calibration_fit=fit_ready,
        sample_can_enter_diagnostic_model=bool(candidate_possible and not fit_ready),
        rejection_reasons=rejection_reasons,
    )


def _median_sample_interval(times: Sequence[float]) -> float | None:
    if len(times) < 2:
        return None
    diffs = sorted(max(0.0, b - a) for a, b in zip(times, times[1:]))
    if not diffs:
        return None
    return diffs[len(diffs) // 2]


def rank_results(results: Sequence[DynamicTrialResult]) -> dict[str, Any]:
    a_rows = [row for row in results if row.sample_can_enter_calibration_fit]
    diagnostic_rows = [row for row in results if row.sample_can_enter_diagnostic_model]
    best = None
    candidates = a_rows or diagnostic_rows or list(results)
    if candidates:
        best = min(
            candidates,
            key=lambda row: (
                0 if row.sample_can_enter_calibration_fit else 1,
                row.eff_positive_integral_pct_s,
                row.dewpoint_span_c if row.dewpoint_span_c is not None else 999.0,
                row.outp1_to_candidate_s if row.outp1_to_candidate_s is not None else 999.0,
            ),
        )
    return {
        "diagnostic_only": True,
        "not_real_acceptance_evidence": True,
        "gas_ppm": DEFAULT_GAS_PPM,
        "best_mode_for_open_flow_dynamic_pressure": best.mode_requested if best else "",
        "best_target_hpa": best.target_hpa if best else "",
        "best_reason": (
            "A row with lowest positive effort/dewpoint span/time"
            if best and best.sample_can_enter_calibration_fit
            else "no A row; best diagnostic row only"
        ),
        "a_calibration_eligible_count": len(a_rows),
        "diagnostic_only_count": len(diagnostic_rows),
        "reject_count": len([row for row in results if row.candidate_row_quality_grade.startswith("C")]),
        "whether_1100_excluded": True,
        "whether_pace_vent_used_for_control": any(row.pace_vent_used_for_control for row in results),
        "whether_positive_effort_micro_topoff_can_remain_A": any(
            row.sample_can_enter_calibration_fit and row.positive_effort_class == "micro_ok"
            for row in results
        ),
        "whether_backdiffusion_risk_seen": any(row.backdiffusion_risk for row in results),
        "recommended_workflow_change": "keep_formal_v1_5_unchanged_until_open_flow_dynamic_real_run_confirms_A_rows",
    }


def _managed_logical_valves(cfg: Mapping[str, Any]) -> list[int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg, Mapping) else {}
    managed: set[int] = set()
    for key in ("co2_path", "co2_path_group2", "gas_main", "h2o_path", "hold", "flow_switch"):
        value = _as_int(valves_cfg.get(key))
        if value is not None:
            managed.add(value)
    for key in ("co2_map", "co2_map_group2"):
        one_map = valves_cfg.get(key, {})
        if isinstance(one_map, Mapping):
            for value in one_map.values():
                numeric = _as_int(value)
                if numeric is not None:
                    managed.add(numeric)
    return sorted(managed)


def _resolve_valve_target(cfg: Mapping[str, Any], logical_valve: int) -> tuple[str, int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg, Mapping) else {}
    relay_map = valves_cfg.get("relay_map", {}) if isinstance(valves_cfg, Mapping) else {}
    entry = relay_map.get(str(logical_valve)) if isinstance(relay_map, Mapping) else None
    relay_name = "relay"
    channel = int(logical_valve)
    if isinstance(entry, Mapping):
        relay_name = str(entry.get("device") or "relay")
        mapped = _as_int(entry.get("channel"))
        if mapped is not None:
            channel = mapped
    return relay_name, channel


def resolve_0ppm_open_flow_valves(cfg: Mapping[str, Any], *, gas_ppm: int = DEFAULT_GAS_PPM) -> dict[str, Any]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg, Mapping) else {}
    ppm_key = str(int(gas_ppm))
    candidates = [
        ("A", valves_cfg.get("co2_map", {}), valves_cfg.get("co2_path")),
        ("B", valves_cfg.get("co2_map_group2", {}), valves_cfg.get("co2_path_group2", valves_cfg.get("co2_path"))),
    ]
    for group, one_map, path_valve in candidates:
        if not isinstance(one_map, Mapping) or ppm_key not in one_map:
            continue
        source = _as_int(one_map.get(ppm_key))
        path = _as_int(path_valve)
        gas_main = _as_int(valves_cfg.get("gas_main"))
        h2o_path = _as_int(valves_cfg.get("h2o_path"))
        path_open_valves = [value for value in (h2o_path, gas_main, path) if value is not None]
        open_valves = [value for value in (*path_open_valves, source) if value is not None]
        return {
            "gas_ppm": int(gas_ppm),
            "group": group,
            "source_valve": source,
            "path_valve": path,
            "path_open_logical_valves": path_open_valves,
            "open_logical_valves": open_valves,
        }
    raise RuntimeError(f"0ppm gas valve mapping not found for gas_ppm={gas_ppm}")


def apply_logical_valves(cfg: Mapping[str, Any], devices: Mapping[str, Any], open_logical_valves: Sequence[int]) -> None:
    open_set = {int(value) for value in open_logical_valves}
    grouped: dict[str, list[tuple[int, bool]]] = {}
    for logical in _managed_logical_valves(cfg):
        relay_name, channel = _resolve_valve_target(cfg, logical)
        grouped.setdefault(relay_name, []).append((channel, logical in open_set))
    for relay_name, updates in grouped.items():
        relay = devices.get(relay_name)
        if relay is None:
            raise RuntimeError(f"relay required for valve operation: {relay_name}")
        bulk = getattr(relay, "set_valves_bulk", None)
        if callable(bulk):
            bulk(updates)
        else:
            for channel, state in updates:
                relay.set_valve(channel, state)


def _query_text(pace: Any, command: str) -> str:
    try:
        return str(pace.query(command))
    except Exception as exc:
        return f"ERROR:{type(exc).__name__}:{exc}"


def read_pace_pressure_hpa(pace: Any) -> tuple[float | None, str]:
    reader = getattr(pace, "read_pressure", None)
    if callable(reader):
        try:
            value = _as_float(reader())
            if value is not None:
                return value, "PACE::read_pressure"
        except Exception:
            pass
    for command in (":SENS:PRES:INL?", ":SENS:PRES:CONT?", ":SENS:PRES?", ":MEAS:PRES?"):
        value = _parse_scpi_value_float(_query_text(pace, command))
        if value is not None:
            return value, f"PACE:{command}"
    return None, ""


def _row_pressure_for_safety(row: Mapping[str, Any]) -> float | None:
    return _as_float(row.get("pace_pressure_hpa"))


def row_exceeds_open_flow_pressure_safety(row: Mapping[str, Any], max_safe_pressure_hpa: float) -> bool:
    pressure = _row_pressure_for_safety(row)
    return bool(pressure is not None and pressure > float(max_safe_pressure_hpa))


def row_exceeds_open_flow_source_rise(
    row: Mapping[str, Any],
    *,
    ambient_hpa: float,
    max_rise_hpa: float,
) -> bool:
    pressure = _row_pressure_for_safety(row)
    if pressure is None:
        return False
    return bool(float(pressure) > float(ambient_hpa) + max(0.0, float(max_rise_hpa)))


def open_flow_pressure_abort_reason(
    row: Mapping[str, Any],
    *,
    max_safe_pressure_hpa: float,
    transient_limit_hpa: float = DEFAULT_OPEN_FLOW_TRANSIENT_LIMIT_HPA,
    transient_grace_s: float = DEFAULT_OPEN_FLOW_TRANSIENT_GRACE_S,
    transient_elapsed_s: float | None = None,
) -> str:
    pressure = _row_pressure_for_safety(row)
    if pressure is None:
        return ""
    if float(pressure) > float(transient_limit_hpa):
        return (
            f"open_flow_pressure_hard_abort:"
            f"{float(pressure):.3f}>{float(transient_limit_hpa):.3f}"
        )
    if float(pressure) <= float(max_safe_pressure_hpa):
        return ""
    if transient_elapsed_s is not None and float(transient_elapsed_s) <= max(0.0, float(transient_grace_s)):
        return ""
    return (
        f"open_flow_pressure_safety_abort:"
        f"{float(pressure):.3f}>{float(max_safe_pressure_hpa):.3f}"
    )


def open_flow_dynamic_control_runaway_reason(
    row: Mapping[str, Any],
    *,
    target_hpa: float | None,
    max_rise_hpa: float,
    transient_grace_s: float,
    transient_elapsed_s: float | None,
) -> str:
    if target_hpa is None:
        return ""
    pressure = _row_pressure_for_safety(row)
    if pressure is None:
        return ""
    if transient_elapsed_s is not None and float(transient_elapsed_s) <= max(0.0, float(transient_grace_s)):
        return ""
    limit = float(target_hpa) + max(0.0, float(max_rise_hpa))
    if float(pressure) <= limit:
        return ""
    return (
        f"open_flow_dynamic_control_runaway_abort:"
        f"{float(pressure):.3f}>{limit:.3f}"
    )


def _confirm_control_command_state(pace: Any, *, target_hpa: float | None) -> dict[str, Any]:
    outp_state = _parse_scpi_value_float(_query_text(pace, ":OUTP:STAT?"))
    setpoint = _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:LEV:IMM:AMPL?"))
    vent_status = _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:LEV:IMM:AMPL:VENT?"))
    pressure, _source = read_pace_pressure_hpa(pace)
    eff = _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:EFF?"))
    syst_err = _query_text(pace, ":SYST:ERR?")
    target = _as_float(target_hpa)
    setpoint_matches = bool(
        target is not None
        and setpoint is not None
        and abs(float(setpoint) - float(target)) <= 0.05
    )
    return {
        "control_command_confirmed": bool(outp_state == 1 and setpoint_matches),
        "control_outp_state_after_command": outp_state,
        "control_setpoint_after_command_hpa": setpoint,
        "control_vent_status_after_command": vent_status,
        "control_pressure_after_command_hpa": pressure,
        "control_eff_after_command_pct": eff,
        "control_syst_err_after_command": syst_err,
        "pace_vent_hold_during_outp1_allowed": False,
    }


def _enable_control_output_confirmed(
    pace: Any,
    *,
    target_hpa: float | None,
    timeout_s: float = 5.0,
    poll_s: float = 0.25,
) -> dict[str, Any]:
    deadline = time.time() + max(0.5, float(timeout_s))
    confirmation: dict[str, Any] = {}
    while time.time() < deadline:
        enable_output = getattr(pace, "enable_control_output", None)
        try:
            if callable(enable_output):
                enable_output(timeout_s=min(1.0, max(0.2, deadline - time.time())), poll_s=0.1)
            else:
                pace.set_output(True)
        except Exception:
            pass
        confirmation = _confirm_control_command_state(pace, target_hpa=target_hpa)
        if confirmation.get("control_command_confirmed"):
            return confirmation
        try:
            # K0472 documents OUTP:STAT 0/1; keep this as a confirmation retry
            # in the sidecar without changing the shared driver shorthand.
            pace.write(":OUTP:STAT 1")
        except Exception:
            pass
        time.sleep(max(0.05, float(poll_s)))
        confirmation = _confirm_control_command_state(pace, target_hpa=target_hpa)
        if confirmation.get("control_command_confirmed"):
            return confirmation
    time.sleep(max(0.05, float(poll_s)))
    confirmation = _confirm_control_command_state(pace, target_hpa=target_hpa)
    return confirmation


def _safe_abort_pace(pace: Any) -> None:
    for action in (
        lambda: pace.set_output(False),
        lambda: pace.set_isolation_open(True),
        lambda: pace.vent(True),
    ):
        try:
            action()
        except Exception:
            pass


def start_open_flow_atmosphere_hold(pace: Any, *, interval_s: float) -> dict[str, Any]:
    interval = max(0.1, float(interval_s or DEFAULT_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S))
    strategy = "enter_atmosphere_mode_hold_open"
    enter = getattr(pace, "enter_atmosphere_mode", None)
    try:
        if callable(enter):
            try:
                enter(timeout_s=10.0, hold_open=True, hold_interval_s=interval)
            except TypeError:
                enter(hold_open=True)
        else:
            set_output = getattr(pace, "set_output", None)
            if callable(set_output):
                set_output(False)
            set_isolation_open = getattr(pace, "set_isolation_open", None)
            if callable(set_isolation_open):
                set_isolation_open(True)
            pace.vent(True)
            start_hold = getattr(pace, "start_atmosphere_hold", None)
            if callable(start_hold):
                start_hold(interval_s=interval)
            strategy = "manual_vent_hold"
    except Exception as exc:
        return {"requested": True, "active": False, "strategy": strategy, "error": str(exc)}
    checker = getattr(pace, "is_atmosphere_hold_active", None)
    active = bool(checker()) if callable(checker) else True
    return {"requested": True, "active": active, "strategy": strategy, "interval_s": interval, "error": ""}


def stop_open_flow_atmosphere_hold_before_control(
    pace: Any,
    *,
    force_output_off: bool = True,
    vent_idle_settle_s: float = 0.5,
) -> dict[str, Any]:
    result = {
        "stopped": False,
        "vent_abort_sent": False,
        "vent_idle_status": "",
        "vent_idle_wait_error": "",
        "output_off_sent": False,
        "error": "",
    }
    try:
        stopper = getattr(pace, "stop_atmosphere_hold", None)
        if callable(stopper):
            result["stopped"] = bool(stopper(timeout_s=2.0))
        else:
            result["stopped"] = True
        set_output = getattr(pace, "set_output", None)
        if force_output_off and callable(set_output):
            set_output(False)
            result["output_off_sent"] = True
        pace.vent(False)
        result["vent_abort_sent"] = True
        wait_idle = getattr(pace, "wait_for_vent_idle", None)
        if callable(wait_idle):
            try:
                result["vent_idle_status"] = wait_idle(timeout_s=5.0, poll_s=0.2)
            except Exception as exc:
                result["vent_idle_wait_error"] = str(exc)
        if float(vent_idle_settle_s) > 0.0:
            time.sleep(float(vent_idle_settle_s))
        set_isolation_open = getattr(pace, "set_isolation_open", None)
        if callable(set_isolation_open):
            set_isolation_open(True)
    except Exception as exc:
        result["error"] = str(exc)
    return result


def _pace_atmosphere_hold_active(pace: Any) -> bool:
    checker = getattr(pace, "is_atmosphere_hold_active", None)
    if not callable(checker):
        return False
    try:
        return bool(checker())
    except Exception:
        return False


def _mark_pressure_safety_abort(result: DynamicTrialResult, reason: str) -> DynamicTrialResult:
    result.pressure_safety_abort = True
    result.pressure_safety_abort_reason = reason
    result.candidate_row_quality_grade = "C_reject"
    result.sample_can_enter_calibration_fit = False
    result.sample_can_enter_diagnostic_model = False
    if "pressure_safety_abort" not in result.rejection_reasons:
        result.rejection_reasons.append("pressure_safety_abort")
    return result


def _collect_sample(
    pace: Any,
    *,
    plan: DynamicTrialPlan,
    dewpoint: Any = None,
    pressure_gauge: Any = None,
    analyzer: Any = None,
    actual_open_valves: Sequence[int] = (),
) -> dict[str, Any]:
    ts = time.time()
    dew_row: Mapping[str, Any] = {}
    if dewpoint is not None:
        try:
            dew_raw = dewpoint.get_current(timeout_s=0.35, attempts=1)
            dew_row = dew_raw if isinstance(dew_raw, Mapping) else {}
        except Exception:
            dew_row = {}
    analyzer_row: Mapping[str, Any] = {}
    if analyzer is not None:
        try:
            parsed = analyzer.read()
            analyzer_row = parsed if isinstance(parsed, Mapping) else {}
        except Exception:
            analyzer_row = {}
    gauge_pressure = None
    if pressure_gauge is not None:
        try:
            gauge_pressure = pressure_gauge.read_pressure()
        except Exception:
            gauge_pressure = None
    pace_pressure, pace_pressure_source = read_pace_pressure_hpa(pace)
    return {
        "ts": ts,
        "trial_id": plan.trial_id,
        "mode_requested": plan.mode_requested,
        "target_hpa": plan.target_hpa,
        "gas_ppm": plan.gas_ppm,
        "phase": "open_flow_dynamic_pressure",
        "open_flow_route_active": True,
        "route_sealed": False,
        "outp_state": _parse_scpi_value_float(_query_text(pace, ":OUTP:STAT?")),
        "outp_mode": _query_text(pace, ":OUTP:MODE?"),
        "vent_status": _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:LEV:IMM:AMPL:VENT?")),
        "isolation_state": _parse_scpi_value_float(_query_text(pace, ":OUTP:ISOL:STAT?")),
        "setpoint_hpa": plan.target_hpa,
        "pace_pressure_hpa": pace_pressure,
        "pace_pressure_source": pace_pressure_source,
        "com22_pressure_hpa": gauge_pressure,
        "sour_pres_eff_pct": _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:EFF?")),
        "sour_pres_comp1_hpa": _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:COMP1?")),
        "sour_pres_comp2_hpa": _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:COMP2?")),
        "source_pressure_range": _query_text(pace, ":SOUR:PRES:RANG?"),
        "sense_pressure_range": _query_text(pace, ":SENS:PRES:RANG?"),
        "sour_pres_slew_hpa_per_s": _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:SLEW?")),
        "sens_pressure_slew_hpa_per_s": _parse_scpi_value_float(_query_text(pace, ":SENS:PRES:SLEW?")),
        "slew_mode": _query_text(pace, ":SOUR:PRES:SLEW:MODE?"),
        "slew_over": _query_text(pace, ":SOUR:PRES:SLEW:OVER?"),
        "slew_value_max_requested": bool(plan.slew_value_max),
        "control_fastest_profile_requested": bool(plan.slew_mode == "MAX" and plan.slew_value_max),
        "vent_rate": _query_text(pace, ":SOUR:PRES:LEV:IMM:AMPL:VENT:RATE?"),
        "vent_rate_unit": _query_text(pace, ":SOUR:PRES:LEV:IMM:AMPL:VENT:UNIT?"),
        "dewpoint_c": _as_float(dew_row.get("dewpoint_c")),
        "dewpoint_ts": dew_row.get("ts") or dew_row.get("timestamp") or ts,
        "dewpoint_age_ms": 0.0 if dew_row else "",
        "dewpoint_source": "live_dewpoint_meter" if dew_row else "",
        "analyzer_co2_ppm": _as_float(analyzer_row.get("co2_ppm")),
        "analyzer_h2o_mmol": _as_float(analyzer_row.get("h2o_mmol")),
        "analyzer_ts": analyzer_row.get("ts") or ts if analyzer_row else "",
        "analyzer_age_ms": 0.0 if analyzer_row else "",
        "analyzer_source": "live_analyzer" if analyzer_row else "",
        "actual_open_valves": ",".join(str(value) for value in actual_open_valves),
        "syst_err": _query_text(pace, ":SYST:ERR?"),
        "pressure_safety_abort": False,
        "pressure_safety_abort_reason": "",
        "precheck_abort_phase": "",
        "source_pressure_rise_abort": False,
        "source_pressure_rise_abort_reason": "",
        "pressure_transient_allowed": False,
        "pressure_transient_elapsed_s": "",
        "pressure_soft_limit_hpa": "",
        "pressure_transient_limit_hpa": "",
        "control_command_confirmed": "",
        "control_outp_state_after_command": "",
        "control_setpoint_after_command_hpa": "",
        "control_vent_status_after_command": "",
        "control_pressure_after_command_hpa": "",
        "control_eff_after_command_pct": "",
        "control_syst_err_after_command": "",
        "control_keepalive_checked": False,
        "control_output_dropout_seen": False,
        "control_output_reasserted": False,
        "control_output_reassert_count": 0,
        "rich_telemetry_collected": True,
        "rich_telemetry_reason": "standard_sample",
        "rich_telemetry_interval_s": "",
        "rich_telemetry_initial_delay_s": "",
        "fast_pressure_loop_interval_s": "",
        "fast_pressure_sample_index": "",
        "runaway_detected_elapsed_s": "",
        "pace_vent_hold_during_outp1_allowed": False,
        "open_flow_atmosphere_hold_active": _pace_atmosphere_hold_active(pace),
        "open_flow_atmosphere_hold_strategy": "",
        "atmosphere_hold_stopped_before_control": False,
    }


def _collect_fast_pressure_sample(
    pace: Any,
    *,
    plan: DynamicTrialPlan,
    actual_open_valves: Sequence[int] = (),
) -> dict[str, Any]:
    ts = time.time()
    pace_pressure, pace_pressure_source = read_pace_pressure_hpa(pace)
    return {
        "ts": ts,
        "trial_id": plan.trial_id,
        "mode_requested": plan.mode_requested,
        "target_hpa": plan.target_hpa,
        "gas_ppm": plan.gas_ppm,
        "phase": "open_flow_dynamic_pressure_fast_control",
        "open_flow_route_active": True,
        "route_sealed": False,
        "outp_state": "",
        "outp_mode": plan.mode_requested,
        "vent_status": "",
        "isolation_state": "",
        "setpoint_hpa": plan.target_hpa,
        "pace_pressure_hpa": pace_pressure,
        "pace_pressure_source": pace_pressure_source,
        "com22_pressure_hpa": "",
        "sour_pres_eff_pct": "",
        "sour_pres_comp1_hpa": "",
        "sour_pres_comp2_hpa": "",
        "source_pressure_range": "",
        "sense_pressure_range": "",
        "sour_pres_slew_hpa_per_s": "",
        "sens_pressure_slew_hpa_per_s": "",
        "slew_mode": plan.slew_mode or "",
        "slew_over": 1 if plan.overshoot_allowed else 0 if plan.overshoot_allowed is not None else "",
        "slew_value_max_requested": bool(plan.slew_value_max),
        "control_fastest_profile_requested": bool(plan.slew_mode == "MAX" and plan.slew_value_max),
        "vent_rate": "",
        "vent_rate_unit": "",
        "dewpoint_c": "",
        "dewpoint_ts": "",
        "dewpoint_age_ms": "",
        "dewpoint_source": "",
        "analyzer_co2_ppm": "",
        "analyzer_h2o_mmol": "",
        "analyzer_ts": "",
        "analyzer_age_ms": "",
        "analyzer_source": "",
        "actual_open_valves": ",".join(str(value) for value in actual_open_valves),
        "syst_err": "",
        "pressure_safety_abort": False,
        "pressure_safety_abort_reason": "",
        "precheck_abort_phase": "",
        "source_pressure_rise_abort": False,
        "source_pressure_rise_abort_reason": "",
        "pressure_transient_allowed": False,
        "pressure_transient_elapsed_s": "",
        "pressure_soft_limit_hpa": "",
        "pressure_transient_limit_hpa": "",
        "control_command_confirmed": "",
        "control_outp_state_after_command": "",
        "control_setpoint_after_command_hpa": "",
        "control_vent_status_after_command": "",
        "control_pressure_after_command_hpa": "",
        "control_eff_after_command_pct": "",
        "control_syst_err_after_command": "",
        "control_keepalive_checked": False,
        "control_output_dropout_seen": False,
        "control_output_reasserted": False,
        "control_output_reassert_count": 0,
        "rich_telemetry_collected": False,
        "rich_telemetry_reason": "",
        "rich_telemetry_interval_s": "",
        "rich_telemetry_initial_delay_s": "",
        "fast_pressure_loop_interval_s": "",
        "fast_pressure_sample_index": "",
        "runaway_detected_elapsed_s": "",
        "pace_vent_hold_during_outp1_allowed": False,
        "open_flow_atmosphere_hold_active": False,
        "open_flow_atmosphere_hold_strategy": "disabled_for_direct_control",
        "atmosphere_hold_stopped_before_control": True,
    }


def refresh_direct_control_keepalive(
    pace: Any,
    row: MutableMapping[str, Any],
    *,
    plan: DynamicTrialPlan,
    state: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    row["control_keepalive_checked"] = True
    confirmation = _confirm_control_command_state(pace, target_hpa=plan.target_hpa)
    row["outp_state"] = confirmation.get("control_outp_state_after_command")
    row["vent_status"] = confirmation.get("control_vent_status_after_command")
    row["setpoint_hpa"] = confirmation.get("control_setpoint_after_command_hpa") or plan.target_hpa
    row["sour_pres_eff_pct"] = confirmation.get("control_eff_after_command_pct")
    row["syst_err"] = confirmation.get("control_syst_err_after_command")
    row["sour_pres_comp1_hpa"] = _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:COMP1?"))
    row["sour_pres_comp2_hpa"] = _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:COMP2?"))
    row["source_pressure_range"] = _query_text(pace, ":SOUR:PRES:RANG?")
    row["sense_pressure_range"] = _query_text(pace, ":SENS:PRES:RANG?")
    row["sour_pres_slew_hpa_per_s"] = _parse_scpi_value_float(_query_text(pace, ":SOUR:PRES:SLEW?"))
    row["sens_pressure_slew_hpa_per_s"] = _parse_scpi_value_float(_query_text(pace, ":SENS:PRES:SLEW?"))
    row["slew_mode"] = _query_text(pace, ":SOUR:PRES:SLEW:MODE?")
    row["slew_over"] = _query_text(pace, ":SOUR:PRES:SLEW:OVER?")
    row["slew_value_max_requested"] = bool(plan.slew_value_max)
    row["control_fastest_profile_requested"] = bool(plan.slew_mode == "MAX" and plan.slew_value_max)
    row.update(confirmation)
    row["control_output_dropout_seen"] = False
    row["control_output_reasserted"] = False
    if plan.target_hpa is not None and not confirmation.get("control_command_confirmed"):
        state["dropout_seen"] = True
        row["control_output_dropout_seen"] = True
        retry = _enable_control_output_confirmed(pace, target_hpa=plan.target_hpa, timeout_s=2.0, poll_s=0.2)
        state["reassert_count"] = int(state.get("reassert_count") or 0) + 1
        row["control_output_reasserted"] = True
        row.update(retry)
        row["outp_state"] = retry.get("control_outp_state_after_command")
        row["vent_status"] = retry.get("control_vent_status_after_command")
        row["setpoint_hpa"] = retry.get("control_setpoint_after_command_hpa") or plan.target_hpa
        row["sour_pres_eff_pct"] = retry.get("control_eff_after_command_pct")
        row["syst_err"] = retry.get("control_syst_err_after_command")
    row["control_output_reassert_count"] = int(state.get("reassert_count") or 0)
    return row


def annotate_fast_pressure_loop_row(
    row: MutableMapping[str, Any],
    *,
    sample_index: int,
    sample_interval_s: float,
    rich_telemetry_interval_s: float,
    rich_telemetry_initial_delay_s: float,
) -> MutableMapping[str, Any]:
    row["fast_pressure_sample_index"] = int(sample_index)
    row["fast_pressure_loop_interval_s"] = float(sample_interval_s)
    row["rich_telemetry_interval_s"] = float(rich_telemetry_interval_s)
    row["rich_telemetry_initial_delay_s"] = float(rich_telemetry_initial_delay_s)
    row.setdefault("rich_telemetry_collected", False)
    row.setdefault("rich_telemetry_reason", "")
    return row


def maybe_refresh_direct_control_rich_telemetry(
    pace: Any,
    row: MutableMapping[str, Any],
    *,
    plan: DynamicTrialPlan,
    state: MutableMapping[str, Any],
    source_open_ts: float | None,
    rich_telemetry_interval_s: float,
    rich_telemetry_initial_delay_s: float,
    reason: str = "periodic",
) -> MutableMapping[str, Any]:
    """Attach slow SCPI telemetry only after the fast safety window is clear.

    Full PACE telemetry requires several serial queries. During open-flow dynamic
    pressure tests that can hide a fast runaway, so pressure safety is evaluated
    from the PACE fast read first; rich telemetry is sampled sparsely afterward.
    """

    row["rich_telemetry_collected"] = False
    row["rich_telemetry_reason"] = ""
    interval_s = max(0.0, float(rich_telemetry_interval_s))
    initial_delay_s = max(0.0, float(rich_telemetry_initial_delay_s))
    row["rich_telemetry_interval_s"] = interval_s
    row["rich_telemetry_initial_delay_s"] = initial_delay_s
    if interval_s <= 0.0 or plan.mode_requested == "OUTP0":
        return row
    ts = _as_float(row.get("ts")) or time.time()
    if source_open_ts is not None and ts - float(source_open_ts) < initial_delay_s:
        return row
    last_ts = _as_float(state.get("last_rich_telemetry_ts"))
    if last_ts is not None and ts - last_ts < interval_s:
        return row
    refresh_direct_control_keepalive(pace, row, plan=plan, state=state)
    row["rich_telemetry_collected"] = True
    row["rich_telemetry_reason"] = reason
    state["last_rich_telemetry_ts"] = time.time()
    return row


def run_offline_plan(
    *,
    output_dir: Path,
    targets_hpa: Sequence[float],
    ambient_hpa: float,
    gas_ppm: int = DEFAULT_GAS_PPM,
    include_pass: bool = False,
    include_gaug: bool = False,
    include_over1: bool = False,
    only_over1: bool = False,
    only_gaug: bool = False,
    set_slew_value_max: bool = False,
    include_outp0_baseline: bool = True,
) -> dict[str, Any]:
    plans = build_default_trial_plan(
        targets_hpa,
        ambient_hpa=ambient_hpa,
        gas_ppm=gas_ppm,
        include_pass=include_pass,
        include_gaug=include_gaug,
        include_over1=include_over1,
        only_over1=only_over1,
        only_gaug=only_gaug,
        set_slew_value_max=set_slew_value_max,
        include_outp0_baseline=include_outp0_baseline,
    )
    command_rows = [
        {
            "trial_id": plan.trial_id,
            "label": plan.label,
            "mode_requested": plan.mode_requested,
            "target_hpa": plan.target_hpa,
            "commands": planned_commands_for_trial(plan),
        }
        for plan in plans
    ]
    payload = {
        "tool": "run_v1_5_open_flow_dynamic_pressure_diagnostic",
        "created_at": _now_iso(),
        "diagnostic_only": True,
        "not_real_acceptance_evidence": True,
        "gas_ppm": int(gas_ppm),
        "targets_hpa": list(targets_hpa),
        "ambient_hpa": float(ambient_hpa),
        "uses_1100": any(abs(float(target) - 1100.0) < 0.001 for target in targets_hpa),
        "route_strategy": "open_flow_dynamic_pressure",
        "pace_vent_control_strategy": "do_not_use_pace_vent_for_sampling_control",
        "set_slew_value_max": bool(set_slew_value_max),
        "include_over1": bool(include_over1),
        "only_over1": bool(only_over1),
        "only_gaug": bool(only_gaug),
        "trial_plan": [asdict(plan) for plan in plans],
        "planned_commands": command_rows,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "open_flow_dynamic_pressure_plan.json", payload)
    return payload


def _build_live_devices(
    cfg: Mapping[str, Any],
    *,
    analyzer_label: str | None = None,
    use_pressure_gauge_secondary: bool = False,
    pace_timeout_s: float | None = None,
) -> dict[str, Any]:
    from gas_calibrator.devices import DewpointMeter, GasAnalyzer, Pace5000, ParoscientificGauge, RelayController

    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    built: dict[str, Any] = {}
    pace_cfg = devices_cfg.get("pressure_controller", {})
    gauge_cfg = devices_cfg.get("pressure_gauge", {})
    dew_cfg = devices_cfg.get("dewpoint_meter", {})
    relay_cfg = devices_cfg.get("relay", {})
    relay8_cfg = devices_cfg.get("relay_8", {})
    try:
        built["pace"] = Pace5000(
            str(pace_cfg.get("port", "COM23")),
            int(pace_cfg.get("baud", 9600)),
            timeout=float(pace_timeout_s if pace_timeout_s is not None else pace_cfg.get("timeout", 1.0)),
        )
        built["pace"].open()
        if use_pressure_gauge_secondary and isinstance(gauge_cfg, Mapping) and gauge_cfg.get("enabled", True):
            built["pressure_gauge"] = ParoscientificGauge(
                str(gauge_cfg.get("port", "COM22")),
                int(gauge_cfg.get("baud", 9600)),
                timeout=float(gauge_cfg.get("timeout", 1.0)),
                dest_id=str(gauge_cfg.get("dest_id", "01")),
            )
            built["pressure_gauge"].open()
        if isinstance(dew_cfg, Mapping) and dew_cfg.get("enabled", True):
            built["dewpoint"] = DewpointMeter(
                str(dew_cfg.get("port", "COM17")),
                baudrate=int(dew_cfg.get("baud", 9600)),
                station=str(dew_cfg.get("station", "001")),
            )
            built["dewpoint"].open()
        if isinstance(relay_cfg, Mapping) and relay_cfg.get("enabled", True):
            built["relay"] = RelayController(
                str(relay_cfg.get("port", "COM20")),
                int(relay_cfg.get("baud", 38400)),
                addr=int(relay_cfg.get("addr", 1)),
            )
            built["relay"].open()
        if isinstance(relay8_cfg, Mapping) and relay8_cfg.get("enabled", True):
            built["relay_8"] = RelayController(
                str(relay8_cfg.get("port", "COM21")),
                int(relay8_cfg.get("baud", 38400)),
                addr=int(relay8_cfg.get("addr", 1)),
            )
            built["relay_8"].open()
        analyzer_cfg = _select_analyzer_cfg(cfg, analyzer_label)
        if analyzer_cfg:
            built["analyzer"] = GasAnalyzer(
                str(analyzer_cfg.get("port")),
                int(analyzer_cfg.get("baud", 115200)),
                device_id=str(analyzer_cfg.get("device_id", "001")),
            )
            built["analyzer"].open()
    except Exception:
        close_devices(built)
        raise
    return built


def _select_analyzer_cfg(cfg: Mapping[str, Any], analyzer_label: str | None) -> Mapping[str, Any] | None:
    if not analyzer_label:
        return None
    wanted = str(analyzer_label).strip().lower()
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    gas_list = devices_cfg.get("gas_analyzers", [])
    if isinstance(gas_list, list):
        for index, item in enumerate(gas_list, start=1):
            if not isinstance(item, Mapping) or not item.get("enabled", True):
                continue
            name = str(item.get("name") or f"ga{index:02d}").lower()
            aliases = {name, f"ga{index:02d}", str(item.get("device_id") or "").strip().lower(), str(index)}
            if wanted in aliases:
                return item
    single = devices_cfg.get("gas_analyzer", {})
    if isinstance(single, Mapping) and single.get("enabled", True) and wanted in {"ga01", "primary", "gas_analyzer"}:
        return single
    return None


def close_devices(devices: Mapping[str, Any]) -> None:
    seen: set[int] = set()
    for item in devices.values():
        if not hasattr(item, "close"):
            continue
        obj_id = id(item)
        if obj_id in seen:
            continue
        seen.add(obj_id)
        try:
            item.close()
        except Exception:
            pass


def run_real_com_diagnostic(
    *,
    config_path: Path,
    output_dir: Path,
    targets_hpa: Sequence[float],
    ambient_hpa: float,
    gas_ppm: int = DEFAULT_GAS_PPM,
    analyzer_label: str | None = None,
    sample_count: int = DEFAULT_SAMPLE_COUNT,
    sample_interval_s: float = DEFAULT_SAMPLE_INTERVAL_S,
    rich_telemetry_interval_s: float = DEFAULT_RICH_TELEMETRY_INTERVAL_S,
    rich_telemetry_initial_delay_s: float = DEFAULT_RICH_TELEMETRY_INITIAL_DELAY_S,
    max_control_s: float = DEFAULT_MAX_CONTROL_S,
    max_safe_pressure_hpa: float = DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    source_max_rise_hpa: float = DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA,
    transient_grace_s: float = DEFAULT_OPEN_FLOW_TRANSIENT_GRACE_S,
    transient_limit_hpa: float = DEFAULT_OPEN_FLOW_TRANSIENT_LIMIT_HPA,
    open_flow_atmosphere_hold: bool = True,
    open_flow_atmosphere_hold_interval_s: float = DEFAULT_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S,
    include_pass: bool = False,
    include_gaug: bool = False,
    include_over1: bool = False,
    only_over1: bool = False,
    only_gaug: bool = False,
    set_slew_value_max: bool = False,
    use_pressure_gauge_secondary: bool = False,
    pace_timeout_s: float | None = None,
    direct_control_only: bool = False,
    keep_atmosphere_hold_during_direct_control: bool = False,
    restore_baseline: bool = True,
) -> dict[str, Any]:
    cfg = load_config(config_path)
    plans = build_default_trial_plan(
        targets_hpa,
        ambient_hpa=ambient_hpa,
        gas_ppm=gas_ppm,
        include_pass=include_pass,
        include_gaug=include_gaug,
        include_over1=include_over1,
        only_over1=only_over1,
        only_gaug=only_gaug,
        set_slew_value_max=set_slew_value_max,
        include_outp0_baseline=not direct_control_only,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    devices = _build_live_devices(
        cfg,
        analyzer_label=analyzer_label,
        use_pressure_gauge_secondary=use_pressure_gauge_secondary,
        pace_timeout_s=pace_timeout_s,
    )
    samples: list[dict[str, Any]] = []
    results: list[DynamicTrialResult] = []
    route = resolve_0ppm_open_flow_valves(cfg, gas_ppm=gas_ppm)
    original_mode = ""
    sample_csv_path = output_dir / "open_flow_dynamic_pressure_samples.csv"
    result_csv_path = output_dir / "open_flow_dynamic_pressure_trial_results.csv"
    abort_all = False
    abort_reason = ""
    precheck_abort_phase = ""
    source_rise_abort = False
    source_rise_abort_reason = ""
    atmosphere_hold_info: dict[str, Any] = {
        "requested": bool(open_flow_atmosphere_hold),
        "active": False,
        "strategy": "disabled",
        "interval_s": "",
        "error": "",
    }
    atmosphere_hold_stopped_before_control = False
    try:
        pace = devices["pace"]
        try:
            pace.set_units_hpa()
        except Exception:
            pass
        try:
            original_mode = pace.get_output_mode()
        except Exception:
            original_mode = "ACT"
        if open_flow_atmosphere_hold and (not direct_control_only or keep_atmosphere_hold_during_direct_control):
            atmosphere_hold_info = start_open_flow_atmosphere_hold(
                pace,
                interval_s=open_flow_atmosphere_hold_interval_s,
            )
        if direct_control_only:
            apply_logical_valves(cfg, devices, route.get("path_open_logical_valves") or [])
        else:
            apply_logical_valves(cfg, devices, route.get("path_open_logical_valves") or [])
            path_precheck_plan = DynamicTrialPlan(
                trial_id="open_flow_path_precheck_no_source",
                label="open-flow path precheck without gas source",
                mode_requested="OUTP0_PATH_PRECHECK",
                target_hpa=None,
                gas_ppm=int(gas_ppm),
                slew_mode=None,
                overshoot_allowed=None,
                outp1_sent=False,
            )
            row = _collect_sample(
                pace,
                plan=path_precheck_plan,
                dewpoint=devices.get("dewpoint"),
                pressure_gauge=devices.get("pressure_gauge"),
                analyzer=devices.get("analyzer"),
                actual_open_valves=route.get("path_open_logical_valves") or [],
            )
            row["open_flow_atmosphere_hold_active"] = bool(atmosphere_hold_info.get("active"))
            row["open_flow_atmosphere_hold_strategy"] = atmosphere_hold_info.get("strategy") or ""
            samples.append(row)
            safety_pressure = _row_pressure_for_safety(row)
            if row_exceeds_open_flow_pressure_safety(row, max_safe_pressure_hpa):
                precheck_abort_phase = "path_precheck_no_source"
                abort_reason = (
                    f"open_flow_pressure_safety_abort:"
                    f"{float(safety_pressure or 0.0):.3f}>{float(max_safe_pressure_hpa):.3f}"
                )
                row["pressure_safety_abort"] = True
                row["pressure_safety_abort_reason"] = abort_reason
                row["precheck_abort_phase"] = precheck_abort_phase
                _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                result = summarize_samples(
                    [row],
                    plan=path_precheck_plan,
                    ambient_hpa=ambient_hpa,
                    candidate_ts=None,
                    candidate_pressure_hpa=None,
                    outp1_ts=None,
                    mode_confirmed=path_precheck_plan.mode_requested,
                )
                result = _mark_pressure_safety_abort(result, abort_reason)
                results.append(result)
                _append_csv_row(result_csv_path, RESULT_FIELDS, asdict(result))
                _safe_abort_pace(pace)
                abort_all = True
            else:
                _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
        if not abort_all and not direct_control_only:
            apply_logical_valves(cfg, devices, route["open_logical_valves"])
        for plan in plans:
            if abort_all:
                break
            mode_confirmed = ""
            candidate_ts = None
            candidate_pressure = None
            outp1_ts = None
            source_open_ts = None
            control_confirmation: dict[str, Any] = {}
            control_keepalive_state: dict[str, Any] = {
                "dropout_seen": False,
                "reassert_count": 0,
                "last_rich_telemetry_ts": None,
            }
            trial_samples: list[dict[str, Any]] = []
            fast_pressure_sample_index = 0
            if plan.mode_requested == "OUTP0":
                pace.set_output(False)
                deadline = time.time() + min(10.0, max_control_s)
            else:
                if (
                    open_flow_atmosphere_hold
                    and not atmosphere_hold_stopped_before_control
                ):
                    stop_info = stop_open_flow_atmosphere_hold_before_control(
                        pace,
                        force_output_off=not direct_control_only,
                    )
                    atmosphere_hold_stopped_before_control = True
                    if stop_info.get("error"):
                        abort_reason = f"open_flow_atmosphere_hold_stop_failed:{stop_info.get('error')}"
                        abort_all = True
                        break
                if plan.mode_requested == "ACT":
                    pace.set_output_mode_active()
                elif plan.mode_requested == "PASS":
                    pace.set_output_mode_passive()
                elif plan.mode_requested == "GAUG":
                    pace.write(":OUTP:MODE GAUG")
                try:
                    mode_confirmed = pace.get_output_mode()
                except Exception as exc:
                    mode_confirmed = f"ERROR:{exc}"
                if plan.overshoot_allowed is not None:
                    pace.set_overshoot_allowed(bool(plan.overshoot_allowed))
                if plan.slew_value_max:
                    pace.write(":SOUR:PRES:SLEW max")
                if plan.slew_mode == "MAX":
                    pace.set_slew_mode_max()
                elif plan.slew_mode == "LIN":
                    pace.set_slew_mode_linear()
                pace.set_setpoint(float(plan.target_hpa))
                control_confirmation = _enable_control_output_confirmed(pace, target_hpa=plan.target_hpa)
                outp1_ts = time.time()
                if direct_control_only and not control_confirmation.get("control_command_confirmed"):
                    abort_reason = "pace_control_output_not_confirmed_before_source"
                    row = _collect_fast_pressure_sample(
                        pace,
                        plan=plan,
                        actual_open_valves=route.get("path_open_logical_valves") or [],
                    )
                    row["open_flow_atmosphere_hold_active"] = bool(
                        open_flow_atmosphere_hold and not atmosphere_hold_stopped_before_control
                    )
                    row["open_flow_atmosphere_hold_strategy"] = atmosphere_hold_info.get("strategy") or ""
                    row["atmosphere_hold_stopped_before_control"] = atmosphere_hold_stopped_before_control
                    row.update(control_confirmation)
                    row["pressure_safety_abort"] = True
                    row["pressure_safety_abort_reason"] = abort_reason
                    samples.append(row)
                    trial_samples.append(row)
                    _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                    result = summarize_samples(
                        trial_samples,
                        plan=plan,
                        ambient_hpa=ambient_hpa,
                        candidate_ts=None,
                        candidate_pressure_hpa=None,
                        outp1_ts=outp1_ts,
                        mode_confirmed=mode_confirmed or plan.mode_requested,
                    )
                    result = _mark_pressure_safety_abort(result, abort_reason)
                    results.append(result)
                    _append_csv_row(result_csv_path, RESULT_FIELDS, asdict(result))
                    _safe_abort_pace(pace)
                    abort_all = True
                    break
                if direct_control_only:
                    apply_logical_valves(cfg, devices, route["open_logical_valves"])
                    source_open_ts = time.time()
                deadline = outp1_ts + max_control_s
            while time.time() < deadline:
                if direct_control_only:
                    row = _collect_fast_pressure_sample(
                        pace,
                        plan=plan,
                        actual_open_valves=route["open_logical_valves"],
                    )
                else:
                    row = _collect_sample(
                        pace,
                        plan=plan,
                        dewpoint=devices.get("dewpoint"),
                        pressure_gauge=devices.get("pressure_gauge"),
                        analyzer=devices.get("analyzer"),
                        actual_open_valves=route["open_logical_valves"],
                    )
                row["open_flow_atmosphere_hold_active"] = bool(
                    open_flow_atmosphere_hold and not atmosphere_hold_stopped_before_control
                )
                row["open_flow_atmosphere_hold_strategy"] = atmosphere_hold_info.get("strategy") or ""
                row["atmosphere_hold_stopped_before_control"] = atmosphere_hold_stopped_before_control
                if direct_control_only:
                    fast_pressure_sample_index += 1
                    annotate_fast_pressure_loop_row(
                        row,
                        sample_index=fast_pressure_sample_index,
                        sample_interval_s=sample_interval_s,
                        rich_telemetry_interval_s=rich_telemetry_interval_s,
                        rich_telemetry_initial_delay_s=max(
                            float(rich_telemetry_initial_delay_s),
                            float(transient_grace_s),
                        ),
                    )
                row.update(control_confirmation)
                samples.append(row)
                trial_samples.append(row)
                safety_pressure = _row_pressure_for_safety(row)
                transient_elapsed = (
                    max(0.0, float(row["ts"]) - float(source_open_ts))
                    if direct_control_only and source_open_ts is not None
                    else None
                )
                row["pressure_transient_elapsed_s"] = transient_elapsed if transient_elapsed is not None else ""
                row["pressure_soft_limit_hpa"] = float(max_safe_pressure_hpa)
                row["pressure_transient_limit_hpa"] = float(transient_limit_hpa)
                row["pressure_transient_allowed"] = bool(
                    direct_control_only
                    and transient_elapsed is not None
                    and safety_pressure is not None
                    and float(max_safe_pressure_hpa) < float(safety_pressure) <= float(transient_limit_hpa)
                    and transient_elapsed <= max(0.0, float(transient_grace_s))
                )
                abort_reason = open_flow_pressure_abort_reason(
                    row,
                    max_safe_pressure_hpa=max_safe_pressure_hpa,
                    transient_limit_hpa=transient_limit_hpa,
                    transient_grace_s=transient_grace_s if direct_control_only else 0.0,
                    transient_elapsed_s=transient_elapsed,
                )
                if abort_reason:
                    row["pressure_safety_abort"] = True
                    row["pressure_safety_abort_reason"] = abort_reason
                    _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                    _safe_abort_pace(pace)
                    abort_all = True
                    break
                runaway_reason = open_flow_dynamic_control_runaway_reason(
                    row,
                    target_hpa=plan.target_hpa,
                    max_rise_hpa=source_max_rise_hpa,
                    transient_grace_s=transient_grace_s if direct_control_only else 0.0,
                    transient_elapsed_s=transient_elapsed,
                )
                if runaway_reason:
                    source_rise_abort = True
                    source_rise_abort_reason = runaway_reason
                    row["source_pressure_rise_abort"] = True
                    row["source_pressure_rise_abort_reason"] = runaway_reason
                    row["runaway_detected_elapsed_s"] = transient_elapsed if transient_elapsed is not None else ""
                    _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                    _safe_abort_pace(pace)
                    abort_all = True
                    break
                if (
                    plan.mode_requested == "OUTP0"
                    and row_exceeds_open_flow_source_rise(
                        row,
                        ambient_hpa=ambient_hpa,
                        max_rise_hpa=source_max_rise_hpa,
                    )
                ):
                    source_rise_abort = True
                    source_rise_abort_reason = (
                        f"open_flow_source_pressure_rise_abort:"
                        f"{float(safety_pressure or 0.0):.3f}>"
                        f"{float(ambient_hpa) + max(0.0, float(source_max_rise_hpa)):.3f}"
                    )
                    row["source_pressure_rise_abort"] = True
                    row["source_pressure_rise_abort_reason"] = source_rise_abort_reason
                    row["runaway_detected_elapsed_s"] = transient_elapsed if transient_elapsed is not None else ""
                    _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                    _safe_abort_pace(pace)
                    abort_all = True
                    break
                if direct_control_only and plan.mode_requested != "OUTP0":
                    maybe_refresh_direct_control_rich_telemetry(
                        pace,
                        row,
                        plan=plan,
                        state=control_keepalive_state,
                        source_open_ts=source_open_ts,
                        rich_telemetry_interval_s=rich_telemetry_interval_s,
                        rich_telemetry_initial_delay_s=max(
                            float(rich_telemetry_initial_delay_s),
                            float(transient_grace_s),
                        ),
                    )
                _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                pressure = _as_float(row.get("pace_pressure_hpa"))
                if plan.target_hpa is not None and pressure is not None:
                    if (
                        float(plan.target_hpa) - DEFAULT_CANDIDATE_WINDOW_LOW_HPA
                        <= pressure
                        <= float(plan.target_hpa) + DEFAULT_CANDIDATE_WINDOW_HIGH_HPA
                    ):
                        candidate_ts = float(row["ts"])
                        candidate_pressure = pressure
                        break
                    if pressure < float(plan.target_hpa) - DEFAULT_CANDIDATE_WINDOW_LOW_HPA:
                        candidate_ts = None
                        candidate_pressure = None
                        break
                time.sleep(max(0.0, sample_interval_s))
            for _ in range(max(0, int(sample_count) - len(trial_samples))):
                if abort_all:
                    break
                if time.time() >= deadline:
                    break
                if direct_control_only:
                    row = _collect_fast_pressure_sample(
                        pace,
                        plan=plan,
                        actual_open_valves=route["open_logical_valves"],
                    )
                else:
                    row = _collect_sample(
                        pace,
                        plan=plan,
                        dewpoint=devices.get("dewpoint"),
                        pressure_gauge=devices.get("pressure_gauge"),
                        analyzer=devices.get("analyzer"),
                        actual_open_valves=route["open_logical_valves"],
                    )
                row["open_flow_atmosphere_hold_active"] = bool(
                    open_flow_atmosphere_hold and not atmosphere_hold_stopped_before_control
                )
                row["open_flow_atmosphere_hold_strategy"] = atmosphere_hold_info.get("strategy") or ""
                row["atmosphere_hold_stopped_before_control"] = atmosphere_hold_stopped_before_control
                if direct_control_only:
                    fast_pressure_sample_index += 1
                    annotate_fast_pressure_loop_row(
                        row,
                        sample_index=fast_pressure_sample_index,
                        sample_interval_s=sample_interval_s,
                        rich_telemetry_interval_s=rich_telemetry_interval_s,
                        rich_telemetry_initial_delay_s=max(
                            float(rich_telemetry_initial_delay_s),
                            float(transient_grace_s),
                        ),
                    )
                row.update(control_confirmation)
                samples.append(row)
                trial_samples.append(row)
                safety_pressure = _row_pressure_for_safety(row)
                transient_elapsed = (
                    max(0.0, float(row["ts"]) - float(source_open_ts))
                    if direct_control_only and source_open_ts is not None
                    else None
                )
                row["pressure_transient_elapsed_s"] = transient_elapsed if transient_elapsed is not None else ""
                row["pressure_soft_limit_hpa"] = float(max_safe_pressure_hpa)
                row["pressure_transient_limit_hpa"] = float(transient_limit_hpa)
                row["pressure_transient_allowed"] = bool(
                    direct_control_only
                    and transient_elapsed is not None
                    and safety_pressure is not None
                    and float(max_safe_pressure_hpa) < float(safety_pressure) <= float(transient_limit_hpa)
                    and transient_elapsed <= max(0.0, float(transient_grace_s))
                )
                abort_reason = open_flow_pressure_abort_reason(
                    row,
                    max_safe_pressure_hpa=max_safe_pressure_hpa,
                    transient_limit_hpa=transient_limit_hpa,
                    transient_grace_s=transient_grace_s if direct_control_only else 0.0,
                    transient_elapsed_s=transient_elapsed,
                )
                if abort_reason:
                    row["pressure_safety_abort"] = True
                    row["pressure_safety_abort_reason"] = abort_reason
                    _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                    _safe_abort_pace(pace)
                    abort_all = True
                    break
                runaway_reason = open_flow_dynamic_control_runaway_reason(
                    row,
                    target_hpa=plan.target_hpa,
                    max_rise_hpa=source_max_rise_hpa,
                    transient_grace_s=transient_grace_s if direct_control_only else 0.0,
                    transient_elapsed_s=transient_elapsed,
                )
                if runaway_reason:
                    source_rise_abort = True
                    source_rise_abort_reason = runaway_reason
                    row["source_pressure_rise_abort"] = True
                    row["source_pressure_rise_abort_reason"] = runaway_reason
                    row["runaway_detected_elapsed_s"] = transient_elapsed if transient_elapsed is not None else ""
                    _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                    _safe_abort_pace(pace)
                    abort_all = True
                    break
                if direct_control_only and plan.mode_requested != "OUTP0":
                    maybe_refresh_direct_control_rich_telemetry(
                        pace,
                        row,
                        plan=plan,
                        state=control_keepalive_state,
                        source_open_ts=source_open_ts,
                        rich_telemetry_interval_s=rich_telemetry_interval_s,
                        rich_telemetry_initial_delay_s=max(
                            float(rich_telemetry_initial_delay_s),
                            float(transient_grace_s),
                        ),
                    )
                _append_csv_row(sample_csv_path, TELEMETRY_FIELDS, row)
                time.sleep(max(0.0, sample_interval_s))
            result = summarize_samples(
                trial_samples,
                plan=plan,
                ambient_hpa=ambient_hpa,
                candidate_ts=candidate_ts,
                candidate_pressure_hpa=candidate_pressure,
                outp1_ts=outp1_ts,
                mode_confirmed=mode_confirmed or plan.mode_requested,
            )
            if abort_all:
                result = _mark_pressure_safety_abort(result, abort_reason)
                if source_rise_abort:
                    result.pressure_safety_abort_reason = source_rise_abort_reason
                    if "open_flow_source_pressure_rise_abort" not in result.rejection_reasons:
                        result.rejection_reasons.append("open_flow_source_pressure_rise_abort")
            results.append(result)
            _append_csv_row(result_csv_path, RESULT_FIELDS, asdict(result))
            try:
                pace.set_output(False)
            except Exception:
                pass
            if abort_all:
                break
        try:
            if original_mode == "PASS":
                pace.set_output_mode_passive()
            elif original_mode == "GAUG":
                pace.write(":OUTP:MODE GAUG")
            else:
                pace.set_output_mode_active()
        except Exception:
            pass
    finally:
        pace = devices.get("pace")
        if restore_baseline:
            try:
                apply_logical_valves(cfg, devices, [])
            except Exception:
                pass
        if pace is not None:
            try:
                _safe_abort_pace(pace)
            except Exception:
                pass
        close_devices(devices)
    result_rows = [asdict(row) for row in results]
    summary = {
        "tool": "run_v1_5_open_flow_dynamic_pressure_diagnostic",
        "run_dir": str(output_dir.resolve()),
        "created_at": _now_iso(),
        "diagnostic_only": True,
        "not_real_acceptance_evidence": True,
        "no_write_clean": True,
        "gas_ppm": int(gas_ppm),
        "gas_route": route,
        "targets_hpa": list(targets_hpa),
        "uses_1100": any(abs(float(target) - 1100.0) < 0.001 for target in targets_hpa),
        "pressure_safety_abort": abort_all,
        "pressure_safety_abort_reason": source_rise_abort_reason or abort_reason,
        "precheck_abort_phase": precheck_abort_phase,
        "source_pressure_rise_abort": source_rise_abort,
        "source_pressure_rise_abort_reason": source_rise_abort_reason,
        "max_safe_pressure_hpa": float(max_safe_pressure_hpa),
        "source_max_rise_hpa": float(source_max_rise_hpa),
        "transient_grace_s": float(transient_grace_s),
        "transient_limit_hpa": float(transient_limit_hpa),
        "primary_pressure_source": "PACE",
        "pressure_safety_source": "PACE",
        "com22_secondary_pressure_enabled": bool(use_pressure_gauge_secondary),
        "pace_serial_timeout_s": (
            float(pace_timeout_s) if pace_timeout_s is not None else ""
        ),
        "fast_pressure_loop_interval_s": float(sample_interval_s),
        "rich_telemetry_interval_s": float(rich_telemetry_interval_s),
        "rich_telemetry_initial_delay_s": float(
            max(float(rich_telemetry_initial_delay_s), float(transient_grace_s))
            if direct_control_only
            else float(rich_telemetry_initial_delay_s)
        ),
        "rich_telemetry_strategy": (
            "fast_pressure_safety_loop_with_delayed_sparse_scpi_telemetry"
            if direct_control_only
            else "standard_serial_snapshot_per_sample"
        ),
        "set_slew_value_max": bool(set_slew_value_max),
        "include_over1": bool(include_over1),
        "only_over1": bool(only_over1),
        "only_gaug": bool(only_gaug),
        "direct_control_only": bool(direct_control_only),
        "keep_atmosphere_hold_during_direct_control": bool(keep_atmosphere_hold_during_direct_control),
        "pace_vent_hold_during_outp1_allowed": False,
        "direct_control_sequence": (
            (
                "atmosphere_hold_pre_equalize -> stop_pace_vent_hold -> path_open_without_source -> "
                "setpoint/profile/outp1_confirmed -> "
                "open_source -> fast_pace_pressure_samples"
            )
            if direct_control_only and keep_atmosphere_hold_during_direct_control
            else "path_open_without_source -> setpoint/profile/outp1_confirmed -> open_source -> fast_pace_pressure_samples"
            if direct_control_only
            else "path_precheck -> source_open_outp0_observe -> setpoint_control"
        ),
        "open_flow_atmosphere_hold": atmosphere_hold_info,
        "atmosphere_hold_stopped_before_control": atmosphere_hold_stopped_before_control,
        "ranking": rank_results(results),
        "result_csv": str(result_csv_path.resolve()),
        "sample_csv": str(sample_csv_path.resolve()),
    }
    if not sample_csv_path.exists():
        _write_csv(sample_csv_path, TELEMETRY_FIELDS, samples)
    if not result_csv_path.exists():
        _write_csv(result_csv_path, RESULT_FIELDS, result_rows)
    _write_json(output_dir / "open_flow_dynamic_pressure_summary.json", {**summary, "results": result_rows})
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/site_v1_5_no_write_current_hardware_co2_20c_0ppm_limited_ambient_2sealed_skip_tempwait.json"))
    parser.add_argument("--ambient-hpa", type=float, default=DEFAULT_AMBIENT_HPA)
    parser.add_argument("--targets", nargs="+", type=float, default=list(DEFAULT_TARGETS_HPA))
    parser.add_argument("--gas-ppm", type=int, default=DEFAULT_GAS_PPM)
    parser.add_argument("--include-pass", action="store_true")
    parser.add_argument("--include-gaug", action="store_true")
    parser.add_argument("--include-over1", action="store_true", help="Add ACT + OVER1 + MAX as a diagnostic-only fastest-response comparison.")
    parser.add_argument("--only-over1", action="store_true", help="Run only ACT + OVER1 + MAX fastest diagnostic trials, skipping the OVER0 comparison.")
    parser.add_argument("--only-gaug", action="store_true", help="Run only GAUG + OVER0 + MAX diagnostic trials, skipping ACT/PASS plans.")
    parser.add_argument(
        "--set-slew-value-max",
        action="store_true",
        help="Before SLEW:MODE MAX, also send documented ':SOUR:PRES:SLEW max' as a diagnostic rate-value ceiling.",
    )
    parser.add_argument("--allow-above-ambient", action="store_true")
    parser.add_argument(
        "--direct-control-only",
        action="store_true",
        help="Skip OUTP0 wash/observe phases; set PACE target first, then open source and sample PACE pressure quickly.",
    )
    parser.add_argument(
        "--keep-atmosphere-hold-during-direct-control",
        action="store_true",
        help=(
            "Diagnostic-only compatibility flag: pre-equalize with atmosphere hold, then stop PACE vent "
            "before OUTP1 because VENT hold and pressure control are mutually exclusive."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("logs") / "v1_5_open_flow_dynamic_pressure")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--analyzer", default=None, help="Optional analyzer label, e.g. ga01. Omit to avoid analyzer serial reads.")
    parser.add_argument("--sample-count", type=int, default=DEFAULT_SAMPLE_COUNT)
    parser.add_argument("--sample-interval-s", type=float, default=DEFAULT_SAMPLE_INTERVAL_S)
    parser.add_argument(
        "--rich-telemetry-interval-s",
        type=float,
        default=DEFAULT_RICH_TELEMETRY_INTERVAL_S,
        help="Direct-control mode: collect slow SCPI telemetry no more often than this; 0 disables it.",
    )
    parser.add_argument(
        "--rich-telemetry-initial-delay-s",
        type=float,
        default=DEFAULT_RICH_TELEMETRY_INITIAL_DELAY_S,
        help="Direct-control mode: delay slow telemetry until the early pressure transient has passed.",
    )
    parser.add_argument("--max-control-s", type=float, default=DEFAULT_MAX_CONTROL_S)
    parser.add_argument("--max-safe-pressure-hpa", type=float, default=DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA)
    parser.add_argument("--source-max-rise-hpa", type=float, default=DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA)
    parser.add_argument("--transient-grace-s", type=float, default=DEFAULT_OPEN_FLOW_TRANSIENT_GRACE_S)
    parser.add_argument("--transient-limit-hpa", type=float, default=DEFAULT_OPEN_FLOW_TRANSIENT_LIMIT_HPA)
    parser.add_argument(
        "--use-com22-secondary-pressure",
        action="store_true",
        help="Optionally record COM22 as secondary evidence. PACE remains the control and safety pressure source.",
    )
    parser.add_argument(
        "--pace-timeout-s",
        type=float,
        default=None,
        help="Diagnostic-only serial timeout override for PACE fast pressure reads.",
    )
    parser.add_argument("--no-open-flow-atmosphere-hold", action="store_true")
    parser.add_argument(
        "--open-flow-atmosphere-hold-interval-s",
        type=float,
        default=DEFAULT_OPEN_FLOW_ATMOSPHERE_HOLD_INTERVAL_S,
    )
    parser.add_argument("--real-com", action="store_true")
    parser.add_argument("--i-understand-open-flow-no-write", action="store_true")
    parser.add_argument("--operator-confirm-0ppm-flow", action="store_true")
    parser.add_argument("--no-restore-baseline", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    targets = validate_dynamic_targets(
        args.targets,
        ambient_hpa=args.ambient_hpa,
        allow_above_ambient=args.allow_above_ambient,
    )
    if args.gas_ppm != 0:
        parser.error("this diagnostic defaults to 0ppm; pass gas-ppm=0 for the current approved run")
    if args.only_over1 and args.only_gaug:
        parser.error("--only-over1 and --only-gaug are mutually exclusive")
    run_root = args.output_dir / (args.run_id or f"run_{_stamp()}")
    if not args.real_com:
        payload = run_offline_plan(
            output_dir=run_root,
            targets_hpa=targets,
            ambient_hpa=args.ambient_hpa,
            gas_ppm=args.gas_ppm,
            include_pass=args.include_pass,
            include_gaug=args.include_gaug,
            include_over1=args.include_over1,
            only_over1=args.only_over1,
            only_gaug=args.only_gaug,
            set_slew_value_max=args.set_slew_value_max,
            include_outp0_baseline=not args.direct_control_only,
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    if not args.i_understand_open_flow_no_write or not args.operator_confirm_0ppm_flow:
        parser.error("--real-com requires --i-understand-open-flow-no-write and --operator-confirm-0ppm-flow")
    summary = run_real_com_diagnostic(
        config_path=args.config,
        output_dir=run_root,
        targets_hpa=targets,
        ambient_hpa=args.ambient_hpa,
        gas_ppm=args.gas_ppm,
        analyzer_label=args.analyzer,
        sample_count=args.sample_count,
        sample_interval_s=args.sample_interval_s,
        rich_telemetry_interval_s=args.rich_telemetry_interval_s,
        rich_telemetry_initial_delay_s=args.rich_telemetry_initial_delay_s,
        max_control_s=args.max_control_s,
        max_safe_pressure_hpa=args.max_safe_pressure_hpa,
        source_max_rise_hpa=args.source_max_rise_hpa,
        transient_grace_s=args.transient_grace_s,
        transient_limit_hpa=args.transient_limit_hpa,
        open_flow_atmosphere_hold=(
            not args.no_open_flow_atmosphere_hold
            and (not args.direct_control_only or args.keep_atmosphere_hold_during_direct_control)
        ),
        open_flow_atmosphere_hold_interval_s=args.open_flow_atmosphere_hold_interval_s,
        include_pass=args.include_pass,
        include_gaug=args.include_gaug,
        include_over1=args.include_over1,
        only_over1=args.only_over1,
        only_gaug=args.only_gaug,
        set_slew_value_max=args.set_slew_value_max,
        use_pressure_gauge_secondary=args.use_com22_secondary_pressure,
        pace_timeout_s=args.pace_timeout_s,
        direct_control_only=args.direct_control_only,
        keep_atmosphere_hold_during_direct_control=args.keep_atmosphere_hold_during_direct_control,
        restore_baseline=not args.no_restore_baseline,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
