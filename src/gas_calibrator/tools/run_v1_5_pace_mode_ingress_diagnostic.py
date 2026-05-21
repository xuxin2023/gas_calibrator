"""Pressure-only PACE mode ingress diagnostic.

This tool is intentionally separate from the V1.5 workflow. It does not open
CO2, does not start HGEN, does not write calibration parameters, and defaults to
offline plan generation. Real COM execution requires explicit pressure-only
no-write confirmation.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_AMBIENT_HPA = 1006.0
DEFAULT_TARGETS_HPA = (980.0, 950.0, 900.0)
DEFAULT_SAMPLE_INTERVAL_S = 0.25
DEFAULT_HOLD_BASELINE_S = 45.0
DEFAULT_MAX_CONTROL_S = 60.0
TARGET_WINDOW_HIGH_HPA = 1.0

REQUIRED_SAMPLE_FIELDS = (
    "ts",
    "trial_id",
    "target_hpa",
    "phase",
    "outp_state",
    "outp_mode",
    "vent_status",
    "isolation_state",
    "setpoint_hpa",
    "sens_pres_cont_hpa",
    "sens_pres_inl",
    "sens_pres_slew",
    "sour_pres_eff_pct",
    "sour_pres_comp1_hpa",
    "sour_pres_comp2_hpa",
    "slew_mode",
    "slew_over",
    "dewpoint_latest_c",
    "com22_pressure_latest_hpa",
    "actual_open_valves",
    "raw_tx",
    "raw_rx",
    "syst_err_after_command_group",
)

FORBIDDEN_WRITE_PATTERNS = (
    re.compile(r"(?<!\*)\bID\b(?!N\?)", re.IGNORECASE),
    re.compile(r"\bSENCO\b", re.IGNORECASE),
    re.compile(r"\bZERO\b", re.IGNORECASE),
    re.compile(r"\bSPAN\b", re.IGNORECASE),
    re.compile(r"\bCOEFF(?:ICIENT)?\b", re.IGNORECASE),
    re.compile(r"\bCAL(?:IBRATION|IBRATE)?\b", re.IGNORECASE),
)


@dataclass(frozen=True)
class TrialPlan:
    trial_id: str
    label: str
    mode_requested: str
    targets_hpa: tuple[float, ...]
    outp1_sent: bool
    slew_mode: str | None = None
    overshoot_allowed: bool | None = None
    hold_s: float = DEFAULT_HOLD_BASELINE_S
    diagnostic_only: bool = True
    not_real_acceptance_evidence: bool = True
    line_contaminated: bool = True
    next_run_requires_full_open_flow_flush: bool = True
    restores_original_mode: bool = False
    continue_control_if_unsupported: bool = False
    candidate_window_low_offset_hpa: float = 0.0
    candidate_window_high_offset_hpa: float = TARGET_WINDOW_HIGH_HPA
    required_sample_fields: tuple[str, ...] = REQUIRED_SAMPLE_FIELDS

    @property
    def over_label(self) -> str:
        if self.overshoot_allowed is None:
            return "NO_OVER"
        return "OVER1" if self.overshoot_allowed else "OVER0"

    @property
    def mode_label(self) -> str:
        parts = [self.mode_requested]
        if self.overshoot_allowed is not None:
            parts.append(self.over_label)
        if self.slew_mode:
            parts.append(self.slew_mode)
        return "_".join(parts)


@dataclass
class TrialResult:
    trial_id: str
    mode_requested: str
    mode_confirmed: str = ""
    mode_supported: bool = True
    target_hpa: float | None = None
    ambient_pressure_hpa: float = DEFAULT_AMBIENT_HPA
    target_below_ambient: bool = True
    syst_err_after_mode_set: str = "0,No error"
    outp1_sent: bool = False
    controller_on_confirmed: bool = False
    pressure_start_hpa: float | None = None
    pressure_candidate_hpa: float | None = None
    pressure_end_hpa: float | None = None
    outp1_to_candidate_s: float | None = None
    total_trial_dwell_s: float = 0.0
    pressure_drop_rate_hpa_per_s: float | None = None
    dewpoint_start_c: float | None = None
    dewpoint_candidate_c: float | None = None
    dewpoint_end_c: float | None = None
    dewpoint_delta_c: float = 0.0
    dewpoint_rise_rate_c_per_s: float = 0.0
    eff_positive_seen: bool = False
    eff_positive_duration_s: float = 0.0
    eff_positive_max_pct: float = 0.0
    eff_positive_integral_pct_s: float = 0.0
    eff_negative_duration_s: float = 0.0
    eff_negative_integral_pct_s: float = 0.0
    supply_involvement_confidence: str = "none"
    exhaust_only_confidence: str = "unknown"
    vent_violation: bool = False
    vent3_count: int = 0
    actual_open_valves_empty: bool = True
    target_crossing_count: int = 0
    pressure_chatter_detected: bool = False
    candidate_row_possible: bool = False
    candidate_row_quality_grade: str = "C"
    trial_recommended_for_workflow: bool = False
    no_write_clean: bool = True
    diagnostic_only: bool = True
    not_real_acceptance_evidence: bool = True
    line_contaminated: bool = True
    next_run_requires_full_open_flow_flush: bool = True
    overshoot_allowed: bool | None = None
    slew_mode: str | None = None
    rejection_reasons: list[str] = field(default_factory=list)
    score: float = 0.0


def normalize_targets(targets: Iterable[float | str]) -> tuple[float, ...]:
    normalized: list[float] = []
    for raw in targets:
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError(f"target is not finite: {raw!r}")
        normalized.append(value)
    if not normalized:
        raise ValueError("at least one below-ambient target is required")
    return tuple(normalized)


def validate_targets_below_ambient(
    targets: Iterable[float | str],
    ambient_hpa: float = DEFAULT_AMBIENT_HPA,
) -> tuple[float, ...]:
    ambient = float(ambient_hpa)
    normalized = normalize_targets(targets)
    blocked: list[str] = []
    for target in normalized:
        if target >= ambient:
            blocked.append(f"{target:g} >= ambient {ambient:g}")
        if abs(target - 1100.0) < 0.001:
            blocked.append("1100 hPa is an up-pressure point for this diagnostic")
    if blocked:
        raise ValueError("; ".join(blocked))
    return normalized


def build_default_trial_plan(
    targets_hpa: Iterable[float | str] = DEFAULT_TARGETS_HPA,
    ambient_hpa: float = DEFAULT_AMBIENT_HPA,
) -> list[TrialPlan]:
    targets = validate_targets_below_ambient(targets_hpa, ambient_hpa)
    return [
        TrialPlan(
            trial_id="trial_0_outp0_sealed_hold",
            label="OUTP0 sealed hold baseline",
            mode_requested="OUTP0",
            targets_hpa=(),
            outp1_sent=False,
            hold_s=DEFAULT_HOLD_BASELINE_S,
        ),
        TrialPlan(
            trial_id="trial_1_act_over0_max",
            label="ACT + OVER0 + MAX",
            mode_requested="ACT",
            targets_hpa=targets,
            outp1_sent=True,
            slew_mode="MAX",
            overshoot_allowed=False,
        ),
        TrialPlan(
            trial_id="trial_2_act_over1_max",
            label="ACT + OVER1 + MAX diagnostic only",
            mode_requested="ACT",
            targets_hpa=targets,
            outp1_sent=True,
            slew_mode="MAX",
            overshoot_allowed=True,
        ),
        TrialPlan(
            trial_id="trial_3_pass_over0_max",
            label="PASS + OVER0 + MAX",
            mode_requested="PASS",
            targets_hpa=targets,
            outp1_sent=True,
            slew_mode="MAX",
            overshoot_allowed=False,
            restores_original_mode=True,
        ),
        TrialPlan(
            trial_id="trial_4_gaug_cautious",
            label="GAUG cautious diagnostic",
            mode_requested="GAUG",
            targets_hpa=targets,
            outp1_sent=True,
            slew_mode="MAX",
            overshoot_allowed=False,
            restores_original_mode=True,
            continue_control_if_unsupported=False,
        ),
    ]


def planned_commands_for_trial(trial: TrialPlan, target_hpa: float | None = None) -> list[str]:
    commands = [
        ":OUTP:STAT?",
        ":OUTP:MODE?",
        ":SOUR:PRES:LEV:IMM:AMPL:VENT?",
        ":OUTP:ISOL:STAT?",
    ]
    if trial.mode_requested in {"ACT", "PASS", "GAUG"}:
        commands.append(f":OUTP:MODE {trial.mode_requested}")
        commands.append(":OUTP:MODE?")
    if trial.overshoot_allowed is not None:
        commands.append(f":SOUR:PRES:SLEW:OVER {1 if trial.overshoot_allowed else 0}")
        commands.append(":SOUR:PRES:SLEW:OVER?")
    if trial.slew_mode:
        commands.append(f":SOUR:PRES:SLEW:MODE {trial.slew_mode}")
        commands.append(":SOUR:PRES:SLEW:MODE?")
    if target_hpa is not None:
        commands.append(f":SOUR:PRES:LEV:IMM:AMPL {float(target_hpa):g}")
    if trial.outp1_sent:
        commands.append(":OUTP:STAT 1")
    commands.extend(
        [
            ":SENS:PRES:CONT?",
            ":SENS:PRES:INL?",
            ":SENS:PRES:SLEW?",
            ":SOUR:PRES:EFF?",
            ":SOUR:PRES:COMP1?",
            ":SOUR:PRES:COMP2?",
            ":SYST:ERR?",
            ":OUTP:STAT 0",
        ]
    )
    if trial.restores_original_mode:
        commands.append(":OUTP:MODE <original_mode>")
    return commands


def command_is_forbidden_write(command: str) -> bool:
    text = str(command or "").strip()
    if not text:
        return False
    upper = text.upper()
    if upper.endswith("?") or "?" in upper:
        return False
    return any(pattern.search(upper) for pattern in FORBIDDEN_WRITE_PATTERNS)


def assert_no_forbidden_writes(commands: Iterable[str]) -> None:
    forbidden = [cmd for cmd in commands if command_is_forbidden_write(cmd)]
    if forbidden:
        raise ValueError(f"forbidden write commands planned: {forbidden}")


def should_continue_control_after_mode_set(
    trial: TrialPlan,
    *,
    mode_supported: bool,
    syst_err_after_mode_set: str | None,
) -> bool:
    if trial.mode_requested == "OUTP0":
        return True
    err = str(syst_err_after_mode_set or "").strip()
    err_ok = not err or err.startswith("0") or "NO ERROR" in err.upper()
    if mode_supported and err_ok:
        return True
    return bool(trial.continue_control_if_unsupported)


def classify_supply_involvement(result: TrialResult) -> dict[str, str]:
    positive = result.eff_positive_seen or result.eff_positive_duration_s > 0.0 or result.eff_positive_integral_pct_s > 0.0
    dewpoint_rising = result.dewpoint_delta_c > 0.5 or result.dewpoint_rise_rate_c_per_s > 0.02
    if positive and dewpoint_rising:
        supply = "high"
    elif positive:
        supply = "medium"
    elif result.eff_negative_duration_s > 0.0 or result.eff_negative_integral_pct_s > 0.0:
        supply = "none"
    else:
        supply = "unknown"

    if not positive and (result.eff_negative_duration_s > 0.0 or result.eff_negative_integral_pct_s > 0.0):
        exhaust = "high"
    elif result.eff_negative_duration_s > result.eff_positive_duration_s:
        exhaust = "medium"
    else:
        exhaust = "low"
    return {
        "supply_involvement_confidence": supply,
        "exhaust_only_confidence": exhaust,
    }


def _mode_key(result: TrialResult) -> str:
    mode = result.mode_requested
    if result.overshoot_allowed is not None:
        mode += "_OVER1" if result.overshoot_allowed else "_OVER0"
    if result.slew_mode:
        mode += f"_{result.slew_mode}"
    return mode


def score_trial_result(result: TrialResult) -> TrialResult:
    reasons: list[str] = []
    hard_reject = False
    if not result.no_write_clean:
        reasons.append("no_write_violation")
        hard_reject = True
    if result.vent_violation:
        reasons.append("vent_violation")
        hard_reject = True
    if result.vent3_count:
        reasons.append("vent3_seen")
        hard_reject = True
    if not result.actual_open_valves_empty:
        reasons.append("actual_open_valves_nonempty")
        hard_reject = True
    if not result.target_below_ambient:
        reasons.append("target_not_below_ambient")
        hard_reject = True
    if not result.mode_supported:
        reasons.append("mode_unsupported")
    if not result.candidate_row_possible:
        reasons.append("candidate_row_not_possible")

    score = 100.0
    if result.outp1_to_candidate_s is not None:
        score -= min(40.0, max(0.0, result.outp1_to_candidate_s) * 1.5)
    else:
        score -= 25.0
    score -= max(0.0, result.dewpoint_delta_c) * 18.0
    score -= max(0.0, result.eff_positive_duration_s) * 6.0
    score -= max(0.0, result.eff_positive_integral_pct_s) * 8.0
    score -= max(0.0, result.eff_positive_max_pct) * 0.5
    score += min(12.0, max(0.0, result.eff_negative_integral_pct_s) * 2.0)
    score -= float(result.target_crossing_count) * 12.0
    if result.pressure_chatter_detected:
        reasons.append("pressure_chatter")
        score -= 20.0
    if result.overshoot_allowed and (result.target_crossing_count or result.pressure_chatter_detected):
        reasons.append("over1_crossing_or_chatter")
    if not result.mode_supported:
        score -= 60.0
    if not result.candidate_row_possible:
        score -= 25.0
    if hard_reject:
        score = min(score, -100.0)

    classified = classify_supply_involvement(result)
    result.supply_involvement_confidence = classified["supply_involvement_confidence"]
    result.exhaust_only_confidence = classified["exhaust_only_confidence"]
    result.rejection_reasons = reasons
    result.score = round(score, 3)
    result.trial_recommended_for_workflow = (
        not hard_reject
        and result.mode_supported
        and result.candidate_row_possible
        and result.score >= 65.0
        and result.supply_involvement_confidence in {"none", "unknown"}
    )
    return result


def rank_trial_results(results: Sequence[TrialResult]) -> dict[str, Any]:
    scored = [score_trial_result(result) for result in results]
    eligible = [one for one in scored if one.score > -100.0]
    best = max(eligible or scored, key=lambda item: item.score, default=None)
    rejected_modes: list[str] = []
    rejected_reason_by_mode: dict[str, list[str]] = {}
    for result in scored:
        mode = _mode_key(result)
        if result.rejection_reasons or result.score < 65.0:
            rejected_modes.append(mode)
            rejected_reason_by_mode[mode] = list(result.rejection_reasons or ["low_score"])

    act_suspect = any(
        result.mode_requested == "ACT"
        and result.supply_involvement_confidence in {"medium", "high"}
        for result in scored
    )
    pass_promote = bool(best and best.mode_requested == "PASS" and best.trial_recommended_for_workflow)
    gaug_not_capable = any(
        result.mode_requested == "GAUG" and (not result.mode_supported or not result.candidate_row_possible)
        for result in scored
    )
    over1_reject = any(
        bool(result.overshoot_allowed) and (result.target_crossing_count > 0 or result.pressure_chatter_detected)
        for result in scored
    )
    max_over0_best = bool(best and best.slew_mode == "MAX" and best.overshoot_allowed is False)
    best_mode = _mode_key(best) if best else None
    best_target = best.target_hpa if best else None
    return {
        "best_mode_for_clean_exhaust_control": best_mode,
        "best_target_for_mode_screening": best_target,
        "best_mode_reason": "lowest dewpoint rise, lowest positive effort, fastest valid candidate" if best else "no trials",
        "rejected_modes": sorted(set(rejected_modes)),
        "rejected_reason_by_mode": rejected_reason_by_mode,
        "recommended_workflow_change": "do_not_change_formal_workflow_until_limited_real_trial_confirms_mode",
        "recommended_pace_mode_for_next_limited_workflow": best.mode_requested if best else None,
        "recommended_slew_mode": best.slew_mode if best else None,
        "recommended_overshoot_mode": "OVER1" if best and best.overshoot_allowed else "OVER0",
        "recommended_control_sequence": "set mode, set OVER0, set MAX, set below-ambient setpoint, OUTP1, materialize candidate immediately",
        "expected_risk": "PACE internal supply/vacuum behavior must be proven by effort and dewpoint timelines",
        "required_guard": "no VENT in active sealed phase, reject VENT3, reject open valves, reject positive effort fail, reject target crossing before row",
        "whether_ACT_is_suspected_of_supply_ingress": act_suspect,
        "whether_PASS_should_be_promoted_to_next_limited_workflow_test": pass_promote,
        "whether_GAUG_is_not_control_capable": gaug_not_capable,
        "whether_OVER1_should_be_rejected_due_to_chatter": over1_reject,
        "whether_MAX_OVER0_is_best_current_candidate": max_over0_best,
        "diagnostic_only": True,
        "not_real_acceptance_evidence": True,
    }


def _parse_float(text: str | None) -> float | None:
    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", str(text or ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _query_text(pace: Any, command: str) -> str:
    try:
        return str(pace.query(command) or "").strip()
    except Exception as exc:
        return f"ERROR:{exc}"


def _query_float(pace: Any, command: str) -> float | None:
    return _parse_float(_query_text(pace, command))


def _load_latest_cache(path: str | Path | None, keys: Sequence[str]) -> Any:
    if not path:
        return None
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping) or key not in current:
            return None
        current = current[key]
    return current


def _collect_sample(
    pace: Any,
    *,
    trial_id: str,
    target_hpa: float | None,
    phase: str,
    setpoint_hpa: float | None,
    dewpoint_cache_path: str | Path | None,
    com22_cache_path: str | Path | None,
    actual_open_valves: Sequence[str],
) -> dict[str, Any]:
    raw: dict[str, str] = {}
    raw["sens_pres_inl"] = _query_text(pace, ":SENS:PRES:INL?")
    pressure_hpa = _parse_float(raw["sens_pres_inl"])
    for field, command in (
        ("outp_state", ":OUTP:STAT?"),
        ("outp_mode", ":OUTP:MODE?"),
        ("vent_status", ":SOUR:PRES:LEV:IMM:AMPL:VENT?"),
        ("isolation_state", ":OUTP:ISOL:STAT?"),
        ("sens_pres_slew", ":SENS:PRES:SLEW?"),
        ("sour_pres_eff_pct", ":SOUR:PRES:EFF?"),
        ("sour_pres_comp1_hpa", ":SOUR:PRES:COMP1?"),
        ("sour_pres_comp2_hpa", ":SOUR:PRES:COMP2?"),
        ("slew_mode", ":SOUR:PRES:SLEW:MODE?"),
        ("slew_over", ":SOUR:PRES:SLEW:OVER?"),
        ("syst_err_after_command_group", ":SYST:ERR?"),
    ):
        raw[field] = _query_text(pace, command)
    return {
        "ts": time.time(),
        "trial_id": trial_id,
        "target_hpa": target_hpa,
        "phase": phase,
        "outp_state": raw["outp_state"],
        "outp_mode": raw["outp_mode"],
        "vent_status": raw["vent_status"],
        "isolation_state": raw["isolation_state"],
        "setpoint_hpa": setpoint_hpa,
        # This field keeps the historical column name, but uses INL?/readback
        # pressure because this field unit returns -113 for SENS:PRES:CONT?.
        "sens_pres_cont_hpa": pressure_hpa,
        "sens_pres_inl": raw["sens_pres_inl"],
        "sens_pres_slew": raw["sens_pres_slew"],
        "sour_pres_eff_pct": _parse_float(raw["sour_pres_eff_pct"]),
        "sour_pres_comp1_hpa": _parse_float(raw["sour_pres_comp1_hpa"]),
        "sour_pres_comp2_hpa": _parse_float(raw["sour_pres_comp2_hpa"]),
        "slew_mode": raw["slew_mode"],
        "slew_over": raw["slew_over"],
        "dewpoint_latest_c": _load_latest_cache(dewpoint_cache_path, ("dewpoint_c",)),
        "com22_pressure_latest_hpa": _load_latest_cache(com22_cache_path, ("pressure_hpa",)),
        "actual_open_valves": list(actual_open_valves),
        "raw_tx": "",
        "raw_rx": raw,
        "syst_err_after_command_group": raw["syst_err_after_command_group"],
    }


def _summarize_samples(
    samples: Sequence[Mapping[str, Any]],
    *,
    plan: TrialPlan,
    target_hpa: float | None,
    ambient_hpa: float,
    outp1_ts: float | None,
    candidate_ts: float | None,
    candidate_pressure: float | None,
    mode_confirmed: str,
    mode_supported: bool,
    syst_err_after_mode_set: str,
) -> TrialResult:
    pressures = [_parse_float(str(row.get("sens_pres_cont_hpa"))) for row in samples]
    pressures = [value for value in pressures if value is not None]
    efforts = [_parse_float(str(row.get("sour_pres_eff_pct"))) for row in samples]
    efforts = [value for value in efforts if value is not None]
    dewpoints = [_parse_float(str(row.get("dewpoint_latest_c"))) for row in samples]
    dewpoints = [value for value in dewpoints if value is not None]
    timestamps = [float(row.get("ts", 0.0)) for row in samples if row.get("ts") is not None]
    dwell = max(timestamps) - min(timestamps) if len(timestamps) >= 2 else 0.0
    sample_interval = dwell / max(1, len(samples) - 1) if samples else DEFAULT_SAMPLE_INTERVAL_S

    positive = [eff for eff in efforts if eff > 0.0]
    negative = [eff for eff in efforts if eff < 0.0]
    pressure_start = pressures[0] if pressures else None
    pressure_end = pressures[-1] if pressures else None
    dewpoint_start = dewpoints[0] if dewpoints else None
    dewpoint_end = dewpoints[-1] if dewpoints else None
    dewpoint_candidate = dewpoint_end
    dewpoint_delta = (dewpoint_end - dewpoint_start) if dewpoint_start is not None and dewpoint_end is not None else 0.0
    pressure_rate = None
    if pressure_start is not None and candidate_pressure is not None and outp1_ts and candidate_ts and candidate_ts > outp1_ts:
        pressure_rate = (pressure_start - candidate_pressure) / (candidate_ts - outp1_ts)

    vent_values = [str(row.get("vent_status", "")).strip() for row in samples]
    vent_violation = any(value.startswith("1") for value in vent_values)
    vent3_count = sum(1 for value in vent_values if value.startswith("3"))
    outp_values = [_parse_float(str(row.get("outp_state"))) for row in samples]
    crossing_count = 0
    pressure_chatter = False
    if target_hpa is not None and pressures:
        crossing_count = sum(1 for value in pressures if value < float(target_hpa))
        signs: list[int] = []
        for value in pressures:
            delta = value - float(target_hpa)
            if abs(delta) <= TARGET_WINDOW_HIGH_HPA:
                signs.append(0)
            elif delta > 0:
                signs.append(1)
            else:
                signs.append(-1)
        non_zero_signs = [value for value in signs if value]
        sign_changes = sum(
            1
            for before, after in zip(non_zero_signs, non_zero_signs[1:])
            if before != after
        )
        pressure_chatter = sign_changes >= 2
    result = TrialResult(
        trial_id=plan.trial_id,
        mode_requested=plan.mode_requested,
        mode_confirmed=mode_confirmed,
        mode_supported=mode_supported,
        target_hpa=target_hpa,
        ambient_pressure_hpa=ambient_hpa,
        target_below_ambient=(target_hpa is None or target_hpa < ambient_hpa),
        syst_err_after_mode_set=syst_err_after_mode_set,
        outp1_sent=plan.outp1_sent,
        controller_on_confirmed=any(value == 1.0 for value in outp_values),
        pressure_start_hpa=pressure_start,
        pressure_candidate_hpa=candidate_pressure,
        pressure_end_hpa=pressure_end,
        outp1_to_candidate_s=(candidate_ts - outp1_ts) if outp1_ts and candidate_ts else None,
        total_trial_dwell_s=dwell,
        pressure_drop_rate_hpa_per_s=pressure_rate,
        dewpoint_start_c=dewpoint_start,
        dewpoint_candidate_c=dewpoint_candidate,
        dewpoint_end_c=dewpoint_end,
        dewpoint_delta_c=dewpoint_delta,
        dewpoint_rise_rate_c_per_s=(dewpoint_delta / dwell) if dwell > 0 else 0.0,
        eff_positive_seen=bool(positive),
        eff_positive_duration_s=len(positive) * sample_interval,
        eff_positive_max_pct=max(positive) if positive else 0.0,
        eff_positive_integral_pct_s=sum(positive) * sample_interval,
        eff_negative_duration_s=len(negative) * sample_interval,
        eff_negative_integral_pct_s=sum(abs(value) for value in negative) * sample_interval,
        vent_violation=vent_violation,
        vent3_count=vent3_count,
        actual_open_valves_empty=all(not row.get("actual_open_valves") for row in samples),
        target_crossing_count=crossing_count,
        pressure_chatter_detected=pressure_chatter,
        candidate_row_possible=candidate_pressure is not None,
        candidate_row_quality_grade="B" if candidate_pressure is not None else "C",
        overshoot_allowed=plan.overshoot_allowed,
        slew_mode=plan.slew_mode,
    )
    return score_trial_result(result)


def _reset_to_atmosphere(pace: Any, *, timeout_s: float = 45.0, poll_s: float = 0.25) -> dict[str, Any]:
    started = time.time()
    status: dict[str, Any] = {"started_ts": started, "ok": False}
    try:
        enter = getattr(pace, "enter_atmosphere_mode", None)
        if callable(enter):
            status["enter_status"] = enter(timeout_s=timeout_s, poll_s=poll_s)
        else:
            pace.set_output(False)
        status["ok"] = True
    except Exception as exc:
        status["error"] = f"{type(exc).__name__}:{exc}"
    status["finished_ts"] = time.time()
    return status


def run_real_com_diagnostic(
    *,
    pace_port: str,
    targets_hpa: Sequence[float],
    ambient_hpa: float,
    output_dir: Path,
    sample_interval_s: float = DEFAULT_SAMPLE_INTERVAL_S,
    max_control_s: float = DEFAULT_MAX_CONTROL_S,
    dewpoint_cache_path: str | Path | None = None,
    com22_cache_path: str | Path | None = None,
    actual_open_valves: Sequence[str] = (),
    reset_to_atmosphere_between_trials: bool = True,
) -> dict[str, Any]:
    from gas_calibrator.devices.pace5000 import Pace5000

    output_dir.mkdir(parents=True, exist_ok=True)
    plan = build_default_trial_plan(targets_hpa, ambient_hpa)
    all_samples: list[dict[str, Any]] = []
    all_results: list[TrialResult] = []
    reset_events: list[dict[str, Any]] = []
    pace = Pace5000(pace_port)
    pace.open()
    original_mode = ""
    try:
        try:
            pace.set_units_hpa()
        except Exception:
            pass
        try:
            original_mode = pace.get_output_mode()
        except Exception:
            original_mode = "ACT"
        for trial_index, trial in enumerate(plan):
            if reset_to_atmosphere_between_trials and trial_index > 1:
                reset_events.append(
                    {
                        "before_trial_id": trial.trial_id,
                        **_reset_to_atmosphere(pace),
                    }
                )
                try:
                    original_mode = pace.get_output_mode()
                except Exception:
                    pass
            targets = trial.targets_hpa or (None,)
            for target in targets:
                mode_confirmed = ""
                mode_supported = True
                syst_err = "0,No error"
                trial_samples: list[dict[str, Any]] = []
                candidate_ts: float | None = None
                candidate_pressure: float | None = None
                outp1_ts: float | None = None
                try:
                    if trial.mode_requested == "ACT":
                        pace.set_output_mode_active()
                    elif trial.mode_requested == "PASS":
                        pace.set_output_mode_passive()
                    elif trial.mode_requested == "GAUG":
                        pace.write(":OUTP:MODE GAUG")
                    if trial.mode_requested in {"ACT", "PASS", "GAUG"}:
                        try:
                            mode_confirmed = pace.get_output_mode()
                        except Exception as exc:
                            mode_confirmed = f"ERROR:{exc}"
                        mode_supported = mode_confirmed == trial.mode_requested
                        syst_err = _query_text(pace, ":SYST:ERR?")
                        if not should_continue_control_after_mode_set(
                            trial,
                            mode_supported=mode_supported,
                            syst_err_after_mode_set=syst_err,
                        ):
                            all_results.append(
                                score_trial_result(
                                    TrialResult(
                                        trial_id=trial.trial_id,
                                        mode_requested=trial.mode_requested,
                                        mode_confirmed=mode_confirmed,
                                        mode_supported=False,
                                        target_hpa=target,
                                        ambient_pressure_hpa=ambient_hpa,
                                        target_below_ambient=(target is None or target < ambient_hpa),
                                        syst_err_after_mode_set=syst_err,
                                        overshoot_allowed=trial.overshoot_allowed,
                                        slew_mode=trial.slew_mode,
                                    )
                                )
                            )
                            continue
                    if trial.overshoot_allowed is not None:
                        pace.set_overshoot_allowed(bool(trial.overshoot_allowed))
                    if trial.slew_mode == "MAX":
                        pace.set_slew_mode_max()
                    elif trial.slew_mode == "LIN":
                        pace.set_slew_mode_linear()
                    if target is not None:
                        pace.set_setpoint(float(target))
                        pace.set_output(True)
                        outp1_ts = time.time()
                        deadline = outp1_ts + max_control_s
                    else:
                        pace.set_output(False)
                        deadline = time.time() + trial.hold_s
                    while time.time() < deadline:
                        sample = _collect_sample(
                            pace,
                            trial_id=trial.trial_id,
                            target_hpa=target,
                            phase="active_control" if trial.outp1_sent else "outp0_hold",
                            setpoint_hpa=target,
                            dewpoint_cache_path=dewpoint_cache_path,
                            com22_cache_path=com22_cache_path,
                            actual_open_valves=actual_open_valves,
                        )
                        trial_samples.append(sample)
                        all_samples.append(sample)
                        pressure = sample.get("sens_pres_cont_hpa")
                        vent_status = _parse_float(str(sample.get("vent_status")))
                        if vent_status in {1.0, 3.0}:
                            break
                        if target is not None and pressure is not None:
                            if float(target) <= float(pressure) <= float(target) + TARGET_WINDOW_HIGH_HPA:
                                candidate_ts = float(sample["ts"])
                                candidate_pressure = float(pressure)
                                break
                            if float(pressure) < float(target):
                                break
                        time.sleep(max(0.2, min(0.5, sample_interval_s)))
                finally:
                    try:
                        pace.set_output(False)
                    except Exception:
                        pass
                    if trial.restores_original_mode and original_mode:
                        try:
                            if original_mode == "ACT":
                                pace.set_output_mode_active()
                            elif original_mode == "PASS":
                                pace.set_output_mode_passive()
                            elif original_mode == "GAUG":
                                pace.write(":OUTP:MODE GAUG")
                        except Exception:
                            pass
                all_results.append(
                    _summarize_samples(
                        trial_samples,
                        plan=trial,
                        target_hpa=target,
                        ambient_hpa=ambient_hpa,
                        outp1_ts=outp1_ts,
                        candidate_ts=candidate_ts,
                        candidate_pressure=candidate_pressure,
                        mode_confirmed=mode_confirmed or trial.mode_requested,
                        mode_supported=mode_supported,
                        syst_err_after_mode_set=syst_err,
                    )
                )
    finally:
        try:
            pace.set_output(False)
        except Exception:
            pass
        if reset_to_atmosphere_between_trials:
            reset_events.append({"before_trial_id": "final_cleanup", **_reset_to_atmosphere(pace)})
        pace.close()

    summary = {
        "diagnostic_only": True,
        "not_real_acceptance_evidence": True,
        "no_write_clean": True,
        "line_contaminated": True,
        "next_run_requires_full_open_flow_flush": True,
        "targets_hpa": list(targets_hpa),
        "ambient_hpa": ambient_hpa,
        "results": [asdict(result) for result in all_results],
        "ranking": rank_trial_results(all_results),
        "reset_events": reset_events,
    }
    _write_samples_csv(output_dir / "pace_mode_ingress_samples.csv", all_samples)
    _write_raw_tap_jsonl(output_dir / "pace_mode_ingress_raw_tap.jsonl", all_samples)
    (output_dir / "pace_mode_ingress_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_summary_md(output_dir / "pace_mode_ingress_summary.md", summary)
    return summary


def _write_samples_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REQUIRED_SAMPLE_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in REQUIRED_SAMPLE_FIELDS})


def _write_raw_tap_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    {
                        "ts": row.get("ts"),
                        "trial_id": row.get("trial_id"),
                        "target_hpa": row.get("target_hpa"),
                        "phase": row.get("phase"),
                        "raw_tx": row.get("raw_tx"),
                        "raw_rx": row.get("raw_rx"),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def _write_summary_md(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# PACE Mode Ingress Diagnostic Summary",
        "",
        f"- diagnostic_only: {summary.get('diagnostic_only')}",
        f"- not_real_acceptance_evidence: {summary.get('not_real_acceptance_evidence')}",
        f"- no_write_clean: {summary.get('no_write_clean')}",
        f"- ambient_hpa: {summary.get('ambient_hpa')}",
        f"- targets_hpa: {summary.get('targets_hpa')}",
        "",
        "## Ranking",
        "",
        "```json",
        json.dumps(summary.get("ranking", {}), indent=2, ensure_ascii=False),
        "```",
        "",
        "## Results",
        "",
    ]
    for result in summary.get("results", []):
        lines.extend(
            [
                f"### {result.get('trial_id')} target={result.get('target_hpa')}",
                "",
                f"- mode: {result.get('mode_requested')} confirmed={result.get('mode_confirmed')} supported={result.get('mode_supported')}",
                f"- outp1_to_candidate_s: {result.get('outp1_to_candidate_s')}",
                f"- pressure_candidate_hpa: {result.get('pressure_candidate_hpa')}",
                f"- dewpoint_delta_c: {result.get('dewpoint_delta_c')}",
                f"- eff_positive_duration_s: {result.get('eff_positive_duration_s')}",
                f"- eff_positive_integral_pct_s: {result.get('eff_positive_integral_pct_s')}",
                f"- eff_negative_integral_pct_s: {result.get('eff_negative_integral_pct_s')}",
                f"- target_crossing_count: {result.get('target_crossing_count')}",
                f"- pressure_chatter_detected: {result.get('pressure_chatter_detected')}",
                f"- candidate_row_possible: {result.get('candidate_row_possible')}",
                f"- rejection_reasons: {result.get('rejection_reasons')}",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def write_offline_plan(output_dir: Path, *, targets_hpa: Sequence[float], ambient_hpa: float) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    plan = build_default_trial_plan(targets_hpa, ambient_hpa)
    for trial in plan:
        for target in trial.targets_hpa or (None,):
            assert_no_forbidden_writes(planned_commands_for_trial(trial, target))
    payload = {
        "diagnostic_only": True,
        "not_real_acceptance_evidence": True,
        "real_com_executed": False,
        "ambient_hpa": ambient_hpa,
        "targets_hpa": list(targets_hpa),
        "trials": [asdict(trial) | {"mode_label": trial.mode_label} for trial in plan],
        "next_run_requires_full_open_flow_flush": True,
        "line_contaminated_after_real_trial": True,
    }
    (output_dir / "pace_mode_ingress_trial_plan.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return payload


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ambient-hpa", type=float, default=DEFAULT_AMBIENT_HPA)
    parser.add_argument("--targets", nargs="+", type=float, default=list(DEFAULT_TARGETS_HPA))
    parser.add_argument("--output-dir", type=Path, default=Path("logs") / "pace_mode_ingress_diagnostic")
    parser.add_argument("--pace-port", default="COM23")
    parser.add_argument("--sample-interval-s", type=float, default=DEFAULT_SAMPLE_INTERVAL_S)
    parser.add_argument("--max-control-s", type=float, default=DEFAULT_MAX_CONTROL_S)
    parser.add_argument("--dewpoint-cache-json")
    parser.add_argument("--com22-pressure-cache-json")
    parser.add_argument("--real-com", action="store_true", help="Open the PACE serial port and run the pressure-only diagnostic.")
    parser.add_argument(
        "--i-understand-pressure-only-no-write",
        action="store_true",
        help="Required with --real-com. Confirms no calibration writes and no full workflow.",
    )
    parser.add_argument(
        "--operator-confirm-sealed-volume",
        action="store_true",
        help="Required with --real-com. Confirms the external route is safely sealed without CO2/HGEN.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    targets = validate_targets_below_ambient(args.targets, args.ambient_hpa)
    if not args.real_com:
        payload = write_offline_plan(args.output_dir, targets_hpa=targets, ambient_hpa=args.ambient_hpa)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    if not args.i_understand_pressure_only_no_write or not args.operator_confirm_sealed_volume:
        parser.error("--real-com requires --i-understand-pressure-only-no-write and --operator-confirm-sealed-volume")
    summary = run_real_com_diagnostic(
        pace_port=args.pace_port,
        targets_hpa=targets,
        ambient_hpa=args.ambient_hpa,
        output_dir=args.output_dir,
        sample_interval_s=args.sample_interval_s,
        max_control_s=args.max_control_s,
        dewpoint_cache_path=args.dewpoint_cache_json,
        com22_cache_path=args.com22_pressure_cache_json,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
