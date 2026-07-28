"""Pure helpers for V1 CO2 steady-state replay and audit flows.

These helpers intentionally do not import V2 or touch live-device runtime.
They provide the same steady-state representative-value semantics used by the
V1 CO2 steady-state QC work, but can be called from offline replay/parity
tools without modifying the production runner path.
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from statistics import mean, stdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


DEFAULT_CO2_STEADY_STATE_QC: Dict[str, Any] = {
    "enabled": True,
    "policy": "warn",
    "min_samples": 4,
    "fallback_samples": 4,
    "max_std_ppm": 3.0,
    "max_range_ppm": 8.0,
    "max_abs_slope_ppm_per_s": 1.0,
}


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _as_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    if numeric <= -999:
        return None
    return numeric


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "on", "ok"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _safe_label(label: str) -> str:
    text = str(label or "").strip().lower()
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in text).strip("_")


def _parse_sample_ts_text(value: Any) -> Tuple[Optional[float], Optional[str]]:
    text = str(value or "").strip()
    if not text:
        return None, None
    try:
        ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None, text
    return float(ts.timestamp()), text


def normalize_co2_steady_state_qc_cfg(raw_cfg: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(DEFAULT_CO2_STEADY_STATE_QC)
    if isinstance(raw_cfg, Mapping):
        payload.update(dict(raw_cfg))
    policy = str(payload.get("policy") or "warn").strip().lower()
    if policy not in {"off", "warn", "reject"}:
        policy = "warn"
    min_samples = _as_int(payload.get("min_samples"))
    fallback_samples = _as_int(payload.get("fallback_samples"))
    payload["enabled"] = bool(payload.get("enabled", True))
    payload["policy"] = policy
    payload["min_samples"] = max(2, min_samples if min_samples is not None else 4)
    payload["fallback_samples"] = max(
        2,
        fallback_samples if fallback_samples is not None else payload["min_samples"],
    )
    payload["max_std_ppm"] = float(payload.get("max_std_ppm", 3.0) or 3.0)
    payload["max_range_ppm"] = float(payload.get("max_range_ppm", 8.0) or 8.0)
    payload["max_abs_slope_ppm_per_s"] = float(payload.get("max_abs_slope_ppm_per_s", 1.0) or 1.0)
    return payload


def numeric_series_metrics(samples: Sequence[Tuple[float, float]]) -> Dict[str, Any]:
    ordered = sorted(
        (
            (float(ts), float(value))
            for ts, value in samples
            if _as_float(ts) is not None and _as_float(value) is not None
        ),
        key=lambda item: item[0],
    )
    if len(ordered) < 2:
        return {}
    values_only = [value for _ts, value in ordered]
    duration_s = max(0.0, ordered[-1][0] - ordered[0][0])
    slope_per_s = 0.0 if duration_s <= 0 else (values_only[-1] - values_only[0]) / duration_s
    return {
        "count": len(ordered),
        "duration_s": duration_s,
        "span": max(values_only) - min(values_only),
        "slope_per_s": slope_per_s,
        "first_value": values_only[0],
        "last_value": values_only[-1],
        "min_value": min(values_only),
        "max_value": max(values_only),
    }


def _discover_analyzer_prefixes(samples: Sequence[Mapping[str, Any]]) -> List[str]:
    prefixes: List[str] = []
    seen: set[str] = set()
    for row in samples:
        for key in row.keys():
            match = re.match(r"^(ga\d+)_", str(key or "").strip().lower())
            if not match:
                continue
            prefix = match.group(1)
            if prefix in seen:
                continue
            prefixes.append(prefix)
            seen.add(prefix)
    return prefixes


def primary_or_first_usable_analyzer_window_series(
    samples: Sequence[Mapping[str, Any]],
    *,
    value_key: str = "co2_ppm",
) -> Dict[str, Any]:
    """Return the primary-or-first-usable analyzer series for replay evaluation."""

    def _build_series(
        rows: Sequence[Mapping[str, Any]],
        *,
        key: str,
        usable_flag_key: str,
        analyzer_source: str,
    ) -> List[Dict[str, Any]]:
        prepared: List[Dict[str, Any]] = []
        any_timestamp = False
        all_timestamp = True
        for idx, row in enumerate(rows, start=1):
            usable = _as_bool(row.get(usable_flag_key))
            if usable is False:
                continue
            value = _as_float(row.get(key))
            if value is None:
                continue
            sample_index = _as_int(row.get("sample_index")) or idx
            parsed_ts, parsed_text = _parse_sample_ts_text(
                row.get("sample_ts") or row.get("sample_start_ts") or row.get("sample_end_ts")
            )
            if parsed_ts is not None:
                any_timestamp = True
            else:
                all_timestamp = False
            prepared.append(
                {
                    "sample_index": sample_index,
                    "value": value,
                    "sample_ts": parsed_ts,
                    "sample_ts_text": parsed_text,
                    "row_index": idx,
                    "analyzer_source": analyzer_source,
                    "value_key": key,
                }
            )
        if not prepared:
            return []
        if not any_timestamp or not all_timestamp:
            for idx, entry in enumerate(prepared):
                entry["sample_ts"] = float(idx)
                entry["sample_ts_text"] = None
        return prepared

    primary = _build_series(
        samples,
        key=value_key,
        usable_flag_key="frame_usable",
        analyzer_source="primary",
    )
    if primary:
        return {
            "series": primary,
            "analyzer_source": "primary",
            "value_key": value_key,
            "timestamp_strategy": (
                "sample_ts"
                if all(item.get("sample_ts_text") for item in primary)
                else "row_index_fallback"
            ),
        }

    for prefix in _discover_analyzer_prefixes(samples):
        prefixed_key = f"{prefix}_{value_key}"
        prefixed_series = _build_series(
            samples,
            key=prefixed_key,
            usable_flag_key=f"{prefix}_frame_usable",
            analyzer_source=prefix,
        )
        if prefixed_series:
            return {
                "series": prefixed_series,
                "analyzer_source": prefix,
                "value_key": prefixed_key,
                "timestamp_strategy": (
                    "sample_ts"
                    if all(item.get("sample_ts_text") for item in prefixed_series)
                    else "row_index_fallback"
                ),
            }
    return {
        "series": [],
        "analyzer_source": "none",
        "value_key": value_key,
        "timestamp_strategy": "missing",
    }


def _co2_steady_state_window_metrics(
    series: Sequence[Mapping[str, Any]],
    *,
    value_key: str,
    analyzer_source: str,
    timestamp_strategy: str,
) -> Dict[str, Any]:
    values = [float(entry["value"]) for entry in series]
    metrics = numeric_series_metrics([(float(entry["sample_ts"]), float(entry["value"])) for entry in series])
    std_ppm = stdev(values) if len(values) > 1 else 0.0
    return {
        "co2_steady_window_analyzer_source": analyzer_source,
        "co2_steady_window_value_key": value_key,
        "co2_steady_window_start_sample_index": _as_int(series[0].get("sample_index")),
        "co2_steady_window_end_sample_index": _as_int(series[-1].get("sample_index")),
        "co2_steady_window_start_ts": series[0].get("sample_ts_text"),
        "co2_steady_window_end_ts": series[-1].get("sample_ts_text"),
        "co2_steady_window_sample_count": len(series),
        "co2_steady_window_mean_ppm": round(mean(values), 6),
        "co2_steady_window_std_ppm": round(float(std_ppm), 6),
        "co2_steady_window_range_ppm": round(float(metrics.get("span") or 0.0), 6),
        "co2_steady_window_slope_ppm_per_s": round(float(metrics.get("slope_per_s") or 0.0), 6),
        "co2_steady_window_timestamp_strategy": str(timestamp_strategy or "missing"),
    }


def _co2_steady_state_window_failures(metrics: Mapping[str, Any], *, cfg: Mapping[str, Any]) -> List[str]:
    reasons: List[str] = []
    sample_count = _as_int(metrics.get("co2_steady_window_sample_count")) or 0
    min_samples = int(cfg.get("min_samples") or 0)
    if sample_count < min_samples:
        reasons.append(f"sample_count={sample_count}<min_samples={min_samples}")
    std_ppm = _as_float(metrics.get("co2_steady_window_std_ppm"))
    max_std_ppm = _as_float(cfg.get("max_std_ppm"))
    if std_ppm is None:
        reasons.append("std_ppm=NA")
    elif max_std_ppm is not None and std_ppm > max_std_ppm:
        reasons.append(f"std_ppm={std_ppm:.3f}>max_std_ppm={max_std_ppm:.3f}")
    range_ppm = _as_float(metrics.get("co2_steady_window_range_ppm"))
    max_range_ppm = _as_float(cfg.get("max_range_ppm"))
    if range_ppm is None:
        reasons.append("range_ppm=NA")
    elif max_range_ppm is not None and range_ppm > max_range_ppm:
        reasons.append(f"range_ppm={range_ppm:.3f}>max_range_ppm={max_range_ppm:.3f}")
    slope_ppm_per_s = _as_float(metrics.get("co2_steady_window_slope_ppm_per_s"))
    max_abs_slope_ppm_per_s = _as_float(cfg.get("max_abs_slope_ppm_per_s"))
    if slope_ppm_per_s is None:
        reasons.append("abs_slope_ppm_per_s=NA")
    elif max_abs_slope_ppm_per_s is not None and abs(slope_ppm_per_s) > max_abs_slope_ppm_per_s:
        reasons.append(
            "abs_slope_ppm_per_s="
            f"{abs(slope_ppm_per_s):.4f}>max_abs_slope_ppm_per_s={max_abs_slope_ppm_per_s:.4f}"
        )
    if str(metrics.get("co2_steady_window_timestamp_strategy") or "") == "row_index_fallback":
        reasons.append("timestamp_strategy=row_index_fallback")
    return reasons


def legacy_co2_representative(
    samples: Sequence[Mapping[str, Any]],
    *,
    value_key: str = "co2_ppm",
) -> Dict[str, Any]:
    series_info = primary_or_first_usable_analyzer_window_series(samples, value_key=value_key)
    series = list(series_info.get("series") or [])
    values = [float(entry["value"]) for entry in series]
    return {
        "legacy_value_source": "primary_or_first_usable_full_window_mean",
        "legacy_analyzer_source": str(series_info.get("analyzer_source") or "none"),
        "legacy_value_key": str(series_info.get("value_key") or value_key),
        "legacy_sample_count": len(values),
        "legacy_representative_value": round(mean(values), 6) if values else None,
        "legacy_timestamp_strategy": str(series_info.get("timestamp_strategy") or "missing"),
    }


def evaluate_co2_steady_state_window_qc(
    samples: Sequence[Mapping[str, Any]],
    *,
    phase: str = "co2",
    qc_cfg: Optional[Mapping[str, Any]] = None,
    value_key: str = "co2_ppm",
) -> Dict[str, Any]:
    cfg = normalize_co2_steady_state_qc_cfg(qc_cfg)
    policy = str(cfg.get("policy") or "warn").lower()
    result: Dict[str, Any] = {
        "measured_value_source": None,
        "co2_steady_window_found": None,
        "co2_steady_window_status": "skipped",
        "co2_steady_window_reason": "",
        "co2_steady_window_analyzer_source": None,
        "co2_steady_window_value_key": None,
        "co2_steady_window_candidate_count": 0,
        "co2_steady_window_start_sample_index": None,
        "co2_steady_window_end_sample_index": None,
        "co2_steady_window_start_ts": None,
        "co2_steady_window_end_ts": None,
        "co2_steady_window_sample_count": None,
        "co2_steady_window_mean_ppm": None,
        "co2_steady_window_std_ppm": None,
        "co2_steady_window_range_ppm": None,
        "co2_steady_window_slope_ppm_per_s": None,
        "co2_steady_window_timestamp_strategy": None,
        "co2_representative_value": None,
    }
    if str(phase or "").strip().lower() != "co2":
        result["co2_steady_window_reason"] = "not_co2_phase"
        return result
    if not bool(cfg.get("enabled")):
        result["co2_steady_window_reason"] = "qc_disabled"
        return result
    if policy == "off":
        result["co2_steady_window_reason"] = "policy_off"
        return result

    series_info = primary_or_first_usable_analyzer_window_series(samples, value_key=value_key)
    series = list(series_info.get("series") or [])
    if not series:
        result.update(
            {
                "co2_steady_window_found": False,
                "co2_steady_window_status": "fail" if policy == "reject" else "warn",
                "co2_steady_window_reason": f"no_usable_co2_samples;policy={policy}",
            }
        )
        return result

    candidate_metrics: List[Dict[str, Any]] = []
    min_samples = int(cfg.get("min_samples") or 0)
    for start_idx in range(len(series)):
        for end_idx in range(start_idx + min_samples - 1, len(series)):
            metrics = _co2_steady_state_window_metrics(
                series[start_idx : end_idx + 1],
                value_key=str(series_info.get("value_key") or value_key),
                analyzer_source=str(series_info.get("analyzer_source") or "primary"),
                timestamp_strategy=str(series_info.get("timestamp_strategy") or "missing"),
            )
            if _co2_steady_state_window_failures(metrics, cfg=cfg):
                continue
            candidate_metrics.append(metrics)

    result["co2_steady_window_candidate_count"] = len(candidate_metrics)
    if candidate_metrics:
        chosen = max(
            candidate_metrics,
            key=lambda item: (
                _as_int(item.get("co2_steady_window_end_sample_index")) or 0,
                _as_int(item.get("co2_steady_window_sample_count")) or 0,
                -(_as_float(item.get("co2_steady_window_std_ppm")) or 0.0),
                -abs(_as_float(item.get("co2_steady_window_slope_ppm_per_s")) or 0.0),
                -(_as_float(item.get("co2_steady_window_range_ppm")) or 0.0),
            ),
        )
        result.update(chosen)
        result["co2_steady_window_found"] = True
        result["co2_steady_window_status"] = "pass"
        result["co2_representative_value"] = chosen.get("co2_steady_window_mean_ppm")
        result["measured_value_source"] = "co2_steady_state_window"
        return result

    fallback_samples = min(
        len(series),
        max(int(cfg.get("fallback_samples") or min_samples), min_samples),
    )
    fallback_series = series[-fallback_samples:] if fallback_samples > 0 else []
    if not fallback_series:
        result.update(
            {
                "co2_steady_window_found": False,
                "co2_steady_window_status": "fail" if policy == "reject" else "warn",
                "co2_steady_window_reason": f"no_fallback_samples;policy={policy}",
            }
        )
        return result

    fallback_metrics = _co2_steady_state_window_metrics(
        fallback_series,
        value_key=str(series_info.get("value_key") or value_key),
        analyzer_source=str(series_info.get("analyzer_source") or "primary"),
        timestamp_strategy=str(series_info.get("timestamp_strategy") or "missing"),
    )
    fallback_reasons = _co2_steady_state_window_failures(fallback_metrics, cfg=cfg)
    result.update(fallback_metrics)
    result["co2_steady_window_found"] = False
    result["co2_steady_window_status"] = "fail" if policy == "reject" else "warn"
    result["co2_steady_window_reason"] = ";".join(
        [
            "no_qualified_steady_state_window",
            *fallback_reasons,
            "fallback=trailing_window",
            f"policy={policy}",
        ]
    )
    result["co2_representative_value"] = fallback_metrics.get("co2_steady_window_mean_ppm")
    result["measured_value_source"] = "co2_trailing_window_fallback"
    return result

