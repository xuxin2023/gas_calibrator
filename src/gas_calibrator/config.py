"""
配置加载模块。

提供：
1. JSON 配置文件读取；
2. 路径字段的相对路径自动展开；
3. 点分路径访问工具函数。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


_RUNTIME_DEFAULTS: Dict[str, Any] = {
    "workflow": {
        "selected_pressure_points": [],
        "analyzer_frame_quality": {
            "min_mode2_fields": 16,
            "bad_status_tokens": ["FAIL", "INVALID", "NO_RESPONSE", "NO_ACK", "ERROR"],
            "runtime_hard_bad_status_tokens": ["FAIL", "INVALID", "ERROR"],
            "runtime_soft_bad_status_tokens": ["NO_RESPONSE", "NO_ACK"],
            "suspicious_co2_ppm_min": 2999.0,
            "suspicious_h2o_mmol_min": 70.0,
            "runtime_relaxed_for_required_key": True,
            "strict_required_keys": ["co2_ratio_f", "h2o_ratio_f", "co2_ppm", "h2o_mmol"],
            "relaxed_required_keys": ["chamber_temp_c", "case_temp_c", "temp_c"],
            "reject_log_window_s": 15.0,
            "reject_negative_co2_ppm": True,
            "reject_negative_h2o_mmol": True,
            "reject_nonpositive_co2_ratio_f": True,
            "reject_nonpositive_h2o_ratio_f": True,
            "invalid_sentinel_values": [-1001, -9999, 999999],
            "invalid_sentinel_tolerance": 0.001,
            "pressure_kpa_min": 30.0,
            "pressure_kpa_max": 150.0,
        },
        "analyzer_live_snapshot": {
            "enabled": True,
            "interval_s": 5.0,
            "cache_ttl_s": 2.5,
            "sampling_worker_enabled": True,
            "sampling_worker_interval_s": 0.2,
            "passive_round_robin_enabled": True,
            "passive_round_robin_interval_s": 0.25,
            "active_ring_buffer_size": 128,
            "active_frame_max_anchor_delta_ms": 250.0,
            "active_frame_right_match_max_ms": 120.0,
            "active_frame_stale_ms": 500.0,
            "active_drain_poll_s": 0.05,
            "anchor_match_enabled": True,
        },
        "sampling": {
            "interval_s": 1.0,
            "co2_interval_s": 1.0,
            "h2o_interval_s": 1.0,
            "fixed_rate_enabled": True,
            "fast_sync_warn_span_ms": 1000.0,
            "fast_signal_worker_enabled": True,
            "fast_signal_worker_interval_s": 0.1,
            "fast_signal_ring_buffer_size": 128,
            "pressure_gauge_continuous_enabled": True,
            "pressure_gauge_continuous_mode": "P4",
            "pressure_gauge_continuous_drain_s": 0.12,
            "pressure_gauge_continuous_read_timeout_s": 0.02,
            "pressure_gauge_stale_ratio_warn_max": None,
            "pressure_gauge_stale_ratio_reject_max": None,
            "pre_sample_freshness_timeout_s": 1.0,
            "pre_sample_freshness_poll_s": 0.05,
            "pre_sample_signal_max_age_s": 0.35,
            "pre_sample_analyzer_max_age_s": 0.6,
            "slow_aux_cache_enabled": True,
            "slow_aux_cache_interval_s": 5.0,
            "pace_state_every_n_samples": 0,
            "pace_state_cache_enabled": True,
            "quality": {
                "per_analyzer": False,
            }
        },
        "summary_alignment": {
            "reference_on_aligned_rows": True,
        },
        "reporting": {
            "include_fleet_stats": False,
            "defer_heavy_exports_during_handoff": True,
            "flush_deferred_exports_on_next_route_soak": True,
        },
        "relay": {
            "bulk_write_enabled": True,
        },
        "humidity_generator": {
            "safe_stop_verify_flow": True,
            "safe_stop_enforce_flow_check": True,
            "safe_stop_max_flow_lpm": 0.05,
            "safe_stop_timeout_s": 5.0,
            "safe_stop_poll_s": 0.5,
        },
        "pressure": {
            "co2_preseal_pressure_gauge_trigger_hpa": 1110.0,
            "h2o_preseal_pressure_gauge_trigger_hpa": 1110.0,
            "preseal_timeout_requires_invalid_gauge": True,
            "preseal_valid_gauge_stall_window_s": 20.0,
            "preseal_valid_gauge_min_rise_hpa": 0.5,
            "transition_pressure_gauge_continuous_enabled": True,
            "transition_pressure_gauge_continuous_mode": "P4",
            "transition_pressure_gauge_continuous_drain_s": 0.12,
            "transition_pressure_gauge_continuous_read_timeout_s": 0.02,
            "post_stable_sample_delay_s": 5.0,
            "co2_post_stable_sample_delay_s": 5.0,
            "transition_trace_enabled": True,
            "transition_trace_poll_s": 0.5,
            "capture_then_hold_enabled": False,
            "disable_output_during_sampling": True,
            "output_off_prefer_gauge": True,
            "output_off_sample_interval_s": 0.5,
            "output_off_retry_count": 1,
            "co2_output_off_hold_s": 6.0,
            "h2o_output_off_hold_s": 10.0,
            "co2_output_off_max_abs_drift_hpa": 0.25,
            "h2o_output_off_max_abs_drift_hpa": 0.40,
            "adaptive_pressure_sampling_enabled": False,
            "use_pressure_gauge_for_sampling_gate": True,
            "sampling_gate_poll_s": 0.5,
            "co2_sampling_gate_window_s": 8.0,
            "h2o_sampling_gate_window_s": 12.0,
            "co2_sampling_gate_pressure_span_hpa": 0.20,
            "h2o_sampling_gate_pressure_span_hpa": 0.30,
            "co2_sampling_gate_pressure_fill_s": 5.0,
            "h2o_sampling_gate_pressure_fill_s": 8.0,
            "co2_sampling_gate_min_samples": 6,
            "h2o_sampling_gate_min_samples": 8,
            "co2_postseal_dewpoint_window_s": 4.0,
            "co2_postseal_dewpoint_timeout_s": 6.0,
            "co2_postseal_dewpoint_span_c": 0.12,
            "co2_postseal_dewpoint_slope_c_per_s": 0.04,
            "co2_postseal_dewpoint_min_samples": 6,
            "co2_postseal_rebound_guard_enabled": False,
            "co2_postseal_rebound_window_s": 8.0,
            "co2_postseal_rebound_min_rise_c": 0.12,
            "co2_postseal_physical_qc_enabled": True,
            "co2_postseal_physical_qc_max_abs_delta_c": 1.0,
            "co2_postseal_physical_qc_policy": "warn",
            "co2_postseal_timeout_policy": "pass",
            "co2_presample_long_guard_enabled": False,
            "co2_presample_long_guard_window_s": 8.0,
            "co2_presample_long_guard_timeout_s": 20.0,
            "co2_presample_long_guard_max_span_c": 0.15,
            "co2_presample_long_guard_max_abs_slope_c_per_s": 0.02,
            "co2_presample_long_guard_max_rise_c": 0.12,
            "co2_presample_long_guard_policy": "off",
            "co2_postsample_late_rebound_guard_enabled": True,
            "co2_postsample_late_rebound_max_rise_c": 0.12,
            "co2_postsample_late_rebound_policy": "warn",
            "co2_sampling_window_qc_enabled": True,
            "co2_sampling_window_qc_max_range_c": 0.20,
            "co2_sampling_window_qc_max_rise_c": 0.12,
            "co2_sampling_window_qc_max_abs_slope_c_per_s": 0.02,
            "co2_sampling_window_qc_policy": "warn",
            "h2o_postseal_dewpoint_window_s": 2.5,
            "h2o_postseal_dewpoint_timeout_s": 5.5,
            "h2o_postseal_dewpoint_span_c": 0.18,
            "h2o_postseal_dewpoint_slope_c_per_s": 0.06,
            "h2o_postseal_dewpoint_min_samples": 4,
            "preseal_ready_snapshot_max_age_s": 6.0,
            "preseal_ready_target_tolerance_hpa": 0.5,
            "preseal_trigger_overshoot_warn_hpa": 10.0,
            "preseal_trigger_overshoot_reject_hpa": None,
            "skip_fixed_post_stable_delay_when_adaptive": True,
            "soft_control_enabled": False,
            "soft_control_use_active_mode": True,
            "soft_control_linear_slew_hpa_per_s": 10.0,
            "soft_control_disallow_overshoot": True,
            "atmosphere_hold_strategy": "legacy_hold_thread",
            "continuous_atmosphere_hold": True,
            "vent_after_valve_open": False,
            "vent_popup_ack_disable_for_automation": False,
            "handoff_fast_enabled": False,
            "handoff_safe_open_delta_hpa": 3.0,
            "handoff_use_pressure_gauge": True,
            "handoff_require_vent_completed": False,
            "fast_gauge_response_timeout_s": 0.6,
            "transition_gauge_response_timeout_s": 1.5,
            "fast_gauge_read_retries": 1,
            "strict_control_ready_check": True,
            "abort_on_vent_off_failure": True,
        },
        "stability": {
            "gas_route_dewpoint_gate_enabled": True,
            "water_route_dewpoint_gate_enabled": False,
            "gas_route_dewpoint_gate_policy": "reject",
            "gas_route_dewpoint_gate_window_s": 60.0,
            "gas_route_dewpoint_gate_max_total_wait_s": 1080.0,
            "gas_route_dewpoint_gate_poll_s": 2.0,
            "gas_route_dewpoint_gate_tail_span_max_c": 0.35,
            "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.003,
            "gas_route_dewpoint_gate_rebound_window_s": 180.0,
            "gas_route_dewpoint_gate_rebound_min_rise_c": 1.0,
            "gas_route_dewpoint_gate_log_interval_s": 15.0,
            "dewpoint": {
                "enabled": True,
                "rh_match_tol_pct": 3.3,
            },
        },
    },
    "coefficients": {
        "ratio_poly_fit": {
            "pressure_source_preference": "reference_first",
        }
    },
    "validation": {
        "offline": {
            "mode": "both",
            "gas": "both",
        },
        "dry_collect": {
            "write_coefficients": False,
            "include_pressure": False,
            "include_temperature": False,
        },
        "pressure_only": {
            "prompt_between_batches": True,
        },
        "coefficient_roundtrip": {
            "write_back_same": False,
            "allow_write_modified": False,
        },
    },
}


def _clone_defaults() -> Dict[str, Any]:
    return json.loads(json.dumps(_RUNTIME_DEFAULTS))


def _merge_missing_defaults(target: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in defaults.items():
        if isinstance(value, dict):
            existing = target.get(key)
            if not isinstance(existing, dict):
                target[key] = dict(value)
                continue
            _merge_missing_defaults(existing, value)
            continue
        if key not in target:
            target[key] = value
    return target


def load_config(path: str | Path) -> Dict[str, Any]:
    """
    读取并标准化配置字典。

    处理逻辑：
    1. 解析 JSON；
    2. 根据配置文件位置推导项目基目录；
    3. 将 `paths` 中的相对路径转换为绝对路径。
    """
    p = Path(path).resolve()
    raw = p.read_text(encoding="utf-8-sig")
    cfg = json.loads(raw)
    cfg = _merge_missing_defaults(cfg, _clone_defaults())

    # 约定：configs/default_config.json 位于项目根目录下的 configs，
    # 所以基目录取其上两级（.../gas_calibrator）。
    base_dir = p.parent.parent
    cfg["_base_dir"] = str(base_dir)

    paths = cfg.get("paths", {})
    for key, value in list(paths.items()):
        try:
            v = Path(value)
            if not v.is_absolute():
                paths[key] = str((base_dir / v).resolve())
        except Exception:
            # 路径字段存在非路径字符串时，保持原值，不中断加载。
            continue
    cfg["paths"] = paths
    return cfg


def get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    使用点分路径读取嵌套字典值。

    示例：`get(cfg, "devices.thermometer.port", "COM1")`
    """
    cur = d
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur
