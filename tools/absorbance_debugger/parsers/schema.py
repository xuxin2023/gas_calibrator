"""Column-label schema helpers.

This module keeps source files ASCII-only by expressing Chinese headers with
Unicode escape sequences.
"""

from __future__ import annotations

from typing import Dict


POINT_COLUMNS: Dict[str, str] = {
    "point_title": "\u70b9\u4f4d\u6807\u9898",
    "point_row": "\u6821\u51c6\u70b9\u884c\u53f7",
    "stage": "\u6d41\u7a0b\u9636\u6bb5",
    "point_tag": "\u70b9\u4f4d\u6807\u7b7e",
    "pressure_mode": "\u538b\u529b\u6267\u884c\u6a21\u5f0f",
    "pressure_target_label": "\u538b\u529b\u76ee\u6807\u6807\u7b7e",
    "temp_set_c": "\u6e29\u7bb1\u76ee\u6807\u6e29\u5ea6C",
    "target_co2_ppm": "\u76ee\u6807\u4e8c\u6c27\u5316\u78b3\u6d53\u5ea6ppm",
    "target_pressure_hpa": "\u76ee\u6807\u538b\u529bhPa",
    "analyzers_expected": "\u5206\u6790\u4eea\u5e94\u5230\u53f0\u6570",
    "analyzers_with_frames": "\u5206\u6790\u4eea\u6709\u5e27\u53f0\u6570",
    "analyzers_usable": "\u5206\u6790\u4eea\u53ef\u7528\u53f0\u6570",
    "analyzer_coverage": "\u5206\u6790\u4eea\u8986\u76d6\u7387",
    "analyzer_integrity": "\u5206\u6790\u4eea\u6570\u636e\u5b8c\u6574\u6027",
    "missing_analyzers": "\u7f3a\u5931\u5206\u6790\u4eea",
    "abnormal_analyzers": "\u5f02\u5e38\u5e27\u5206\u6790\u4eea",
    "point_quality": "\u70b9\u4f4d\u8d28\u91cf\u7ed3\u679c",
    "point_quality_reason": "\u70b9\u4f4d\u8d28\u91cf\u539f\u56e0",
    "point_quality_flag": "\u70b9\u4f4d\u8d28\u91cf\u6807\u8bb0",
    "point_quality_blocking": "\u70b9\u4f4d\u8d28\u91cf\u662f\u5426\u963b\u65ad",
    "pressure_ctrl_hpa_mean": "\u538b\u529b\u63a7\u5236\u5668\u538b\u529bhPa_\u5e73\u5747\u503c",
    "pressure_gauge_hpa_mean": "\u6570\u5b57\u538b\u529b\u8ba1\u538b\u529bhPa_\u5e73\u5747\u503c",
    "temp_std_c_mean": "\u6570\u5b57\u6e29\u5ea6\u8ba1\u6e29\u5ea6C_\u5e73\u5747\u503c",
    "ratio_co2_filt_mean": "\u4e8c\u6c27\u5316\u78b3\u6bd4\u503c\u6ee4\u6ce2\u540e_\u5e73\u5747\u503c",
    "ratio_co2_raw_mean": "\u4e8c\u6c27\u5316\u78b3\u6bd4\u503c\u539f\u59cb\u503c_\u5e73\u5747\u503c",
    "ref_signal_mean": "\u53c2\u8003\u4fe1\u53f7_\u5e73\u5747\u503c",
    "co2_signal_mean": "\u4e8c\u6c27\u5316\u78b3\u4fe1\u53f7_\u5e73\u5747\u503c",
    "temp_shell_c_mean": "\u673a\u58f3\u6e29\u5ea6C_\u5e73\u5747\u503c",
    "pressure_dev_kpa_mean": "\u5206\u6790\u4eea\u538b\u529bkPa_\u5e73\u5747\u503c",
}

SAMPLE_COLUMNS: Dict[str, str] = {
    "point_title": "\u70b9\u4f4d\u6807\u9898",
    "sample_index": "\u6837\u672c\u5e8f\u53f7",
    "sample_ts": "\u91c7\u6837\u65f6\u95f4",
    "sample_target_ts": "\u91c7\u6837\u76ee\u6807\u65f6\u95f4",
    "sample_start_ts": "\u91c7\u6837\u5f00\u59cb\u65f6\u95f4",
    "sample_lag_ms": "\u91c7\u6837\u6ede\u540ems",
    "stage": "\u6d41\u7a0b\u9636\u6bb5",
    "point_tag": "\u70b9\u4f4d\u6807\u7b7e",
    "point_row": "\u6821\u51c6\u70b9\u884c\u53f7",
    "stage_tag": "\u6d41\u7a0b\u9636\u6bb5\u6807\u7b7e",
    "route": "\u91c7\u6837\u8def\u7ebf",
    "pressure_mode": "\u538b\u529b\u6267\u884c\u6a21\u5f0f",
    "pressure_target_label": "\u538b\u529b\u76ee\u6807\u6807\u7b7e",
    "pressure_gauge_hpa": "\u6570\u5b57\u538b\u529b\u8ba1\u538b\u529bhPa",
    "temp_std_c": "\u6570\u5b57\u6e29\u5ea6\u8ba1\u6e29\u5ea6C",
    "target_co2_ppm": "\u76ee\u6807\u4e8c\u6c27\u5316\u78b3\u6d53\u5ea6ppm",
    "target_pressure_hpa": "\u76ee\u6807\u538b\u529bhPa",
    "temp_set_c": "\u6e29\u7bb1\u8bbe\u5b9a\u6e29\u5ea6C",
    "sampling_duration_ms": "\u91c7\u6837\u8017\u65f6ms",
}

ANALYZER_FIELDS: Dict[str, str] = {
    "raw_message": "\u539f\u59cb\u62a5\u6587",
    "device_id": "\u8bbe\u5907ID",
    "mode": "\u6a21\u5f0f",
    "mode2_field_count": "MODE2\u5b57\u6bb5\u6570",
    "co2_ppm": "\u4e8c\u6c27\u5316\u78b3\u6d53\u5ea6ppm",
    "h2o_mmol": "\u6c34\u6d53\u5ea6mmol\u6bcfmol",
    "co2_density": "\u4e8c\u6c27\u5316\u78b3\u5bc6\u5ea6",
    "h2o_density": "\u6c34\u5bc6\u5ea6",
    "ratio_co2_filt": "\u4e8c\u6c27\u5316\u78b3\u6bd4\u503c\u6ee4\u6ce2\u540e",
    "ratio_co2_raw": "\u4e8c\u6c27\u5316\u78b3\u6bd4\u503c\u539f\u59cb\u503c",
    "ratio_h2o_filt": "\u6c34\u6bd4\u503c\u6ee4\u6ce2\u540e",
    "ratio_h2o_raw": "\u6c34\u6bd4\u503c\u539f\u59cb\u503c",
    "ref_signal": "\u53c2\u8003\u4fe1\u53f7",
    "co2_signal": "\u4e8c\u6c27\u5316\u78b3\u4fe1\u53f7",
    "h2o_signal": "\u6c34\u4fe1\u53f7",
    "temp_cavity_c": "\u6e29\u5ea6\u7bb1\u6e29\u5ea6C",
    "temp_shell_c": "\u673a\u58f3\u6e29\u5ea6C",
    "pressure_dev_kpa": "\u5206\u6790\u4eea\u538b\u529bkPa",
    "status": "\u72b6\u6001",
    "frame_ts": "\u5206\u6790\u4eea\u7f13\u5b58\u65f6\u95f4",
    "frame_age_ms": "\u5206\u6790\u4eea\u7f13\u5b58\u5e74\u9f84ms",
    "selected_frame_ts": "\u5206\u6790\u4eea\u9009\u4e2d\u5e27\u63a5\u6536\u65f6\u95f4",
    "selected_frame_seq": "\u5206\u6790\u4eea\u9009\u4e2d\u5e27\u5e8f\u53f7",
    "frame_offset_ms": "\u5206\u6790\u4eea\u5e27\u8ddd\u951a\u70b9ms",
    "frame_side": "\u5206\u6790\u4eea\u5e27\u951a\u70b9\u4fa7",
    "frame_match_strategy": "\u5206\u6790\u4eea\u5e27\u5339\u914d\u7b56\u7565",
    "frame_is_stale": "\u5206\u6790\u4eea\u5e27\u662f\u5426\u9648\u65e7",
    "frame_source": "\u5206\u6790\u4eea\u5e27\u6765\u6e90",
    "frame_is_realtime": "\u5206\u6790\u4eea\u5e27\u662f\u5426\u5b9e\u65f6",
    "has_frame": "\u5206\u6790\u4eea\u6709\u5e27",
    "usable_frame": "\u5206\u6790\u4eea\u53ef\u7528\u5e27",
    "frame_status": "\u5206\u6790\u4eea\u5e27\u72b6\u6001",
}


def analyzer_prefix(slot: int) -> str:
    """Return the CSV prefix for one analyzer slot."""

    return f"\u6c14\u4f53\u5206\u6790\u4eea{slot}_"


def build_analyzer_column(slot: int, field_name: str) -> str:
    """Return the concrete sample CSV column name for one analyzer field."""

    return analyzer_prefix(slot) + ANALYZER_FIELDS[field_name]


def normalize_analyzer_label(raw: str) -> str:
    """Normalize runtime analyzer labels to GAxx."""

    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.upper()
    if text.startswith("GA"):
        return text
    if text.startswith("ANALYZER"):
        digits = "".join(ch for ch in text if ch.isdigit())
        if digits:
            return f"GA{int(digits):02d}"
    if text.startswith("GAS_ANALYZER"):
        digits = "".join(ch for ch in text if ch.isdigit())
        if digits:
            return f"GA{int(digits):02d}"
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        return f"GA{int(digits):02d}"
    return text


def analyzer_slot_from_label(label: str) -> int:
    """Infer analyzer slot from GAxx label."""

    digits = "".join(ch for ch in str(label) if ch.isdigit())
    if not digits:
        raise ValueError(f"Unable to infer analyzer slot from {label!r}")
    return int(digits)
