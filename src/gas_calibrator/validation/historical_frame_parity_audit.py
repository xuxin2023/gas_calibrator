"""Read-only V1.5 historical frame parity audit for the 0620/0621 baseline."""

from __future__ import annotations

from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.devices.gas_analyzer import GasAnalyzer


_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CATALOG_PATH = (
    _REPO_ROOT
    / "docs"
    / "v1_5_flow_contract"
    / "legacy_historical_evidence_catalog"
    / "v1_5_legacy_historical_evidence_catalog.json"
)
DEFAULT_OBSERVED_FIXTURE_PATH = (
    _REPO_ROOT
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "metrology"
    / "ga_d5_0620_0621_observed_gap_fixture_v1.json"
)

_VALUE_FIELDS = (
    "id",
    "co2_ppm",
    "h2o_mmol",
    "co2_density",
    "h2o_density",
    "co2_ratio_f",
    "co2_ratio_raw",
    "h2o_ratio_f",
    "h2o_ratio_raw",
    "ref_signal",
    "co2_signal",
    "h2o_signal",
    "chamber_temp_c",
    "case_temp_c",
    "pressure_kpa",
)
_BOUNDARY = {
    "source_mode": "historical_read_only_replay",
    "opens_com_ports": False,
    "writes_devices": False,
    "writes_coefficients": False,
    "writes_database": False,
    "refreshes_real_primary_latest": False,
    "not_real_acceptance_evidence": True,
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _accepted_0620_points(catalog: Mapping[str, Any]) -> list[dict[str, Any]]:
    points = [
        dict(item)
        for item in list(catalog.get("points") or [])
        if bool(item.get("accepted_manifest_member"))
        and str(item.get("route_kind") or "") == "co2"
    ]
    points.sort(key=lambda item: str(item.get("point_dir") or "").casefold())
    return points


def _expected_id_by_port(config: Mapping[str, Any]) -> dict[str, str]:
    devices = dict(config.get("devices") or {})
    rows = list(devices.get("gas_analyzers") or [])
    mapping: dict[str, str] = {}
    for row in rows:
        item = dict(row or {})
        port = str(item.get("port") or "").strip().upper()
        device_id = str(
            item.get("protocol_device_id") or item.get("device_id") or ""
        ).strip()
        if port and device_id:
            mapping[port] = device_id.zfill(3)
    return mapping


def _single_file(directory: Path, pattern: str) -> Path:
    candidates = sorted(path for path in directory.glob(pattern) if path.is_file())
    if len(candidates) != 1:
        raise ValueError(
            f"{directory} expected exactly one {pattern}; found {len(candidates)}"
        )
    return candidates[0]


def _same_value(expected: Any, actual: Any) -> bool:
    if expected in ("", None) and actual in ("", None):
        return True
    if isinstance(actual, (int, float)):
        try:
            return abs(float(expected) - float(actual)) <= 1e-9
        except (TypeError, ValueError):
            return False
    return str(expected or "").strip() == str(actual or "").strip()


def _audit_sampled_frames(
    samples_path: Path,
    *,
    analyzer: GasAnalyzer,
) -> tuple[int, list[dict[str, Any]]]:
    compared = 0
    mismatches: list[dict[str, Any]] = []
    with samples_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row_index, row in enumerate(csv.DictReader(stream), start=2):
            for channel in range(1, 7):
                prefix = f"ga{channel:02d}"
                raw = str(row.get(f"{prefix}_raw") or "").strip()
                if not raw:
                    continue
                parsed = analyzer.parse_line_mode2(raw)
                compared += 1
                if parsed is None:
                    mismatches.append(
                        {
                            "row": row_index,
                            "channel": prefix,
                            "field": "parse",
                            "expected": "mode2",
                            "actual": "unparsed",
                        }
                    )
                    continue
                for field in _VALUE_FIELDS:
                    expected = row.get(f"{prefix}_{field}")
                    actual = parsed.get(field)
                    if not _same_value(expected, actual):
                        mismatches.append(
                            {
                                "row": row_index,
                                "channel": prefix,
                                "field": field,
                                "expected": expected,
                                "actual": actual,
                            }
                        )
    return compared, mismatches


def audit_historical_frames(
    *,
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    observed_fixture_path: str | Path = DEFAULT_OBSERVED_FIXTURE_PATH,
) -> dict[str, Any]:
    catalog_file = Path(catalog_path)
    fixture_file = Path(observed_fixture_path)
    input_hashes_before: dict[str, str] = {
        str(path): _sha256(path) for path in (catalog_file, fixture_file)
    }
    catalog = _load_json(catalog_file)
    observed = _load_json(fixture_file)
    points = _accepted_0620_points(catalog)
    analyzer = GasAnalyzer("HISTORICAL_READ_ONLY_AUDIT")

    frame_rows: list[dict[str, Any]] = []
    point_rows: list[dict[str, Any]] = []
    mismatch_reasons: Counter[str] = Counter()
    sampled_mismatches: list[dict[str, Any]] = []
    input_paths: list[Path] = [catalog_file, fixture_file]

    for point in points:
        directory = Path(str(point.get("point_dir") or ""))
        samples_path = directory / "samples_machine_readable.csv"
        frame_quality_path = directory / "frame_quality_summary.csv"
        config_path = directory / "runtime_config_snapshot.json"
        io_path = _single_file(directory, "io_*.csv")
        required = (samples_path, frame_quality_path, config_path, io_path)
        missing = [str(path) for path in required if not path.is_file()]
        if missing:
            raise FileNotFoundError(
                "missing historical point inputs: " + ", ".join(missing)
            )
        input_paths.extend(required)
        for path in required:
            input_hashes_before[str(path)] = _sha256(path)
        config = _load_json(config_path)
        expected_by_port = _expected_id_by_port(config)
        if not expected_by_port:
            raise ValueError(f"analyzer identity mapping missing: {config_path}")

        counts_by_port: Counter[str] = Counter()
        valid_by_port: Counter[str] = Counter()
        with io_path.open("r", encoding="utf-8-sig", newline="") as stream:
            for source_row, row in enumerate(csv.DictReader(stream), start=2):
                if str(row.get("device") or "") != "gas_analyzer":
                    continue
                if str(row.get("direction") or "") != "RX":
                    continue
                raw = str(row.get("response") or "").strip()
                # Query acknowledgements such as ``YGAS,ID,T`` are not analyzer
                # measurement frames. Long YGAS responses remain in scope so a
                # malformed measurement frame still fails the audit.
                if "YGAS" not in raw.upper() or raw.count(",") < 14:
                    continue
                port = str(row.get("port") or "").strip().upper()
                counts_by_port[port] += 1
                parsed = analyzer.parse_line_mode2(raw)
                expected_id = expected_by_port.get(port, "")
                parsed_id = str((parsed or {}).get("id") or "")
                reasons: list[str] = []
                if parsed is None:
                    reasons.append("current_parser_rejected_frame")
                if not expected_id:
                    reasons.append("port_missing_from_runtime_identity_map")
                elif parsed_id != expected_id:
                    reasons.append("frame_id_mismatch")
                status = "pass" if not reasons else "fail"
                if status == "pass":
                    valid_by_port[port] += 1
                for reason in reasons:
                    mismatch_reasons[reason] += 1
                frame_rows.append(
                    {
                        "point_name": str(point.get("point_name") or directory.name),
                        "source_row": source_row,
                        "timestamp": str(row.get("timestamp") or ""),
                        "port": port,
                        "expected_id": expected_id,
                        "parsed_id": parsed_id,
                        "schema_version": str(
                            (parsed or {}).get("mode2_schema_version") or ""
                        ),
                        "field_count": (parsed or {}).get("mode2_field_count"),
                        "status": status,
                        "reason": ";".join(reasons),
                        "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                        "raw": raw,
                    }
                )

        sampled_count, point_sample_mismatches = _audit_sampled_frames(
            samples_path,
            analyzer=analyzer,
        )
        for mismatch in point_sample_mismatches:
            sampled_mismatches.append(
                {
                    "point_name": str(point.get("point_name") or directory.name),
                    **mismatch,
                }
            )
        with frame_quality_path.open("r", encoding="utf-8-sig", newline="") as stream:
            frame_quality_rows = list(csv.DictReader(stream))
        point_rows.append(
            {
                "point_name": str(point.get("point_name") or directory.name),
                "point_dir": str(directory),
                "raw_frames": sum(counts_by_port.values()),
                "parsed_identity_pass_frames": sum(valid_by_port.values()),
                "sampled_frames_compared": sampled_count,
                "sampled_value_mismatches": len(point_sample_mismatches),
                "expected_analyzer_count": len(expected_by_port),
                "observed_port_count": len(counts_by_port),
                "frame_quality_rows": len(frame_quality_rows),
                "status": (
                    "pass"
                    if sum(counts_by_port.values()) == sum(valid_by_port.values())
                    and not point_sample_mismatches
                    and len(frame_quality_rows) == len(expected_by_port)
                    else "fail"
                ),
            }
        )

    unique_input_paths = list(dict.fromkeys(input_paths))
    input_hashes_after = {str(path): _sha256(path) for path in unique_input_paths}
    source_mutation_count = sum(
        1
        for path, before in input_hashes_before.items()
        if input_hashes_after[path] != before
    )

    observed_state = dict(observed.get("observed_state") or {})
    method = dict(observed.get("method_contract") or {})
    expected_points = int(observed_state.get("co2_0620_accepted_point_count") or 0)
    total_frames = len(frame_rows)
    parsed_pass = sum(1 for row in frame_rows if row["status"] == "pass")
    sampled_compared = sum(int(row["sampled_frames_compared"]) for row in point_rows)
    status = (
        "PASS"
        if len(points) == expected_points == 45
        and total_frames > 0
        and total_frames == parsed_pass
        and not sampled_mismatches
        and all(row["status"] == "pass" for row in point_rows)
        and source_mutation_count == 0
        else "FAIL"
    )
    return {
        "artifact_type": "historical_0620_0621_frame_parity_audit",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "historical_0620_0621_frame_parity_audit_v1",
        "status": status,
        "boundary": dict(_BOUNDARY),
        "method_invariants": {
            "mature_route_contract_id": method.get("mature_route_contract_id"),
            "production_default_profile_id": method.get(
                "production_default_profile_id"
            ),
            "formal_calibration_sample_hz": method.get("formal_calibration_sample_hz"),
            "average1": method.get("average1"),
            "average2": method.get("average2"),
            "pressure_first_senco9_required": method.get(
                "pressure_first_senco9_required"
            ),
            "co2_zero_separate_from_h2o_dry": method.get(
                "co2_zero_separate_from_h2o_dry"
            ),
            "filtered_ratio_required": method.get("filtered_ratio_required"),
        },
        "historical_flow_comparison": {
            "co2_0620_expected_points": observed_state.get(
                "co2_0620_expected_point_count"
            ),
            "co2_0620_accepted_points": observed_state.get(
                "co2_0620_accepted_point_count"
            ),
            "co2_0621_completed_entry_points": observed_state.get(
                "co2_0621_completed_entry_points"
            ),
            "co2_0621_incomplete_zero_attempt_count": observed_state.get(
                "co2_0621_incomplete_zero_attempt_count"
            ),
            "h2o_historical_device_count": observed_state.get(
                "h2o_historical_device_count"
            ),
            "h2o_historical_blocked_device_count": observed_state.get(
                "h2o_historical_blocked_device_count"
            ),
            "current_execution_changes_made_by_audit": False,
        },
        "summary": {
            "point_count": len(points),
            "point_pass_count": sum(1 for row in point_rows if row["status"] == "pass"),
            "raw_frame_count": total_frames,
            "raw_frame_parser_identity_pass_count": parsed_pass,
            "raw_frame_failure_count": total_frames - parsed_pass,
            "sampled_frame_count": sampled_compared,
            "sampled_value_mismatch_count": len(sampled_mismatches),
            "source_file_count": len(unique_input_paths),
            "source_mutation_count": source_mutation_count,
        },
        "mismatch_reasons": dict(sorted(mismatch_reasons.items())),
        "point_rows": point_rows,
        "frame_rows": frame_rows,
        "sampled_value_mismatches": sampled_mismatches,
        "source_hashes": input_hashes_after,
    }


def write_historical_frame_parity_artifacts(
    result: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, str]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    frame_rows = list(result.get("frame_rows") or [])
    frame_path = target / "historical_frame_by_frame_rows.csv"
    fieldnames = (
        "point_name",
        "source_row",
        "timestamp",
        "port",
        "expected_id",
        "parsed_id",
        "schema_version",
        "field_count",
        "status",
        "reason",
        "raw_sha256",
        "raw",
    )
    with frame_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(frame_rows)

    summary_payload = {
        key: value
        for key, value in dict(result).items()
        if key not in {"frame_rows", "source_hashes"}
    }
    summary_payload["frame_rows_file"] = frame_path.name
    summary_payload["frame_rows_sha256"] = _sha256(frame_path)
    summary_path = target / "historical_frame_parity_summary.json"
    summary_path.write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    summary = dict(result.get("summary") or {})
    comparison = dict(result.get("historical_flow_comparison") or {})
    method = dict(result.get("method_invariants") or {})
    report_lines = [
        "# 0620/0621 与当前解析链逐帧核对",
        "",
        "> 本报告是历史证据的只读重放，不打开 COM、不写设备、不拟合、不写数据库，也不构成真实验收。",
        "",
        "## 结论",
        "",
        f"- 总状态：`{result.get('status')}`",
        f"- 0620 接受点：`{summary.get('point_pass_count')}/{summary.get('point_count')}`",
        f"- 原始分析仪帧：`{summary.get('raw_frame_parser_identity_pass_count')}/{summary.get('raw_frame_count')}` 通过当前解析器与端口身份核对",
        f"- 正式采样帧字段：`{summary.get('sampled_frame_count')}` 帧，字段差异 `{summary.get('sampled_value_mismatch_count')}`",
        f"- 源文件修改：`{summary.get('source_mutation_count')}`",
        "",
        "## 0620/0621 流程差异",
        "",
        f"- 0620：期望 `{comparison.get('co2_0620_expected_points')}` 点，接受 `{comparison.get('co2_0620_accepted_points')}` 点。",
        f"- 0621：已完成入口点 `{comparison.get('co2_0621_completed_entry_points')}`；未形成完整结论的零点尝试 `{comparison.get('co2_0621_incomplete_zero_attempt_count')}` 次。",
        f"- 历史 H2O：`{comparison.get('h2o_historical_device_count')}` 台中 `{comparison.get('h2o_historical_blocked_device_count')}` 台在闭环复核中被阻断。",
        "",
        "## 当前必须保持的物理与程序口径",
        "",
        f"- 生产拟合档案：`{method.get('production_default_profile_id')}`。",
        f"- 采样：`{method.get('formal_calibration_sample_hz')} Hz`；`AVERAGE1={method.get('average1')}`；`AVERAGE2={method.get('average2')}`。",
        f"- pressure-first SENCO9：`{method.get('pressure_first_senco9_required')}`。",
        f"- CO2 零气与 H2O 干气点保持独立：`{method.get('co2_zero_separate_from_h2o_dry')}`。",
        f"- 拟合使用过滤后比值：`{method.get('filtered_ratio_required')}`。",
        "",
        "## 边界",
        "",
        "- 本轮没有修改 V1、`run_app.py`、点表、校准状态机、拟合算法或设备系数。",
        "- `historical_frame_by_frame_rows.csv` 保留每一帧的源行、端口、期望 ID、解析 ID、字段数、状态和原始帧哈希。",
        "",
    ]
    report_path = target / "historical_frame_parity_report.md"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    return {
        "execution_rows": str(frame_path),
        "execution_summary": str(summary_path),
        "diagnostic_analysis": str(report_path),
    }


__all__ = [
    "DEFAULT_CATALOG_PATH",
    "DEFAULT_OBSERVED_FIXTURE_PATH",
    "audit_historical_frames",
    "write_historical_frame_parity_artifacts",
]
