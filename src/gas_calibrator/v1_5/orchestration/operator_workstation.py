"""Thin, dry-run-only operator entry for the mature V1.5 calibration routes.

The workstation deliberately calls the reviewed 45/13 queue runners instead
of reproducing their physical logic.  It is an integration seam for a future
operator UI, not a replacement calibration kernel.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from ...config import load_config
from ...tools.run_v1_5_formal_co2_open_flow_queue import (
    _load_queue_rows as _load_co2_queue_rows,
)
from ...tools.run_v1_5_formal_co2_open_flow_queue import (
    _parse_text_filter as _parse_co2_roles,
)
from ...tools.run_v1_5_formal_co2_open_flow_queue import (
    _select_queue_rows as _select_co2_queue_rows,
)
from ...tools.run_v1_5_formal_co2_open_flow_queue import main as _run_co2_queue
from ...tools.run_v1_5_formal_h2o_open_flow_queue import (
    _load_queue_rows as _load_h2o_queue_rows,
)
from ...tools.run_v1_5_formal_h2o_open_flow_queue import (
    _select_queue_rows as _select_h2o_queue_rows,
)
from ...tools.run_v1_5_formal_h2o_open_flow_queue import main as _run_h2o_queue


SCHEMA = "v1_5_operator_workstation_dry_run_v1"
PRODUCT_NAME = "V1.5 气体分析仪校准工作站"
CALIBRATION_KERNEL = "v1_5_legacy_ratio_0613_0620_0621"
PROFILE_ID = "legacy_ratio_production"
CO2_RUNNER = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
H2O_RUNNER = "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
EXPECTED_POINT_COUNTS = {"co2": 45, "h2o": 13}
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _path_text(path: Path) -> str:
    return str(path.resolve())


def _inspect_certificate_registry(path: str | Path | None) -> tuple[dict[str, Any], list[str]]:
    """Inspect optional certificate metadata without turning it into a start gate."""

    if path is None:
        return (
            {
                "configured": False,
                "readable": False,
                "policy": "advisory_for_start_formal_release_reviewed_separately",
            },
            ["certificate_registry_not_configured_non_blocking"],
        )
    registry_path = Path(path).resolve()
    if not registry_path.exists():
        return (
            {
                "configured": True,
                "readable": False,
                "path": _path_text(registry_path),
                "policy": "advisory_for_start_formal_release_reviewed_separately",
            },
            ["certificate_registry_missing_non_blocking"],
        )
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return (
            {
                "configured": True,
                "readable": False,
                "path": _path_text(registry_path),
                "error": f"{type(exc).__name__}: {exc}",
                "policy": "advisory_for_start_formal_release_reviewed_separately",
            },
            ["certificate_registry_unreadable_non_blocking"],
        )
    return (
        {
            "configured": True,
            "readable": isinstance(payload, Mapping),
            "path": _path_text(registry_path),
            "schema_version": payload.get("schema_version") if isinstance(payload, Mapping) else None,
            "policy": "advisory_for_start_formal_release_reviewed_separately",
        },
        [] if isinstance(payload, Mapping) else ["certificate_registry_shape_invalid_non_blocking"],
    )


def _queue_point_counts(
    co2_queue_csv: Path,
    h2o_queue_csv: Path,
) -> tuple[dict[str, int], list[str]]:
    counts = {"co2": 0, "h2o": 0}
    blockers: list[str] = []
    if not co2_queue_csv.exists():
        blockers.append("co2_queue_csv_missing")
    else:
        try:
            counts["co2"] = len(
                _select_co2_queue_rows(
                    _load_co2_queue_rows(co2_queue_csv),
                    temps=None,
                    roles=_parse_co2_roles("fit,verification"),
                    max_points=None,
                )
            )
        except Exception as exc:
            blockers.append(f"co2_queue_csv_invalid:{type(exc).__name__}")
    if not h2o_queue_csv.exists():
        blockers.append("h2o_queue_csv_missing")
    else:
        try:
            counts["h2o"] = len(
                _select_h2o_queue_rows(
                    _load_h2o_queue_rows(h2o_queue_csv),
                    temps=None,
                    max_points=None,
                )
            )
        except Exception as exc:
            blockers.append(f"h2o_queue_csv_invalid:{type(exc).__name__}")
    for route_kind, expected in EXPECTED_POINT_COUNTS.items():
        if counts[route_kind] != expected:
            blockers.append(
                f"{route_kind}_legacy_point_count_mismatch:"
                f"expected={expected},observed={counts[route_kind]}"
            )
    return counts, blockers


def _route_plan(
    *,
    route_kind: str,
    runner_module: str,
    config_path: Path,
    queue_csv: Path,
    output_dir: Path,
    queue_run_id: str,
) -> dict[str, Any]:
    argv = [
        "--config",
        _path_text(config_path),
        "--queue-csv",
        _path_text(queue_csv),
        "--output-dir",
        _path_text(output_dir),
        "--run-id",
        queue_run_id,
        "--dry-run",
        "--no-prompt",
        "--no-ftd-write",
    ]
    if route_kind == "co2":
        argv.extend(["--temperature-order", "desc", "--roles", "fit,verification"])
    else:
        argv.extend(
            [
                "--temperature-order",
                "asc",
                "--h2o-pressure-presample-policy",
                "skip",
            ]
        )
    return {
        "route_kind": route_kind,
        "runner_module": runner_module,
        "queue_csv": _path_text(queue_csv),
        "output_dir": _path_text(output_dir),
        "queue_run_id": queue_run_id,
        "expected_point_count": EXPECTED_POINT_COUNTS[route_kind],
        "argv": argv,
        "command_preview": " ".join([sys.executable, "-m", runner_module, *argv]),
        "execution_mode": "mature_runner_dry_run",
        "opens_com_ports": False,
        "writes_coefficients": False,
    }


def build_v1_5_operator_workstation_plan(
    *,
    config_path: str | Path,
    co2_queue_csv: str | Path,
    h2o_queue_csv: str | Path,
    output_dir: str | Path,
    run_id: str,
    certificate_registry_json: str | Path | None = None,
) -> dict[str, Any]:
    """Build the V1.5-first operator plan without executing either route."""

    config = Path(config_path).resolve()
    co2_queue = Path(co2_queue_csv).resolve()
    h2o_queue = Path(h2o_queue_csv).resolve()
    root = Path(output_dir).resolve()
    blockers: list[str] = []

    if not run_id or not _RUN_ID_PATTERN.fullmatch(run_id):
        blockers.append("run_id_must_use_ascii_letters_digits_dot_dash_or_underscore")
    if not config.exists():
        blockers.append("runtime_config_missing")
    else:
        try:
            load_config(config)
        except Exception as exc:
            blockers.append(f"runtime_config_invalid:{type(exc).__name__}")

    point_counts, queue_blockers = _queue_point_counts(co2_queue, h2o_queue)
    blockers.extend(queue_blockers)
    certificate, warnings = _inspect_certificate_registry(certificate_registry_json)

    routes = [
        _route_plan(
            route_kind="co2",
            runner_module=CO2_RUNNER,
            config_path=config,
            queue_csv=co2_queue,
            output_dir=root / "co2",
            queue_run_id=f"{run_id}_co2",
        ),
        _route_plan(
            route_kind="h2o",
            runner_module=H2O_RUNNER,
            config_path=config,
            queue_csv=h2o_queue,
            output_dir=root / "h2o",
            queue_run_id=f"{run_id}_h2o",
        ),
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "product_name": PRODUCT_NAME,
        "calibration_kernel": CALIBRATION_KERNEL,
        "profile_id": PROFILE_ID,
        "overall_status": "blocked" if blockers else "ready_for_v1_5_dry_run",
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id,
        "runtime_config": _path_text(config),
        "point_counts": point_counts,
        "expected_point_counts": dict(EXPECTED_POINT_COUNTS),
        "route_order": ["co2", "h2o"],
        "routes": routes,
        "certificate_registry": certificate,
        "certificate_start_gate": "non_blocking",
        "formal_release_assessment": "not_evaluated_by_workstation_start_gate",
        "evidence_source": "dry_run",
        "not_real_acceptance_evidence": True,
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "modifies_mature_runners": False,
        "modifies_run_app": False,
        "v1_fallback_preserved": True,
        "v2_role": "temporary_migration_and_deletion_pool_not_product_runtime",
    }


def _find_queue_summary(route: Mapping[str, Any]) -> tuple[Path | None, dict[str, Any]]:
    output_dir = Path(str(route["output_dir"]))
    run_id = str(route["queue_run_id"])
    for path in sorted(output_dir.rglob("queue_summary.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        if str(payload.get("queue_run_id") or "") == run_id:
            return path, payload
    return None, {}


def execute_v1_5_operator_workstation_dry_run(
    plan: Mapping[str, Any],
    *,
    runner_overrides: Mapping[str, Callable[[Iterable[str]], int]] | None = None,
) -> dict[str, Any]:
    """Execute only the mature runners' built-in dry-run branches."""

    if plan.get("blockers"):
        return {
            **dict(plan),
            "overall_status": "blocked",
            "execution_started": False,
            "route_results": [],
        }
    runners: dict[str, Callable[[Iterable[str]], int]] = {
        "co2": _run_co2_queue,
        "h2o": _run_h2o_queue,
    }
    runners.update(dict(runner_overrides or {}))
    route_results: list[dict[str, Any]] = []
    for route in plan.get("routes", []):
        route_kind = str(route["route_kind"])
        returncode = int(runners[route_kind](list(route["argv"])))
        summary_path, summary = _find_queue_summary(route)
        result_blockers: list[str] = []
        if returncode != 0:
            result_blockers.append(f"runner_returncode={returncode}")
        if not summary_path:
            result_blockers.append("queue_summary_missing")
        if summary and summary.get("dry_run") is not True:
            result_blockers.append("queue_summary_not_dry_run")
        if summary and int(summary.get("dry_run_points") or 0) != int(route["expected_point_count"]):
            result_blockers.append(
                "dry_run_point_count_mismatch:"
                f"expected={route['expected_point_count']},"
                f"observed={summary.get('dry_run_points')}"
            )
        if summary and summary.get("no_write") is not True:
            result_blockers.append("queue_summary_no_write_false")
        route_results.append(
            {
                "route_kind": route_kind,
                "status": "pass" if not result_blockers else "failed",
                "returncode": returncode,
                "blockers": result_blockers,
                "queue_summary": _path_text(summary_path) if summary_path else "",
                "dry_run_points": summary.get("dry_run_points"),
                "opens_com_ports": False,
                "writes_coefficients": False,
            }
        )
        if result_blockers:
            break
    execution_blockers = [
        f"{row['route_kind']}:{reason}"
        for row in route_results
        for reason in row.get("blockers", [])
    ]
    return {
        **dict(plan),
        "completed_at": _now(),
        "overall_status": "pass" if not execution_blockers else "failed",
        "execution_started": True,
        "execution_blockers": execution_blockers,
        "route_results": route_results,
    }


def write_v1_5_operator_workstation_outputs(
    result: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_operator_workstation_dry_run.json"
    markdown_path = root / "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md"
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8-sig",
    )
    lines = [
        "# V1.5 气体分析仪校准工作站 dry-run",
        "",
        f"- 状态：`{result.get('overall_status')}`",
        f"- 校准内核：`{result.get('calibration_kernel')}`",
        f"- 生产配置：`{result.get('profile_id')}`",
        (
            "- CO2/H2O 点数："
            f"`{(result.get('point_counts') or {}).get('co2')}` / "
            f"`{(result.get('point_counts') or {}).get('h2o')}`"
        ),
        "- 证书资料：不阻断工作站启动和 dry-run；正式签发资格另行评审。",
        "- 边界：不打开 COM、不控制气路/水路、不写 SENCO/设备 ID、不构成真实验收。",
        "- V1 fallback 与 run_app.py 未修改；V2 仅保留分析、报告和治理职责。",
        "",
        "## 路由结果",
        "",
    ]
    for row in result.get("route_results", []):
        lines.append(
            f"- `{row.get('route_kind')}`: `{row.get('status')}`, "
            f"dry-run points=`{row.get('dry_run_points')}`"
        )
    if result.get("warnings"):
        lines.extend(["", "## 非阻断提醒", ""])
        lines.extend(f"- `{warning}`" for warning in result.get("warnings", []))
    if result.get("blockers") or result.get("execution_blockers"):
        lines.extend(["", "## 阻断项", ""])
        lines.extend(
            f"- `{blocker}`"
            for blocker in [
                *result.get("blockers", []),
                *result.get("execution_blockers", []),
            ]
        )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def run_v1_5_operator_workstation_application(
    plan: Mapping[str, Any],
    *,
    output_dir: str | Path,
    executor: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Path]]:
    """Execute once and write once through the shared GUI/CLI seam."""

    active_executor = executor or execute_v1_5_operator_workstation_dry_run
    result = dict(active_executor(plan))
    outputs = write_v1_5_operator_workstation_outputs(result, output_dir)
    return result, outputs


__all__ = [
    "CALIBRATION_KERNEL",
    "EXPECTED_POINT_COUNTS",
    "PRODUCT_NAME",
    "PROFILE_ID",
    "build_v1_5_operator_workstation_plan",
    "execute_v1_5_operator_workstation_dry_run",
    "run_v1_5_operator_workstation_application",
    "write_v1_5_operator_workstation_outputs",
]
