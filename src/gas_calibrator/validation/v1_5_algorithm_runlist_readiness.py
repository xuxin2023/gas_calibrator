"""Offline V1.5 new-algorithm runlist readiness gate.

This sidecar validates the queue-compatible runlist preview before any future
runner integration. It never opens COM ports, controls gas/water routes,
connects PostgreSQL, or writes analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_algorithm_runlist_readiness_v1"
RUNLIST_MANIFEST = "v1_5_algorithm_formal_runlist_preview_manifest.json"
CO2_RUNLIST = "v1_5_new_algorithm_formal_co2_runlist_preview.csv"
H2O_RUNLIST = "v1_5_new_algorithm_formal_h2o_runlist_preview.csv"
REQUIRED_CO2_SUPPLEMENTS = {"-20/600", "-10/600"}
REQUIRED_H2O_SUPPLEMENTS = {"40/30/30"}


@dataclass(frozen=True)
class AlgorithmRunlistReadinessCheck:
    check: str
    status: str
    evidence_role: str
    evidence_path: str
    reasons: tuple[str, ...]
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _path_text(path: Path) -> str:
    return str(path.resolve()) if path.exists() else ""


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _segment_values(rows: Sequence[Mapping[str, Any]], *, temp_c: float, value_key: str) -> list[int]:
    out: list[int] = []
    for row in rows:
        temp = _as_float(row.get("temp_c"))
        value = _as_float(row.get(value_key))
        if temp is None or value is None:
            continue
        if abs(temp - temp_c) <= 1e-9:
            out.append(int(value))
    return sorted(out)


def _h2o_40_hgen30_segment(rows: Sequence[Mapping[str, Any]]) -> list[int]:
    out: list[int] = []
    for row in rows:
        temp = _as_float(row.get("temp_c"))
        hgen = _as_float(row.get("hgen_temp_c"))
        rh = _as_float(row.get("hgen_rh_pct"))
        if temp is None or hgen is None or rh is None:
            continue
        if abs(temp - 40.0) <= 1e-9 and abs(hgen - 30.0) <= 1e-9:
            out.append(int(rh))
    return sorted(out)


def _required_columns(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> tuple[str, ...]:
    if not rows:
        return tuple(columns)
    missing = [column for column in columns if column not in rows[0]]
    return tuple(missing)


def _supplement_keys(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for row in rows:
        if row.get("point_role") != "new_algorithm_required_supplemental_formal_point":
            continue
        if row.get("historical_missing_point_semantics") != "formal_required_point_not_historical_resampling":
            continue
        if row.get("runner_integration_status") != "preview_only_not_runner_wired":
            continue
        key = str(row.get("source_point_key") or "").strip()
        if key:
            keys.add(key)
    return keys


def build_v1_5_algorithm_runlist_readiness(
    *,
    runlist_dir: str | Path,
    manifest_path: str | Path | None = None,
    co2_runlist_csv: str | Path | None = None,
    h2o_runlist_csv: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(runlist_dir).resolve()
    manifest_file = Path(manifest_path).resolve() if manifest_path else root / RUNLIST_MANIFEST
    co2_file = Path(co2_runlist_csv).resolve() if co2_runlist_csv else root / CO2_RUNLIST
    h2o_file = Path(h2o_runlist_csv).resolve() if h2o_runlist_csv else root / H2O_RUNLIST

    manifest = _read_json(manifest_file)
    co2_rows = _read_csv(co2_file)
    h2o_rows = _read_csv(h2o_file)
    checks: list[AlgorithmRunlistReadinessCheck] = []

    manifest_reasons: list[str] = []
    if not manifest_file.exists():
        manifest_reasons.append("runlist_preview_manifest_missing")
    if manifest.get("status") != "pass":
        manifest_reasons.append(f"runlist_preview_status={manifest.get('status') or 'missing'}")
    if manifest.get("runner_integration_status") != "preview_only_not_runner_wired":
        manifest_reasons.append("runner_integration_status_not_preview_only")
    if manifest.get("opens_com_ports") is not False:
        manifest_reasons.append("preview_must_not_open_com_ports")
    if manifest.get("controls_water_or_gas_routes") is not False:
        manifest_reasons.append("preview_must_not_control_routes")
    if manifest.get("writes_coefficients") is not False:
        manifest_reasons.append("preview_must_not_write_coefficients")
    if manifest.get("legacy_co2_formal_point_count") != 45:
        manifest_reasons.append("legacy_co2_count_not_45")
    if manifest.get("legacy_h2o_formal_point_count") != 13:
        manifest_reasons.append("legacy_h2o_count_not_13")
    checks.append(
        AlgorithmRunlistReadinessCheck(
            check="runlist_preview_manifest_contract",
            status="ready" if not manifest_reasons else "blocker",
            evidence_role="algorithm_formal_runlist_preview_manifest",
            evidence_path=_path_text(manifest_file),
            reasons=tuple(manifest_reasons),
            physical_meaning="The preflight consumes an offline preview only; it must not become a hidden live runner.",
            next_action="Regenerate the runlist preview from the profile before considering runner integration.",
            details={
                "profile_id": manifest.get("profile_id"),
                "runner_integration_status": manifest.get("runner_integration_status"),
                "legacy_co2_formal_point_count": manifest.get("legacy_co2_formal_point_count"),
                "legacy_h2o_formal_point_count": manifest.get("legacy_h2o_formal_point_count"),
            },
        )
    )

    co2_missing_columns = _required_columns(
        co2_rows,
        (
            "point_id",
            "component",
            "temp_c",
            "source_nominal_ppm",
            "co2_group",
            "sample_role",
            "runner",
            "source_point_key",
            "point_role",
            "historical_missing_point_semantics",
            "runner_integration_status",
        ),
    )
    co2_segments = {
        "-20C": _segment_values(co2_rows, temp_c=-20.0, value_key="source_nominal_ppm"),
        "-10C": _segment_values(co2_rows, temp_c=-10.0, value_key="source_nominal_ppm"),
    }
    co2_reasons: list[str] = []
    if not co2_file.exists():
        co2_reasons.append("co2_runlist_csv_missing")
    if len(co2_rows) != 47:
        co2_reasons.append(f"co2_runlist_count={len(co2_rows)}")
    if co2_missing_columns:
        co2_reasons.append(f"co2_missing_columns={';'.join(co2_missing_columns)}")
    if co2_segments["-20C"] != [0, 400, 600, 1000]:
        co2_reasons.append("co2_minus20_segment_missing_600ppm_or_order")
    if co2_segments["-10C"] != [0, 400, 600, 1000]:
        co2_reasons.append("co2_minus10_segment_missing_600ppm_or_order")
    checks.append(
        AlgorithmRunlistReadinessCheck(
            check="new_algorithm_co2_runlist_47_point_gate",
            status="ready" if not co2_reasons else "blocker",
            evidence_role="new_algorithm_co2_runlist_preview",
            evidence_path=_path_text(co2_file),
            reasons=tuple(co2_reasons),
            physical_meaning="New-algorithm CO2 must run 47 formal points; the low-temperature 600ppm points are normal scheduled gas points.",
            next_action="Regenerate the new-algorithm CO2 runlist preview; do not run the legacy 45-point queue as the new-algorithm flow.",
            details={"row_count": len(co2_rows), "segments": co2_segments},
        )
    )

    h2o_missing_columns = _required_columns(
        h2o_rows,
        (
            "point_id",
            "component",
            "temp_c",
            "hgen_temp_c",
            "hgen_rh_pct",
            "reference_bridge_status",
            "sample_role",
            "runner",
            "source_point_key",
            "point_role",
            "historical_missing_point_semantics",
            "runner_integration_status",
        ),
    )
    h2o_segment = _h2o_40_hgen30_segment(h2o_rows)
    h2o_reasons: list[str] = []
    if not h2o_file.exists():
        h2o_reasons.append("h2o_runlist_csv_missing")
    if len(h2o_rows) != 14:
        h2o_reasons.append(f"h2o_runlist_count={len(h2o_rows)}")
    if h2o_missing_columns:
        h2o_reasons.append(f"h2o_missing_columns={';'.join(h2o_missing_columns)}")
    if h2o_segment != [30, 50, 70]:
        h2o_reasons.append("h2o_40c_hgen30_segment_missing_30rh_or_order")
    if not any(
        row.get("source_point_key") == "40/30/30"
        and row.get("reference_bridge_status") == "requires_humidity_reference_bridge_before_fit_or_release"
        for row in h2o_rows
    ):
        h2o_reasons.append("h2o_40c_30rh_reference_bridge_status_missing")
    checks.append(
        AlgorithmRunlistReadinessCheck(
            check="new_algorithm_h2o_runlist_14_point_gate",
            status="ready" if not h2o_reasons else "blocker",
            evidence_role="new_algorithm_h2o_runlist_preview",
            evidence_path=_path_text(h2o_file),
            reasons=tuple(h2o_reasons),
            physical_meaning="New-algorithm H2O must run 14 wet points; 40C/HGEN30C/30RH is a formal water point and its reference bridge remains explicit.",
            next_action="Regenerate the new-algorithm H2O runlist preview and close the humidity reference bridge before fit/release.",
            details={"row_count": len(h2o_rows), "40C_HGEN30C_RH": h2o_segment},
        )
    )

    observed_supplements = _supplement_keys([*co2_rows, *h2o_rows])
    expected_supplements = REQUIRED_CO2_SUPPLEMENTS | REQUIRED_H2O_SUPPLEMENTS
    supplement_reasons: list[str] = []
    missing_supplements = sorted(expected_supplements - observed_supplements)
    if missing_supplements:
        supplement_reasons.append(f"missing_formal_supplements={';'.join(missing_supplements)}")
    extra_supplements = sorted(observed_supplements - expected_supplements)
    if extra_supplements:
        supplement_reasons.append(f"unexpected_formal_supplements={';'.join(extra_supplements)}")
    checks.append(
        AlgorithmRunlistReadinessCheck(
            check="formal_supplemental_point_semantics_gate",
            status="ready" if not supplement_reasons else "blocker",
            evidence_role="new_algorithm_supplemental_point_semantics",
            evidence_path=";".join(path for path in (_path_text(co2_file), _path_text(h2o_file)) if path),
            reasons=tuple(supplement_reasons),
            physical_meaning="Supplemental points are formal required points in the future runlist, not historical targeted-resampling labels.",
            next_action="Repair point roles/semantics before allowing a new-algorithm runlist to feed any runner.",
            details={"observed": sorted(observed_supplements), "expected": sorted(expected_supplements)},
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    status = "blocked" if blocker_count else "ready_for_new_algorithm_runner_integration_review"
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": status,
        "blocker_count": blocker_count,
        "review_required_count": 0,
        "profile_id": manifest.get("profile_id") or "absorption_ratio_shadow",
        "not_real_acceptance_evidence": True,
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "runner_integration_status": "preview_only_not_runner_wired",
        "legacy_co2_formal_point_count": manifest.get("legacy_co2_formal_point_count"),
        "legacy_h2o_formal_point_count": manifest.get("legacy_h2o_formal_point_count"),
        "co2_runlist_count": len(co2_rows),
        "h2o_runlist_count": len(h2o_rows),
        "required_before_new_algorithm_physical_run": [
            "new_algorithm_co2_runlist_47_point_gate",
            "new_algorithm_h2o_runlist_14_point_gate",
            "formal_supplemental_point_semantics_gate",
        ],
        "next_action": (
            "Review this readiness sidecar before wiring any profile-driven runner. "
            "Passing this gate does not authorize COM, route control, coefficient writes, archive release, or database import."
        ),
        "checks": [row.to_json() for row in checks],
    }


def write_v1_5_algorithm_runlist_readiness_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_algorithm_runlist_readiness.json",
        "checks_csv": out / "v1_5_algorithm_runlist_readiness_checks.csv",
        "markdown": out / "V1_5_ALGORITHM_RUNLIST_READINESS.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    lines = [
        "# V1.5 algorithm runlist readiness",
        "",
        "This is an offline readiness gate for the new-algorithm formal runlist preview.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- legacy CO2/H2O counts: `{model.get('legacy_co2_formal_point_count')}` / `{model.get('legacy_h2o_formal_point_count')}`",
        f"- new-algorithm CO2/H2O runlist counts: `{model.get('co2_runlist_count')}` / `{model.get('h2o_runlist_count')}`",
        "- Required supplemental formal points: `-20/600`, `-10/600`, `40/30/30`.",
        "- This sidecar is not real acceptance evidence and does not authorize route execution.",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
