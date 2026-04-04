from __future__ import annotations

import argparse
import difflib
import json
import csv
from collections import defaultdict
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from statistics import pstdev
from typing import Any, Iterable, Optional


KEY_ACTION_GROUPS: dict[str, tuple[str, ...]] = {
    "vent": ("set_vent",),
    "valves": (
        "set_h2o_path",
        "set_co2_valves",
        "route_baseline",
        "restore_baseline",
        "cleanup",
        "final_safe_stop_routes",
    ),
    "pressure": (
        "set_output",
        "set_pressure",
        "seal_route",
        "wait_post_pressure",
        "startup_pressure_precheck",
        "startup_pressure_precheck_route_ready",
        "startup_pressure_hold",
        "final_safe_stop_pressure",
    ),
    "sample": ("sample_start", "sample_end"),
}


def key_action_groups() -> dict[str, tuple[str, ...]]:
    """Central maintenance point for grouped route-trace actions."""
    return {name: tuple(actions) for name, actions in KEY_ACTION_GROUPS.items()}


@dataclass(frozen=True)
class RouteTraceEvent:
    route: str
    action: str
    point_tag: str
    point_index: Optional[int]
    result: str

    @property
    def token(self) -> str:
        suffix = self.point_tag or (f"row{self.point_index}" if self.point_index is not None else "-")
        return f"{self.action}@{suffix}"


@dataclass(frozen=True)
class RouteDiffSummary:
    route: str
    v1_count: int
    v2_count: int
    missing_in_v2: list[str]
    extra_in_v2: list[str]
    order_mismatches: list[tuple[int, str, str]]
    unified_diff: list[str]

    @property
    def matches(self) -> bool:
        return not self.missing_in_v2 and not self.extra_in_v2 and not self.order_mismatches


@dataclass(frozen=True)
class CompareMetricSummary:
    metric: str
    passed: int
    total: int
    average_delta: float
    average_abs_delta: float
    delta_std: float
    consistent_sign: bool
    route_average_deltas: dict[str, float]


@dataclass(frozen=True)
class AnalyzerMatchSummary:
    analyzer_id: str
    source_id: Optional[str]
    average_relative_error: float
    metric_pass_counts: dict[str, tuple[int, int]]


@dataclass(frozen=True)
class CompareReportDiagnosis:
    report_path: Path
    v1_source: Optional[Path]
    v2_run_dir: Optional[Path]
    metric_summaries: list[CompareMetricSummary]
    analyzer_matches: list[AnalyzerMatchSummary]
    best_metric_analyzers: dict[str, str]
    v1_primary_analyzer_ids: list[str]
    v2_runtime_analyzer_ids: dict[str, str]
    likely_causes: list[str]
    recommended_fixes: list[str]


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare V1/V2 route trace jsonl files.")
    parser.add_argument("--v1-trace", help="Path to V1 route_trace.jsonl")
    parser.add_argument("--v2-trace", help="Path to V2 route_trace.jsonl")
    parser.add_argument("--compare-report", help="Path to compare_report_*.json for numeric diagnosis")
    parser.add_argument(
        "--max-diff-lines",
        type=int,
        default=12,
        help="Maximum unified-diff lines per route section (default: 12)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def load_route_trace(path: str | Path) -> list[RouteTraceEvent]:
    trace_path = Path(path)
    events: list[RouteTraceEvent] = []
    for raw_line in trace_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        route = str(payload.get("route") or "").strip().lower()
        action = str(payload.get("action") or "").strip()
        if not route or not action:
            continue
        point_tag = str(payload.get("point_tag") or "").strip()
        point_index = payload.get("point_index")
        try:
            if point_index is not None:
                point_index = int(point_index)
        except Exception:
            point_index = None
        result = str(payload.get("result") or "ok").strip().lower() or "ok"
        events.append(
            RouteTraceEvent(
                route=route,
                action=action,
                point_tag=point_tag,
                point_index=point_index,
                result=result,
            )
        )
    return events


def _mean(values: Iterable[float]) -> float:
    items = [float(value) for value in values]
    if not items:
        return 0.0
    return float(sum(items) / len(items))


def _std(values: Iterable[float]) -> float:
    items = [float(value) for value in values]
    if len(items) < 2:
        return 0.0
    return float(pstdev(items))


def _coerce_compare_report_path(path: str | Path) -> Path:
    compare_path = Path(path)
    if not compare_path.exists():
        raise FileNotFoundError(compare_path)
    return compare_path


def load_compare_report(path: str | Path) -> dict[str, Any]:
    report_path = _coerce_compare_report_path(path)
    return json.loads(report_path.read_text(encoding="utf-8"))


def _load_v2_results_samples(run_dir: Path) -> list[dict[str, Any]]:
    results_path = run_dir / "results.json"
    if not results_path.exists():
        return []
    payload = json.loads(results_path.read_text(encoding="utf-8"))
    samples = payload.get("samples")
    return list(samples) if isinstance(samples, list) else []


def _load_v2_runtime_analyzer_ids(run_dir: Path) -> dict[str, str]:
    runtime_path = run_dir / "samples_runtime.csv"
    if not runtime_path.exists():
        return {}
    with runtime_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        first_row = next(reader, None)
    if not first_row:
        return {}
    ids: dict[str, str] = {}
    for key, value in first_row.items():
        if not key.endswith("_id"):
            continue
        label = key[:-3]
        if not value:
            continue
        ids[str(label)] = str(value)
    return dict(sorted(ids.items()))


def _load_v1_primary_analyzer_ids(v1_source: Optional[Path]) -> list[str]:
    if v1_source is None or not v1_source.exists() or v1_source.suffix.lower() != ".csv":
        return []
    try:
        with v1_source.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except Exception:
        return []
    if not rows:
        return []
    column = "设备ID_平均值"
    values: list[str] = []
    for row in rows:
        raw = str(row.get(column) or "").strip()
        if raw and raw not in values:
            values.append(raw)
    return values


def summarize_compare_metrics(report: dict[str, Any]) -> list[CompareMetricSummary]:
    point_results = list(report.get("point_results") or [])
    per_metric_deltas: dict[str, list[float]] = defaultdict(list)
    per_metric_routes: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    pass_counts: dict[str, tuple[int, int]] = {}
    for point in point_results:
        route = str(point.get("route") or "").strip().lower()
        if not route:
            point_tag = str(point.get("point_tag") or "").strip().lower()
            if point_tag.startswith("h2o_"):
                route = "h2o"
            elif point_tag.startswith("co2_"):
                route = "co2"
        route = route or "unknown"
        metrics = point.get("metrics") or {}
        for metric_name, payload in metrics.items():
            if not isinstance(payload, dict):
                continue
            v1_mean = payload.get("v1_mean")
            v2_mean = payload.get("v2_mean")
            if v1_mean is None or v2_mean is None:
                continue
            delta = float(v2_mean) - float(v1_mean)
            per_metric_deltas[metric_name].append(delta)
            per_metric_routes[metric_name][route].append(delta)
            passed = bool(payload.get("passed", False))
            passed_count, total_count = pass_counts.get(metric_name, (0, 0))
            pass_counts[metric_name] = (passed_count + int(passed), total_count + 1)
    summaries: list[CompareMetricSummary] = []
    for metric_name in sorted(per_metric_deltas):
        deltas = per_metric_deltas[metric_name]
        sign_set = {0 if abs(delta) < 1e-12 else (1 if delta > 0 else -1) for delta in deltas}
        sign_set.discard(0)
        route_average_deltas = {
            route: _mean(values) for route, values in sorted(per_metric_routes[metric_name].items())
        }
        passed_count, total_count = pass_counts.get(metric_name, (0, len(deltas)))
        summaries.append(
            CompareMetricSummary(
                metric=metric_name,
                passed=passed_count,
                total=total_count,
                average_delta=_mean(deltas),
                average_abs_delta=_mean(abs(delta) for delta in deltas),
                delta_std=_std(deltas),
                consistent_sign=len(sign_set) <= 1,
                route_average_deltas=route_average_deltas,
            )
        )
    return summaries


def summarize_analyzer_matches(
    report: dict[str, Any],
    *,
    samples: list[dict[str, Any]],
    runtime_analyzer_ids: dict[str, str],
) -> list[AnalyzerMatchSummary]:
    point_results = list(report.get("point_results") or [])
    analyzers = sorted({str(sample.get("analyzer_id") or "").strip() for sample in samples if sample.get("analyzer_id")})
    summaries: list[AnalyzerMatchSummary] = []
    for analyzer_id in analyzers:
        relative_errors: list[float] = []
        metric_pass_counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        for point in point_results:
            point_tag = str(point.get("point_tag") or "").strip()
            point_samples = [sample for sample in samples if sample.get("point_tag") == point_tag and sample.get("analyzer_id") == analyzer_id]
            if not point_samples:
                continue
            metrics = point.get("metrics") or {}
            for metric_name, payload in metrics.items():
                if not isinstance(payload, dict):
                    continue
                v1_mean = payload.get("v1_mean")
                if v1_mean is None:
                    continue
                values = [sample.get(metric_name) for sample in point_samples if sample.get(metric_name) is not None]
                if not values:
                    continue
                v2_mean = _mean(float(value) for value in values)
                v1_mean_float = float(v1_mean)
                relative_error = abs(v2_mean - v1_mean_float) / abs(v1_mean_float) if v1_mean_float else 0.0
                threshold = payload.get("threshold")
                if threshold is None:
                    threshold_keys = {
                        "co2_ppm": "co2_ppm_mean_rel_err",
                        "h2o_mmol": "h2o_mmol_mean_rel_err",
                        "pressure_hpa": "pressure_hpa_mean_rel_err",
                        "co2_ratio_f": "co2_ratio_f_mean_rel_err",
                        "h2o_ratio_f": "h2o_ratio_f_mean_rel_err",
                    }
                    threshold = (report.get("thresholds") or {}).get(threshold_keys.get(metric_name, ""), 0.0)
                relative_errors.append(relative_error)
                metric_pass_counts[metric_name][1] += 1
                if relative_error <= float(threshold):
                    metric_pass_counts[metric_name][0] += 1
        if not relative_errors:
            continue
        summaries.append(
            AnalyzerMatchSummary(
                analyzer_id=analyzer_id,
                source_id=runtime_analyzer_ids.get(analyzer_id),
                average_relative_error=_mean(relative_errors),
                metric_pass_counts={metric: (counts[0], counts[1]) for metric, counts in sorted(metric_pass_counts.items())},
            )
        )
    return sorted(summaries, key=lambda item: (item.average_relative_error, item.analyzer_id))


def diagnose_compare_report(path: str | Path) -> CompareReportDiagnosis:
    report_path = _coerce_compare_report_path(path)
    report = load_compare_report(report_path)
    v1_source_raw = report.get("v1_source")
    v2_run_dir_raw = report.get("v2_run_dir")
    v1_source = Path(v1_source_raw) if isinstance(v1_source_raw, str) and v1_source_raw else None
    v2_run_dir = Path(v2_run_dir_raw) if isinstance(v2_run_dir_raw, str) and v2_run_dir_raw else None
    metric_summaries = summarize_compare_metrics(report)
    samples = _load_v2_results_samples(v2_run_dir) if v2_run_dir is not None else []
    runtime_analyzer_ids = _load_v2_runtime_analyzer_ids(v2_run_dir) if v2_run_dir is not None else {}
    analyzer_matches = summarize_analyzer_matches(report, samples=samples, runtime_analyzer_ids=runtime_analyzer_ids)
    best_metric_analyzers: dict[str, str] = {}
    metric_names = sorted(
        {
            metric_name
            for match in analyzer_matches
            for metric_name, (_, total) in match.metric_pass_counts.items()
            if total
        }
    )
    for metric_name in metric_names:
        ranked = sorted(
            analyzer_matches,
            key=lambda match: (
                -(match.metric_pass_counts.get(metric_name, (0, 0))[0]),
                match.average_relative_error,
                match.analyzer_id,
            ),
        )
        if ranked:
            best_metric_analyzers[metric_name] = ranked[0].analyzer_id
    v1_primary_ids = _load_v1_primary_analyzer_ids(v1_source)

    likely_causes: list[str] = []
    recommended_fixes: list[str] = []

    h2o_summary = next((summary for summary in metric_summaries if summary.metric in {"h2o_mmol", "h2o_ratio_f"}), None)
    co2_summary = next((summary for summary in metric_summaries if summary.metric in {"co2_ppm", "co2_ratio_f"}), None)

    if h2o_summary and h2o_summary.consistent_sign and h2o_summary.average_delta > 0 and h2o_summary.average_abs_delta > 0.02:
        likely_causes.append(
            "H2O metrics show a stable positive offset across points, which is more consistent with route settling / residual moisture / control timing bias than a random parse failure."
        )
        recommended_fixes.append(
            "Tighten H2O-side validation around the sealed route: capture post-seal live dewpoint snapshots and gate sampling on analyzer-side H2O stabilization, not only generator-side target-band readiness."
        )
    if co2_summary and co2_summary.route_average_deltas.get("co2", 0.0) < 0:
        likely_causes.append(
            "CO2 metrics are mainly low on CO2-route points, which points to CO2 route conditioning / sampling-window alignment rather than a global CO2 field mapping bug."
        )
        recommended_fixes.append(
            "Recheck CO2-route pre-seal / pre-sample timing in bench runs and compare the admitted sample window against V1 before changing parser fields."
        )
    if v1_primary_ids and runtime_analyzer_ids:
        runtime_id_values = {value for value in runtime_analyzer_ids.values() if value}
        if not runtime_id_values.intersection(v1_primary_ids):
            likely_causes.append(
                "The V1 baseline primary analyzer ID set does not overlap with the V2 runtime analyzer IDs, so this report is comparing different physical analyzers or a different analyzer ordering."
            )
            recommended_fixes.append(
                "For V1-replacement validation, rerun V1 and V2 with the same analyzer fleet and an explicit analyzer-ID mapping, then compare only the mapped primary analyzer first."
            )
    best_h2o = best_metric_analyzers.get("h2o_mmol") or best_metric_analyzers.get("h2o_ratio_f")
    best_co2 = best_metric_analyzers.get("co2_ppm") or best_metric_analyzers.get("co2_ratio_f")
    if analyzer_matches:
        best_overall = analyzer_matches[0]
        likely_causes.append(
            f"Per-analyzer matching is route-dependent: best overall V2 analyzer is {best_overall.analyzer_id}"
            + (f" (source id={best_overall.source_id})" if best_overall.source_id else "")
            + ", which indicates analyzer ordering / fleet composition differences can materially change the score."
        )
        if best_h2o and best_co2 and best_h2o != best_co2:
            likely_causes.append(
                f"The analyzer that best matches H2O ({best_h2o}) is different from the analyzer that best matches CO2 ({best_co2}), which strongly suggests analyzer ordering or fleet mismatch in the comparison inputs."
            )
        recommended_fixes.append(
            "Add an analyzer-selection or analyzer-ID mapping step to the compare workflow so the report can distinguish `primary-analyzer` validation from `all-analyzers` fleet drift."
        )
    if not likely_causes:
        likely_causes.append("No dominant systematic pattern was detected from the compare report alone.")

    return CompareReportDiagnosis(
        report_path=report_path,
        v1_source=v1_source,
        v2_run_dir=v2_run_dir,
        metric_summaries=metric_summaries,
        analyzer_matches=analyzer_matches,
        best_metric_analyzers=best_metric_analyzers,
        v1_primary_analyzer_ids=v1_primary_ids,
        v2_runtime_analyzer_ids=runtime_analyzer_ids,
        likely_causes=likely_causes,
        recommended_fixes=recommended_fixes,
    )


def format_compare_report_diagnosis(diagnosis: CompareReportDiagnosis) -> str:
    lines = [
        "Compare Report Diagnosis",
        f"Report: {diagnosis.report_path}",
        f"V1 source: {diagnosis.v1_source or '-'}",
        f"V2 run dir: {diagnosis.v2_run_dir or '-'}",
        "",
        "Metric Bias Summary",
    ]
    for summary in diagnosis.metric_summaries:
        route_bits = ", ".join(
            f"{route}={avg_delta:+.6f}" for route, avg_delta in summary.route_average_deltas.items()
        ) or "-"
        lines.append(
            f"- {summary.metric}: passed {summary.passed}/{summary.total}, "
            f"avg_delta={summary.average_delta:+.6f}, avg_abs_delta={summary.average_abs_delta:.6f}, "
            f"delta_std={summary.delta_std:.6f}, consistent_sign={'yes' if summary.consistent_sign else 'no'}, "
            f"route_avg=[{route_bits}]"
        )
    lines.append("")
    lines.append("Analyzer Scope Check")
    if diagnosis.v1_primary_analyzer_ids:
        lines.append(f"- V1 primary analyzer ids: {', '.join(diagnosis.v1_primary_analyzer_ids)}")
    else:
        lines.append("- V1 primary analyzer ids: unavailable")
    if diagnosis.v2_runtime_analyzer_ids:
        joined = ", ".join(
            f"{analyzer}={source_id}" for analyzer, source_id in diagnosis.v2_runtime_analyzer_ids.items()
        )
        lines.append(f"- V2 runtime analyzer ids: {joined}")
    else:
        lines.append("- V2 runtime analyzer ids: unavailable")
    if diagnosis.analyzer_matches:
        for match in diagnosis.analyzer_matches[:4]:
            pass_bits = ", ".join(
                f"{metric}={passed}/{total}" for metric, (passed, total) in match.metric_pass_counts.items()
            )
            source_text = f" source_id={match.source_id}" if match.source_id else ""
            lines.append(
                f"- {match.analyzer_id}:{source_text} avg_rel_err={match.average_relative_error:.4f}, passes[{pass_bits}]"
            )
        if diagnosis.best_metric_analyzers:
            best_bits = ", ".join(
                f"{metric}={analyzer_id}" for metric, analyzer_id in sorted(diagnosis.best_metric_analyzers.items())
            )
            lines.append(f"- Best analyzer by metric: {best_bits}")
    else:
        lines.append("- Analyzer match summary unavailable")
    lines.append("")
    lines.append("Likely Causes")
    for item in diagnosis.likely_causes:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("Recommended Fixes")
    for item in diagnosis.recommended_fixes:
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def compare_route_traces(
    v1_events: list[RouteTraceEvent],
    v2_events: list[RouteTraceEvent],
    *,
    max_diff_lines: int = 12,
) -> list[RouteDiffSummary]:
    routes = sorted({event.route for event in v1_events + v2_events})
    summaries: list[RouteDiffSummary] = []
    for route in routes:
        v1_seq = [event.token for event in v1_events if event.route == route]
        v2_seq = [event.token for event in v2_events if event.route == route]
        v1_counter = Counter(v1_seq)
        v2_counter = Counter(v2_seq)

        missing_in_v2: list[str] = []
        extra_in_v2: list[str] = []
        for token in sorted(v1_counter):
            delta = v1_counter[token] - v2_counter.get(token, 0)
            if delta > 0:
                missing_in_v2.extend([token] * delta)
        for token in sorted(v2_counter):
            delta = v2_counter[token] - v1_counter.get(token, 0)
            if delta > 0:
                extra_in_v2.extend([token] * delta)

        order_mismatches: list[tuple[int, str, str]] = []
        for idx, (left, right) in enumerate(zip(v1_seq, v2_seq), start=1):
            if left != right:
                order_mismatches.append((idx, left, right))
            if len(order_mismatches) >= 5:
                break

        diff_lines = list(
            difflib.unified_diff(
                v1_seq,
                v2_seq,
                fromfile=f"v1:{route}",
                tofile=f"v2:{route}",
                lineterm="",
            )
        )
        if max_diff_lines >= 0:
            diff_lines = diff_lines[:max_diff_lines]

        summaries.append(
            RouteDiffSummary(
                route=route,
                v1_count=len(v1_seq),
                v2_count=len(v2_seq),
                missing_in_v2=missing_in_v2,
                extra_in_v2=extra_in_v2,
                order_mismatches=order_mismatches,
                unified_diff=diff_lines,
            )
        )
    return summaries


def format_route_diff_report(
    v1_path: str | Path,
    v2_path: str | Path,
    summaries: list[RouteDiffSummary],
) -> str:
    matches = all(summary.matches for summary in summaries)
    lines = [
        "Route Trace Diff",
        f"V1 trace: {Path(v1_path)}",
        f"V2 trace: {Path(v2_path)}",
        f"Overall status: {'MATCH' if matches else 'MISMATCH'}",
        "",
    ]
    for summary in summaries:
        lines.append(f"[{summary.route.upper()}]")
        lines.append(f"  V1 events: {summary.v1_count}")
        lines.append(f"  V2 events: {summary.v2_count}")
        lines.append(f"  Missing in V2: {len(summary.missing_in_v2)}")
        for token in summary.missing_in_v2[:5]:
            lines.append(f"    - {token}")
        lines.append(f"  Extra in V2: {len(summary.extra_in_v2)}")
        for token in summary.extra_in_v2[:5]:
            lines.append(f"    + {token}")
        lines.append(f"  Order mismatches: {len(summary.order_mismatches)}")
        for index, left, right in summary.order_mismatches:
            lines.append(f"    ! #{index}: V1={left} | V2={right}")
        if summary.unified_diff:
            lines.append("  Unified diff:")
            for line in summary.unified_diff:
                lines.append(f"    {line}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if args.compare_report:
        diagnosis = diagnose_compare_report(args.compare_report)
        print(format_compare_report_diagnosis(diagnosis), end="")
        return 0
    if not args.v1_trace or not args.v2_trace:
        raise SystemExit("Either --compare-report or both --v1-trace/--v2-trace are required.")
    v1_events = load_route_trace(args.v1_trace)
    v2_events = load_route_trace(args.v2_trace)
    summaries = compare_route_traces(v1_events, v2_events, max_diff_lines=int(args.max_diff_lines))
    report = format_route_diff_report(args.v1_trace, args.v2_trace, summaries)
    print(report, end="")
    return 0 if all(summary.matches for summary in summaries) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
