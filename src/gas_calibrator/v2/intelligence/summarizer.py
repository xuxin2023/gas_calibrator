from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from ..config import AIConfig
from .llm_client import LLMClient, LLMConfig, MockLLMClient, complete_with_fallback


class Summarizer:
    """Generates a natural-language summary for one completed run."""

    def __init__(self, llm_client: LLMClient | None = None, config: AIConfig | None = None):
        self.config = config or AIConfig()
        self.llm = llm_client or MockLLMClient(LLMConfig(provider="mock", model="mock"))

    def summarize_run_directory(
        self,
        run_dir: str | Path,
        *,
        anomaly_diagnosis: str = "",
    ) -> str:
        run_path = Path(run_dir)
        payload = self._load_artifacts(run_path)
        fallback = self._build_fallback_summary(payload, anomaly_diagnosis=anomaly_diagnosis)
        prompt = self._build_prompt(payload, anomaly_diagnosis=anomaly_diagnosis)
        return complete_with_fallback(self.llm, prompt, fallback, temperature=0.2, max_tokens=900)

    def write_summary(
        self,
        run_dir: str | Path,
        *,
        anomaly_diagnosis: str = "",
    ) -> str:
        run_path = Path(run_dir)
        text = self.summarize_run_directory(run_path, anomaly_diagnosis=anomaly_diagnosis)
        summary_path = run_path / "summary.json"
        summary_payload: dict[str, Any] = {}
        if summary_path.exists():
            try:
                summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                summary_payload = {}
        summary_payload["ai_summary"] = text
        summary_payload["ai_summary_generated_at"] = datetime.now().isoformat(timespec="seconds")
        summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (run_path / "run_summary.txt").write_text(text.rstrip() + "\n", encoding="utf-8")
        return text

    def _load_artifacts(self, run_dir: Path) -> dict[str, Any]:
        summary = self._load_json(run_dir / "summary.json")
        qc_report = self._load_json(run_dir / "qc_report.json")
        samples = self._load_samples(run_dir / "samples.csv")
        point_summaries = list(((summary.get("stats") or {}).get("point_summaries") or []))
        return {
            "run_dir": str(run_dir),
            "run_id": str(summary.get("run_id") or run_dir.name),
            "summary": summary,
            "qc_report": qc_report,
            "samples": samples,
            "point_summaries": point_summaries,
        }

    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    @staticmethod
    def _load_samples(path: Path) -> list[dict[str, str]]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))

    def _build_prompt(self, payload: dict[str, Any], *, anomaly_diagnosis: str) -> str:
        run_id = payload["run_id"]
        summary = payload["summary"]
        qc_report = payload["qc_report"]
        point_summaries = payload["point_summaries"]
        samples = payload["samples"]
        compact_points = [
            {
                "index": item.get("point", {}).get("index"),
                "route": item.get("point", {}).get("route"),
                "temperature_c": item.get("point", {}).get("temperature_c"),
                "co2_ppm": item.get("point", {}).get("co2_ppm"),
                "humidity_pct": item.get("point", {}).get("humidity_pct"),
                "valid": item.get("stats", {}).get("valid"),
                "reason": item.get("stats", {}).get("reason"),
                "ai_explanation": item.get("stats", {}).get("ai_explanation"),
                "total_time_s": item.get("stats", {}).get("total_time_s"),
            }
            for item in point_summaries[:12]
        ]
        sample_meta = {
            "sample_count": len(samples),
            "analyzers": sorted({row.get("analyzer_id", "") for row in samples if row.get("analyzer_id")}),
        }
        request = {
            "task": "Generate a concise Chinese calibration run summary for operators.",
            "requirements": [
                "Mention run status, successful and failed point counts",
                "Explain the dominant failure pattern if any",
                "Provide actionable suggestions",
                "Do not mention API or model details",
                "Use only the aggregated statistics below",
            ],
            "run_id": run_id,
            "status": summary.get("status", {}),
            "qc_report": {
                "total_points": qc_report.get("total_points"),
                "valid_points": qc_report.get("valid_points"),
                "invalid_points": qc_report.get("invalid_points"),
                "overall_score": qc_report.get("overall_score"),
                "grade": qc_report.get("grade"),
                "recommendations": qc_report.get("recommendations", []),
            },
            "sample_meta": sample_meta,
            "points": compact_points,
            "anomaly_diagnosis": anomaly_diagnosis,
        }
        return json.dumps(request, ensure_ascii=False, indent=2)

    def _build_fallback_summary(self, payload: dict[str, Any], *, anomaly_diagnosis: str) -> str:
        summary = payload["summary"]
        qc_report = payload["qc_report"]
        point_summaries = payload["point_summaries"]
        samples = payload["samples"]
        run_id = payload["run_id"]
        start_text = self._run_started_text(run_id)
        status = (summary.get("status") or {}).get("phase") or "unknown"
        total_points = int(
            qc_report.get("total_points")
            or len(point_summaries)
            or ((summary.get("status") or {}).get("total_points") or 0)
        )
        valid_points = int(qc_report.get("valid_points") or sum(1 for item in point_summaries if item.get("stats", {}).get("valid")))
        invalid_points = max(0, int(qc_report.get("invalid_points") or (total_points - valid_points)))
        successful = [item for item in point_summaries if item.get("stats", {}).get("valid")]
        failed = [item for item in point_summaries if not item.get("stats", {}).get("valid")]
        sample_count = len(samples)
        quality_score = float(qc_report.get("overall_score") or 0.0) * 100.0
        grade = str(qc_report.get("grade") or "")

        lines = [f"本次校准运行于 {start_text} 启动，当前状态为 {self._status_text(status)}。"]
        lines.append(f"共执行 {total_points} 个点位，其中 {valid_points} 个成功，{invalid_points} 个失败；累计采样 {sample_count} 条。")

        if successful:
            success_preview = "；".join(self._format_point(item) for item in successful[:3])
            lines.append(f"成功点位示例：{success_preview}。")

        if failed:
            lines.append("失败点位：")
            for item in failed[:5]:
                reason = str(item.get("stats", {}).get("ai_explanation") or item.get("stats", {}).get("reason") or "原因待确认")
                lines.append(f"- {self._format_point(item)}：{self._single_line(reason)}")
        else:
            lines.append("本次运行未发现 QC 失败点位。")

        recommendations = list(qc_report.get("recommendations") or [])
        recommendations.extend(self._derive_recommendations(failed, anomaly_diagnosis))
        recommendations = self._dedupe_preserve(recommendations)
        if recommendations:
            lines.append("建议：")
            for index, item in enumerate(recommendations[:5], start=1):
                lines.append(f"{index}. {self._single_line(str(item))}")

        score_suffix = f"，等级 {grade}" if grade else ""
        lines.append(f"整体质量评分：{quality_score:.2f}%{score_suffix}。")
        return "\n".join(lines)

    @staticmethod
    def _single_line(text: str) -> str:
        compact = " ".join(str(text or "").split())
        return compact[:220] + ("..." if len(compact) > 220 else "")

    @staticmethod
    def _dedupe_preserve(items: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for item in items:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            ordered.append(text)
        return ordered

    def _derive_recommendations(self, failed: list[dict[str, Any]], anomaly_diagnosis: str) -> list[str]:
        if anomaly_diagnosis:
            lines = [line.strip("- ").strip() for line in anomaly_diagnosis.splitlines() if line.strip().startswith(("1.", "2.", "3.", "4.", "5.", "-", "建议"))]
            extracted = [line for line in lines if line and not line.startswith("建议")]
            if extracted:
                return extracted

        reasons = Counter(
            str(item.get("stats", {}).get("reason") or "").strip().lower()
            for item in failed
            if str(item.get("stats", {}).get("reason") or "").strip()
        )
        combined = " ".join(reasons.keys())
        recommendations: list[str] = []
        if "humidity" in combined or "h2o" in combined or "dew" in combined:
            recommendations.extend(
                [
                    "检查湿度发生器、露点仪和相关气路的工作状态。",
                    "适当增加湿度稳定等待时间，确认低温工况下的响应速度。",
                ]
            )
        if "pressure" in combined or "leak" in combined:
            recommendations.extend(
                [
                    "检查压力控制器、压力表和管路密封性。",
                    "确认目标压力切换后的缓冲时间是否足够。",
                ]
            )
        if "communication" in combined or "time_not_continuous" in combined:
            recommendations.extend(
                [
                    "检查分析仪通讯链路和采样节拍，避免丢帧。",
                    "核对串口参数和设备缓存设置。",
                ]
            )
        if "outlier" in combined or "signal_span" in combined:
            recommendations.extend(
                [
                    "检查采样稳定性，必要时增加稳定等待时间并复测异常点位。",
                    "检查气路密封和混气均匀性，降低信号波动。",
                ]
            )
        if not recommendations and failed:
            recommendations.append("优先复核失败点位的设备状态、稳定等待时间和采样完整性。")
        if not recommendations:
            recommendations.append("当前运行整体稳定，可进入后续拟合和复核流程。")
        return recommendations

    @staticmethod
    def _status_text(status: str) -> str:
        mapping = {
            "completed": "已完成",
            "finalizing": "正在收尾",
            "error": "异常结束",
            "stopped": "已停止",
        }
        return mapping.get(str(status or "").strip().lower(), str(status or "未知状态"))

    @staticmethod
    def _run_started_text(run_id: str) -> str:
        try:
            stamp = str(run_id).split("_", 1)[1]
            dt = datetime.strptime(stamp, "%Y%m%d_%H%M%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(run_id)

    @staticmethod
    def _format_point(item: dict[str, Any]) -> str:
        point = item.get("point", {}) if isinstance(item.get("point"), dict) else {}
        route = str(point.get("route") or item.get("route") or "point").upper()
        index = point.get("index") or item.get("point_index") or "?"
        temp = point.get("temperature_c")
        co2 = point.get("co2_ppm")
        humidity = point.get("humidity_pct")
        parts = [f"点位 {index}", route]
        if temp is not None:
            parts.append(f"{float(temp):.1f}°C")
        if co2 is not None:
            parts.append(f"CO2 {float(co2):.0f} ppm")
        if humidity is not None:
            parts.append(f"RH {float(humidity):.1f}%")
        return " / ".join(parts)
