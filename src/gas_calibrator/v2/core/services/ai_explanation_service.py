from __future__ import annotations

import csv
import json
from datetime import datetime
from typing import Any

from ..orchestration_context import OrchestrationContext
from ..run_state import RunState


class AIExplanationService:
    """AI anomaly and run-summary helpers with non-blocking failure handling."""

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def generate_ai_anomaly_report(self, advisor: Any) -> str:
        ai = getattr(self.context.config, "ai", None)
        if ai is None or not bool(getattr(ai, "feature_enabled", lambda name: False)("anomaly_diagnosis")):
            return ""
        if advisor is None or self.run_state.qc.qc_report is None:
            return ""
        point_details = list(getattr(self.run_state.qc.qc_report, "point_details", []) or [])
        failed_points = [item for item in point_details if not bool(item.get("valid", True))]
        alarms = self._anomaly_alarm_payload()
        device_events = self._anomaly_device_events()
        if not failed_points and not alarms:
            return ""
        diagnosis = advisor.diagnose_run(
            failed_points=failed_points,
            device_events=device_events,
            alarms=alarms,
        )
        if not diagnosis:
            return ""
        txt_path = self.context.result_store.run_dir / "anomaly_diagnosis.txt"
        json_path = self.context.result_store.run_dir / "anomaly_diagnosis.json"
        txt_path.write_text(str(diagnosis).rstrip() + "\n", encoding="utf-8")
        json_path.write_text(
            json.dumps(
                {
                    "run_id": self.context.session.run_id,
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "failed_points": failed_points,
                    "alarms": alarms,
                    "device_events": device_events[:100],
                    "diagnosis": diagnosis,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        self.host._remember_output_file(str(txt_path))
        self.host._remember_output_file(str(json_path))
        self.host._log("AI anomaly diagnosis generated")
        return str(diagnosis)

    def generate_ai_run_summary(self, summarizer: Any, *, anomaly_diagnosis: str = "") -> str:
        if summarizer is None:
            return ""
        text = summarizer.write_summary(
            self.context.session.output_dir,
            anomaly_diagnosis=anomaly_diagnosis,
        )
        if text:
            self.host._log("AI run summary generated")
        return str(text or "")

    def generate_ai_outputs(self) -> None:
        ai = getattr(self.context.config, "ai", None)
        if ai is None or not bool(getattr(ai, "enabled", False)):
            return
        runtime = getattr(getattr(self.host, "service", None), "ai_runtime", None)
        if runtime is None:
            return
        try:
            anomaly_text = self.generate_ai_anomaly_report(runtime.anomaly_advisor)
        except Exception as exc:
            anomaly_text = ""
            self.host._log(f"AI anomaly diagnosis warning: {exc}")
        try:
            self.generate_ai_run_summary(
                runtime.summarizer,
                anomaly_diagnosis=anomaly_text,
            )
        except Exception as exc:
            self.host._log(f"AI summary warning: {exc}")

    def _anomaly_alarm_payload(self) -> list[dict[str, Any]]:
        alarms: list[dict[str, Any]] = []
        for message in list(self.context.session.warnings):
            alarms.append({"severity": "warning", "category": self._alarm_category(message), "message": message})
        for message in list(self.context.session.errors):
            alarms.append({"severity": "error", "category": self._alarm_category(message), "message": message})
        return alarms

    def _anomaly_device_events(self) -> list[dict[str, Any]]:
        path = self.context.run_logger.io_log_path
        if not path.exists():
            return []
        events: list[dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    events.append(
                        {
                            "timestamp": row.get("timestamp"),
                            "device": row.get("device"),
                            "direction": row.get("direction"),
                        }
                    )
        except Exception:
            return []
        return events

    @staticmethod
    def _alarm_category(message: str) -> str:
        text = str(message or "").lower()
        if "humidity" in text or "dew" in text or "h2o" in text:
            return "humidity"
        if "pressure" in text or "leak" in text:
            return "pressure"
        if "commun" in text or "serial" in text or "frame" in text:
            return "communication"
        if "qc" in text or "outlier" in text or "stability" in text:
            return "qc"
        return "general"
