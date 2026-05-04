from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from ..models import CalibrationStatus
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState
from ..session import RunSession


class ArtifactService:
    """Summary, manifest, export, and storage-sync helpers for run artifacts."""

    STATUS_OK = "ok"
    STATUS_SKIPPED = "skipped"
    STATUS_MISSING = "missing"
    STATUS_ERROR = "error"

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def export_summary(
        self,
        session: RunSession,
        *,
        current_status: Optional[CalibrationStatus] = None,
        remember: bool = False,
        startup_pressure_precheck: Optional[dict[str, Any]] = None,
        extra_stats: Optional[dict[str, Any]] = None,
    ) -> Path:
        path = Path(self.context.result_store.data_writer.summary_path)
        summary_extra_stats = self._host_summary_extra_stats()
        if extra_stats:
            summary_extra_stats.update(dict(extra_stats))
        self._set_export_status(
            "run_summary",
            role="execution_summary",
            status="ok",
            path=str(path),
        )
        self.context.result_store.save_run_summary(
            session,
            current_status,
            output_files=self.run_state.artifacts.output_files,
            startup_pressure_precheck=startup_pressure_precheck,
            export_statuses=self.run_state.artifacts.export_statuses,
            extra_stats=summary_extra_stats,
        )
        if remember:
            self.host._remember_output_file(str(path))
        return path

    def export_manifest(
        self,
        session: RunSession,
        *,
        source_points_file: Optional[str | Path] = None,
        remember: bool = True,
        startup_pressure_precheck: Optional[dict[str, Any]] = None,
        extra_sections: Optional[dict[str, Any]] = None,
    ) -> Path:
        effective_source = source_points_file
        if effective_source is None:
            effective_source = getattr(getattr(self.host, "service", None), "_points_path", None)
        path = self.context.result_store.save_run_manifest(
            session,
            source_points_file=effective_source,
            output_files=self.run_state.artifacts.output_files,
            startup_pressure_precheck=startup_pressure_precheck,
            extra_sections=extra_sections,
        )
        if remember:
            self.host._remember_output_file(str(path))
        self._set_export_status(
            "manifest",
            role="execution_summary",
            status="ok",
            path=str(path),
        )
        return path

    def export_all_artifacts(self) -> None:
        startup_pressure_precheck = self._startup_pressure_precheck_payload()
        source_points_file = getattr(getattr(self.host, "service", None), "_points_path", None)
        try:
            self._set_export_status(
                "runtime_points",
                role="execution_rows",
                status=self.STATUS_OK,
                path=str(self.context.run_logger.points_path),
            )
            self._set_export_status(
                "io_log",
                role="execution_rows",
                status=self.STATUS_OK,
                path=str(self.context.run_logger.io_log_path),
            )
            results = list(self.host.get_results() or [])
            try:
                points_readable_path = self.context.result_store.export_points_readable(self.context.session)
            except Exception as exc:
                self._set_export_status(
                    "points_readable",
                    role="execution_summary",
                    status=self.STATUS_ERROR,
                    error=str(exc),
                )
                self.host._log(f"points_readable export failed: {exc}")
            else:
                if points_readable_path is None:
                    self._set_export_status(
                        "points_readable",
                        role="execution_summary",
                        status=self.STATUS_SKIPPED,
                        error="no point execution summaries available",
                    )
                else:
                    self.host._remember_output_file(str(points_readable_path))
                    self._set_export_status(
                        "points_readable",
                        role="execution_summary",
                        status=self.STATUS_OK if points_readable_path.exists() else self.STATUS_MISSING,
                        path=str(points_readable_path),
                    )
            if results:
                self._run_export(
                    "samples_csv",
                    role="execution_rows",
                    producer=lambda: self.context.result_store.export_csv(),
                    remember=True,
                )
                self._run_export(
                    "samples_excel",
                    role="execution_rows",
                    producer=lambda: self.context.result_store.export_excel(),
                    remember=True,
                )
                self._run_export(
                    "results_json",
                    role="execution_rows",
                    producer=lambda: self.context.result_store.export_json(),
                    remember=True,
                )
                if self._formal_calibration_report_enabled():
                    self._run_host_export("coefficient_report", role="formal_analysis", callback=self.host._export_coefficient_report)
                else:
                    message = (
                        "Formal calibration report skipped: "
                        f"run_mode={self._run_mode()} does not require the calibration-report chain"
                    )
                    self.host._log(message)
                    self._set_export_status(
                        "coefficient_report",
                        role="formal_analysis",
                        status=self.STATUS_SKIPPED,
                        error=message,
                    )
            else:
                self._set_export_status(
                    "samples_csv",
                    role="execution_rows",
                    status=self.STATUS_SKIPPED,
                    error="no samples collected",
                )
                self._set_export_status(
                    "samples_excel",
                    role="execution_rows",
                    status=self.STATUS_SKIPPED,
                    error="no samples collected",
                )
                self._set_export_status(
                    "results_json",
                    role="execution_rows",
                    status=self.STATUS_SKIPPED,
                    error="no samples collected",
                )
                self._set_export_status(
                    "coefficient_report",
                    role="formal_analysis",
                    status=self.STATUS_SKIPPED,
                    error="no samples collected",
                )
            self._run_host_export("qc_report", role="diagnostic_analysis", callback=self.host._export_qc_report)
            self._run_host_export(
                "temperature_snapshots",
                role="diagnostic_analysis",
                callback=self.host._export_temperature_snapshots,
            )
            self.export_manifest(
                self.context.session,
                source_points_file=source_points_file,
                remember=True,
                startup_pressure_precheck=startup_pressure_precheck,
            )
            try:
                self.export_summary(
                    self.context.session,
                    remember=True,
                    startup_pressure_precheck=startup_pressure_precheck,
                )
            except Exception as exc:
                self._set_export_status("run_summary", role="execution_summary", status=self.STATUS_ERROR, error=str(exc))
                self.host._log(f"run_summary export failed: {exc}")
            try:
                offline_payload = self.context.result_store.export_offline_artifacts(
                    self.context.session,
                    source_points_file=source_points_file,
                    output_files=self.run_state.artifacts.output_files,
                    export_statuses=self.run_state.artifacts.export_statuses,
                )
            except Exception as exc:
                self.host._log(f"offline artifact export failed: {exc}")
                offline_payload = {}
            for name, payload in dict(offline_payload.get("artifact_statuses") or {}).items():
                self.run_state.artifacts.export_statuses[str(name)] = dict(payload)
            for path in list(offline_payload.get("remembered_files") or []):
                self.host._remember_output_file(str(path))
            if offline_payload:
                self.export_manifest(
                    self.context.session,
                    source_points_file=source_points_file,
                    remember=True,
                    startup_pressure_precheck=startup_pressure_precheck,
                    extra_sections=dict(offline_payload.get("manifest_sections") or {}),
                )
                self.export_summary(
                    self.context.session,
                    remember=True,
                    startup_pressure_precheck=startup_pressure_precheck,
                    extra_stats=dict(offline_payload.get("summary_stats") or {}),
                )
            self._export_run001_a1_artifacts()
            self._export_run001_a2_artifacts()
            self.host._remember_output_file(str(self.context.data_writer.log_path))
            self.host._remember_output_file(str(self.context.run_logger.points_path))
            self.host._remember_output_file(str(self.context.run_logger.io_log_path))
        finally:
            self.context.run_logger.finalize()

    def _startup_pressure_precheck_payload(self) -> Optional[dict[str, Any]]:
        producer = getattr(self.host, "_startup_pressure_precheck_payload", None)
        if callable(producer):
            try:
                return producer()
            except Exception:
                return None
        return None

    def _host_summary_extra_stats(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        device_policy = getattr(self.host, "_device_init_policy_summary", None)
        if callable(device_policy):
            try:
                policy_payload = device_policy()
            except Exception:
                policy_payload = {}
            if isinstance(policy_payload, dict):
                payload.update(policy_payload)
        return payload

    def _run_mode(self) -> str:
        workflow = getattr(self.context.config, "workflow", None)
        return str(getattr(workflow, "run_mode", "auto_calibration") or "auto_calibration")

    def _formal_calibration_report_enabled(self) -> bool:
        return self._run_mode() == "auto_calibration"

    def _run_export(
        self,
        name: str,
        *,
        role: str,
        producer: Any,
        remember: bool = False,
    ) -> Optional[Path]:
        try:
            raw_path = producer()
        except Exception as exc:
            self._set_export_status(name, role=role, status=self.STATUS_ERROR, error=str(exc))
            self.host._log(f"{name} export failed: {exc}")
            return None
        path = None if raw_path is None else Path(raw_path)
        if path is not None and remember:
            self.host._remember_output_file(str(path))
        self._set_export_status(
            name,
            role=role,
            status=self.STATUS_MISSING if path is None else (self.STATUS_OK if path.exists() else self.STATUS_MISSING),
            path="" if path is None else str(path),
        )
        return path

    def _run_host_export(self, name: str, *, role: str, callback: Any) -> None:
        before = set(self.host.get_output_files()) if hasattr(self.host, "get_output_files") else set(self.run_state.artifacts.output_files)
        try:
            callback_result = callback()
        except Exception as exc:
            self._set_export_status(name, role=role, status=self.STATUS_ERROR, error=str(exc))
            self.host._log(f"{name} export failed: {exc}")
            return
        payload = self._normalize_export_result(callback_result, before=before)
        self._set_export_status(
            name,
            role=role,
            status=payload["status"],
            path=payload["path"],
            error=payload["error"],
        )

    def _normalize_export_result(self, callback_result: Any, *, before: set[str]) -> dict[str, str]:
        if isinstance(callback_result, dict):
            return {
                "status": self._normalize_status(str(callback_result.get("status", self.STATUS_OK) or self.STATUS_OK)),
                "path": str(callback_result.get("path", "") or ""),
                "error": str(callback_result.get("error", "") or ""),
            }
        path = ""
        if callback_result is not None:
            path = str(Path(callback_result))
        if not path:
            after = (
                set(self.host.get_output_files())
                if hasattr(self.host, "get_output_files")
                else set(self.run_state.artifacts.output_files)
            )
            new_files = sorted(after - before)
            path = new_files[-1] if new_files else ""
        status = self.STATUS_OK if path else self.STATUS_MISSING
        if path and not Path(path).exists():
            status = self.STATUS_MISSING
        return {"status": status, "path": path, "error": ""}

    def _normalize_status(self, status: str) -> str:
        text = str(status or "").strip().lower()
        if text in {self.STATUS_OK, self.STATUS_SKIPPED, self.STATUS_MISSING, self.STATUS_ERROR}:
            return text
        return self.STATUS_ERROR

    def _set_export_status(
        self,
        name: str,
        *,
        role: str,
        status: str,
        path: str = "",
        error: str = "",
    ) -> None:
        self.run_state.artifacts.export_statuses[str(name)] = {
            "role": str(role),
            "status": self._normalize_status(status),
            "path": str(path or ""),
            "error": str(error or ""),
        }

    def _export_run001_a1_artifacts(self) -> None:
        service = getattr(self.host, "service", None)
        raw_cfg = getattr(service, "_raw_cfg", None)
        if isinstance(raw_cfg, dict) and raw_cfg.get("run001_a2") and not raw_cfg.get("run001_a1"):
            return
        try:
            from ..run001_a1_dry_run import export_runtime_run001_a1_artifacts

            written = export_runtime_run001_a1_artifacts(self.host, self.context.result_store.run_dir)
        except Exception as exc:
            self._set_export_status(
                "run001_a1_evidence",
                role="diagnostic_analysis",
                status=self.STATUS_ERROR,
                error=str(exc),
            )
            self.host._log(f"Run-001/A1 evidence export failed: {exc}")
            return
        if not written:
            return
        for key, path in written.items():
            self.host._remember_output_file(str(path))
            self._set_export_status(
                f"run001_a1_{key}",
                role="diagnostic_analysis",
                status=self.STATUS_OK if Path(path).exists() else self.STATUS_MISSING,
                path=str(path),
            )

    def _export_run001_a2_artifacts(self) -> None:
        try:
            from ..run001_a2_no_write import export_runtime_run001_a2_artifacts

            written = export_runtime_run001_a2_artifacts(self.host, self.context.result_store.run_dir)
        except Exception as exc:
            self._set_export_status(
                "run001_a2_evidence",
                role="diagnostic_analysis",
                status=self.STATUS_ERROR,
                error=str(exc),
            )
            self.host._log(f"Run-001/A2 evidence export failed: {exc}")
            return
        if not written:
            return
        for key, path in written.items():
            self.host._remember_output_file(str(path))
            self._set_export_status(
                f"run001_a2_{key}",
                role="diagnostic_analysis",
                status=self.STATUS_OK if Path(path).exists() else self.STATUS_MISSING,
                path=str(path),
            )

    def sync_results_to_storage(self) -> None:
        storage = getattr(self.context.config, "storage", None)
        if storage is None or not getattr(storage, "database_enabled", False):
            return
        if not bool(getattr(storage, "auto_import", True)):
            return
        implementation = self._sync_results_to_storage_impl
        service = getattr(self.host, "service", None)
        service_impl = getattr(service, "_sync_results_to_storage_impl", None)
        if callable(service_impl):
            implementation = service_impl
        try:
            implementation()
        except Exception as exc:
            self.host._log(f"Storage warning: {exc}; falling back to file mode")

    def _sync_results_to_storage_impl(self) -> None:
        from ...storage import ArtifactImporter, DatabaseManager

        database = DatabaseManager.from_config(self.context.config.storage)
        try:
            database.initialize()
            importer = ArtifactImporter(database)
            importer.import_run_directory(self.context.session.output_dir)
        finally:
            database.dispose()
