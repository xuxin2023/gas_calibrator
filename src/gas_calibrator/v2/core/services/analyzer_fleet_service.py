from __future__ import annotations

import time
from typing import Any, Optional

from ...config.models import (
    _normalize_analyzer_mode2_init_config,
    _normalize_analyzer_setup_config,
    _normalize_sensor_precheck_config,
)
from ...exceptions import WorkflowValidationError
from ..no_write_guard import NoWriteViolation
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState
from ..device_factory import DeviceType


class AnalyzerFleetService:
    """Analyzer fleet configuration, filtering, and re-probe helpers."""

    SENSOR_FRAME_KEYS = (
        "co2_ratio_f",
        "co2_ppm",
        "co2",
        "h2o_ratio_f",
        "h2o_mmol",
        "h2o",
        "co2_signal",
        "h2o_signal",
    )

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def all_gas_analyzers(self) -> list[tuple[str, Any, Any]]:
        analyzers = self.context.device_manager.get_devices_by_type(DeviceType.GAS_ANALYZER)
        configs = list(getattr(self.context.config.devices, "gas_analyzers", []) or [])
        out: list[tuple[str, Any, Any]] = []
        for index, (device_name, analyzer) in enumerate(sorted(analyzers.items())):
            cfg = configs[index] if index < len(configs) else None
            label = str(getattr(cfg, "name", "") or device_name).strip() or device_name
            out.append((label, analyzer, cfg))
        return out

    def active_gas_analyzers(self) -> list[tuple[str, Any, Any]]:
        return [entry for entry in self.all_gas_analyzers() if entry[0] not in self.run_state.analyzers.disabled]

    def disable_analyzers(self, labels: list[str], reason: str) -> None:
        dropped: list[str] = []
        for label in labels:
            if label in self.run_state.analyzers.disabled:
                continue
            self.run_state.analyzers.disabled.add(label)
            self.run_state.analyzers.disabled_reasons[label] = reason
            dropped.append(label)
        if dropped:
            self.host._log(f"Analyzers dropped from active set: {', '.join(dropped)} reason={reason}")

    def analyzer_reprobe_cooldown_s(self) -> float:
        return max(0.0, float(self.host._cfg_get("workflow.analyzer_reprobe.cooldown_s", 300.0)))

    def live_snapshot_interval_s(self) -> float:
        try:
            return max(0.5, float(self.host._cfg_get("workflow.analyzer_live_snapshot.interval_s", 5.0)))
        except Exception:
            return 5.0

    def refresh_live_snapshots(self, *, force: bool = False, reason: str = "") -> bool:
        analyzers = self.active_gas_analyzers() or self.all_gas_analyzers()
        if not analyzers:
            return False

        now = time.monotonic()
        last_ts = float(getattr(self.run_state.analyzers, "last_live_snapshot_ts", 0.0) or 0.0)
        interval_s = self.live_snapshot_interval_s()
        if not force and last_ts > 0.0 and (now - last_ts) < interval_s:
            return False

        failures: list[str] = []
        refreshed = 0
        for label, analyzer, _cfg in analyzers:
            try:
                self._read_sensor_snapshot_with_retry(
                    analyzer,
                    label=label,
                    required_keys=self.SENSOR_FRAME_KEYS,
                    validation_mode="snapshot",
                    log_failures=False,
                )
                refreshed += 1
            except Exception as exc:
                failures.append(f"{label}: {exc}")

        self.run_state.analyzers.last_live_snapshot_ts = now
        setattr(self.host, "_last_live_analyzer_snapshot_ts", now)
        if failures:
            reason_text = f" reason={reason}" if reason else ""
            self.host._log(
                "Analyzer live snapshot refresh had read failures:"
                f"{reason_text} {', '.join(failures)}"
            )
        return refreshed > 0

    def gas_analyzer_runtime_settings(self, cfg: Any) -> dict[str, Any]:
        defaults = list(getattr(self.context.config.devices, "gas_analyzers", []) or [])
        default_cfg = defaults[0] if defaults else None
        return {
            "mode": self.host._as_int(getattr(cfg, "mode", None) if cfg is not None else None)
            or self.host._as_int(getattr(default_cfg, "mode", None))
            or 2,
            "active_send": bool(getattr(cfg, "active_send", getattr(default_cfg, "active_send", True))),
            "ftd_hz": self.host._as_int(getattr(cfg, "ftd_hz", None) if cfg is not None else None)
            or self.host._as_int(getattr(default_cfg, "ftd_hz", None))
            or 10,
            "avg_co2": self.host._as_int(getattr(cfg, "average_co2", None) if cfg is not None else None)
            or self.host._as_int(getattr(default_cfg, "average_co2", None))
            or 1,
            "avg_h2o": self.host._as_int(getattr(cfg, "average_h2o", None) if cfg is not None else None)
            or self.host._as_int(getattr(default_cfg, "average_h2o", None))
            or 1,
            "avg_filter": self.host._as_int(getattr(cfg, "average_filter", None) if cfg is not None else None)
            or self.host._as_int(getattr(default_cfg, "average_filter", None))
            or 49,
        }

    def sensor_precheck_config(self) -> dict[str, Any]:
        return _normalize_sensor_precheck_config(
            getattr(self.context.config.workflow, "sensor_precheck", {}) or {}
        )

    def sensor_precheck_settings(self, cfg: Any) -> dict[str, Any]:
        pcfg = self.sensor_precheck_config()
        runtime = self.gas_analyzer_runtime_settings(cfg)
        active_send = pcfg.get("active_send")
        return {
            "mode": 2,
            "active_send": runtime["active_send"] if active_send is None else bool(active_send),
            "ftd_hz": self.host._as_int(pcfg.get("ftd_hz")) or 1,
            "avg_co2": self.host._as_int(pcfg.get("average_co2")) or 1,
            "avg_h2o": self.host._as_int(pcfg.get("average_h2o")) or 1,
            "avg_filter": self.host._as_int(pcfg.get("average_filter")) or 49,
            "duration_s": max(0.5, float(pcfg.get("duration_s", 8.0) or 8.0)),
            "poll_s": max(0.0, float(pcfg.get("poll_s", 0.2) or 0.0)),
            "min_valid_frames": max(1, self.host._as_int(pcfg.get("min_valid_frames")) or 3),
            "strict": bool(pcfg.get("strict", True)),
        }

    def sensor_precheck_scope(self) -> str:
        pcfg = self.sensor_precheck_config()
        raw_scope = pcfg.get("scope")
        raw_mode = pcfg.get("mode")
        raw_value = raw_scope if raw_scope not in (None, "") else raw_mode
        if raw_value in (None, ""):
            return "all_analyzers"

        normalized = str(raw_value).strip().lower()
        if normalized in {"v1_compatible", "first_analyzer_only"}:
            return "first_analyzer_only"
        if normalized in {"full", "all_analyzers"}:
            return "all_analyzers"
        self.host._log(f"Sensor precheck scope '{raw_value}' not recognized; fallback to all_analyzers")
        return "all_analyzers"

    def sensor_precheck_validation_mode(self) -> str:
        """Return the validation mode for sensor precheck.

        Returns:
            "v1_frame_like": Raw-frame-first V1-like validation (closest compatibility mode)
            "v1_mode2_like": V1-compatible MODE2-like frame validation (first analyzer, relaxed keys)
            "snapshot": Full snapshot-based validation (default, stronger)
        """
        pcfg = self.sensor_precheck_config()
        raw_profile = str(pcfg.get("profile", "") or "").strip().lower()
        if raw_profile == "raw_frame_first":
            return "v1_frame_like"
        if raw_profile == "mode2_like":
            return "v1_mode2_like"
        if raw_profile == "snapshot":
            return "snapshot"
        raw_mode = pcfg.get("validation_mode")
        if raw_mode is None:
            return "snapshot"
        normalized = str(raw_mode).strip().lower()
        if normalized in {"v1_frame_like", "v1_raw_frame_like", "raw_frame", "v1_like"}:
            return "v1_frame_like"
        if normalized in {"v1_mode2_like", "mode2_like", "v1_compatible"}:
            return "v1_mode2_like"
        if normalized in {"snapshot", "full", "strict"}:
            return "snapshot"
        return "snapshot"

    def sensor_precheck_profile(self) -> dict[str, str]:
        scope = self.sensor_precheck_scope()
        validation_mode = self.sensor_precheck_validation_mode()
        requested_profile = str(self.sensor_precheck_config().get("profile", "") or "").strip().lower()
        profile = "snapshot"
        if requested_profile in {"snapshot", "mode2_like", "raw_frame_first"}:
            profile = requested_profile
        elif validation_mode == "v1_frame_like":
            profile = "raw_frame_first"
        elif validation_mode == "v1_mode2_like":
            profile = "mode2_like"
        return {
            "profile": profile,
            "scope": scope,
            "validation_mode": validation_mode,
        }

    def analyzer_mode2_init_config(self) -> dict[str, Any]:
        return _normalize_analyzer_mode2_init_config(
            getattr(self.context.config.workflow, "analyzer_mode2_init", {}) or {}
        )

    def analyzer_setup_config(self) -> dict[str, Any]:
        return _normalize_analyzer_setup_config(
            getattr(self.context.config.workflow, "analyzer_setup", {}) or {}
        )

    @staticmethod
    def _device_id_to_int(device_id: Any) -> Optional[int]:
        text = str(device_id or "").strip()
        if not text.isdigit():
            return None
        return int(text)

    @staticmethod
    def _format_device_id(value: int) -> str:
        return f"{int(value):03d}"

    def _planned_device_ids(self, analyzer_count: int) -> list[str]:
        setup = self.analyzer_setup_config()
        assignment_mode = str(setup.get("device_id_assignment_mode", "automatic") or "automatic").strip().lower()
        manual_ids = [str(item).strip() for item in list(setup.get("manual_device_ids", []) or []) if str(item).strip()]
        start_value = self._device_id_to_int(setup.get("start_device_id"))
        next_id = 1 if start_value is None else start_value
        planned: list[str] = []
        used: set[str] = set()

        if assignment_mode == "manual":
            for device_id in manual_ids:
                if len(planned) >= analyzer_count:
                    break
                planned.append(device_id)
                used.add(device_id)

        while len(planned) < analyzer_count:
            candidate = self._format_device_id(next_id)
            next_id += 1
            if candidate in used:
                continue
            planned.append(candidate)
            used.add(candidate)
        return planned

    def _apply_device_id_to_analyzer(self, analyzer: Any, *, label: str, device_id: str) -> tuple[str, str]:
        method = self.host._first_method(
            analyzer,
            (
                "set_device_id_with_ack",
                "set_device_id",
                "write_device_id",
                "assign_device_id",
                "set_id",
            ),
        )
        if method is None:
            return "skipped", "no supported device-id method"
        try:
            self._call_with_optional_ack(method, device_id)
        except NoWriteViolation:
            raise
        except Exception as exc:
            return "warn", f"device-id apply failed: {exc}"

        readback_method = self.host._first_method(
            analyzer,
            ("read_device_id", "get_device_id", "device_id"),
        )
        if readback_method is None:
            return "ok", "assigned without readback"
        try:
            if str(getattr(readback_method, "__name__", "")).lower() == "device_id":
                readback = readback_method()
            else:
                readback = readback_method()
        except Exception as exc:
            return "warn", f"assigned but readback failed: {exc}"
        readback_text = str(readback or "").strip()
        if readback_text and readback_text != device_id:
            return "warn", f"assigned but readback={readback_text}"
        return "ok", "assigned and verified" if readback_text else "assigned without readback"

    def apply_analyzer_setup(self) -> None:
        analyzers = self.all_gas_analyzers()
        if not analyzers:
            self.host._log("Analyzer setup skipped: gas analyzer unavailable")
            return

        setup = self.analyzer_setup_config()
        planned_ids = self._planned_device_ids(len(analyzers))
        raw_apply_device_id = setup.get("apply_device_id", True)
        apply_device_id = (
            bool(raw_apply_device_id)
            if isinstance(raw_apply_device_id, bool)
            else str(raw_apply_device_id).strip().lower() not in {"0", "false", "no", "off"}
        )
        self.host._log(
            "Analyzer setup "
            f"software_version={setup.get('software_version')} "
            f"device_id_assignment_mode={setup.get('device_id_assignment_mode')} "
            f"start_device_id={setup.get('start_device_id')} "
            f"apply_device_id={apply_device_id} analyzers={len(analyzers)}"
        )
        self._record_route_trace(
            action="analyzer_setup_profile",
            target=dict(setup),
            actual={
                "analyzers": [label for label, _, _ in analyzers],
                "planned_device_ids": planned_ids,
            },
            result="ok",
            message="Analyzer setup resolved",
        )

        for index, (label, analyzer, _cfg) in enumerate(analyzers):
            desired_id = planned_ids[index]
            if not apply_device_id:
                detail = "device-id apply skipped by configuration; existing id retained"
                self.host._log(f"Analyzer setup device-id keep ({label}): id={desired_id} result=ok detail={detail}")
                self._record_route_trace(
                    action="analyzer_device_id_keep",
                    target={
                        "analyzer": label,
                        "device_id": desired_id,
                        "software_version": setup.get("software_version"),
                    },
                    actual={"detail": detail},
                    result="ok",
                    message="Analyzer device-id apply skipped",
                )
                continue
            result, detail = self._apply_device_id_to_analyzer(
                analyzer,
                label=label,
                device_id=desired_id,
            )
            self.host._log(f"Analyzer setup device-id ({label}): id={desired_id} result={result} detail={detail}")
            self._record_route_trace(
                action="analyzer_device_id_assignment",
                target={
                    "analyzer": label,
                    "device_id": desired_id,
                    "software_version": setup.get("software_version"),
                },
                actual={"detail": detail},
                result=result,
                message="Analyzer device-id assignment",
            )

    @staticmethod
    def _call_with_optional_ack(method: Any, *args: Any, **kwargs: Any) -> bool:
        if not callable(method):
            return False
        if str(getattr(method, "__name__", "")).endswith("with_ack"):
            payload = dict(kwargs)
            payload.setdefault("require_ack", False)
            method(*args, **payload)
        else:
            method(*args)
        return True

    @staticmethod
    def _summarize_sensor_line(line: Any, limit: int = 120) -> str:
        text = str(line or "").replace("\x00", " ").strip()
        if not text:
            return ""
        return " ".join(text.split())[:limit]

    def _read_mode2_frame(
        self,
        analyzer: Any,
        *,
        prefer_stream: bool,
        ftd_hz: int,
        attempts: int,
        retry_delay_s: float,
    ) -> tuple[str, Optional[dict[str, Any]]]:
        last_text = ""
        last_parsed: Optional[dict[str, Any]] = None
        read_latest = getattr(analyzer, "read_latest_data", None)
        drain_lines = getattr(analyzer, "_drain_stream_lines", None)
        passive_read = getattr(analyzer, "read_data_passive", None)
        drain_s = max(0.2, 2.0 / max(1, int(ftd_hz)))

        for attempt in range(max(1, int(attempts))):
            if prefer_stream and callable(drain_lines):
                try:
                    raw_lines = list(drain_lines(drain_s=drain_s, read_timeout_s=0.05) or [])
                except Exception:
                    raw_lines = []
                for raw_line in reversed(raw_lines):
                    parsed, summary = self._parse_sensor_line_payload(
                        analyzer,
                        raw_line,
                        source_method="read_latest_data",
                    )
                    last_text = summary or str(raw_line or "")
                    if self._has_valid_sensor_frame(parsed, validation_mode="v1_frame_like"):
                        return last_text, parsed

            if callable(read_latest):
                try:
                    payload = read_latest(
                        prefer_stream=prefer_stream,
                        drain_s=drain_s,
                        read_timeout_s=0.05,
                        allow_passive_fallback=False,
                    )
                except TypeError:
                    payload = read_latest(prefer_stream=prefer_stream, allow_passive_fallback=False)
                except Exception:
                    payload = ""
            elif not prefer_stream and callable(passive_read):
                try:
                    payload = passive_read()
                except Exception:
                    payload = ""
            else:
                payload = ""

            parsed, summary = self._parse_sensor_line_payload(
                analyzer,
                payload,
                source_method="read_latest_data" if callable(read_latest) else "read_data_passive",
            )
            last_text = summary or str(payload or "")
            if self._has_valid_sensor_frame(
                parsed,
                validation_mode="v1_frame_like" if prefer_stream else "v1_mode2_like",
            ):
                return last_text, parsed
            last_parsed = parsed or None
            if attempt + 1 < max(1, int(attempts)) and retry_delay_s > 0:
                time.sleep(max(0.0, float(retry_delay_s)))
        return last_text, last_parsed

    def _run_mode2_init_sequence(
        self,
        analyzer: Any,
        *,
        label: str,
        settings: dict[str, Any],
    ) -> bool:
        init_cfg = self.analyzer_mode2_init_config()
        if not bool(init_cfg.get("enabled", True)):
            return False

        set_comm_way = self.host._first_method(analyzer, ("set_comm_way_with_ack", "set_comm_way"))
        set_mode = self.host._first_method(analyzer, ("set_mode_with_ack", "set_mode"))
        set_average_filter_channel = self.host._first_method(analyzer, ("set_average_filter_channel_with_ack",))
        set_average_filter = self.host._first_method(analyzer, ("set_average_filter_with_ack", "set_average_filter"))
        set_warning_phase = getattr(analyzer, "set_warning_phase", None)
        success_ack = getattr(analyzer, "_is_success_ack", None)
        if not callable(set_comm_way) or not callable(set_mode):
            return False
        if not callable(set_average_filter_channel) and not callable(set_average_filter):
            return False
        if not callable(getattr(analyzer, "read_latest_data", None)) and not callable(getattr(analyzer, "read_data_passive", None)):
            return False

        reapply_attempts = max(1, int(init_cfg.get("reapply_attempts", 4)))
        stream_attempts = max(1, int(init_cfg.get("stream_attempts", 10)))
        passive_attempts = max(1, int(init_cfg.get("passive_attempts", 4)))
        retry_delay_s = max(0.0, float(init_cfg.get("retry_delay_s", 0.2)))
        reapply_delay_s = max(0.0, float(init_cfg.get("reapply_delay_s", 0.35)))
        command_gap_s = max(0.0, float(init_cfg.get("command_gap_s", 0.15)))
        post_enable_stream_wait_s = max(0.0, float(init_cfg.get("post_enable_stream_wait_s", 2.0)))
        post_enable_stream_ack_wait_s = max(0.0, float(init_cfg.get("post_enable_stream_ack_wait_s", 8.0)))
        last_error: Optional[Exception] = None

        try:
            if callable(set_warning_phase):
                set_warning_phase("startup")
            for attempt_index in range(reapply_attempts):
                self._call_with_optional_ack(set_comm_way, False)
                if command_gap_s > 0:
                    time.sleep(command_gap_s)
                self._call_with_optional_ack(set_mode, settings["mode"])
                if command_gap_s > 0:
                    time.sleep(command_gap_s)
                if callable(set_average_filter_channel):
                    self._call_with_optional_ack(set_average_filter_channel, 1, settings["avg_filter"])
                    if command_gap_s > 0:
                        time.sleep(command_gap_s)
                    self._call_with_optional_ack(set_average_filter_channel, 2, settings["avg_filter"])
                else:
                    self._call_with_optional_ack(set_average_filter, settings["avg_filter"])
                if command_gap_s > 0:
                    time.sleep(command_gap_s)

                if settings["active_send"]:
                    self._call_with_optional_ack(set_comm_way, True)
                    if post_enable_stream_wait_s > 0:
                        time.sleep(post_enable_stream_wait_s)
                    stream_line, stream_parsed = self._read_mode2_frame(
                        analyzer,
                        prefer_stream=True,
                        ftd_hz=int(settings["ftd_hz"]),
                        attempts=stream_attempts,
                        retry_delay_s=retry_delay_s,
                    )
                    if stream_parsed:
                        return True
                    if callable(success_ack) and success_ack(stream_line):
                        self.host._log(
                            "Analyzer MODE2 stream ack observed: "
                            f"{label or 'gas_analyzer'} wait={post_enable_stream_ack_wait_s:.1f}s for MODE2 data"
                        )
                        if post_enable_stream_ack_wait_s > 0:
                            time.sleep(post_enable_stream_ack_wait_s)
                        follow_line, follow_parsed = self._read_mode2_frame(
                            analyzer,
                            prefer_stream=True,
                            ftd_hz=int(settings["ftd_hz"]),
                            attempts=max(stream_attempts, 3),
                            retry_delay_s=retry_delay_s,
                        )
                        if follow_parsed:
                            return True
                        if follow_line:
                            stream_line = follow_line
                    last_error = RuntimeError(
                        "stream verify not full MODE2 "
                        f"last={self._summarize_sensor_line(stream_line)}"
                    )
                else:
                    passive_line, passive_parsed = self._read_mode2_frame(
                        analyzer,
                        prefer_stream=False,
                        ftd_hz=int(settings["ftd_hz"]),
                        attempts=passive_attempts,
                        retry_delay_s=max(0.0, retry_delay_s / 2.0),
                    )
                    if passive_parsed:
                        return True
                    last_error = RuntimeError(
                        "passive verify not full MODE2 "
                        f"last={self._summarize_sensor_line(passive_line)}"
                    )

                if attempt_index + 1 < reapply_attempts:
                    self.host._log(
                        "Analyzer MODE2 verify retry: "
                        f"{label or 'gas_analyzer'} attempt {attempt_index + 2}/{reapply_attempts} "
                        f"reason={last_error}"
                    )
                    if reapply_delay_s > 0:
                        time.sleep(reapply_delay_s)
            if last_error is not None:
                raise last_error
        finally:
            if callable(set_warning_phase):
                set_warning_phase("")
        return False

    def _apply_basic_gas_analyzer_settings(
        self,
        analyzer: Any,
        *,
        label: str,
        settings: dict[str, Any],
        skip_mode2_init_fields: bool,
    ) -> None:
        try:
            mode_method = self.host._first_method(analyzer, ("set_mode_with_ack", "set_mode"))
            if not skip_mode2_init_fields and mode_method is not None:
                self._call_with_optional_ack(mode_method, settings["mode"])
        except Exception as exc:
            self.host._log(f"Analyzer {label} mode setup failed: {exc}")
        try:
            method = self.host._first_method(analyzer, ("set_comm_way_with_ack", "set_comm_way"))
            if not skip_mode2_init_fields and method is not None:
                self._call_with_optional_ack(method, settings["active_send"])
        except Exception as exc:
            self.host._log(f"Analyzer {label} comm-way setup failed: {exc}")
        try:
            method = self.host._first_method(analyzer, ("set_active_freq_with_ack", "set_active_freq"))
            if method is not None:
                self._call_with_optional_ack(method, settings["ftd_hz"])
        except Exception as exc:
            self.host._log(f"Analyzer {label} active frequency setup failed: {exc}")
        try:
            method = self.host._first_method(analyzer, ("set_average_filter_with_ack", "set_average_filter"))
            if not skip_mode2_init_fields and method is not None:
                self._call_with_optional_ack(method, settings["avg_filter"])
        except Exception as exc:
            self.host._log(f"Analyzer {label} average filter setup failed: {exc}")
        try:
            method = self.host._first_method(analyzer, ("set_average_with_ack", "set_average"))
            if method is not None:
                if str(getattr(method, "__name__", "")).endswith("with_ack"):
                    method(co2_n=settings["avg_co2"], h2o_n=settings["avg_h2o"], require_ack=False)
                else:
                    method(settings["avg_co2"], settings["avg_h2o"])
        except Exception as exc:
            self.host._log(f"Analyzer {label} average setup failed: {exc}")

    def configure_gas_analyzer(
        self,
        analyzer: Any,
        *,
        label: str,
        cfg: Any,
        overrides: Optional[dict[str, Any]] = None,
    ) -> None:
        settings = self.gas_analyzer_runtime_settings(cfg)
        if overrides:
            settings.update(dict(overrides))
        mode2_init_handled = False
        setup = self.analyzer_setup_config()
        software_version = str(setup.get("software_version", "v5_plus") or "v5_plus").strip().lower()
        if software_version == "pre_v5":
            self.host._log(f"Analyzer {label} MODE2 init skipped: analyzer_setup.software_version=pre_v5")
        else:
            try:
                mode2_init_handled = self._run_mode2_init_sequence(
                    analyzer,
                    label=label,
                    settings=settings,
                )
            except Exception as exc:
                self.host._log(f"Analyzer {label} MODE2 init fallback to basic setup: {exc}")
        self._apply_basic_gas_analyzer_settings(
            analyzer,
            label=label,
            settings=settings,
            skip_mode2_init_fields=mode2_init_handled,
        )

    def run_sensor_precheck(self) -> None:
        pcfg = self.sensor_precheck_config()
        if not pcfg or not bool(pcfg.get("enabled", False)):
            return

        analyzers = self.all_gas_analyzers()
        if not analyzers:
            self.host._log("Sensor precheck skipped: gas analyzer unavailable")
            return

        profile = self.sensor_precheck_profile()
        scope = profile["scope"]
        validation_mode = profile["validation_mode"]
        if scope == "first_analyzer_only":
            analyzers = analyzers[:1]
        self.host._log(
            "Sensor precheck "
            f"profile={profile['profile']} scope={scope} validation_mode={validation_mode} analyzers={len(analyzers)}"
        )
        self._record_route_trace(
            action="sensor_precheck_profile",
            target={
                "profile": profile["profile"],
                "scope": scope,
                "validation_mode": validation_mode,
            },
            actual={
                "analyzer_count": len(analyzers),
                "analyzers": [label for label, _, _ in analyzers],
            },
            result="ok",
            message="Sensor precheck profile resolved",
        )

        for label, analyzer, cfg in analyzers:
            settings = self.sensor_precheck_settings(cfg)
            self.host._log(
                f"Sensor precheck start ({label}): profile={profile['profile']} mode={settings['mode']} "
                f"active_send={settings['active_send']} ftd={settings['ftd_hz']}Hz "
                f"avg=({settings['avg_co2']},{settings['avg_h2o']}) "
                f"filter={settings['avg_filter']} validation_mode={validation_mode}"
            )
            self.configure_gas_analyzer(
                analyzer,
                label=label,
                cfg=cfg,
                overrides=settings,
            )

            valid_frames = 0
            last_valid = ""
            last_error = ""
            deadline = time.time() + float(settings["duration_s"])
            while time.time() < deadline:
                if self.context.stop_event.is_set():
                    return
                try:
                    snapshot, source, last_error = self._read_sensor_precheck_frame(
                        analyzer,
                        label=label,
                        validation_mode=validation_mode,
                        log_failures=False,
                    )
                except Exception as exc:
                    source = ""
                    last_error = str(exc)
                    snapshot = {}
                if self._has_valid_sensor_frame(snapshot, validation_mode=validation_mode):
                    valid_frames += 1
                    last_valid = str(snapshot.get("raw") or self._snapshot_summary(snapshot))
                    fallback_reason = ""
                    if source.startswith("snapshot_fallback:") and last_error:
                        fallback_reason = last_error
                    if valid_frames >= int(settings["min_valid_frames"]):
                        fallback_suffix = f" fallback_reason={fallback_reason[:160]}" if fallback_reason else ""
                        self.host._log(
                            f"Sensor precheck passed ({label}): profile={profile['profile']} "
                            f"valid_frames={valid_frames} source={source or 'unknown'}{fallback_suffix}"
                        )
                        actual_payload = {
                            "valid_frames": valid_frames,
                            "last_valid": last_valid,
                            "source": source,
                        }
                        if fallback_reason:
                            actual_payload["fallback_reason"] = fallback_reason
                        self._record_route_trace(
                            action="sensor_precheck_analyzer",
                            target={
                                "analyzer": label,
                                "profile": profile["profile"],
                                "scope": scope,
                                "validation_mode": validation_mode,
                                "min_valid_frames": int(settings["min_valid_frames"]),
                                "strict": bool(settings["strict"]),
                            },
                            actual=actual_payload,
                            result="ok",
                            message="Sensor precheck passed",
                        )
                        break
                poll_s = float(settings["poll_s"])
                if poll_s > 0:
                    time.sleep(poll_s)
            else:
                message = (
                    f"Sensor precheck failed ({label}): valid_frames={valid_frames}/{int(settings['min_valid_frames'])}"
                )
                if last_valid:
                    message += f" last={last_valid[:120]}"
                elif last_error:
                    message += f" last_error={last_error[:120]}"
                self._record_route_trace(
                    action="sensor_precheck_analyzer",
                    target={
                        "analyzer": label,
                        "profile": profile["profile"],
                        "scope": scope,
                        "validation_mode": validation_mode,
                        "min_valid_frames": int(settings["min_valid_frames"]),
                        "strict": bool(settings["strict"]),
                    },
                    actual={
                        "valid_frames": valid_frames,
                        "last_valid": last_valid,
                        "last_error": last_error,
                        "source": source,
                    },
                    result="fail" if bool(settings["strict"]) else "warn",
                    message=message,
                )
                if bool(settings["strict"]):
                    raise WorkflowValidationError(
                        "Sensor precheck failed",
                        details={
                            "analyzer": label,
                            "valid_frames": valid_frames,
                            "min_valid_frames": int(settings["min_valid_frames"]),
                            "last_valid": last_valid,
                            "last_error": last_error,
                        },
                    )
                self.host._log(message)

    def _has_valid_sensor_frame(self, snapshot: dict[str, Any], *, validation_mode: str = "snapshot") -> bool:
        """Check if a sensor snapshot contains a valid frame.

        Args:
            snapshot: The sensor snapshot to validate.
            validation_mode: "snapshot" for full validation, "v1_mode2_like" for V1-compatible relaxed validation.

        Returns:
            True if the frame is valid according to the validation mode.
        """
        if validation_mode in {"v1_mode2_like", "v1_frame_like"}:
            mode_value = snapshot.get("mode")
            if mode_value not in (None, 2, "2", 2.0, "2.0"):
                return False
            return (
                self.host._pick_numeric(
                    snapshot,
                    "co2_ppm",
                    "co2",
                    "co2_ratio_f",
                ) is not None
                and self.host._pick_numeric(
                    snapshot,
                    "h2o_mmol",
                    "h2o",
                    "h2o_ratio_f",
                ) is not None
            )
        return self.host._pick_numeric(
            snapshot,
            "co2_ratio_f",
            "co2_ppm",
            "co2",
            "h2o_ratio_f",
            "h2o_mmol",
            "h2o",
            "co2_signal",
            "h2o_signal",
        ) is not None

    def _record_route_trace(self, **kwargs: Any) -> None:
        status_service = getattr(self.host, "status_service", None)
        recorder = getattr(status_service, "record_route_trace", None)
        if callable(recorder):
            recorder(**kwargs)

    def _sensor_read_methods_for_validation_mode(self, validation_mode: str) -> tuple[str, ...]:
        if validation_mode == "v1_frame_like":
            return ("read_latest_data", "read_data_active", "read_data_passive", "read", "status", "fetch_all", "get_current")
        if validation_mode == "v1_mode2_like":
            return ("read", "status", "fetch_all", "get_current")
        return ("fetch_all", "get_current", "read", "status")

    @staticmethod
    def _sensor_raw_frame_methods() -> tuple[str, ...]:
        return ("read_latest_data", "read_data_active", "read_data_passive")

    def _read_sensor_precheck_frame(
        self,
        analyzer: Any,
        *,
        label: str,
        validation_mode: str,
        log_failures: bool,
    ) -> tuple[dict[str, Any], str, str]:
        if validation_mode == "v1_frame_like":
            snapshot, source, reason = self._read_raw_sensor_frame_with_retry(
                analyzer,
                label=label,
                log_failures=log_failures,
            )
            if self._has_valid_sensor_frame(snapshot, validation_mode=validation_mode):
                return snapshot, source, reason
            fallback_snapshot, fallback_source, fallback_reason = self._read_sensor_snapshot_with_retry_ex(
                analyzer,
                label=label,
                required_keys=self.SENSOR_FRAME_KEYS,
                validation_mode="v1_mode2_like",
                log_failures=log_failures,
            )
            return fallback_snapshot, f"snapshot_fallback:{fallback_source or 'none'}", reason or fallback_reason
        snapshot, source, reason = self._read_sensor_snapshot_with_retry_ex(
            analyzer,
            label=label,
            required_keys=self.SENSOR_FRAME_KEYS,
            validation_mode=validation_mode,
            log_failures=log_failures,
        )
        return snapshot, source, reason

    @staticmethod
    def _snapshot_from_status(payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        out = dict(payload)
        out.pop("ok", None)
        return out

    def _normalize_sensor_snapshot(self, raw_snapshot: Any, *, source_method: str) -> dict[str, Any]:
        snapshot = self.host._normalize_snapshot(raw_snapshot)
        if source_method == "status" and isinstance(snapshot, dict):
            return self._snapshot_from_status(snapshot)
        return snapshot

    def _parse_sensor_line_payload(self, analyzer: Any, payload: Any, *, source_method: str) -> tuple[dict[str, Any], str]:
        if isinstance(payload, dict):
            snapshot = self._normalize_sensor_snapshot(payload, source_method=source_method)
            raw_text = str(snapshot.get("raw") or "").strip()
            if raw_text:
                parsed = self._parse_sensor_raw_line(analyzer, raw_text)
                if parsed:
                    return parsed, raw_text
            return snapshot, raw_text or self._snapshot_summary(snapshot)
        raw_text = str(payload or "").strip()
        if not raw_text:
            return {}, ""
        parsed = self._parse_sensor_raw_line(analyzer, raw_text)
        if parsed:
            return parsed, raw_text
        return {}, raw_text

    def _parse_sensor_raw_line(self, analyzer: Any, line: str) -> dict[str, Any]:
        if not line:
            return {}
        parse_mode2 = getattr(analyzer, "parse_line_mode2", None)
        parse_line = getattr(analyzer, "parse_line", None)
        try:
            if callable(parse_mode2):
                parsed = parse_mode2(line)
                if isinstance(parsed, dict):
                    return dict(parsed)
            if callable(parse_line):
                parsed = parse_line(line)
                if isinstance(parsed, dict):
                    return dict(parsed)
        except Exception:
            return {}
        return {}

    @staticmethod
    def _snapshot_has_retry_signal(snapshot: dict[str, Any], *, required_keys: tuple[str, ...]) -> bool:
        return bool(snapshot) and any(snapshot.get(key) is not None for key in required_keys)

    def _read_raw_sensor_frame_with_retry(
        self,
        analyzer: Any,
        *,
        label: str,
        log_failures: bool,
    ) -> tuple[dict[str, Any], str, str]:
        retries, delay_s = self.sensor_read_retry_settings()
        attempts = 1 + max(0, retries)
        context = f"analyzer {label} raw sensor read"
        last_reason = ""
        supported_methods = False
        for attempt in range(attempts):
            for method_name in self._sensor_raw_frame_methods():
                method = getattr(analyzer, method_name, None)
                if not callable(method):
                    continue
                supported_methods = True
                try:
                    if method_name == "read_latest_data":
                        try:
                            payload = method(prefer_stream=None, allow_passive_fallback=True)
                        except TypeError:
                            payload = method()
                    else:
                        payload = method()
                except Exception as exc:
                    last_reason = f"error={exc}"
                    continue
                snapshot, summary = self._parse_sensor_line_payload(
                    analyzer,
                    payload,
                    source_method=method_name,
                )
                if self._has_valid_sensor_frame(snapshot, validation_mode="v1_frame_like"):
                    return snapshot, method_name, ""
                if summary:
                    last_reason = f"invalid raw frame via={method_name} last={summary[:120]}"
                else:
                    last_reason = f"invalid raw frame via={method_name}"
            if attempt + 1 < attempts:
                if log_failures and last_reason:
                    self.host._log(f"Sensor read retry ({context}) {attempt + 1}/{retries}: {last_reason}")
                if delay_s > 0:
                    time.sleep(max(0.0, delay_s))
            elif log_failures and last_reason:
                self.host._log(f"Sensor read failed ({context}) after {attempts} attempts: {last_reason}")
        if not supported_methods and not last_reason:
            last_reason = "no supported raw sensor read method"
        return {}, "", last_reason

    def _read_sensor_snapshot_with_retry(
        self,
        analyzer: Any,
        *,
        label: str,
        required_keys: tuple[str, ...],
        validation_mode: str,
        log_failures: bool,
    ) -> dict[str, Any]:
        snapshot, _source, _reason = self._read_sensor_snapshot_with_retry_ex(
            analyzer,
            label=label,
            required_keys=required_keys,
            validation_mode=validation_mode,
            log_failures=log_failures,
        )
        return snapshot

    def _read_sensor_snapshot_with_retry_ex(
        self,
        analyzer: Any,
        *,
        label: str,
        required_keys: tuple[str, ...],
        validation_mode: str,
        log_failures: bool,
    ) -> tuple[dict[str, Any], str, str]:
        method_names = self._sensor_read_methods_for_validation_mode(validation_mode)
        method = self.host._first_method(analyzer, method_names)
        if method is None:
            return {}, "", "no supported sensor read method"
        source_method = getattr(method, "__name__", "") or str(method_names[0])
        retries, delay_s = self.sensor_read_retry_settings()
        attempts = 1 + max(0, retries)
        context = f"analyzer {label} sensor read"
        last_snapshot: dict[str, Any] = {}
        last_reason = ""
        for attempt in range(attempts):
            snapshot: dict[str, Any] = {}
            summary = ""
            try:
                raw_snapshot = method()
                if validation_mode in {"v1_mode2_like", "v1_frame_like"}:
                    snapshot, summary = self._parse_sensor_line_payload(
                        analyzer,
                        raw_snapshot,
                        source_method=source_method,
                    )
                    if self._has_valid_sensor_frame(snapshot, validation_mode=validation_mode):
                        return snapshot, source_method, ""
                    if self._snapshot_has_retry_signal(snapshot, required_keys=required_keys):
                        return snapshot, source_method, ""
                    last_snapshot = snapshot
                    if summary:
                        last_reason = f"invalid MODE2-like frame via={source_method} last={summary[:120]}"
                    else:
                        last_reason = f"missing signal for keys={','.join(required_keys)} via={source_method}"
                    raise ValueError(last_reason)
                snapshot = self._normalize_sensor_snapshot(raw_snapshot, source_method=source_method)
            except Exception as exc:
                last_reason = str(exc) if validation_mode in {"v1_mode2_like", "v1_frame_like"} else f"error={exc}"
            else:
                if self._snapshot_has_retry_signal(snapshot, required_keys=required_keys):
                    return snapshot, source_method, ""
                last_snapshot = snapshot
                last_reason = f"missing signal for keys={','.join(required_keys)} via={source_method}"
            if attempt + 1 < attempts:
                if log_failures:
                    self.host._log(f"Sensor read retry ({context}) {attempt + 1}/{retries}: {last_reason}")
                if delay_s > 0:
                    time.sleep(max(0.0, delay_s))
            elif log_failures:
                self.host._log(f"Sensor read failed ({context}) after {attempts} attempts: {last_reason}")
        return last_snapshot, source_method, last_reason

    @staticmethod
    def _snapshot_summary(snapshot: dict[str, Any]) -> str:
        ordered_keys = (
            "co2_ratio_f",
            "co2_ppm",
            "co2",
            "h2o_ratio_f",
            "h2o_mmol",
            "h2o",
            "co2_signal",
            "h2o_signal",
        )
        parts: list[str] = []
        for key in ordered_keys:
            value = snapshot.get(key)
            if value is None:
                continue
            parts.append(f"{key}={value}")
        return ", ".join(parts)

    def attempt_reenable_disabled_analyzers(self) -> None:
        if not self.run_state.analyzers.disabled:
            return
        now = time.time()
        cooldown_s = self.analyzer_reprobe_cooldown_s()
        restored: list[str] = []
        failed: list[str] = []
        for label, analyzer, cfg in self.all_gas_analyzers():
            if label not in self.run_state.analyzers.disabled:
                continue
            last_probe = self.run_state.analyzers.disabled_last_reprobe_ts.get(label)
            if last_probe is not None and (now - last_probe) < cooldown_s:
                continue
            try:
                self.configure_gas_analyzer(analyzer, label=label, cfg=cfg)
                snapshot = self._read_sensor_snapshot_with_retry(
                    analyzer,
                    label=label,
                    required_keys=("co2_ratio_f", "co2_ppm", "h2o_ratio_f", "h2o_mmol"),
                    validation_mode="snapshot",
                    log_failures=False,
                )
                if self.host._pick_numeric(snapshot, "co2_ppm", "co2", "co2_ratio_f", "h2o_mmol", "h2o_ratio_f") is not None:
                    self.run_state.analyzers.disabled.discard(label)
                    self.run_state.analyzers.disabled_reasons.pop(label, None)
                    self.run_state.analyzers.disabled_last_reprobe_ts.pop(label, None)
                    restored.append(label)
                    continue
            except Exception as exc:
                self.host._log(f"Analyzer re-probe failed: {label} err={exc}")
            self.run_state.analyzers.disabled_last_reprobe_ts[label] = now
            failed.append(label)
        if restored:
            self.host._log(f"Analyzers restored to active set: {', '.join(restored)}")
        if failed:
            self.host._log(f"Analyzers still disabled after re-probe: {', '.join(failed)}")

    def sensor_read_retry_settings(self) -> tuple[int, float]:
        cfg = dict(getattr(self.context.config.workflow, "sensor_read_retry", {}) or {})
        try:
            retries = max(0, int(cfg.get("retries", 1)))
        except Exception:
            retries = 1
        try:
            delay_s = max(0.0, float(cfg.get("delay_s", 0.05)))
        except Exception:
            delay_s = 0.05
        return retries, delay_s
