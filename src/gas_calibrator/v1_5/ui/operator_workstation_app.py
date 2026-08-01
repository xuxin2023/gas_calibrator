"""Final-product V1.5 desktop operator workstation.

The window is intentionally dry-run-only.  It delegates every calibration
point to the mature V1.5 45/13 queue runners through ``operator_workstation``
and never changes ``run_app.py`` or the V1 fallback.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Iterable, Mapping

from ..certificate_metrics_registry import CertificateMetricsRegistry
from ..orchestration.operator_workstation import (
    V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
    build_v1_5_operator_workstation_plan,
    execute_v1_5_operator_workstation_dry_run,
    inspect_v1_5_runtime_config,
    preflight_v1_5_controlled_mature_route,
    run_v1_5_operator_workstation_application,
    write_v1_5_archive_authority_confirmation_receipt,
    write_v1_5_controlled_route_preflight_receipt,
    write_v1_5_operator_workstation_startup_receipt,
)
from ..workstation_snapshot import (
    CONFIGURED_CHANNEL_COUNT,
    build_workstation_snapshot,
)
from .pages import (
    CertificateMetricsPage,
    ReadOnlySummaryPage,
    SiteProfilePage,
    VisitorShowcasePage,
)


_TEXT = {
    "zh_CN": {
        "dialog.authority_archive": "正式归档索引（可选，只读）",
        "dialog.authority_run_id": "期望正式批次号（绑定归档时必填）",
        "dialog.authority_device_ids": "期望设备编号（逗号分隔，绑定归档时必填）",
        "dialog.authority_operator": "归档确认操作员（生成回执时必填）",
        "handoff.authority": (
            "决策权威归档：{status}｜索引 {index_sha}｜运行状态 {formal_sha}｜报告 {report_sha}"
        ),
        "handoff.authority.not_configured": "未配置（写入与签发保持锁定）",
        "handoff.authority.ready": "已按归档哈希绑定",
        "handoff.authority.blocked": "校验失败（写入与签发锁定）",
        "handoff.identity": (
            "归档批次身份：{status}｜批次 {run_id}｜设备 {device_ids}｜配置 {config_sha}"
        ),
        "handoff.identity.not_configured": "未配置",
        "handoff.identity.ready": "四层一致性已通过",
        "handoff.identity.blocked": "不一致或确认信息不足",
        "title": "V1.5 气体分析仪校准工作站",
        "kernel": "生产校准内核：0613 / 0620 / 0621",
        "coverage": "成熟流程 45 CO₂ 点 + 13 H₂O 点",
        "mode": "当前模式：仿真演练",
        "offline": "未连接真实设备",
        "nav.run": "运行",
        "nav.qc": "质控",
        "nav.results": "结果",
        "nav.devices": "设备",
        "nav.site": "现场配置",
        "nav.algorithm": "算法",
        "nav.report": "报告",
        "nav.review": "审核摘要",
        "nav.plan": "计划",
        "nav.certificate": "证书指标",
        "nav.visitor": "参观展示",
        "nav.auxiliary": "分析、报告与证据",
        "route.title": "校准路径与进度",
        "route.co2": "CO₂ 路径",
        "route.h2o": "H₂O 路径",
        "route.zero": "CO₂ 零气锚点",
        "route.dry": "H₂O 干气锚点（独立证据）",
        "route.low": "低量程段",
        "route.mid": "中量程段",
        "route.high": "高量程段",
        "route.finish": "结束",
        "measure.title": "当前点测量",
        "measure.route": "当前路径",
        "measure.point": "当前点",
        "measure.target": "目标值",
        "measure.mean": "多通道均值",
        "measure.tolerance": "允许偏差",
        "measure.judgement": "当前判定",
        "measure.dwell": "驻留状态",
        "value.waiting": "等待演练",
        "value.not_started": "尚未启动",
        "value.dry_run": "仅 dry-run",
        "value.not_applicable": "--",
        "channels.title": "分析仪只读状态（COM35–COM42，共八个通道）",
        "channels.waiting": "等待仿真演练",
        "channels.co2": "CO₂ 只读值",
        "channels.h2o": "H₂O 只读值",
        "channels.status": "状态",
        "channels.trend": "趋势",
        "channels.note": "备注",
        "channels.no_artifact": "无成熟运行工件",
        "channels.read_only": "只读工件 / NO COM",
        "evidence.temperature": "温度箱真值（箱内铂电阻数字测温仪）",
        "evidence.reference": "压力链（数字压力计真值 / 控制器反馈）与露点",
        "evidence.source": "流量来源（露点仪输出）",
        "evidence.certificate": "证书资料不阻断启动",
        "evidence.release": "正式签发另行审核",
        "evidence.unknown": "未知｜未发现可信新鲜工件",
        "evidence.flow_unknown": "L/min｜未知｜仅监测存在性与稳定性",
        "evidence.pressure_gauge_short": "表",
        "evidence.pressure_controller_short": "控",
        "evidence.pressure_delta_short": "Δ控-表",
        "evidence.dewpoint_short": "露点",
        "evidence.pressure_chain_ready": "压力链：读数与时序就绪",
        "evidence.pressure_chain_reference_missing": "压力链：未就绪｜数字压力计无有效读数",
        "evidence.pressure_chain_controller_missing": "压力链：未就绪｜压力控制器无有效反馈",
        "evidence.pressure_chain_pair_missing": "压力链：未就绪｜缺少同帧双设备读数",
        "evidence.pressure_chain_timing_invalid": "压力链：未就绪｜双设备读数时序不一致",
        "evidence.pressure_chain_stale": "压力链：未就绪｜读数工件已过期",
        "evidence.pressure_chain_reference_unconfigured": "压力链：未就绪｜数字压力计未配置",
        "evidence.pressure_chain_controller_unconfigured": "压力链：未就绪｜压力控制器未配置",
        "evidence.pressure_chain_unknown": "压力链：未就绪｜缺少可信读回证据",
        "aside.next": "下一步操作",
        "aside.heading": "成熟 V1.5 路径演练",
        "aside.step1": "1. 校验 45/13 canonical 队列",
        "aside.step2": "2. 调用 0620/0621 成熟运行器",
        "aside.step3": "3. 生成无 COM、无写入证据",
        "aside.start": "开始演练",
        "aside.running": "正在演练…",
        "aside.settings": "运行设置",
        "aside.handoff": "查看受控交接预览",
        "aside.open": "打开证据目录",
        "handoff.title": "V1.5 受控执行交接预览",
        "handoff.heading": "成熟运行器交接（只读）",
        "handoff.notice": (
            "此窗口不执行任何命令。真实 no-write 工程探针必须在执行时"
            "重新提供操作员确认，当前仍被阻断。"
        ),
        "handoff.status.pending": "状态：等待显式双重解锁",
        "handoff.status.gate_blocked": "状态：启动门禁未通过",
        "handoff.safety": (
            "执行权限：否｜系数写入：否｜设备 ID 写入：否｜"
            "FTD 写入：否｜正式验收证据：否"
        ),
        "handoff.conditional": (
            "若未来另行获准：仅响应检查会打开 COM 但不控制气路；"
            "路由采样才会控制对应气路 / 水路"
        ),
        "handoff.hash": "绑定配置 SHA256：{sha}",
        "handoff.scope": "可选范围：{scope}｜{status}",
        "handoff.scope.simulation_ready_real_locked": "仿真已就绪，真实执行锁定",
        "handoff.scope.blocked_pending_explicit_double_unlock": "等待显式双重解锁",
        "handoff.scope.blocked_by_startup_gate": "启动门禁未通过",
        "handoff.response_boundary": (
            "仅响应边界：不切气路、不设压力、不改变 MODE、不采校准点、不写设备"
        ),
        "handoff.decision": "{label}：{status}｜{reasons}",
        "handoff.decision.start_simulation": "仿真演练启动",
        "handoff.decision.start_real_execution": "真实执行启动",
        "handoff.decision.write_coefficients": "受控系数写入",
        "handoff.decision.issue_formal_certificate": "正式证书签发",
        "handoff.decision.allowed": "允许",
        "handoff.decision.blocked": "锁定",
        "handoff.command": "{route} 成熟运行器参数（仅预览）",
        "handoff.close": "关闭",
        "handoff.save_confirmation": "保存批次确认回执",
        "handoff.confirmation_title": "V1.5 归档批次确认",
        "handoff.confirmation_prompt": (
            "请确认：已选正式归档与当前批次号、设备编号和配置哈希一致。\n\n"
            "本回执只记录归档选择，不授权打开 COM、不授权写设备或系数、"
            "不执行正式证书签发。是否继续？"
        ),
        "handoff.confirmation_binding_blocked": (
            "归档批次身份核验未通过，不能生成 confirmed 回执。\n{reasons}"
        ),
        "handoff.confirmation_operator_missing": "请先在设置中填写归档确认操作员。",
        "handoff.confirmation_saved": "批次确认回执已不可覆盖地保存：\n{path}\nSHA-256: {sha}",
        "handoff.confirmation_failed": "批次确认回执保存失败：\n{reason}",
        "config.title": "运行配置门禁",
        "config.static": "静态配置｜控制器 {controller}｜压力计 {gauge}",
        "config.bound": "现场绑定已核验｜控制器 {controller}｜压力计 {gauge}",
        "config.blocked": "配置被阻断｜请检查运行设置",
        "config.hash": "SHA256 {sha}",
        "boundary.title": "安全边界（仿真模式）",
        "boundary.com": "真实 COM",
        "boundary.route": "气路 / 水路控制",
        "boundary.write": "SENCO / 设备 ID 写入",
        "boundary.acceptance": "真实验收证据",
        "boundary.disabled": "禁用",
        "boundary.no": "否",
        "boundary.note": "V1 fallback 与 run_app.py 保持不变",
        "note.title": "运行说明",
        "note.simulation": "当前为仿真演练，不导入任何真实串口。",
        "note.kernel": "45/13 点均由成熟 V1.5 运行器解释。",
        "note.certificate": "证书缺口只提醒；正式签发时单独审核。",
        "note.auxiliary": "分析、报告与证据均属于 V1.5，只读取统一运行状态。",
        "status.ready": "系统状态：等待演练",
        "status.running": "系统状态：V1.5 dry-run 执行中",
        "status.pass": "系统状态：45/13 dry-run 已通过",
        "status.failed": "系统状态：演练未通过",
        "footer.role": "角色：操作员",
        "dialog.title": "V1.5 工作站运行设置",
        "dialog.config": "运行配置",
        "dialog.co2": "CO₂ 45 点队列",
        "dialog.h2o": "H₂O 13 点队列",
        "dialog.output": "证据输出目录",
        "dialog.runtime": "成熟运行工件根目录（只读）",
        "dialog.certificate": "证书资料（可选，不阻断）",
        "dialog.browse": "浏览",
        "dialog.save": "保存",
        "dialog.cancel": "取消",
        "error.blocked": "V1.5 演练入口被阻断：\n{reasons}",
        "error.failed": "V1.5 dry-run 未通过：\n{reasons}",
        "info.pass": "成熟 V1.5 路径演练通过：CO₂ 45 点，H₂O 13 点。",
        "handoff.preflight.title": "成熟路线离线预检",
        "handoff.preflight.route": "预检路线",
        "handoff.preflight.run": "离线预检（不执行）",
        "handoff.preflight.locked": "尚未预检｜真实执行保持锁定",
        "handoff.preflight.ready": (
            "预检通过｜{route} 配置、队列与 45/13 点合同一致｜真实执行仍锁定"
        ),
        "handoff.preflight.blocked": "预检阻断｜{route}｜{reasons}",
        "handoff.preflight.note": (
            "仅校验绑定和参数，不打开 COM、不控制气路、不调用成熟运行器。"
        ),
        "handoff.preflight.save": "保存预检回执",
        "handoff.preflight.receipt_title": "V1.5 离线预检回执",
        "handoff.preflight.receipt_missing": "请先完成一次 CO₂ 或 H₂O 离线预检。",
        "handoff.preflight.receipt_saved": (
            "不可覆盖的预检回执已保存：\n{path}\nSHA-256: {sha}\n状态：{status}"
        ),
        "handoff.preflight.receipt_failed": "预检回执保存失败：\n{reason}",
    },
    "en_US": {
        "handoff.preflight.save": "Save preflight receipt",
        "handoff.preflight.receipt_title": "V1.5 offline preflight receipt",
        "handoff.preflight.receipt_missing": (
            "Run one CO₂ or H₂O offline preflight first."
        ),
        "handoff.preflight.receipt_saved": (
            "Immutable preflight receipt saved:\n{path}\nSHA-256: {sha}\nStatus: {status}"
        ),
        "handoff.preflight.receipt_failed": (
            "Preflight receipt could not be saved:\n{reason}"
        ),
        "handoff.preflight.title": "Mature route offline preflight",
        "handoff.preflight.route": "Route",
        "handoff.preflight.run": "Offline preflight (no execution)",
        "handoff.preflight.locked": "Not checked | real execution remains locked",
        "handoff.preflight.ready": (
            "Preflight passed | {route} config, queue, and 45/13 contract match | "
            "real execution remains locked"
        ),
        "handoff.preflight.blocked": "Preflight blocked | {route} | {reasons}",
        "handoff.preflight.note": (
            "Validates bindings and arguments only; no COM, route control, or mature runner call."
        ),
        "dialog.authority_archive": "Formal archive index (optional, read-only)",
        "dialog.authority_run_id": "Expected formal batch ID (required with archive)",
        "dialog.authority_device_ids": "Expected device IDs (comma-separated, required)",
        "dialog.authority_operator": "Archive confirmation operator",
        "handoff.authority": (
            "Decision authority archive: {status} | index {index_sha} | "
            "run status {formal_sha} | report {report_sha}"
        ),
        "handoff.authority.not_configured": "not configured; write and issue stay locked",
        "handoff.authority.ready": "bound by archive hashes",
        "handoff.authority.blocked": "validation failed; write and issue locked",
        "handoff.identity": (
            "Archive batch identity: {status} | batch {run_id} | devices {device_ids} | "
            "config {config_sha}"
        ),
        "handoff.identity.not_configured": "not configured",
        "handoff.identity.ready": "four-source identity matched",
        "handoff.identity.blocked": "mismatch or confirmation incomplete",
        "title": "V1.5 Gas Analyzer Calibration Workstation",
        "kernel": "Production kernel: 0613 / 0620 / 0621",
        "coverage": "Mature route: 45 CO₂ + 13 H₂O points",
        "mode": "Mode: simulation rehearsal",
        "offline": "No real devices connected",
        "nav.site": "Site Mapping",
        "evidence.reference": (
            "Pressure Chain (Gauge Truth / Controller Feedback) and Dew Point"
        ),
        "evidence.pressure_gauge_short": "Gauge",
        "evidence.pressure_controller_short": "Controller",
        "evidence.pressure_delta_short": "ΔCtrl-Gauge",
        "evidence.dewpoint_short": "Dew Point",
        "evidence.pressure_chain_ready": "Pressure chain: readback timing ready",
        "evidence.pressure_chain_reference_missing": "Pressure chain: gauge readback missing",
        "evidence.pressure_chain_controller_missing": "Pressure chain: controller feedback missing",
        "evidence.pressure_chain_pair_missing": "Pressure chain: paired readback missing",
        "evidence.pressure_chain_timing_invalid": "Pressure chain: readback timing invalid",
        "evidence.pressure_chain_stale": "Pressure chain: readback artifact stale",
        "evidence.pressure_chain_reference_unconfigured": "Pressure chain: gauge not configured",
        "evidence.pressure_chain_controller_unconfigured": "Pressure chain: controller not configured",
        "evidence.pressure_chain_unknown": "Pressure chain: trusted readback unavailable",
        "config.title": "Runtime Config Gate",
        "config.static": "Static config | Controller {controller} | Gauge {gauge}",
        "config.bound": "Evidence-bound | Controller {controller} | Gauge {gauge}",
        "config.blocked": "Config blocked | Review runtime settings",
        "config.hash": "SHA256 {sha}",
        "aside.handoff": "View Controlled Handoff",
        "handoff.title": "V1.5 Controlled Execution Handoff Preview",
        "handoff.heading": "Mature runner handoff (read-only)",
        "handoff.notice": (
            "This window does not execute commands. A real no-write engineering probe "
            "requires fresh operator confirmation at execution time and remains blocked."
        ),
        "handoff.status.pending": "Status: pending explicit double unlock",
        "handoff.status.gate_blocked": "Status: startup gate blocked",
        "handoff.safety": (
            "Execution: no | coefficient write: no | device ID write: no | "
            "FTD write: no | real acceptance evidence: no"
        ),
        "handoff.conditional": (
            "If separately authorized later, response-only opens COM without route control; "
            "route sampling may control the selected route"
        ),
        "handoff.hash": "Bound config SHA256: {sha}",
        "handoff.scope": "Available scope: {scope} | {status}",
        "handoff.scope.simulation_ready_real_locked": (
            "simulation ready, real execution locked"
        ),
        "handoff.scope.blocked_pending_explicit_double_unlock": (
            "pending explicit double unlock"
        ),
        "handoff.scope.blocked_by_startup_gate": "startup gate blocked",
        "handoff.response_boundary": (
            "Response-only boundary: no route, pressure, mode, sampling, or device write"
        ),
        "handoff.decision": "{label}: {status} | {reasons}",
        "handoff.decision.start_simulation": "Simulation start",
        "handoff.decision.start_real_execution": "Real execution start",
        "handoff.decision.write_coefficients": "Controlled coefficient write",
        "handoff.decision.issue_formal_certificate": "Formal certificate issue",
        "handoff.decision.allowed": "allowed",
        "handoff.decision.blocked": "locked",
        "handoff.command": "{route} mature runner arguments (preview only)",
        "handoff.close": "Close",
        "handoff.save_confirmation": "Save batch confirmation receipt",
        "handoff.confirmation_title": "V1.5 archive batch confirmation",
        "handoff.confirmation_prompt": (
            "Confirm that the selected formal archive matches the current batch ID, "
            "device IDs, and configuration hash.\n\nThis receipt records archive "
            "selection only. It does not authorize COM access, device or coefficient "
            "writes, or formal certificate issue. Continue?"
        ),
        "handoff.confirmation_binding_blocked": (
            "Archive batch identity validation is not ready; a confirmed receipt "
            "cannot be created.\n{reasons}"
        ),
        "handoff.confirmation_operator_missing": (
            "Enter the archive confirmation operator in Settings first."
        ),
        "handoff.confirmation_saved": (
            "Immutable batch confirmation receipt saved:\n{path}\nSHA-256: {sha}"
        ),
        "handoff.confirmation_failed": (
            "Batch confirmation receipt could not be saved:\n{reason}"
        ),
    },
}

_COLORS = {
    "bg": "#07111d",
    "nav": "#0a1623",
    "surface": "#0c1927",
    "card": "#102131",
    "card_alt": "#0e1d2b",
    "border": "#21384b",
    "text": "#f2f6fa",
    "muted": "#90a4b7",
    "blue": "#2f8fff",
    "blue_dark": "#153b63",
    "green": "#2bd9a8",
    "green_dark": "#123d37",
    "amber": "#f2af3a",
    "amber_dark": "#49351b",
}


def _t(key: str, *, locale: str = "zh_CN", **kwargs: Any) -> str:
    text = _TEXT.get(locale, {}).get(key) or _TEXT["zh_CN"].get(key) or key
    try:
        return text.format_map(kwargs)
    except (KeyError, ValueError):
        return text


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_paths() -> dict[str, str]:
    root = _repo_root()
    queue_root = Path(
        "D:/gas_calibrator/_handoff/v1_5_formal_queue_migration_20260624/"
        "canonical_open_flow_points"
    )
    return {
        "config": str(root / "configs" / "default_config.json"),
        "co2": str(queue_root / "co2_runner_queue.csv"),
        "h2o": str(queue_root / "h2o_runner_queue.csv"),
        "output": str(root / "output" / "v1_5_operator_workstation_ui"),
        "runtime": str(root / "logs"),
        "certificate": "",
        "authority_archive": "",
        "authority_run_id": "",
        "authority_device_ids": "",
        "authority_operator": "",
    }


class OperatorWorkstationApp:
    """Single-screen V1.5 operator shell backed by the verified dry-run seam."""

    def __init__(
        self,
        root: tk.Tk,
        *,
        locale: str = "zh_CN",
        initial_settings: Mapping[str, str] | None = None,
        executor: Callable[[Mapping[str, Any]], dict[str, Any]] = (
            execute_v1_5_operator_workstation_dry_run
        ),
    ) -> None:
        self.root = root
        self.locale = locale
        self.executor = executor
        paths = _default_paths()
        for key, value in dict(initial_settings or {}).items():
            if key in paths and str(value or "").strip():
                paths[key] = str(value)
        self.settings = {
            key: tk.StringVar(master=root, value=value)
            for key, value in paths.items()
        }
        self.last_result: dict[str, Any] | None = None
        self._result_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self._settings_dialog: tk.Toplevel | None = None
        self._handoff_dialog: tk.Toplevel | None = None
        self._handoff_preview_widget: tk.Text | None = None
        self.controlled_route_var = tk.StringVar(master=root, value="CO₂")
        self.controlled_preflight_var = tk.StringVar(
            master=root,
            value=_t("handoff.preflight.locked", locale=locale),
        )
        self.last_controlled_preflight: dict[str, Any] | None = None
        self._last_controlled_preflight_plan: dict[str, Any] | None = None
        self.controlled_preflight_button: ttk.Button | None = None
        self.controlled_preflight_receipt_button: ttk.Button | None = None
        self.pages: dict[str, tk.Widget] = {}
        self.nav_buttons: dict[str, ttk.Button] = {}
        self._presentation_active = False
        self.current_snapshot = build_workstation_snapshot(
            output_dir=self.settings["output"].get(),
            runtime_output_dir=self.settings["runtime"].get(),
        )
        self.status_var = tk.StringVar(master=root, value=_t("status.ready", locale=locale))
        self.route_var = tk.StringVar(master=root, value=_t("value.not_started", locale=locale))
        self.point_var = tk.StringVar(master=root, value=_t("value.waiting", locale=locale))
        self.judgement_var = tk.StringVar(master=root, value=_t("value.dry_run", locale=locale))
        self.dwell_var = tk.StringVar(master=root, value="0 / 58")
        self.config_gate_var = tk.StringVar(master=root, value="")
        self.config_hash_var = tk.StringVar(master=root, value="")
        self._configure_root()
        self._configure_styles()
        self._build()
        self._refresh_config_gate()

    def _configure_root(self) -> None:
        self.root.title(_t("title", locale=self.locale))
        self.root.geometry("1920x1080+0+0")
        self.root.minsize(1440, 860)
        self.root.configure(bg=_COLORS["bg"])
        try:
            self.root.tk.call("tk", "scaling", 1.25)
        except tk.TclError:
            pass

    def _configure_styles(self) -> None:
        style = ttk.Style(self.root)
        style.theme_use("clam")
        style.configure(
            "Primary.TButton",
            background=_COLORS["blue"],
            foreground="white",
            borderwidth=0,
            padding=(16, 12),
            font=("Microsoft YaHei UI", 11, "bold"),
        )
        style.map("Primary.TButton", background=[("active", "#55a5ff"), ("disabled", "#29445f")])
        style.configure(
            "Secondary.TButton",
            background=_COLORS["card"],
            foreground=_COLORS["text"],
            bordercolor=_COLORS["border"],
            padding=(12, 9),
            font=("Microsoft YaHei UI", 9),
        )
        style.map("Secondary.TButton", background=[("active", _COLORS["blue_dark"])])
        style.configure(
            "Nav.TButton",
            background=_COLORS["nav"],
            foreground=_COLORS["muted"],
            borderwidth=0,
            anchor="w",
            padding=(18, 12),
            font=("Microsoft YaHei UI", 10),
        )
        style.map(
            "Nav.TButton",
            background=[("active", _COLORS["card"]), ("selected", _COLORS["blue_dark"])],
            foreground=[("active", _COLORS["text"]), ("selected", _COLORS["text"])],
        )
        style.configure("Card.TFrame", background=_COLORS["surface"])
        style.configure(
            "Title.TLabel",
            background=_COLORS["surface"],
            foreground=_COLORS["text"],
            font=("Microsoft YaHei UI", 18, "bold"),
        )
        style.configure(
            "Muted.TLabel",
            background=_COLORS["surface"],
            foreground=_COLORS["muted"],
            font=("Microsoft YaHei UI", 9),
        )
        style.configure(
            "Accent.TButton",
            background=_COLORS["blue"],
            foreground="white",
            borderwidth=0,
            padding=(12, 9),
            font=("Microsoft YaHei UI", 9, "bold"),
        )
        style.map(
            "Accent.TButton",
            background=[("active", "#55a5ff"), ("disabled", "#29445f")],
        )
        style.configure(
            "Treeview",
            background=_COLORS["card"],
            fieldbackground=_COLORS["card"],
            foreground=_COLORS["text"],
            rowheight=28,
        )
        style.configure(
            "Treeview.Heading",
            background=_COLORS["card_alt"],
            foreground=_COLORS["text"],
        )
        style.configure(
            "Site.TLabelframe",
            background=_COLORS["surface"],
            bordercolor=_COLORS["border"],
        )
        style.configure(
            "Site.TLabelframe.Label",
            background=_COLORS["surface"],
            foreground=_COLORS["text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.configure(
            "Site.TEntry",
            fieldbackground=_COLORS["card"],
            foreground=_COLORS["text"],
            insertcolor=_COLORS["text"],
            bordercolor=_COLORS["border"],
            padding=(7, 6),
        )
        style.configure(
            "Site.TCombobox",
            fieldbackground=_COLORS["card"],
            background=_COLORS["card"],
            foreground=_COLORS["text"],
            arrowcolor=_COLORS["muted"],
            bordercolor=_COLORS["border"],
            padding=(7, 5),
        )
        style.configure(
            "Site.TSpinbox",
            fieldbackground=_COLORS["card"],
            background=_COLORS["card"],
            foreground=_COLORS["text"],
            insertcolor=_COLORS["text"],
            arrowcolor=_COLORS["muted"],
            bordercolor=_COLORS["border"],
            padding=(7, 5),
        )
        style.map(
            "Site.TCombobox",
            fieldbackground=[("readonly", _COLORS["card"])],
            foreground=[("readonly", _COLORS["text"])],
        )
        style.configure(
            "Site.TCheckbutton",
            background=_COLORS["surface"],
            foreground=_COLORS["text"],
            indicatorcolor=_COLORS["card"],
            font=("Microsoft YaHei UI", 9),
        )
        style.map(
            "Site.TCheckbutton",
            background=[("active", _COLORS["surface"])],
            foreground=[("active", _COLORS["text"])],
            indicatorcolor=[
                ("selected", _COLORS["blue"]),
                ("!selected", _COLORS["card"]),
            ],
        )

    def _label(
        self,
        parent: tk.Misc,
        text: str = "",
        *,
        textvariable: tk.StringVar | None = None,
        size: int = 10,
        color: str = "text",
        weight: str = "normal",
        bg: str = "surface",
        anchor: str = "w",
        justify: str = "left",
        wraplength: int = 0,
    ) -> tk.Label:
        return tk.Label(
            parent,
            text=text,
            textvariable=textvariable,
            bg=_COLORS[bg],
            fg=_COLORS[color],
            font=("Microsoft YaHei UI", size, weight),
            anchor=anchor,
            justify=justify,
            wraplength=wraplength,
        )

    def _panel(self, parent: tk.Misc, *, bg: str = "surface") -> tk.Frame:
        return tk.Frame(
            parent,
            bg=_COLORS[bg],
            highlightbackground=_COLORS["border"],
            highlightthickness=1,
            bd=0,
        )

    def _build(self) -> None:
        self.root.grid_rowconfigure(1, weight=1)
        self.root.grid_columnconfigure(0, weight=1)
        self._build_header()
        body = tk.Frame(self.root, bg=_COLORS["bg"])
        body.grid(row=1, column=0, sticky="nsew")
        body.grid_rowconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=1)
        self._build_navigation(body)
        self._build_page_host(body)
        self._build_footer()

    def _build_header(self) -> None:
        header = tk.Frame(self.root, bg=_COLORS["nav"], height=62)
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False)
        for column, weight in ((0, 0), (1, 0), (2, 1), (3, 0), (4, 0)):
            header.grid_columnconfigure(column, weight=weight)
        self._label(header, _t("title", locale=self.locale), size=17, weight="bold", bg="nav").grid(
            row=0, column=0, padx=(24, 24), pady=16
        )
        self._label(header, _t("kernel", locale=self.locale), color="muted", bg="nav").grid(
            row=0, column=1, padx=(0, 28)
        )
        self._label(header, _t("coverage", locale=self.locale), color="muted", bg="nav").grid(
            row=0, column=2, sticky="w"
        )
        self._status_badge(header, _t("mode", locale=self.locale), "blue_dark", "blue").grid(
            row=0, column=3, padx=8
        )
        self._status_badge(header, _t("offline", locale=self.locale), "amber_dark", "amber").grid(
            row=0, column=4, padx=(8, 24)
        )

    def _status_badge(self, parent: tk.Misc, text: str, bg: str, fg: str) -> tk.Label:
        return tk.Label(
            parent,
            text=text,
            bg=_COLORS[bg],
            fg=_COLORS[fg],
            font=("Microsoft YaHei UI", 9, "bold"),
            padx=12,
            pady=7,
        )

    def _build_navigation(self, body: tk.Frame) -> None:
        nav = tk.Frame(body, bg=_COLORS["nav"], width=176)
        self.navigation = nav
        nav.grid(row=0, column=0, sticky="nsw")
        nav.grid_propagate(False)
        for index, key in enumerate(
            (
                "run",
                "qc",
                "results",
                "devices",
                "site",
                "algorithm",
                "report",
                "review",
                "plan",
                "certificate",
                "visitor",
            )
        ):
            button = ttk.Button(
                nav,
                text=_t(f"nav.{key}", locale=self.locale),
                style="Nav.TButton",
                command=(
                    (lambda page=key: self.show_page(page))
                    if key
                    in {
                        "run",
                        "qc",
                        "results",
                        "devices",
                        "site",
                        "algorithm",
                        "report",
                        "review",
                        "plan",
                        "certificate",
                        "visitor",
                    }
                    else None
                ),
            )
            button.pack(fill="x", padx=8, pady=(8 if index == 0 else 1, 1))
            self.nav_buttons[key] = button
            if index == 0:
                button.state(["selected"])
        tk.Frame(nav, bg=_COLORS["border"], height=1).pack(fill="x", padx=14, pady=16)
        secondary = self._panel(nav, bg="card_alt")
        secondary.pack(side="bottom", fill="x", padx=10, pady=12)
        self._label(
            secondary,
            _t("nav.auxiliary", locale=self.locale),
            size=9,
            color="muted",
            bg="card_alt",
        ).pack(anchor="w", padx=12, pady=12)

    def _build_page_host(self, body: tk.Frame) -> None:
        host = tk.Frame(body, bg=_COLORS["bg"])
        host.grid(row=0, column=1, sticky="nsew")
        host.grid_rowconfigure(0, weight=1)
        host.grid_columnconfigure(0, weight=1)
        self.page_host = host

        run_page = tk.Frame(host, bg=_COLORS["bg"])
        run_page.grid(row=0, column=0, sticky="nsew")
        run_page.grid_rowconfigure(0, weight=1)
        run_page.grid_columnconfigure(1, weight=1)
        self._build_main(run_page)
        self._build_aside(run_page)

        registry_path = (
            Path(self.settings["output"].get())
            / "certificate_metrics_registry.json"
        )
        certificate_page = CertificateMetricsPage(
            host,
            registry=CertificateMetricsRegistry(registry_path),
            locale=self.locale,
        )
        certificate_page.grid(row=0, column=0, sticky="nsew")

        results_page = ReadOnlySummaryPage(
            host,
            page_kind="results",
            locale=self.locale,
        )
        results_page.grid(row=0, column=0, sticky="nsew")

        reports_page = ReadOnlySummaryPage(
            host,
            page_kind="reports",
            locale=self.locale,
        )
        reports_page.grid(row=0, column=0, sticky="nsew")

        review_page = ReadOnlySummaryPage(
            host,
            page_kind="review",
            locale=self.locale,
        )
        review_page.grid(row=0, column=0, sticky="nsew")

        plan_page = ReadOnlySummaryPage(
            host,
            page_kind="plan",
            locale=self.locale,
        )
        plan_page.grid(row=0, column=0, sticky="nsew")

        qc_page = ReadOnlySummaryPage(
            host,
            page_kind="qc",
            locale=self.locale,
        )
        qc_page.grid(row=0, column=0, sticky="nsew")

        devices_page = ReadOnlySummaryPage(
            host,
            page_kind="devices",
            locale=self.locale,
        )
        devices_page.grid(row=0, column=0, sticky="nsew")

        site_profile_page = SiteProfilePage(
            host,
            profile_path=(
                Path(self.settings["output"].get())
                / "v1_5_real_acceptance_site_profile.json"
            ),
            locale=self.locale,
        )
        site_profile_page.grid(row=0, column=0, sticky="nsew")

        algorithm_page = ReadOnlySummaryPage(
            host,
            page_kind="algorithm",
            locale=self.locale,
        )
        algorithm_page.grid(row=0, column=0, sticky="nsew")

        visitor_page = VisitorShowcasePage(
            host,
            on_enter_presentation=self.enter_visitor_presentation,
            on_exit_presentation=self.exit_visitor_presentation,
            locale=self.locale,
        )
        visitor_page.grid(row=0, column=0, sticky="nsew")

        self.pages = {
            "run": run_page,
            "qc": qc_page,
            "results": results_page,
            "devices": devices_page,
            "site": site_profile_page,
            "algorithm": algorithm_page,
            "report": reports_page,
            "review": review_page,
            "plan": plan_page,
            "certificate": certificate_page,
            "visitor": visitor_page,
        }
        self.results_page = results_page
        self.reports_page = reports_page
        self.review_page = review_page
        self.plan_page = plan_page
        self.qc_page = qc_page
        self.devices_page = devices_page
        self.site_profile_page = site_profile_page
        self.algorithm_page = algorithm_page
        self.certificate_page = certificate_page
        self.visitor_page = visitor_page
        self.refresh_workstation_snapshot()
        self.show_page("run")

    def show_page(self, page_name: str) -> None:
        """Raise one V1.5-owned page without changing the calibration runner."""

        page = self.pages.get(page_name)
        if page is None:
            return
        if page_name in {
            "qc",
            "results",
            "devices",
            "site",
            "algorithm",
            "report",
            "review",
            "plan",
            "visitor",
        }:
            self.refresh_workstation_snapshot()
        page.tkraise()
        for key, button in self.nav_buttons.items():
            if key == page_name:
                button.state(["selected"])
            else:
                button.state(["!selected"])

    def _visitor_snapshot(self) -> dict[str, Any]:
        return self.current_snapshot

    def refresh_workstation_snapshot(self) -> dict[str, Any]:
        """Rebuild once, then render every read-only V1.5 view from it."""

        certificate_error = ""
        try:
            certificate_records = self.certificate_page.registry.list_records()
        except Exception as exc:
            certificate_records = []
            certificate_error = f"{type(exc).__name__}: {exc}"
        try:
            plan = self._plan()
            decision_model = dict(plan.get("decision_model") or {})
            decision_authority_binding = dict(
                plan.get("decision_authority_binding") or {}
            )
        except Exception as exc:  # pragma: no cover - defensive UI boundary
            decision_model = {
                "schema": "v1_5_workstation_decision_model_v1",
                "aggregate_status": "blocked",
                "decisions": {},
                "fail_closed": True,
                "error": f"{type(exc).__name__}: {exc}",
            }
            decision_authority_binding = {
                "status": "blocked",
                "blockers": [
                    f"decision_authority_preview_failed:{type(exc).__name__}"
                ],
                "opens_com_ports": False,
                "writes_coefficients": False,
            }
        snapshot = build_workstation_snapshot(
            execution=self.last_result,
            output_dir=self.settings["output"].get(),
            runtime_output_dir=self.settings["runtime"].get(),
            site_profile=self.site_profile_page.profile,
            certificate_records=certificate_records,
            certificate_error=certificate_error,
            decision_model=decision_model,
            decision_authority_binding=decision_authority_binding,
        )
        self.current_snapshot = snapshot
        self.results_page.render(snapshot)
        self.reports_page.render(snapshot)
        self.review_page.render(snapshot)
        self.plan_page.render(snapshot)
        self.qc_page.render(snapshot)
        self.devices_page.render(snapshot)
        self.site_profile_page.render(snapshot)
        self.algorithm_page.render(snapshot)
        self.visitor_page.render(snapshot)
        self._refresh_run_readback(snapshot)
        return snapshot

    def enter_visitor_presentation(self) -> None:
        """Enter a reversible display-only full-screen mode."""

        self._presentation_active = True
        self.navigation.grid_remove()
        try:
            self.root.attributes("-fullscreen", True)
        except tk.TclError:
            pass
        self.root.bind("<Escape>", self._on_escape_presentation, add="+")
        self.visitor_page.set_presentation_active(True)

    def exit_visitor_presentation(self) -> None:
        self._presentation_active = False
        try:
            self.root.attributes("-fullscreen", False)
        except tk.TclError:
            pass
        self.navigation.grid()
        self.visitor_page.set_presentation_active(False)

    def _on_escape_presentation(self, _event: tk.Event[tk.Misc]) -> None:
        if self._presentation_active:
            self.exit_visitor_presentation()

    def _build_main(self, body: tk.Frame) -> None:
        main = tk.Frame(body, bg=_COLORS["bg"])
        main.grid(row=0, column=1, sticky="nsew", padx=14, pady=14)
        main.grid_columnconfigure(0, weight=1)
        main.grid_rowconfigure(2, weight=1)
        self._build_routes(main)
        self._build_measurements(main)
        self._build_channels(main)
        self._build_evidence(main)

    def _section_title(self, parent: tk.Misc, key: str) -> None:
        self._label(parent, _t(key, locale=self.locale), size=12, weight="bold").pack(
            anchor="w", padx=16, pady=(12, 8)
        )

    def _build_routes(self, main: tk.Frame) -> None:
        panel = self._panel(main)
        panel.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        self._section_title(panel, "route.title")
        route_grid = tk.Frame(panel, bg=_COLORS["surface"])
        route_grid.pack(fill="x", padx=12, pady=(0, 12))
        route_grid.grid_columnconfigure(1, weight=1)
        self._route_row(
            route_grid,
            0,
            "route.co2",
            ("route.zero", "route.low", "route.mid", "route.high", "route.finish"),
        )
        self._route_row(
            route_grid,
            1,
            "route.h2o",
            ("route.dry", "route.low", "route.mid", "route.high", "route.finish"),
        )

    def _route_row(
        self,
        parent: tk.Frame,
        row: int,
        title_key: str,
        stage_keys: tuple[str, ...],
    ) -> None:
        self._label(parent, _t(title_key, locale=self.locale), weight="bold").grid(
            row=row, column=0, sticky="w", padx=(4, 16), pady=8
        )
        stages = tk.Frame(parent, bg=_COLORS["card_alt"])
        stages.grid(row=row, column=1, sticky="ew", pady=5)
        for index, key in enumerate(stage_keys):
            stages.grid_columnconfigure(index, weight=1)
            active = index == 0
            tk.Label(
                stages,
                text=_t(key, locale=self.locale),
                bg=_COLORS["blue_dark"] if active else _COLORS["card_alt"],
                fg=_COLORS["text"] if active else _COLORS["muted"],
                font=("Microsoft YaHei UI", 9, "bold" if active else "normal"),
                padx=8,
                pady=10,
            ).grid(row=0, column=index, sticky="ew", padx=2)

    def _build_measurements(self, main: tk.Frame) -> None:
        panel = self._panel(main)
        panel.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        self._section_title(panel, "measure.title")
        grid = tk.Frame(panel, bg=_COLORS["surface"])
        grid.pack(fill="x", padx=12, pady=(0, 12))
        cards = (
            ("measure.route", self.route_var),
            ("measure.point", self.point_var),
            ("measure.target", None),
            ("measure.mean", None),
            ("measure.tolerance", None),
            ("measure.judgement", self.judgement_var),
            ("measure.dwell", self.dwell_var),
        )
        for index, (key, variable) in enumerate(cards):
            grid.grid_columnconfigure(index, weight=1, uniform="measurement")
            card = self._panel(grid, bg="card")
            card.grid(row=0, column=index, sticky="nsew", padx=3)
            self._label(card, _t(key, locale=self.locale), size=8, color="muted", bg="card").pack(
                anchor="w", padx=12, pady=(10, 4)
            )
            self._label(
                card,
                _t("value.not_applicable", locale=self.locale),
                textvariable=variable,
                size=12,
                weight="bold",
                bg="card",
            ).pack(anchor="w", padx=12, pady=(0, 12))

    def _build_channels(self, main: tk.Frame) -> None:
        panel = self._panel(main)
        panel.grid(row=2, column=0, sticky="nsew", pady=(0, 12))
        self._section_title(panel, "channels.title")
        grid = tk.Frame(panel, bg=_COLORS["surface"])
        grid.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        grid.grid_rowconfigure(0, weight=1)
        self.channel_card_vars: list[dict[str, tk.StringVar]] = []
        for index in range(CONFIGURED_CHANNEL_COUNT):
            grid.grid_columnconfigure(index, weight=1, uniform="channel")
            variables = {
                "name": tk.StringVar(master=self.root, value=f"通道 {index + 1:02d}"),
                "co2": tk.StringVar(master=self.root, value="--"),
                "h2o": tk.StringVar(master=self.root, value="--"),
                "status": tk.StringVar(
                    master=self.root,
                    value=_t("channels.no_artifact", locale=self.locale),
                ),
                "trend": tk.StringVar(master=self.root, value="—"),
                "note": tk.StringVar(master=self.root, value="freshness = unknown"),
                "tag": tk.StringVar(
                    master=self.root,
                    value=_t("channels.read_only", locale=self.locale),
                ),
            }
            self.channel_card_vars.append(variables)
            card = self._panel(grid, bg="card")
            card.grid(row=0, column=index, sticky="nsew", padx=3)
            self._label(
                card,
                textvariable=variables["name"],
                size=11,
                weight="bold",
                bg="card",
            ).pack(
                anchor="w", padx=14, pady=(14, 12)
            )
            for key, variable in (
                ("channels.co2", variables["co2"]),
                ("channels.h2o", variables["h2o"]),
            ):
                self._label(card, _t(key, locale=self.locale), size=9, color="muted", bg="card").pack(
                    anchor="w", padx=14
                )
                self._label(
                    card,
                    textvariable=variable,
                    size=12,
                    bg="card",
                ).pack(anchor="w", padx=14, pady=(0, 10))
            tk.Frame(card, bg=_COLORS["border"], height=1).pack(fill="x", padx=14, pady=5)
            for key, variable in (
                ("channels.status", variables["status"]),
                ("channels.trend", variables["trend"]),
                ("channels.note", variables["note"]),
            ):
                row = tk.Frame(card, bg=_COLORS["card"])
                row.pack(fill="x", padx=14, pady=4)
                self._label(
                    row,
                    _t(key, locale=self.locale),
                    size=8,
                    color="muted",
                    bg="card",
                ).pack(side="left")
                self._label(
                    row,
                    textvariable=variable,
                    size=8,
                    color="green" if key == "channels.status" else "muted",
                    bg="card",
                ).pack(side="right")
            self._label(
                card,
                textvariable=variables["tag"],
                size=8,
                color="blue",
                bg="card",
            ).pack(side="bottom", anchor="w", padx=14, pady=14)

    def _build_evidence(self, main: tk.Frame) -> None:
        grid = tk.Frame(main, bg=_COLORS["bg"])
        grid.grid(row=3, column=0, sticky="ew")
        self.evidence_vars = {
            "temperature": tk.StringVar(
                master=self.root,
                value=_t("evidence.unknown", locale=self.locale),
            ),
            "reference": tk.StringVar(
                master=self.root,
                value=_t("evidence.unknown", locale=self.locale),
            ),
            "flow": tk.StringVar(
                master=self.root,
                value=_t("evidence.flow_unknown", locale=self.locale),
            ),
            "certificate": tk.StringVar(
                master=self.root,
                value=_t("evidence.release", locale=self.locale),
            ),
        }
        for index, (key, variable) in enumerate(
            (
                ("evidence.temperature", self.evidence_vars["temperature"]),
                ("evidence.reference", self.evidence_vars["reference"]),
                ("evidence.source", self.evidence_vars["flow"]),
                ("evidence.certificate", self.evidence_vars["certificate"]),
            )
        ):
            grid.grid_columnconfigure(index, weight=1, uniform="evidence")
            card = self._panel(grid, bg="card_alt")
            card.grid(row=0, column=index, sticky="nsew", padx=(0 if index == 0 else 4, 0))
            self._label(card, _t(key, locale=self.locale), size=9, weight="bold", bg="card_alt").pack(
                anchor="w", padx=12, pady=(10, 4)
            )
            value_label = self._label(
                card,
                textvariable=variable,
                size=8,
                color="muted",
                bg="card_alt",
            )
            value_label.configure(justify="left")
            value_label.pack(
                anchor="w", padx=12, pady=(0, 10)
            )

    @staticmethod
    def _format_reference_observation(
        observation: Mapping[str, Any],
        *,
        digits: int,
    ) -> str:
        value = observation.get("value")
        unit = str(observation.get("unit") or "")
        freshness = str(observation.get("freshness_status") or "unknown")
        if value is None:
            return f"-- {unit}｜{freshness}".strip()
        try:
            number = f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            number = str(value)
        return f"{number} {unit}｜{freshness}".strip()

    @staticmethod
    def _format_pressure_delta(pressure_chain: Mapping[str, Any]) -> str:
        value = pressure_chain.get("controller_minus_reference_hpa")
        if value is None:
            return "-- hPa"
        try:
            return f"{float(value):+.1f} hPa"
        except (TypeError, ValueError):
            return f"{value} hPa"

    @staticmethod
    def _format_pressure_chain_status(
        pressure_chain: Mapping[str, Any],
        *,
        locale: str = "zh_CN",
    ) -> str:
        status = str(pressure_chain.get("status") or "unknown")
        key = {
            "fresh_coincident_observation": "evidence.pressure_chain_ready",
            "reference_missing": "evidence.pressure_chain_reference_missing",
            "controller_feedback_missing": (
                "evidence.pressure_chain_controller_missing"
            ),
            "coincident_pair_missing": "evidence.pressure_chain_pair_missing",
            "pair_timestamp_missing": "evidence.pressure_chain_timing_invalid",
            "pair_age_unknown": "evidence.pressure_chain_timing_invalid",
            "pair_not_coincident": "evidence.pressure_chain_timing_invalid",
            "stale_observation": "evidence.pressure_chain_stale",
            "pair_stale": "evidence.pressure_chain_stale",
            "reference_not_configured": (
                "evidence.pressure_chain_reference_unconfigured"
            ),
            "controller_not_configured": (
                "evidence.pressure_chain_controller_unconfigured"
            ),
        }.get(status, "evidence.pressure_chain_unknown")
        return _t(key, locale=locale)

    def _refresh_run_readback(self, snapshot: Mapping[str, Any]) -> None:
        """Render only normalized artifacts; never perform a hardware refresh."""

        devices = dict(snapshot.get("devices") or {})
        channels = [
            dict(item) for item in devices.get("channels") or ()
            if isinstance(item, Mapping)
        ]
        freshness = dict(devices.get("runtime_freshness") or {})
        freshness_status = str(freshness.get("status") or "unknown")
        for index, variables in enumerate(self.channel_card_vars):
            if index >= len(channels):
                variables["name"].set(f"通道 {index + 1:02d}")
                variables["co2"].set("--")
                variables["h2o"].set("--")
                variables["status"].set(
                    _t("channels.no_artifact", locale=self.locale)
                )
                variables["trend"].set("—")
                variables["note"].set("freshness = unknown")
                continue
            row = channels[index]
            variables["name"].set(
                str(row.get("display_name") or f"通道 {index + 1:02d}")
            )
            variables["co2"].set("--")
            variables["h2o"].set("--")
            variables["status"].set(
                str(row.get("connection_status") or "unknown")
            )
            variables["trend"].set(
                str(row.get("health_status") or "not_evaluated")
            )
            variables["note"].set(
                f"frame={row.get('last_frame_status') or 'unknown'}"
            )
            variables["tag"].set(
                _t("channels.read_only", locale=self.locale)
            )

        reference = dict(snapshot.get("physical_reference") or {})
        observations = dict(reference.get("observations") or {})
        temperature = dict(observations.get("temperature") or {})
        pressure = dict(observations.get("pressure") or {})
        pressure_controller = dict(
            observations.get("pressure_controller") or {}
        )
        pressure_chain = dict(reference.get("pressure_chain") or {})
        dewpoint = dict(observations.get("dewpoint") or {})
        flow = dict(observations.get("flow") or {})
        if freshness_status == "unknown":
            self.evidence_vars["temperature"].set(
                _t("evidence.unknown", locale=self.locale)
            )
            self.evidence_vars["reference"].set(
                _t("evidence.unknown", locale=self.locale)
            )
            self.evidence_vars["flow"].set(
                _t("evidence.flow_unknown", locale=self.locale)
            )
            return
        self.evidence_vars["temperature"].set(
            self._format_reference_observation(temperature, digits=2)
        )
        pressure_line = " · ".join(
            (
                (
                    f"{_t('evidence.pressure_gauge_short', locale=self.locale)} "
                    f"{self._format_reference_observation(pressure, digits=1)}"
                ),
                (
                    f"{_t('evidence.pressure_controller_short', locale=self.locale)} "
                    f"{self._format_reference_observation(pressure_controller, digits=1)}"
                ),
                (
                    f"{_t('evidence.pressure_delta_short', locale=self.locale)} "
                    f"{self._format_pressure_delta(pressure_chain)}"
                ),
            )
        )
        dewpoint_line = (
            f"{_t('evidence.dewpoint_short', locale=self.locale)} "
            f"{self._format_reference_observation(dewpoint, digits=2)}"
        )
        self.evidence_vars["reference"].set(
            (
                f"{pressure_line}\n{dewpoint_line}\n"
                f"{self._format_pressure_chain_status(pressure_chain, locale=self.locale)}"
            )
        )
        flow_text = self._format_reference_observation(flow, digits=2)
        self.evidence_vars["flow"].set(
            f"{flow_text}｜仅监测存在性与稳定性"
        )

    def _build_aside(self, body: tk.Frame) -> None:
        aside = tk.Frame(body, bg=_COLORS["bg"], width=294)
        self.aside_frame = aside
        aside.grid(row=0, column=2, sticky="nse", padx=(0, 14), pady=14)
        aside.grid_propagate(False)
        action = self._panel(aside)
        action.pack(fill="x", pady=(0, 12))
        self._section_title(action, "aside.next")
        self._label(action, _t("aside.heading", locale=self.locale), size=13, weight="bold").pack(
            anchor="w", padx=16, pady=(0, 10)
        )
        for key in ("aside.step1", "aside.step2", "aside.step3"):
            self._label(action, _t(key, locale=self.locale), size=9, color="muted").pack(
                anchor="w", padx=16, pady=3
            )
        self.start_button = ttk.Button(
            action,
            text=_t("aside.start", locale=self.locale),
            style="Primary.TButton",
            command=self.start_dry_run,
        )
        self.start_button.pack(fill="x", padx=16, pady=(16, 8))
        ttk.Button(
            action,
            text=_t("aside.settings", locale=self.locale),
            style="Secondary.TButton",
            command=self.open_settings,
        ).pack(fill="x", padx=16, pady=(0, 8))
        ttk.Button(
            action,
            text=_t("aside.handoff", locale=self.locale),
            style="Secondary.TButton",
            command=self.open_controlled_handoff_preview,
        ).pack(fill="x", padx=16, pady=(0, 8))
        ttk.Button(
            action,
            text=_t("aside.open", locale=self.locale),
            style="Secondary.TButton",
            command=self.open_output_directory,
        ).pack(fill="x", padx=16, pady=(0, 16))

        config_gate = self._panel(aside)
        config_gate.pack(fill="x", pady=(0, 12))
        self._section_title(config_gate, "config.title")
        self._label(
            config_gate,
            textvariable=self.config_gate_var,
            size=8,
            color="text",
            wraplength=250,
            justify="left",
        ).pack(anchor="w", padx=16, pady=(0, 6))
        self._label(
            config_gate,
            textvariable=self.config_hash_var,
            size=8,
            color="muted",
        ).pack(anchor="w", padx=16, pady=(0, 14))

        boundary = self._panel(aside)
        boundary.pack(fill="x")
        self._section_title(boundary, "boundary.title")
        for key, value_key in (
            ("boundary.com", "boundary.disabled"),
            ("boundary.route", "boundary.disabled"),
            ("boundary.write", "boundary.disabled"),
            ("boundary.acceptance", "boundary.no"),
        ):
            row = tk.Frame(boundary, bg=_COLORS["surface"])
            row.pack(fill="x", padx=16, pady=5)
            self._label(row, _t(key, locale=self.locale), size=9).pack(side="left")
            self._status_badge(
                row,
                _t(value_key, locale=self.locale),
                "green_dark",
                "green",
            ).pack(side="right")
        self._label(
            boundary,
            _t("boundary.note", locale=self.locale),
            size=8,
            color="muted",
        ).pack(anchor="w", padx=16, pady=(10, 16))

    def _build_footer(self) -> None:
        footer = tk.Frame(self.root, bg=_COLORS["nav"], height=38)
        footer.grid(row=2, column=0, sticky="ew")
        footer.grid_propagate(False)
        self._label(
            footer,
            textvariable=self.status_var,
            size=9,
            color="green",
            bg="nav",
        ).pack(side="left", padx=20, pady=9)
        self._label(
            footer,
            _t("footer.role", locale=self.locale),
            size=9,
            color="muted",
            bg="nav",
        ).pack(side="right", padx=20, pady=9)

    def open_settings(self) -> None:
        if self._settings_dialog is not None and self._settings_dialog.winfo_exists():
            self._settings_dialog.lift()
            return
        dialog = tk.Toplevel(self.root)
        self._settings_dialog = dialog
        dialog.title(_t("dialog.title", locale=self.locale))
        dialog.geometry("900x760")
        dialog.minsize(820, 700)
        dialog.configure(bg=_COLORS["surface"])
        dialog.transient(self.root)
        dialog.grab_set()
        rows = (
            ("config", "dialog.config", "file"),
            ("co2", "dialog.co2", "file"),
            ("h2o", "dialog.h2o", "file"),
            ("output", "dialog.output", "directory"),
            ("runtime", "dialog.runtime", "directory"),
            ("certificate", "dialog.certificate", "file"),
            ("authority_archive", "dialog.authority_archive", "file"),
            ("authority_run_id", "dialog.authority_run_id", None),
            ("authority_device_ids", "dialog.authority_device_ids", None),
            ("authority_operator", "dialog.authority_operator", None),
        )
        dialog.grid_columnconfigure(1, weight=1)
        for index, (setting, label_key, browse_mode) in enumerate(rows):
            self._label(dialog, _t(label_key, locale=self.locale), size=9).grid(
                row=index, column=0, sticky="w", padx=(18, 10), pady=10
            )
            ttk.Entry(dialog, textvariable=self.settings[setting]).grid(
                row=index, column=1, sticky="ew", pady=10
            )
            if browse_mode is not None:
                ttk.Button(
                    dialog,
                    text=_t("dialog.browse", locale=self.locale),
                    style="Secondary.TButton",
                    command=lambda key=setting, mode=browse_mode: self._browse(
                        key,
                        mode == "directory",
                    ),
                ).grid(row=index, column=2, padx=12, pady=8)
        self._label(
            dialog,
            textvariable=self.config_gate_var,
            size=8,
            color="text",
        ).grid(row=len(rows), column=0, columnspan=3, sticky="w", padx=18, pady=(8, 2))
        self._label(
            dialog,
            textvariable=self.config_hash_var,
            size=8,
            color="muted",
        ).grid(row=len(rows) + 1, column=0, columnspan=3, sticky="w", padx=18, pady=(0, 8))
        actions = tk.Frame(dialog, bg=_COLORS["surface"])
        actions.grid(row=len(rows) + 2, column=0, columnspan=3, sticky="e", padx=18, pady=14)
        ttk.Button(
            actions,
            text=_t("dialog.cancel", locale=self.locale),
            style="Secondary.TButton",
            command=dialog.destroy,
        ).pack(side="right", padx=(8, 0))
        ttk.Button(
            actions,
            text=_t("dialog.save", locale=self.locale),
            style="Primary.TButton",
            command=lambda: self._save_settings(dialog),
        ).pack(side="right")

    def _browse(self, key: str, directory: bool) -> None:
        selected = (
            filedialog.askdirectory(parent=self._settings_dialog)
            if directory
            else filedialog.askopenfilename(parent=self._settings_dialog)
        )
        if selected:
            self.settings[key].set(selected)
            if key == "config":
                self._refresh_config_gate()

    def _save_settings(self, dialog: tk.Toplevel) -> None:
        self._refresh_config_gate()
        dialog.destroy()

    def _refresh_config_gate(self, inspection: Mapping[str, Any] | None = None) -> None:
        current = dict(
            inspection
            or inspect_v1_5_runtime_config(self.settings["config"].get())
        )
        devices = current.get("pressure_devices", {})
        controller = str(
            (devices.get("pressure_controller") or {}).get("runtime_port") or "--"
        )
        gauge = str(
            (devices.get("pressure_gauge") or {}).get("runtime_port") or "--"
        )
        status = str(current.get("status") or "blocked")
        if status == "ready_bound_runtime_config":
            key = "config.bound"
        elif status == "ready_static_runtime_config":
            key = "config.static"
        else:
            key = "config.blocked"
        self.config_gate_var.set(
            _t(
                key,
                locale=self.locale,
                controller=controller,
                gauge=gauge,
            )
        )
        sha = str(current.get("sha256") or "")
        self.config_hash_var.set(
            _t("config.hash", locale=self.locale, sha=sha[:12] if sha else "--")
        )

    def _plan(self) -> dict[str, Any]:
        run_id = f"operator_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        certificate = self.settings["certificate"].get().strip() or None
        authority_archive = (
            self.settings["authority_archive"].get().strip() or None
        )
        authority_run_id = self.settings["authority_run_id"].get().strip() or None
        authority_device_ids = (
            self.settings["authority_device_ids"].get().strip() or None
        )
        plan = build_v1_5_operator_workstation_plan(
            config_path=self.settings["config"].get(),
            co2_queue_csv=self.settings["co2"].get(),
            h2o_queue_csv=self.settings["h2o"].get(),
            output_dir=self.settings["output"].get(),
            run_id=run_id,
            certificate_registry_json=certificate,
            decision_authority_archive_json=authority_archive,
            expected_authority_run_id=authority_run_id,
            expected_authority_device_ids=authority_device_ids,
        )
        self._refresh_config_gate(plan.get("runtime_config_inspection"))
        return plan

    def _controlled_handoff_preview_text(
        self,
        handoff: Mapping[str, Any],
        decision_model: Mapping[str, Any] | None = None,
        authority_binding: Mapping[str, Any] | None = None,
    ) -> str:
        status_key = (
            "handoff.status.gate_blocked"
            if handoff.get("status") == "blocked_by_startup_gate"
            else "handoff.status.pending"
        )
        lines = [
            _t(status_key, locale=self.locale),
            _t("handoff.safety", locale=self.locale),
            _t("handoff.conditional", locale=self.locale),
            _t(
                "handoff.hash",
                locale=self.locale,
                sha=str(handoff.get("runtime_config_sha256") or "--"),
            ),
        ]
        blockers = list(handoff.get("blockers") or [])
        if blockers:
            lines.extend(["", *[f"- {reason}" for reason in blockers]])
        authority = dict(authority_binding or {})
        if authority:
            authority_status = str(authority.get("status") or "blocked")
            archive = dict(authority.get("archive_index") or {})
            artifacts = dict(authority.get("artifacts") or {})
            formal = dict(artifacts.get("formal_run_status") or {})
            report = dict(artifacts.get("report_model") or {})
            lines.append(
                _t(
                    "handoff.authority",
                    locale=self.locale,
                    status=_t(
                        f"handoff.authority.{authority_status}",
                        locale=self.locale,
                    ),
                    index_sha=str(archive.get("sha256") or "--")[:12],
                    formal_sha=str(formal.get("actual_sha256") or "--")[:12],
                    report_sha=str(report.get("actual_sha256") or "--")[:12],
                )
            )
            identity = dict(authority.get("identity_binding") or {})
            identity_status = str(identity.get("status") or "blocked")
            expected = dict(identity.get("expected") or {})
            observed = dict(identity.get("observed") or {})
            observed_run_ids = dict(observed.get("run_ids") or {})
            display_run_id = str(
                expected.get("run_id")
                or observed_run_ids.get("archive_index")
                or "--"
            )
            display_devices = list(expected.get("device_ids") or [])
            display_config_sha = str(
                expected.get("runtime_config_sha256")
                or observed.get("runtime_config_sha256")
                or "--"
            )
            lines.append(
                _t(
                    "handoff.identity",
                    locale=self.locale,
                    status=_t(
                        f"handoff.identity.{identity_status}",
                        locale=self.locale,
                    ),
                    run_id=display_run_id,
                    device_ids=",".join(str(item) for item in display_devices)
                    or "--",
                    config_sha=display_config_sha[:12],
                )
            )
        decisions = dict((decision_model or {}).get("decisions") or {})
        for decision_key in (
            "start_simulation",
            "start_real_execution",
            "write_coefficients",
            "issue_formal_certificate",
        ):
            decision = dict(decisions.get(decision_key) or {})
            if not decision:
                continue
            reason_key = "reasons_zh" if self.locale == "zh_CN" else "reasons_en"
            lines.append(
                _t(
                    "handoff.decision",
                    locale=self.locale,
                    label=_t(
                        f"handoff.decision.{decision_key}",
                        locale=self.locale,
                    ),
                    status=_t(
                        f"handoff.decision.{decision.get('status') or 'blocked'}",
                        locale=self.locale,
                    ),
                    reasons="；".join(
                        str(reason) for reason in decision.get(reason_key) or []
                    )
                    or "--",
                )
            )
        for scope in handoff.get("available_scopes") or []:
            label_key = "label_zh" if self.locale == "zh_CN" else "label_en"
            label = str(scope.get(label_key) or scope.get("scope_id") or "--")
            scope_status = str(scope.get("status") or "blocked_by_startup_gate")
            status = _t(
                f"handoff.scope.{scope_status}",
                locale=self.locale,
            )
            lines.extend(
                [
                    "",
                    _t(
                        "handoff.scope",
                        locale=self.locale,
                        scope=label,
                        status=status,
                    ),
                ]
            )
            if scope.get("scope_id") == "response_only":
                lines.append(_t("handoff.response_boundary", locale=self.locale))
        for command in handoff.get("commands") or []:
            route_kind = str(command.get("route_kind") or "")
            route_key = "route.co2" if route_kind == "co2" else "route.h2o"
            lines.extend(
                [
                    "",
                    _t(
                        "handoff.command",
                        locale=self.locale,
                        route=_t(route_key, locale=self.locale),
                    ),
                    str(command.get("runner_module") or ""),
                    json.dumps(
                        list(command.get("argv_template") or []),
                        ensure_ascii=False,
                        indent=2,
                    ),
                ]
            )
        return "\n".join(lines)

    def run_controlled_route_preflight(self) -> dict[str, Any]:
        """Validate one mature route while keeping execution unconditionally off."""

        route_kind = "h2o" if self.controlled_route_var.get() == "H₂O" else "co2"
        plan: dict[str, Any] | None = None
        route_label = _t(
            "route.h2o" if route_kind == "h2o" else "route.co2",
            locale=self.locale,
        )
        try:
            plan = self._plan()
            result = preflight_v1_5_controlled_mature_route(
                plan,
                route_kind=route_kind,
            )
        except Exception as exc:  # pragma: no cover - defensive UI boundary
            result = {
                "overall_status": "blocked",
                "status": "preflight_exception",
                "blockers": [f"{type(exc).__name__}: {exc}"],
                "execution_started": False,
                "execution_allowed": False,
                "runner_invocation_count": 0,
            }
        self.last_controlled_preflight = result
        self._last_controlled_preflight_plan = plan
        ready = (
            result.get("status") == "preflight_ready_execution_locked"
            and result.get("execution_started") is False
            and result.get("execution_allowed") is False
            and int(result.get("runner_invocation_count") or 0) == 0
        )
        if ready:
            text = _t(
                "handoff.preflight.ready",
                locale=self.locale,
                route=route_label,
            )
        else:
            reasons = list(result.get("blockers") or []) or [
                "preflight_result_not_execution_locked"
            ]
            text = _t(
                "handoff.preflight.blocked",
                locale=self.locale,
                route=route_label,
                reasons="；".join(str(reason) for reason in reasons),
            )
        self.controlled_preflight_var.set(text)
        if self.controlled_preflight_receipt_button is not None:
            self.controlled_preflight_receipt_button.state(
                ["!disabled"] if plan is not None else ["disabled"]
            )
        return result

    def save_controlled_route_preflight_receipt(self) -> dict[str, Any] | None:
        """Save the last offline preflight as an immutable local audit record."""

        plan = self._last_controlled_preflight_plan
        preflight = self.last_controlled_preflight
        if plan is None or preflight is None:
            messagebox.showerror(
                _t("handoff.preflight.receipt_title", locale=self.locale),
                _t("handoff.preflight.receipt_missing", locale=self.locale),
                parent=self._handoff_dialog or self.root,
            )
            return None
        route_kind = str(preflight.get("route_kind") or "route")
        run_id = re.sub(
            r"[^A-Za-z0-9_.-]+",
            "_",
            str(plan.get("run_id") or "preflight"),
        ).strip("_")
        output_path = filedialog.asksaveasfilename(
            parent=self._handoff_dialog or self.root,
            title=_t("handoff.preflight.receipt_title", locale=self.locale),
            defaultextension=".json",
            filetypes=(("JSON", "*.json"), ("All files", "*.*")),
            initialdir=self.settings["output"].get(),
            initialfile=(
                f"v1_5_{route_kind}_preflight_{run_id or 'preflight'}.json"
            ),
        )
        if not output_path:
            return None
        try:
            written = write_v1_5_controlled_route_preflight_receipt(
                plan,
                preflight,
                output_path,
            )
        except OSError as exc:
            messagebox.showerror(
                _t("handoff.preflight.receipt_title", locale=self.locale),
                _t(
                    "handoff.preflight.receipt_failed",
                    locale=self.locale,
                    reason=f"{type(exc).__name__}: {exc}",
                ),
                parent=self._handoff_dialog or self.root,
            )
            return None
        messagebox.showinfo(
            _t("handoff.preflight.receipt_title", locale=self.locale),
            _t(
                "handoff.preflight.receipt_saved",
                locale=self.locale,
                path=written["path"],
                sha=written["sha256"],
                status=written["status"],
            ),
            parent=self._handoff_dialog or self.root,
        )
        return written

    def open_controlled_handoff_preview(self) -> None:
        if self._handoff_dialog is not None and self._handoff_dialog.winfo_exists():
            self._handoff_dialog.lift()
            return
        plan = self._plan()
        handoff = dict(plan.get("controlled_execution_handoff") or {})
        dialog = tk.Toplevel(self.root)
        self._handoff_dialog = dialog
        dialog.title(_t("handoff.title", locale=self.locale))
        dialog.geometry("980x720")
        dialog.minsize(760, 600)
        dialog.configure(bg=_COLORS["surface"])
        dialog.transient(self.root)

        self._label(
            dialog,
            _t("handoff.heading", locale=self.locale),
            size=16,
            weight="bold",
        ).pack(anchor="w", padx=20, pady=(18, 4))
        self._label(
            dialog,
            _t("handoff.notice", locale=self.locale),
            size=9,
            color="amber",
            wraplength=920,
            justify="left",
        ).pack(anchor="w", padx=20, pady=(0, 12))

        preview_frame = tk.Frame(dialog, bg=_COLORS["surface"])
        preview_frame.pack(fill="both", expand=True, padx=20, pady=(0, 12))
        preview = tk.Text(
            preview_frame,
            bg=_COLORS["card"],
            fg=_COLORS["text"],
            insertbackground=_COLORS["text"],
            selectbackground=_COLORS["blue_dark"],
            relief="flat",
            borderwidth=0,
            font=("Cascadia Mono", 9),
            padx=14,
            pady=12,
            wrap="word",
        )
        preview_scrollbar = ttk.Scrollbar(
            preview_frame,
            orient="vertical",
            command=preview.yview,
        )
        preview.configure(yscrollcommand=preview_scrollbar.set)
        preview.insert(
            "1.0",
            self._controlled_handoff_preview_text(
                handoff,
                dict(plan.get("decision_model") or {}),
                dict(plan.get("decision_authority_binding") or {}),
            ),
        )
        preview.configure(state="disabled")
        preview.pack(side="left", fill="both", expand=True)
        preview_scrollbar.pack(side="right", fill="y")
        self._handoff_preview_widget = preview

        preflight = self._panel(dialog)
        preflight.pack(fill="x", padx=20, pady=(0, 12))
        self._label(
            preflight,
            _t("handoff.preflight.title", locale=self.locale),
            size=11,
            weight="bold",
        ).grid(row=0, column=0, columnspan=4, sticky="w", padx=14, pady=(12, 4))
        self._label(
            preflight,
            _t("handoff.preflight.route", locale=self.locale),
            size=9,
        ).grid(row=1, column=0, sticky="w", padx=(14, 8), pady=6)
        self.controlled_route_selector = ttk.Combobox(
            preflight,
            textvariable=self.controlled_route_var,
            values=("CO₂", "H₂O"),
            state="readonly",
            width=10,
            style="Site.TCombobox",
        )
        self.controlled_route_selector.grid(row=1, column=1, sticky="w", pady=6)
        self.controlled_preflight_button = ttk.Button(
            preflight,
            text=_t("handoff.preflight.run", locale=self.locale),
            style="Accent.TButton",
            command=self.run_controlled_route_preflight,
        )
        self.controlled_preflight_button.grid(
            row=1,
            column=2,
            sticky="e",
            padx=14,
            pady=6,
        )
        self.controlled_preflight_receipt_button = ttk.Button(
            preflight,
            text=_t("handoff.preflight.save", locale=self.locale),
            style="Secondary.TButton",
            command=self.save_controlled_route_preflight_receipt,
        )
        self.controlled_preflight_receipt_button.state(["disabled"])
        self.controlled_preflight_receipt_button.grid(
            row=1,
            column=3,
            sticky="e",
            padx=(0, 14),
            pady=6,
        )
        preflight.grid_columnconfigure(2, weight=1)
        self._label(
            preflight,
            textvariable=self.controlled_preflight_var,
            size=9,
            color="amber",
            wraplength=900,
            justify="left",
        ).grid(row=2, column=0, columnspan=4, sticky="w", padx=14, pady=(4, 2))
        self._label(
            preflight,
            _t("handoff.preflight.note", locale=self.locale),
            size=8,
            color="muted",
            wraplength=900,
            justify="left",
        ).grid(row=3, column=0, columnspan=4, sticky="w", padx=14, pady=(2, 12))

        actions = tk.Frame(dialog, bg=_COLORS["surface"])
        actions.pack(fill="x", padx=20, pady=(0, 18))
        ttk.Button(
            actions,
            text=_t("handoff.save_confirmation", locale=self.locale),
            style="Primary.TButton",
            command=self.save_authority_confirmation_receipt,
        ).pack(side="left")
        ttk.Button(
            actions,
            text=_t("handoff.close", locale=self.locale),
            style="Secondary.TButton",
            command=dialog.destroy,
        ).pack(side="right")

    def save_authority_confirmation_receipt(self) -> None:
        """Save an immutable archive-selection receipt after explicit confirmation."""

        plan = self._plan()
        binding = dict(plan.get("decision_authority_binding") or {})
        identity = dict(binding.get("identity_binding") or {})
        if binding.get("status") != "ready" or identity.get("status") != "ready":
            reasons = list(binding.get("blockers") or []) or [
                "decision_authority_binding_not_ready"
            ]
            messagebox.showerror(
                _t("handoff.confirmation_title", locale=self.locale),
                _t(
                    "handoff.confirmation_binding_blocked",
                    locale=self.locale,
                    reasons="\n".join(str(item) for item in reasons),
                ),
                parent=self._handoff_dialog or self.root,
            )
            return
        operator_name = self.settings["authority_operator"].get().strip()
        if not operator_name:
            messagebox.showerror(
                _t("handoff.confirmation_title", locale=self.locale),
                _t("handoff.confirmation_operator_missing", locale=self.locale),
                parent=self._handoff_dialog or self.root,
            )
            return
        confirmed = messagebox.askyesno(
            _t("handoff.confirmation_title", locale=self.locale),
            _t("handoff.confirmation_prompt", locale=self.locale),
            parent=self._handoff_dialog or self.root,
        )
        if not confirmed:
            return
        expected_run_id = str(
            (identity.get("expected") or {}).get("run_id") or "batch"
        )
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", expected_run_id).strip("_")
        output_path = filedialog.asksaveasfilename(
            parent=self._handoff_dialog or self.root,
            title=_t("handoff.confirmation_title", locale=self.locale),
            defaultextension=".json",
            filetypes=(("JSON", "*.json"), ("All files", "*.*")),
            initialdir=self.settings["output"].get(),
            initialfile=(
                f"v1_5_archive_authority_confirmation_{safe_run_id or 'batch'}.json"
            ),
        )
        if not output_path:
            return
        try:
            result = write_v1_5_archive_authority_confirmation_receipt(
                plan,
                output_path,
                operator_name=operator_name,
                confirmation_text=V1_5_ARCHIVE_AUTHORITY_CONFIRMATION_TEXT,
            )
        except OSError as exc:
            messagebox.showerror(
                _t("handoff.confirmation_title", locale=self.locale),
                _t(
                    "handoff.confirmation_failed",
                    locale=self.locale,
                    reason=f"{type(exc).__name__}: {exc}",
                ),
                parent=self._handoff_dialog or self.root,
            )
            return
        if result.get("status") != "confirmed":
            messagebox.showerror(
                _t("handoff.confirmation_title", locale=self.locale),
                _t(
                    "handoff.confirmation_failed",
                    locale=self.locale,
                    reason=str(result.get("status") or "blocked"),
                ),
                parent=self._handoff_dialog or self.root,
            )
            return
        messagebox.showinfo(
            _t("handoff.confirmation_title", locale=self.locale),
            _t(
                "handoff.confirmation_saved",
                locale=self.locale,
                path=result["path"],
                sha=result["sha256"],
            ),
            parent=self._handoff_dialog or self.root,
        )

    def start_dry_run(self) -> None:
        plan = self._plan()
        if plan.get("blockers"):
            messagebox.showerror(
                _t("title", locale=self.locale),
                _t(
                    "error.blocked",
                    locale=self.locale,
                    reasons="\n".join(plan["blockers"]),
                ),
                parent=self.root,
            )
            return
        self.start_button.state(["disabled"])
        self.start_button.configure(text=_t("aside.running", locale=self.locale))
        self.status_var.set(_t("status.running", locale=self.locale))
        self.route_var.set("CO₂ → H₂O")
        self.point_var.set(_t("value.dry_run", locale=self.locale))
        self.judgement_var.set(_t("value.waiting", locale=self.locale))
        self.dwell_var.set("0 / 58")
        output_dir = self.settings["output"].get()

        def worker() -> None:
            try:
                result, _outputs = run_v1_5_operator_workstation_application(
                    plan,
                    output_dir=output_dir,
                    executor=self.executor,
                )
            except Exception as exc:  # pragma: no cover - UI safety net
                result = {
                    **plan,
                    "overall_status": "failed",
                    "execution_blockers": [f"{type(exc).__name__}: {exc}"],
                }
            self._result_queue.put(result)

        self.root.after(30, self._poll_dry_run_result)
        threading.Thread(target=worker, name="v1_5_operator_dry_run", daemon=True).start()

    def _poll_dry_run_result(self) -> None:
        try:
            result = self._result_queue.get_nowait()
        except queue.Empty:
            self.root.after(30, self._poll_dry_run_result)
            return
        self._finish_dry_run(result)

    def _finish_dry_run(self, result: dict[str, Any]) -> None:
        self.last_result = result
        self.refresh_workstation_snapshot()
        self.start_button.state(["!disabled"])
        self.start_button.configure(text=_t("aside.start", locale=self.locale))
        passed = result.get("overall_status") == "pass"
        self.status_var.set(
            _t("status.pass" if passed else "status.failed", locale=self.locale)
        )
        self.judgement_var.set("PASS" if passed else "FAILED")
        self.dwell_var.set("58 / 58" if passed else "-- / 58")
        if passed:
            messagebox.showinfo(
                _t("title", locale=self.locale),
                _t("info.pass", locale=self.locale),
                parent=self.root,
            )
        else:
            reasons = result.get("execution_blockers") or result.get("blockers") or ["unknown"]
            messagebox.showerror(
                _t("title", locale=self.locale),
                _t(
                    "error.failed",
                    locale=self.locale,
                    reasons="\n".join(str(item) for item in reasons),
                ),
                parent=self.root,
            )

    def open_output_directory(self) -> None:
        path = Path(self.settings["output"].get()).resolve()
        path.mkdir(parents=True, exist_ok=True)
        if hasattr(os, "startfile"):
            os.startfile(str(path))  # type: ignore[attr-defined]  # pragma: no cover


def build_application(
    *,
    root: tk.Tk | None = None,
    locale: str = "zh_CN",
    initial_settings: Mapping[str, str] | None = None,
) -> tuple[tk.Tk, OperatorWorkstationApp]:
    active_root = root or tk.Tk()
    return active_root, OperatorWorkstationApp(
        active_root,
        locale=locale,
        initial_settings=initial_settings,
    )


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch the V1.5 dry-run workstation with a fixed runtime configuration."
    )
    parser.add_argument("--config", default=None)
    parser.add_argument("--co2-queue-csv", default=None)
    parser.add_argument("--h2o-queue-csv", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--runtime-dir", default=None)
    parser.add_argument("--certificate", default=None)
    parser.add_argument(
        "--decision-authority-archive-json",
        default=None,
        help=(
            "Read an existing V1.5 formal archive closure index and bind its "
            "decision authorities by SHA-256."
        ),
    )
    parser.add_argument(
        "--expected-authority-run-id",
        default=None,
        help="Expected formal batch run ID; required when an authority archive is configured.",
    )
    parser.add_argument(
        "--expected-authority-device-ids",
        default=None,
        help=(
            "Comma-separated expected analyzer device IDs; required when an authority "
            "archive is configured."
        ),
    )
    parser.add_argument(
        "--startup-receipt-json",
        default=None,
        help=(
            "Write one immutable no-COM startup receipt. Existing files are never "
            "overwritten."
        ),
    )
    parser.add_argument(
        "--authority-confirmation-receipt-json",
        default=None,
        help=(
            "Write one immutable archive batch confirmation receipt. Existing "
            "files are never overwritten."
        ),
    )
    parser.add_argument(
        "--authority-confirmation-operator",
        default=None,
        help="Operator name recorded in the archive batch confirmation receipt.",
    )
    parser.add_argument(
        "--authority-confirmation-text",
        default=None,
        help="Exact acknowledgement required for a confirmed archive receipt.",
    )
    parser.add_argument(
        "--controlled-route-preflight-route",
        choices=("co2", "h2o"),
        default=None,
        help=(
            "Run one hash-bound CO2/H2O offline preflight and exit without "
            "constructing Tk or invoking a mature runner."
        ),
    )
    parser.add_argument(
        "--controlled-route-preflight-receipt-json",
        default=None,
        help=(
            "Write the selected route preflight as an immutable JSON receipt. "
            "Requires --controlled-route-preflight-route."
        ),
    )
    parser.add_argument(
        "--validate-startup-only",
        action="store_true",
        help="Validate config and 45/13 queues without constructing Tk or opening COM.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _initial_settings_from_args(args: argparse.Namespace) -> dict[str, str]:
    values = {
        "config": args.config,
        "co2": args.co2_queue_csv,
        "h2o": args.h2o_queue_csv,
        "output": args.output_dir,
        "runtime": args.runtime_dir,
        "certificate": args.certificate,
        "authority_archive": args.decision_authority_archive_json,
        "authority_run_id": args.expected_authority_run_id,
        "authority_device_ids": args.expected_authority_device_ids,
        "authority_operator": args.authority_confirmation_operator,
    }
    return {
        key: str(value)
        for key, value in values.items()
        if str(value or "").strip()
    }


def _startup_preflight(settings: Mapping[str, str]) -> dict[str, Any]:
    paths = _default_paths()
    paths.update(dict(settings))
    certificate = str(paths.get("certificate") or "").strip() or None
    authority_archive = str(paths.get("authority_archive") or "").strip() or None
    authority_run_id = str(paths.get("authority_run_id") or "").strip() or None
    authority_device_ids = (
        str(paths.get("authority_device_ids") or "").strip() or None
    )
    return build_v1_5_operator_workstation_plan(
        config_path=paths["config"],
        co2_queue_csv=paths["co2"],
        h2o_queue_csv=paths["h2o"],
        output_dir=paths["output"],
        run_id="v1_5_workstation_startup_preflight",
        certificate_registry_json=certificate,
        decision_authority_archive_json=authority_archive,
        expected_authority_run_id=authority_run_id,
        expected_authority_device_ids=authority_device_ids,
    )


def _print_receipt_write_error(
    *,
    receipt_key: str,
    output_path: str | Path,
    exc: OSError,
) -> None:
    print(
        json.dumps(
            {
                "status": "blocked",
                "blockers": [
                    f"{receipt_key}_write_failed:{type(exc).__name__}"
                ],
                receipt_key: {
                    "path": str(Path(output_path).resolve()),
                    "written": False,
                },
                "opens_com_ports": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        file=sys.stderr,
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    initial_settings = _initial_settings_from_args(args)
    fixed_start_requested = bool(
        initial_settings
        or args.validate_startup_only
        or args.startup_receipt_json
        or args.authority_confirmation_receipt_json
        or args.controlled_route_preflight_route
        or args.controlled_route_preflight_receipt_json
    )
    if fixed_start_requested:
        plan = _startup_preflight(initial_settings)
        receipt = None
        authority_confirmation_receipt = None
        controlled_route_preflight = None
        controlled_route_preflight_receipt = None
        argument_blockers: list[str] = []
        if str(args.startup_receipt_json or "").strip():
            try:
                receipt = write_v1_5_operator_workstation_startup_receipt(
                    plan,
                    args.startup_receipt_json,
                )
            except OSError as exc:
                _print_receipt_write_error(
                    receipt_key="startup_receipt",
                    output_path=args.startup_receipt_json,
                    exc=exc,
                )
                return 2
        if str(args.authority_confirmation_receipt_json or "").strip():
            try:
                authority_confirmation_receipt = (
                    write_v1_5_archive_authority_confirmation_receipt(
                        plan,
                        args.authority_confirmation_receipt_json,
                        operator_name=str(
                            args.authority_confirmation_operator or ""
                        ),
                        confirmation_text=str(args.authority_confirmation_text or ""),
                    )
                )
            except OSError as exc:
                _print_receipt_write_error(
                    receipt_key="authority_confirmation_receipt",
                    output_path=args.authority_confirmation_receipt_json,
                    exc=exc,
                )
                return 2
        controlled_route = str(
            args.controlled_route_preflight_route or ""
        ).strip()
        controlled_receipt_path = str(
            args.controlled_route_preflight_receipt_json or ""
        ).strip()
        if controlled_receipt_path and not controlled_route:
            argument_blockers.append(
                "controlled_route_preflight_receipt_requires_route"
            )
        if controlled_route:
            controlled_route_preflight = preflight_v1_5_controlled_mature_route(
                plan,
                route_kind=controlled_route,
            )
            if controlled_receipt_path:
                try:
                    controlled_route_preflight_receipt = (
                        write_v1_5_controlled_route_preflight_receipt(
                            plan,
                            controlled_route_preflight,
                            controlled_receipt_path,
                        )
                    )
                except OSError as exc:
                    _print_receipt_write_error(
                        receipt_key="controlled_route_preflight_receipt",
                        output_path=controlled_receipt_path,
                        exc=exc,
                    )
                    return 2
            if (
                controlled_route_preflight.get("status")
                != "preflight_ready_execution_locked"
            ):
                argument_blockers.extend(
                    f"controlled_route_preflight:{reason}"
                    for reason in controlled_route_preflight.get("blockers") or []
                )
                if not controlled_route_preflight.get("blockers"):
                    argument_blockers.append(
                        "controlled_route_preflight:not_execution_locked"
                    )
        summary_blockers = [
            *list(plan.get("blockers") or []),
            *argument_blockers,
        ]
        summary = {
            "status": (
                "blocked" if summary_blockers else plan.get("overall_status")
            ),
            "blockers": summary_blockers,
            "warnings": list(plan.get("warnings") or []),
            "runtime_config": plan.get("runtime_config"),
            "runtime_config_inspection": plan.get("runtime_config_inspection"),
            "point_counts": plan.get("point_counts"),
            "controlled_execution_handoff": plan.get(
                "controlled_execution_handoff"
            ),
            "decision_model": plan.get("decision_model"),
            "decision_authority_binding": plan.get(
                "decision_authority_binding"
            ),
            "opens_com_ports": False,
            "startup_receipt": receipt,
            "authority_confirmation_receipt": authority_confirmation_receipt,
            "controlled_route_preflight": controlled_route_preflight,
            "controlled_route_preflight_receipt": (
                controlled_route_preflight_receipt
            ),
        }
        if summary_blockers or (
            authority_confirmation_receipt is not None
            and authority_confirmation_receipt.get("status") != "confirmed"
        ):
            print(json.dumps(summary, ensure_ascii=False, indent=2), file=sys.stderr)
            return 2
        if args.validate_startup_only or controlled_route:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0

    root, _ = build_application(initial_settings=initial_settings)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "OperatorWorkstationApp",
    "build_application",
    "main",
]
