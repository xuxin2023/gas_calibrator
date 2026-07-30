"""Small V1.5-owned translation catalog for migrated product pages.

The catalog is deliberately independent from the legacy V2 UI package.  It
keeps Chinese as the product default while retaining an English fallback for
demonstration and review use.
"""

from __future__ import annotations

from typing import Any, Callable


_ZH_CN = {
    "pages.certificate_metrics.title": "计量证书指标中心",
    "pages.certificate_metrics.boundary": (
        "安全边界：这里填写和更新的是可审计的证书资料草稿，不会自动进入"
        "校准点表、拟合、系数写入、设备通信或正式数据库。提交后仍需独立复核。"
    ),
    "pages.certificate_metrics.records.title": "证书资料记录",
    "pages.certificate_metrics.records.count": "共 {count} 条记录",
    "pages.certificate_metrics.records.column.asset": "资产",
    "pages.certificate_metrics.records.column.certificate": "证书编号",
    "pages.certificate_metrics.records.column.value": "证书值",
    "pages.certificate_metrics.records.column.state": "复核状态",
    "pages.certificate_metrics.records.column.revision": "版本",
    "pages.certificate_metrics.form.title": "证书指标与溯源资料",
    "pages.certificate_metrics.field.asset_id": "资产编号 *",
    "pages.certificate_metrics.field.asset_name": "资产名称",
    "pages.certificate_metrics.field.asset_type": "资产类型",
    "pages.certificate_metrics.field.measurand": "被测量",
    "pages.certificate_metrics.field.certificate_id": "证书编号",
    "pages.certificate_metrics.field.certificate_version": "证书版本",
    "pages.certificate_metrics.field.nominal_value": "标称值",
    "pages.certificate_metrics.field.certified_value": "证书实际值",
    "pages.certificate_metrics.field.unit": "数值单位",
    "pages.certificate_metrics.field.standard_uncertainty": "标准不确定度",
    "pages.certificate_metrics.field.expanded_uncertainty": "扩展不确定度",
    "pages.certificate_metrics.field.coverage_factor": "包含因子 k",
    "pages.certificate_metrics.field.uncertainty_unit": "不确定度单位",
    "pages.certificate_metrics.field.cylinder_serial_number": "气瓶/设备序列号",
    "pages.certificate_metrics.field.manufacturer": "生产/校准机构",
    "pages.certificate_metrics.field.balance_gas": "平衡气",
    "pages.certificate_metrics.field.gas_matrix": "气体基体",
    "pages.certificate_metrics.field.preparation_method": "制备方法",
    "pages.certificate_metrics.field.issue_date": "签发日期 YYYY-MM-DD",
    "pages.certificate_metrics.field.valid_from": "有效期开始",
    "pages.certificate_metrics.field.valid_until": "有效期截止",
    "pages.certificate_metrics.field.traceability_chain": "溯源链",
    "pages.certificate_metrics.field.evidence_file_path": "证书文件路径",
    "pages.certificate_metrics.field.evidence_file_sha256": "文件 SHA-256",
    "pages.certificate_metrics.field.notes": "备注",
    "pages.certificate_metrics.actions.new": "新建证书记录",
    "pages.certificate_metrics.actions.save_draft": "保存草稿",
    "pages.certificate_metrics.actions.submit_review": "提交独立复核",
    "pages.certificate_metrics.actions.reload": "重新载入",
    "pages.certificate_metrics.review_state.draft": "草稿",
    "pages.certificate_metrics.review_state.pending_review": "待复核",
    "pages.certificate_metrics.review_state.reviewed": "已复核",
    "pages.certificate_metrics.review_state.rejected": "已退回",
    "pages.certificate_metrics.status.ready": "证书资料层已就绪。",
    "pages.certificate_metrics.status.loaded": "已载入 {count} 条证书资料。",
    "pages.certificate_metrics.status.load_failed": "载入失败：{message}",
    "pages.certificate_metrics.status.new_record": "正在填写新的证书资料草稿。",
    "pages.certificate_metrics.status.saved": (
        "草稿已保存，版本 {revision}；尚未接入校准。"
    ),
    "pages.certificate_metrics.status.submitted": (
        "已提交独立复核，版本 {revision}；尚未接入校准。"
    ),
    "pages.certificate_metrics.status.selected": "已选择版本 {revision}。",
    "pages.certificate_metrics.status.validation_failed": "资料校验未通过：{message}",
    "pages.certificate_metrics.status.save_failed": "保存失败：{message}",
    "pages.certificate_metrics.validation.title": "证书资料校验",
    "pages.readonly.results.title": "校准结果摘要",
    "pages.readonly.results.boundary": (
        "只读结果页：仅显示 V1.5 工作站已经产生的仿真演练结果；"
        "不执行拟合选择、不写系数、不控制设备。"
    ),
    "pages.readonly.reports.title": "报告与工件摘要",
    "pages.readonly.reports.boundary": (
        "只读报告页：工件角色和导出状态来自统一快照；"
        "不在此页面生成正式证书或作出发布批准。"
    ),
    "pages.readonly.review.title": "审核与安全摘要",
    "pages.readonly.review.boundary": (
        "只读审核页：汇总证书、安全边界和人工复核下一步；"
        "没有批准、设备控制或系数写入动作。"
    ),
    "pages.readonly.plan.title": "成熟校准计划预览",
    "pages.readonly.plan.boundary": (
        "只读计划页：固定预览 V1.5 成熟 45/13 点队列及执行边界；"
        "不允许编辑点表、改写流程或绕过成熟运行器。"
    ),
    "pages.readonly.qc.title": "仿真质量控制摘要",
    "pages.readonly.qc.boundary": (
        "只读质控页：仅核验当前工作站快照中的点数、物理锚点、"
        "无写入边界和干跑闭环；真实样本稳定性与设备回读不作虚假判定。"
    ),
    "pages.readonly.devices.title": "设备身份与健康摘要",
    "pages.readonly.devices.boundary": (
        "只读设备页：六个位置仅代表 V1.5 配置通道槽位，不代表当前在线真机；"
        "不扫描端口、不连接设备，也不提供设备控制。"
    ),
    "pages.readonly.algorithm.title": "生产算法与候选边界",
    "pages.readonly.algorithm.boundary": (
        "只读算法页：生产默认配置锁定为成熟 legacy ratio；"
        "吸收比算法仅作离线候选展示，不自动选优、不切换生产配置。"
    ),
    "pages.readonly.metric.status": "运行状态",
    "pages.readonly.metric.co2_points": "CO₂ 点数",
    "pages.readonly.metric.h2o_points": "H₂O 点数",
    "pages.readonly.metric.routes": "已执行路径",
    "pages.readonly.metric.artifacts": "登记工件",
    "pages.readonly.metric.present": "已生成",
    "pages.readonly.metric.roles": "工件角色",
    "pages.readonly.metric.review_status": "审核状态",
    "pages.readonly.metric.safety": "安全边界",
    "pages.readonly.metric.certificates": "证书记录",
    "pages.readonly.metric.release": "正式放行",
    "pages.readonly.metric.profile": "成熟配置",
    "pages.readonly.metric.total_points": "计划总点数",
    "pages.readonly.metric.qc_status": "质控状态",
    "pages.readonly.metric.point_contract": "45/13 契约",
    "pages.readonly.metric.route_closure": "干跑闭环",
    "pages.readonly.metric.real_samples": "真实样本",
    "pages.readonly.metric.device_status": "设备模式",
    "pages.readonly.metric.configured_channels": "配置通道",
    "pages.readonly.metric.connected_channels": "当前连接",
    "pages.readonly.metric.unknown_health": "健康未评估",
    "pages.readonly.metric.algorithm_status": "算法状态",
    "pages.readonly.metric.production_profile": "生产配置",
    "pages.readonly.metric.algorithm_mode": "拟合输入",
    "pages.readonly.metric.shadow_candidates": "离线候选",
    "pages.readonly.section.routes": "路径结果",
    "pages.readonly.section.anchors": "物理锚点",
    "pages.readonly.section.warnings": "非阻断提醒",
    "pages.readonly.section.blockers": "阻断项",
    "pages.readonly.section.artifacts": "工件清单",
    "pages.readonly.section.roles": "允许的工件角色",
    "pages.readonly.section.evidence": "证据口径",
    "pages.readonly.section.report_boundary": "报告边界",
    "pages.readonly.section.safety": "安全状态",
    "pages.readonly.section.certificate": "证书状态",
    "pages.readonly.section.release": "发布状态",
    "pages.readonly.section.next_actions": "下一步审核",
    "pages.readonly.section.plan_routes": "成熟路径预览",
    "pages.readonly.section.plan_boundary": "计划边界",
    "pages.readonly.section.plan_certificate": "证书启动策略",
    "pages.readonly.section.qc_checks": "质控检查项",
    "pages.readonly.section.qc_point_evidence": "点级证据合同",
    "pages.readonly.section.qc_rule_governance": "规则与阈值治理",
    "pages.readonly.section.qc_boundary": "质控证据边界",
    "pages.readonly.section.device_channels": "通道槽位",
    "pages.readonly.section.device_identity": "身份判定",
    "pages.readonly.section.device_health": "健康与数据",
    "pages.readonly.section.device_boundary": "设备安全边界",
    "pages.readonly.section.production_profile": "成熟生产配置",
    "pages.readonly.section.shadow_candidates": "离线候选配置",
    "pages.readonly.section.physical_contract": "物理校准合同",
    "pages.readonly.section.algorithm_boundary": "算法治理边界",
    "pages.readonly.value.not_started": "尚未执行 V1.5 仿真演练。",
    "pages.readonly.value.none": "无",
    "pages.readonly.value.route": "{route}：{status}，{count} 点",
    "pages.readonly.value.co2_anchor": "CO₂：零气锚点（独立物理证据）",
    "pages.readonly.value.h2o_anchor": "H₂O：干气锚点（独立物理证据）",
    "pages.readonly.value.artifact": "{name}｜{role}｜{status}",
    "pages.readonly.value.simulated_evidence": (
        "evidence_source = simulated"
    ),
    "pages.readonly.value.no_paths": "快照不包含端口、序列号或本地文件路径。",
    "pages.readonly.value.report_authority": "报告工件权威来源：{authority}",
    "pages.readonly.value.report_statuses": "统一导出状态：{statuses}",
    "pages.readonly.value.report_read_only": "本页不执行导出、签发或批准。",
    "pages.readonly.value.report_release": "正式签发状态：{status}；仍需独立人工复核。",
    "pages.readonly.value.not_acceptance": "不构成真机验收证据。",
    "pages.readonly.value.safety": "安全检查：{status}",
    "pages.readonly.value.no_device_actions": (
        "无 COM、无气路控制、无设备 ID/SENCO 写入。"
    ),
    "pages.readonly.value.certificate": "证书记录 {count} 条；启动门禁 {gate}。",
    "pages.readonly.value.certificate_isolated": (
        "证书草稿未连接校准输入，正式签发仍需独立复核。"
    ),
    "pages.readonly.value.no_approval": "工作站当前不提供批准动作。",
    "pages.readonly.value.not_released": "未放行",
    "pages.readonly.value.plan_route": (
        "{route}：{status}，{count} 点，{mode}"
    ),
    "pages.readonly.value.plan_read_only": (
        "计划只读；点表和成熟 45/13 队列不可在本页修改。"
    ),
    "pages.readonly.value.plan_dry_run_only": (
        "execution_mode = mature_runner_dry_run"
    ),
    "pages.readonly.value.plan_certificate": (
        "证书资料启动门禁：{gate}；不阻断仿真演练。"
    ),
    "pages.readonly.value.qc_check": "{check}：{status}",
    "pages.readonly.value.qc_dry_run_only": (
        "质控结论只覆盖成熟运行器的仿真干跑证据。"
    ),
    "pages.readonly.value.qc_not_evaluated": (
        "样本稳定性和真实设备回读保持 not_evaluated。"
    ),
    "pages.readonly.value.qc_point_status": (
        "点级判定：{status}；唯一权威来源：{authority}。"
    ),
    "pages.readonly.value.qc_point_roles": "工件角色：{roles}",
    "pages.readonly.value.qc_point_fields": "必需追溯字段：{fields}",
    "pages.readonly.value.qc_rule_status": (
        "规则状态：{status}；来源：{source}。"
    ),
    "pages.readonly.value.qc_rule_ui_edit": "UI 可编辑：{allowed}",
    "pages.readonly.value.qc_reject_status": (
        "拒绝原因汇总：{status}；来源工件角色：{role}。"
    ),
    "pages.readonly.value.device_channel": (
        "{channel}：连接 {connection}；身份 {identity}；健康 {health}"
    ),
    "pages.readonly.value.device_slots_not_devices": (
        "六个槽位来自工作站通道合同，不等同于六台设备当前在线。"
    ),
    "pages.readonly.value.device_identity_not_evaluated": (
        "未读取序列号、设备ID或固件身份，identity_status = not_evaluated。"
    ),
    "pages.readonly.value.device_health_not_evaluated": (
        "未采集真实诊断数据，health_status = not_evaluated。"
    ),
    "pages.readonly.value.device_frames_not_evaluated": (
        "未读取实时帧，last_frame_status = not_evaluated。"
    ),
    "pages.readonly.value.device_simulation_only": (
        "mode = simulation_only；connected_count = 0。"
    ),
    "pages.readonly.value.device_runtime_authority": (
        "真实运行状态权威来源：{authority}；本页不扫描 COM。"
    ),
    "pages.readonly.value.device_initialization_contract": (
        "初始化合同：{owner} 负责 {mode}、{rate} Hz 上传及 "
        "{temperature}；必须保留中性化与读回证据。"
    ),
    "pages.readonly.value.device_no_v2_workbench": (
        "V2 仿真预设、故障注入和第二套设备状态源不进入 V1.5 产品。"
    ),
    "pages.readonly.value.production_profile": (
        "{profile}｜{mode}｜{review}"
    ),
    "pages.readonly.value.production_45_13": (
        "生产点数合同固定为 CO₂ 45点 + H₂O 13点。"
    ),
    "pages.readonly.value.shadow_candidate": (
        "{profile}｜{mode}｜promotion_state = {state}"
    ),
    "pages.readonly.value.pressure_contract": "压力顺序：{value}",
    "pages.readonly.value.temperature_contract": "温度系数：{value}",
    "pages.readonly.value.algorithm_no_auto_select": (
        "auto_select = false；不根据单次指标自动选出生产算法。"
    ),
    "pages.readonly.value.algorithm_no_switch": (
        "无生产配置切换、拟合执行或系数写入动作。"
    ),
    "pages.visitor_showcase.eyebrow": "INTELLIGENT METROLOGY · 智能计量",
    "pages.visitor_showcase.title": "V1.5 气体分析仪智能校准中心",
    "pages.visitor_showcase.subtitle": (
        "从标准气体与环境真值，到多通道同步采样、质量判定和可追溯证据归档"
    ),
    "pages.visitor_showcase.badge.simulated": "仿真展示基线",
    "pages.visitor_showcase.badge.read_only": "只读参观模式",
    "pages.visitor_showcase.badge.not_acceptance": "非真机验收证据",
    "pages.visitor_showcase.metric.calibration_points": "成熟 CO₂ / H₂O 校准点",
    "pages.visitor_showcase.metric.analyzers": "历史多通道分析仪",
    "pages.visitor_showcase.metric.sample_rate": "正式采样口径",
    "pages.visitor_showcase.metric.filter": "双通道平均参数",
    "pages.visitor_showcase.process.title": (
        "一条可解释、可复核、可回放的校准证据链"
    ),
    "pages.visitor_showcase.process.reference": "参考标准",
    "pages.visitor_showcase.process.temperature": "温度稳定",
    "pages.visitor_showcase.process.pressure": "压力调节",
    "pages.visitor_showcase.process.sampling": "同步采样",
    "pages.visitor_showcase.process.qc": "质量判定",
    "pages.visitor_showcase.process.archive": "证据归档",
    "pages.visitor_showcase.chart.title": "六通道响应一致性",
    "pages.visitor_showcase.chart.caption": "仿真示意曲线 · 非实时设备数据",
    "pages.visitor_showcase.chart.axis_start": "气体切换",
    "pages.visitor_showcase.chart.axis_end": "稳定窗口",
    "pages.visitor_showcase.traceability.title": "五层计量溯源",
    "pages.visitor_showcase.traceability.certificate": "证书值与不确定度",
    "pages.visitor_showcase.traceability.environment": "温度、压力与露点真值",
    "pages.visitor_showcase.traceability.frames": "原始帧与 1 Hz 时间轴",
    "pages.visitor_showcase.traceability.qc": "稳定性、完整性与异常判定",
    "pages.visitor_showcase.traceability.artifacts": "报告、哈希与审阅记录",
    "pages.visitor_showcase.actions.enter": "进入全屏展示",
    "pages.visitor_showcase.actions.exit": "退出展示  Esc",
    "pages.visitor_showcase.footer": "V1.5 最终产品 · 中文默认 · 1920×1080 优化",
    "pages.visitor_showcase.footer_boundary": "只读展示 · 无设备控制 · 无系数写入",
}

_EN_US = {
    "pages.certificate_metrics.title": "Metrology Certificate Metrics",
    "pages.certificate_metrics.boundary": (
        "Safety boundary: these are auditable certificate metadata drafts. "
        "They do not automatically enter calibration plans, fitting, "
        "coefficient writeback, device communications, or the formal database. "
        "Independent review remains required."
    ),
    "pages.certificate_metrics.records.title": "Certificate Records",
    "pages.certificate_metrics.records.count": "{count} records",
    "pages.certificate_metrics.records.column.asset": "Asset",
    "pages.certificate_metrics.records.column.certificate": "Certificate",
    "pages.certificate_metrics.records.column.value": "Certified Value",
    "pages.certificate_metrics.records.column.state": "Review State",
    "pages.certificate_metrics.records.column.revision": "Revision",
    "pages.certificate_metrics.form.title": "Certificate Metrics and Traceability",
    "pages.certificate_metrics.actions.new": "New Certificate Record",
    "pages.certificate_metrics.actions.save_draft": "Save Draft",
    "pages.certificate_metrics.actions.submit_review": "Submit for Independent Review",
    "pages.certificate_metrics.actions.reload": "Reload",
    "pages.certificate_metrics.review_state.draft": "Draft",
    "pages.certificate_metrics.review_state.pending_review": "Pending Review",
    "pages.certificate_metrics.review_state.reviewed": "Reviewed",
    "pages.certificate_metrics.review_state.rejected": "Rejected",
    "pages.certificate_metrics.status.ready": "Certificate metadata layer is ready.",
    "pages.certificate_metrics.status.loaded": "Loaded {count} certificate records.",
    "pages.certificate_metrics.status.load_failed": "Load failed: {message}",
    "pages.certificate_metrics.status.new_record": (
        "Creating a new certificate metadata draft."
    ),
    "pages.certificate_metrics.status.saved": (
        "Draft saved as revision {revision}; not connected to calibration."
    ),
    "pages.certificate_metrics.status.submitted": (
        "Submitted for independent review as revision {revision}; "
        "not connected to calibration."
    ),
    "pages.certificate_metrics.status.selected": "Selected revision {revision}.",
    "pages.certificate_metrics.status.validation_failed": (
        "Validation failed: {message}"
    ),
    "pages.certificate_metrics.status.save_failed": "Save failed: {message}",
    "pages.certificate_metrics.validation.title": "Certificate Metadata Validation",
    "pages.readonly.results.title": "Calibration Result Summary",
    "pages.readonly.results.boundary": (
        "Read-only results from the V1.5 simulation rehearsal; no device "
        "control, model selection, or coefficient writeback."
    ),
    "pages.readonly.reports.title": "Report and Artifact Summary",
    "pages.readonly.reports.boundary": (
        "Read-only artifact roles and export states from the shared snapshot."
    ),
    "pages.readonly.value.report_authority": (
        "Report artifact authority: {authority}"
    ),
    "pages.readonly.value.report_statuses": (
        "Unified export states: {statuses}"
    ),
    "pages.readonly.value.report_release": (
        "Formal release state: {status}; independent review is still required."
    ),
    "pages.readonly.review.title": "Review and Safety Summary",
    "pages.readonly.review.boundary": (
        "Read-only certificate, safety-boundary, and review-next-step summary."
    ),
    "pages.readonly.plan.title": "Mature Calibration Plan Preview",
    "pages.readonly.plan.boundary": (
        "Read-only preview of the mature V1.5 45/13 queues and execution "
        "boundaries; point-table editing is unavailable."
    ),
    "pages.readonly.qc.title": "Simulation Quality-Control Summary",
    "pages.readonly.qc.boundary": (
        "Read-only checks for the shared dry-run snapshot; real sample "
        "stability and device readback remain not evaluated."
    ),
    "pages.readonly.section.qc_point_evidence": "Point Evidence Contract",
    "pages.readonly.section.qc_rule_governance": (
        "Rule and Threshold Governance"
    ),
    "pages.readonly.value.qc_point_status": (
        "Point decisions: {status}; authority: {authority}."
    ),
    "pages.readonly.value.qc_point_roles": "Artifact roles: {roles}",
    "pages.readonly.value.qc_point_fields": (
        "Required traceability fields: {fields}"
    ),
    "pages.readonly.value.qc_rule_status": (
        "Rule state: {status}; source: {source}."
    ),
    "pages.readonly.value.qc_rule_ui_edit": "UI editable: {allowed}",
    "pages.readonly.value.qc_reject_status": (
        "Reject-reason summary: {status}; source artifact role: {role}."
    ),
    "pages.readonly.devices.title": "Device Identity and Health Summary",
    "pages.readonly.devices.boundary": (
        "Read-only configured channel slots; no port scan, device connection, "
        "or hardware control."
    ),
    "pages.readonly.value.device_runtime_authority": (
        "Real runtime authority: {authority}; this page does not scan COM."
    ),
    "pages.readonly.value.device_initialization_contract": (
        "Initialization contract: {owner} owns {mode}, {rate} Hz upload, and "
        "{temperature}; neutralization and readback evidence are required."
    ),
    "pages.readonly.value.device_no_v2_workbench": (
        "V2 simulation presets, fault injection, and a second device-state "
        "source are excluded from the V1.5 product."
    ),
    "pages.readonly.algorithm.title": "Production Algorithm Boundary",
    "pages.readonly.algorithm.boundary": (
        "The mature legacy-ratio profile remains locked as production default; "
        "absorption ratio remains an offline shadow candidate."
    ),
    "pages.visitor_showcase.eyebrow": "INTELLIGENT METROLOGY",
    "pages.visitor_showcase.title": (
        "V1.5 Gas Analyzer Intelligent Calibration Center"
    ),
    "pages.visitor_showcase.subtitle": (
        "From reference standards and environmental truth to synchronized "
        "multi-channel sampling, quality decisions, and traceable evidence"
    ),
    "pages.visitor_showcase.badge.simulated": "Simulated Baseline",
    "pages.visitor_showcase.badge.read_only": "Read-only Visitor Mode",
    "pages.visitor_showcase.badge.not_acceptance": "Not Real Acceptance Evidence",
    "pages.visitor_showcase.metric.calibration_points": (
        "Mature CO₂ / H₂O Calibration Points"
    ),
    "pages.visitor_showcase.metric.analyzers": "Historical Analyzer Channels",
    "pages.visitor_showcase.metric.sample_rate": "Formal Sampling Cadence",
    "pages.visitor_showcase.metric.filter": "Dual-channel Averaging",
    "pages.visitor_showcase.process.title": (
        "An explainable, reviewable, replayable calibration evidence chain"
    ),
    "pages.visitor_showcase.process.reference": "Reference",
    "pages.visitor_showcase.process.temperature": "Temperature",
    "pages.visitor_showcase.process.pressure": "Pressure",
    "pages.visitor_showcase.process.sampling": "Sampling",
    "pages.visitor_showcase.process.qc": "Quality",
    "pages.visitor_showcase.process.archive": "Evidence",
    "pages.visitor_showcase.chart.title": "Six-channel Response Consistency",
    "pages.visitor_showcase.chart.caption": (
        "Simulated illustration · not live device data"
    ),
    "pages.visitor_showcase.chart.axis_start": "Gas transition",
    "pages.visitor_showcase.chart.axis_end": "Stable window",
    "pages.visitor_showcase.traceability.title": (
        "Five-layer Metrology Traceability"
    ),
    "pages.visitor_showcase.traceability.certificate": (
        "Certified values and uncertainty"
    ),
    "pages.visitor_showcase.traceability.environment": (
        "Temperature, pressure, and dew-point truth"
    ),
    "pages.visitor_showcase.traceability.frames": "Raw frames and 1 Hz timebase",
    "pages.visitor_showcase.traceability.qc": (
        "Stability, completeness, and anomaly decisions"
    ),
    "pages.visitor_showcase.traceability.artifacts": (
        "Reports, hashes, and review records"
    ),
    "pages.visitor_showcase.actions.enter": "Enter Full-screen Showcase",
    "pages.visitor_showcase.actions.exit": "Exit Showcase  Esc",
    "pages.visitor_showcase.footer": (
        "V1.5 final product · Chinese by default · optimized for 1920×1080"
    ),
    "pages.visitor_showcase.footer_boundary": (
        "Read-only · no device control or coefficient writeback"
    ),
}


def translate(
    key: str,
    *,
    locale: str = "zh_CN",
    default: str | None = None,
    **kwargs: Any,
) -> str:
    """Resolve one V1.5 page label without importing any V2 package."""

    catalog = _EN_US if locale == "en_US" else _ZH_CN
    text = catalog.get(key) or _ZH_CN.get(key) or default or key
    try:
        return text.format_map(kwargs)
    except (KeyError, ValueError):
        return text


def translator_for(locale: str) -> Callable[..., str]:
    """Bind a locale for page constructors while preserving format arguments."""

    def bound(
        key: str,
        *,
        default: str | None = None,
        **kwargs: Any,
    ) -> str:
        return translate(
            key,
            locale=locale,
            default=default,
            **kwargs,
        )

    return bound


__all__ = ["translate", "translator_for"]
