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
    "pages.site_profile.title": "现场设备配置与只读初始化准备",
    "pages.site_profile.boundary": (
        "本页只填写和校验现场映射，不扫描、不打开串口、不发送命令。"
        "只有已通电且经操作员确认的分析仪才会进入只读初始化清单；信息不完整时保持阻断。"
    ),
    "pages.site_profile.status.empty": "尚未载入现场配置；请导入端口清单新建模板，或载入已有配置。",
    "pages.site_profile.metric.expected_connected": "报告接入台数",
    "pages.site_profile.metric.expected_powered": "报告通电台数",
    "pages.site_profile.metric.mapped": "已映射 接入/通电",
    "pages.site_profile.metric.status": "只读准备状态",
    "pages.site_profile.actions.load_profile": "载入现场配置",
    "pages.site_profile.actions.new_from_inventory": "从端口清单新建",
    "pages.site_profile.actions.apply_row": "应用当前设备",
    "pages.site_profile.actions.validate": "校验全部映射",
    "pages.site_profile.actions.save": "保存配置与清单",
    "pages.site_profile.column.port": "端口",
    "pages.site_profile.column.visible": "系统可见",
    "pages.site_profile.column.connected": "已接入",
    "pages.site_profile.column.powered": "已通电",
    "pages.site_profile.column.ga": "通道标签",
    "pages.site_profile.column.protocol": "协议ID",
    "pages.site_profile.column.sn": "8位SN",
    "pages.site_profile.column.algorithm": "算法",
    "pages.site_profile.column.state": "状态",
    "pages.site_profile.editor.title": "选中设备的现场映射",
    "pages.site_profile.field.port": "端口",
    "pages.site_profile.field.ga_label": "GA标签",
    "pages.site_profile.field.protocol_device_id": "协议ID",
    "pages.site_profile.field.sn_code": "8位SN",
    "pages.site_profile.field.algorithm": "算法类型",
    "pages.site_profile.field.ftd_hz": "上传频率 Hz",
    "pages.site_profile.field.average1": "AVERAGE1",
    "pages.site_profile.field.average2": "AVERAGE2",
    "pages.site_profile.field.algorithm_evidence_type": "算法证据类型",
    "pages.site_profile.field.algorithm_evidence_reference": "算法证据编号或说明",
    "pages.site_profile.algorithm_evidence.production_batch": "生产或批次记录",
    "pages.site_profile.algorithm_evidence.firmware_manifest": "固件清单",
    "pages.site_profile.algorithm_evidence.manufacturer_record": "厂家设备档案",
    "pages.site_profile.algorithm_evidence.boundary": (
        "算法类别必须来自与当前端口、协议 ID 和 SN 一致的设备记录；"
        "不得按 COM 端口、协议 ID 或系数形状猜测。"
    ),
    "pages.site_profile.field.connected": "已接入",
    "pages.site_profile.field.powered": "已通电",
    "pages.site_profile.field.operator_confirmed": "操作员已确认",
    "pages.site_profile.field.check_capable": "支持CHECK",
    "pages.site_profile.field.check_required": "要求CHECK",
    "pages.site_profile.confirmation.title": "当前现场状态确认（与本次映射哈希绑定）",
    "pages.site_profile.confirmation.operator_name": "确认人",
    "pages.site_profile.confirmation.observation_basis": "观察依据",
    "pages.site_profile.confirmation.action": "确认并绑定当前4/2映射",
    "pages.site_profile.confirmation.status.pending": "状态：待确认；历史身份不能代替当前接线与通电观察。",
    "pages.site_profile.confirmation.status.confirmed": "状态：已确认；任何映射修改都会使本确认失效。",
    "pages.site_profile.confirmation.status.stale": "状态：映射已修改，原确认失效，请重新核对并确认。",
    "pages.site_profile.confirmation.status.failed": "状态：确认失败；请先完成4台接入、2台通电及逐台确认。",
    "pages.site_profile.value.ready": "就绪",
    "pages.site_profile.value.review": "待完善",
    "pages.site_profile.value.historical_prefill": "历史身份待确认",
    "pages.site_profile.value.current_probe": "当前通电身份已读，待人工确认",
    "pages.site_profile.value.not_selected": "未用于本次",
    "pages.site_profile.value.not_loaded": "未载入",
    "pages.site_profile.value.yes": "是",
    "pages.site_profile.value.no": "否",
    "pages.site_profile.value.unknown": "待确认",
    "pages.site_profile.reasons.none": "现场映射完整，可生成只读初始化输入；仍需另行明确授权才能打开真实COM。",
    "pages.site_profile.status.select_row": "请先在表格中选择一个端口。",
    "pages.site_profile.status.row_applied": "已应用 {port} 的编辑内容；尚未执行任何设备操作。",
    "pages.site_profile.status.loaded": "已载入 {path}，并完成离线校验。",
    "pages.site_profile.status.loaded_historical": (
        "已载入 {path}；其中 {count} 台为历史身份预填，仍需确认当前4台接入和2台通电状态。"
    ),
    "pages.site_profile.status.template_created": "已从端口清单建立4台接入/2台通电模板，请逐台确认。",
    "pages.site_profile.status.confirmation_saved": "当前现场状态已确认，并与这一本映射逐字段哈希绑定。",
    "pages.site_profile.status.confirmation_failed": "当前映射尚不满足确认条件；请核对4台接入、2台通电、逐台确认和确认人信息。",
    "pages.site_profile.status.valid": "现场映射校验通过；只读执行仍需独立授权。",
    "pages.site_profile.status.invalid": "现场映射仍有 {count} 项需要处理。",
    "pages.site_profile.status.saved_ready": "已保存 {path}，并生成经哈希绑定的只读输入清单。",
    "pages.site_profile.status.saved_blocked": "已保存阻断版 {path}；派生清单已清空，不能用于执行。",
    "pages.site_profile.dialog.validation": "现场映射校验",
    "pages.site_profile.dialog.json": "JSON 文件",
    "pages.site_profile.dialog.load_failed": "载入现场配置失败",
    "pages.site_profile.dialog.save_failed": "保存现场配置失败",
    "pages.site_profile.dialog.confirmation_failed": "当前现场确认失败",
    "pages.site_profile.reason.inventory_missing": "端口清单缺失，请重新选择有效的端口清单。",
    "pages.site_profile.reason.inventory_changed": "端口清单在建档后发生变化，请重新从清单新建现场配置。",
    "pages.site_profile.reason.bank_invalid": "现场配置必须完整包含 COM35 至 COM42 八个候选端口。",
    "pages.site_profile.reason.reported_counts_invalid": "报告的接入台数或通电台数不是有效整数。",
    "pages.site_profile.reason.confirmation_missing": "缺少当前现场确认；历史6台身份记录不能证明本次4台接入、2台通电状态。",
    "pages.site_profile.reason.confirmation_not_confirmed": "当前现场确认已失效或尚未完成，请核对映射后重新确认。",
    "pages.site_profile.reason.confirmation_operator_missing": "当前现场确认缺少确认人。",
    "pages.site_profile.reason.confirmation_time_missing": "当前现场确认缺少确认时间。",
    "pages.site_profile.reason.confirmation_basis_missing": "当前现场确认缺少物理观察依据。",
    "pages.site_profile.reason.confirmation_connected_ports_mismatch": "当前接入端口与已确认记录不一致，请重新确认。",
    "pages.site_profile.reason.confirmation_powered_ports_mismatch": "当前通电端口与已确认记录不一致，请重新确认。",
    "pages.site_profile.reason.confirmation_connected_count_mismatch": "报告接入台数与已确认记录不一致，请重新确认。",
    "pages.site_profile.reason.confirmation_powered_count_mismatch": "报告通电台数与已确认记录不一致，请重新确认。",
    "pages.site_profile.reason.confirmation_state_changed": "现场映射内容在确认后发生变化，原确认哈希已失效。",
    "pages.site_profile.reason.probe_invalid": "当前现场探针证据、文件哈希或逐端口身份不一致；已阻断并要求工程复核。",
    "pages.site_profile.reason.connected_count": "报告接入 {expected} 台，当前只确认 {actual} 台。",
    "pages.site_profile.reason.powered_count": "报告通电 {expected} 台，当前只确认 {actual} 台。",
    "pages.site_profile.reason.active_count": "当前通电分析仪为 {actual} 台；只读初始化要求 1 至 6 台。",
    "pages.site_profile.reason.schema": "现场配置格式不正确（当前版本：{value}）。",
    "pages.site_profile.reason.duplicate_ga": "GA 标签重复：{value}。",
    "pages.site_profile.reason.duplicate_sn": "8 位 SN 重复：{value}。",
    "pages.site_profile.reason.powered_without_connected": "{port} 已标记通电，但尚未标记接入。",
    "pages.site_profile.reason.not_confirmed": "{port} 已接入，但操作员尚未确认。",
    "pages.site_profile.reason.not_visible": "{port} 已接入，但端口清单中不可见。",
    "pages.site_profile.reason.ga_missing": "{port} 已接入，但未填写 GA 标签。",
    "pages.site_profile.reason.protocol_missing": "{port} 已通电，但未填写协议 ID。",
    "pages.site_profile.reason.sn_invalid": "{port} 的 SN 必须是 8 位数字。",
    "pages.site_profile.reason.algorithm_invalid": "{port} 尚未选择有效算法类型。",
    "pages.site_profile.reason.algorithm_evidence_missing": (
        "{port} 已选择算法，但尚未填写与当前 SN 绑定的生产、固件或厂家记录。"
    ),
    "pages.site_profile.reason.algorithm_evidence_invalid": (
        "{port} 的算法证据类型、编号或算法类别不完整，不能用于放行。"
    ),
    "pages.site_profile.reason.algorithm_evidence_identity": (
        "{port} 的算法证据与当前端口、协议 ID 或 SN 不一致。"
    ),
    "pages.site_profile.reason.algorithm_evidence_file": (
        "{port} 的算法证据文件路径与 SHA-256 不完整或文件已发生变化。"
    ),
    "pages.site_profile.reason.legacy_check": "{port} 使用旧算法时，支持 CHECK 和要求 CHECK 必须均不勾选。",
    "pages.site_profile.reason.new_check": "{port} 使用新算法时，支持 CHECK 和要求 CHECK 必须均勾选。",
    "pages.site_profile.reason.runtime_1hz": "{port} 缺少经确认的 1 Hz 上传频率。",
    "pages.site_profile.reason.average": "{port} 缺少 AVERAGE1 或 AVERAGE2 现场记录。",
    "pages.site_profile.reason.unknown": "存在未识别的现场映射问题，请由工程师查看验证 JSON。",
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
    "pages.site_profile.title": "Site Device Mapping and Read-only Initialization",
    "pages.site_profile.boundary": (
        "This page only edits and validates site mappings. It never scans or opens serial ports "
        "and never sends commands. Only powered, operator-confirmed analyzers may enter the "
        "read-only initialization list; incomplete mappings remain blocked."
    ),
    "pages.site_profile.status.empty": "No site profile loaded. Import a port inventory or load a profile.",
    "pages.site_profile.metric.expected_connected": "Reported Connected",
    "pages.site_profile.metric.expected_powered": "Reported Powered",
    "pages.site_profile.metric.mapped": "Mapped Connected / Powered",
    "pages.site_profile.metric.status": "Read-only Readiness",
    "pages.site_profile.actions.load_profile": "Load Site Profile",
    "pages.site_profile.actions.new_from_inventory": "New from Port Inventory",
    "pages.site_profile.actions.apply_row": "Apply Selected Device",
    "pages.site_profile.actions.validate": "Validate All Mappings",
    "pages.site_profile.actions.save": "Save Profile and Lists",
    "pages.site_profile.column.port": "Port",
    "pages.site_profile.column.visible": "OS Visible",
    "pages.site_profile.column.connected": "Connected",
    "pages.site_profile.column.powered": "Powered",
    "pages.site_profile.column.ga": "GA Label",
    "pages.site_profile.column.protocol": "Protocol ID",
    "pages.site_profile.column.sn": "8-digit SN",
    "pages.site_profile.column.algorithm": "Algorithm",
    "pages.site_profile.column.state": "State",
    "pages.site_profile.editor.title": "Selected Device Site Mapping",
    "pages.site_profile.field.port": "Port",
    "pages.site_profile.field.ga_label": "GA Label",
    "pages.site_profile.field.protocol_device_id": "Protocol ID",
    "pages.site_profile.field.sn_code": "8-digit SN",
    "pages.site_profile.field.algorithm": "Algorithm",
    "pages.site_profile.field.ftd_hz": "Upload Hz",
    "pages.site_profile.field.average1": "AVERAGE1",
    "pages.site_profile.field.average2": "AVERAGE2",
    "pages.site_profile.field.algorithm_evidence_type": "Algorithm Evidence Type",
    "pages.site_profile.field.algorithm_evidence_reference": "Evidence Record or Reference",
    "pages.site_profile.algorithm_evidence.production_batch": "Production or Batch Record",
    "pages.site_profile.algorithm_evidence.firmware_manifest": "Firmware Manifest",
    "pages.site_profile.algorithm_evidence.manufacturer_record": "Manufacturer Device Record",
    "pages.site_profile.algorithm_evidence.boundary": (
        "The algorithm must come from a device record bound to the current port, "
        "protocol ID, and SN. Do not infer it from a COM port, protocol ID, or "
        "coefficient shape."
    ),
    "pages.site_profile.field.connected": "Connected",
    "pages.site_profile.field.powered": "Powered",
    "pages.site_profile.field.operator_confirmed": "Operator Confirmed",
    "pages.site_profile.field.check_capable": "CHECK Capable",
    "pages.site_profile.field.check_required": "CHECK Required",
    "pages.site_profile.confirmation.title": "Current Site Confirmation (hash-bound to this mapping)",
    "pages.site_profile.confirmation.operator_name": "Confirmed by",
    "pages.site_profile.confirmation.observation_basis": "Observation basis",
    "pages.site_profile.confirmation.action": "Confirm and Bind Current 4/2 Mapping",
    "pages.site_profile.confirmation.status.pending": (
        "Status: pending. Historical identity cannot prove current cabling and power."
    ),
    "pages.site_profile.confirmation.status.confirmed": (
        "Status: confirmed. Any mapping edit invalidates this confirmation."
    ),
    "pages.site_profile.confirmation.status.stale": (
        "Status: mapping changed. Recheck and confirm the current site state."
    ),
    "pages.site_profile.confirmation.status.failed": (
        "Status: failed. Complete four connected, two powered, and row confirmations."
    ),
    "pages.site_profile.value.ready": "Ready",
    "pages.site_profile.value.review": "Review Required",
    "pages.site_profile.value.historical_prefill": "Historical Identity - Confirm",
    "pages.site_profile.value.current_probe": "Current Powered Identity Read - Confirm",
    "pages.site_profile.value.not_selected": "Not Used in This Run",
    "pages.site_profile.value.not_loaded": "Not Loaded",
    "pages.site_profile.value.yes": "Yes",
    "pages.site_profile.value.no": "No",
    "pages.site_profile.value.unknown": "Confirm",
    "pages.site_profile.reasons.none": (
        "The site mapping is complete. Separate explicit authorization is still required "
        "before opening any real COM port."
    ),
    "pages.site_profile.status.select_row": "Select a port row first.",
    "pages.site_profile.status.row_applied": "Applied edits for {port}; no device action was performed.",
    "pages.site_profile.status.loaded": "Loaded and validated {path} offline.",
    "pages.site_profile.status.loaded_historical": (
        "Loaded {path}; {count} historical identities were prefilled. Confirm the current "
        "four connected and two powered analyzers."
    ),
    "pages.site_profile.status.template_created": (
        "Created the four-connected/two-powered template; confirm each device."
    ),
    "pages.site_profile.status.confirmation_saved": (
        "Current site state confirmed and field-by-field hash-bound to this mapping."
    ),
    "pages.site_profile.status.confirmation_failed": (
        "The mapping is not confirmable yet; verify four connected, two powered, "
        "each row, and the confirmer information."
    ),
    "pages.site_profile.status.valid": "Site mapping passed; read-only execution still requires authorization.",
    "pages.site_profile.status.invalid": "{count} site-mapping items still require review.",
    "pages.site_profile.status.saved_ready": "Saved {path} and generated hash-bound read-only input lists.",
    "pages.site_profile.status.saved_blocked": (
        "Saved blocked profile {path}; derived lists were cleared and cannot execute."
    ),
    "pages.site_profile.dialog.validation": "Site Mapping Validation",
    "pages.site_profile.dialog.json": "JSON files",
    "pages.site_profile.dialog.load_failed": "Failed to Load Site Profile",
    "pages.site_profile.dialog.save_failed": "Failed to Save Site Profile",
    "pages.site_profile.dialog.confirmation_failed": "Current Site Confirmation Failed",
    "pages.site_profile.reason.inventory_missing": "The port inventory is missing; select a valid inventory.",
    "pages.site_profile.reason.inventory_changed": "The port inventory changed after drafting; rebuild the profile.",
    "pages.site_profile.reason.bank_invalid": "The profile must contain all eight candidate ports from COM35 through COM42.",
    "pages.site_profile.reason.reported_counts_invalid": "The reported connected or powered count is not a valid integer.",
    "pages.site_profile.reason.confirmation_missing": (
        "Current-site confirmation is missing. Historical six-unit identities do not "
        "prove the current four-connected/two-powered state."
    ),
    "pages.site_profile.reason.confirmation_not_confirmed": (
        "The current-site confirmation is stale or incomplete; recheck and confirm."
    ),
    "pages.site_profile.reason.confirmation_operator_missing": (
        "The current-site confirmation has no confirmer."
    ),
    "pages.site_profile.reason.confirmation_time_missing": (
        "The current-site confirmation has no timestamp."
    ),
    "pages.site_profile.reason.confirmation_basis_missing": (
        "The current-site confirmation has no physical observation basis."
    ),
    "pages.site_profile.reason.confirmation_connected_ports_mismatch": (
        "Connected ports differ from the confirmed record; confirm again."
    ),
    "pages.site_profile.reason.confirmation_powered_ports_mismatch": (
        "Powered ports differ from the confirmed record; confirm again."
    ),
    "pages.site_profile.reason.confirmation_connected_count_mismatch": (
        "The reported connected count differs from the confirmed record; confirm again."
    ),
    "pages.site_profile.reason.confirmation_powered_count_mismatch": (
        "The reported powered count differs from the confirmed record; confirm again."
    ),
    "pages.site_profile.reason.confirmation_state_changed": (
        "The mapping changed after confirmation, so the confirmation hash is invalid."
    ),
    "pages.site_profile.reason.probe_invalid": (
        "Current probe evidence, source hashes, or per-port identity is inconsistent; "
        "engineering review is required."
    ),
    "pages.site_profile.reason.connected_count": "Reported {expected} connected; {actual} are currently confirmed.",
    "pages.site_profile.reason.powered_count": "Reported {expected} powered; {actual} are currently confirmed.",
    "pages.site_profile.reason.active_count": "{actual} analyzers are powered; read-only initialization requires 1 to 6.",
    "pages.site_profile.reason.schema": "The site-profile schema is invalid (current: {value}).",
    "pages.site_profile.reason.duplicate_ga": "Duplicate GA label: {value}.",
    "pages.site_profile.reason.duplicate_sn": "Duplicate 8-digit SN: {value}.",
    "pages.site_profile.reason.powered_without_connected": "{port} is powered but not marked connected.",
    "pages.site_profile.reason.not_confirmed": "{port} is connected but not operator-confirmed.",
    "pages.site_profile.reason.not_visible": "{port} is connected but not visible in the port inventory.",
    "pages.site_profile.reason.ga_missing": "{port} is connected but has no GA label.",
    "pages.site_profile.reason.protocol_missing": "{port} is powered but has no protocol ID.",
    "pages.site_profile.reason.sn_invalid": "{port} must have an 8-digit numeric SN.",
    "pages.site_profile.reason.algorithm_invalid": "{port} has no valid algorithm selection.",
    "pages.site_profile.reason.algorithm_evidence_missing": (
        "{port} has an algorithm selection but no current-SN production, firmware, "
        "or manufacturer record."
    ),
    "pages.site_profile.reason.algorithm_evidence_invalid": (
        "{port} has incomplete algorithm evidence type, reference, or classification."
    ),
    "pages.site_profile.reason.algorithm_evidence_identity": (
        "{port} algorithm evidence does not match the current port, protocol ID, or SN."
    ),
    "pages.site_profile.reason.algorithm_evidence_file": (
        "{port} algorithm evidence has an incomplete path/SHA-256 binding or the file changed."
    ),
    "pages.site_profile.reason.legacy_check": "{port} uses the legacy algorithm, so both CHECK flags must be clear.",
    "pages.site_profile.reason.new_check": "{port} uses the new algorithm, so both CHECK flags must be selected.",
    "pages.site_profile.reason.runtime_1hz": "{port} has no confirmed 1 Hz upload evidence.",
    "pages.site_profile.reason.average": "{port} is missing an AVERAGE1 or AVERAGE2 record.",
    "pages.site_profile.reason.unknown": "An unrecognized mapping issue exists; ask an engineer to review the validation JSON.",
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
