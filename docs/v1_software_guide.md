# 气体分析仪自动校准系统 - V1 软件维护手册

## 文档信息

- **文档名称**: V1 软件维护手册
- **适用范围**: V1 生产基线、离线审计、工程维护、受控在线验证
- **版本**: v1.0
- **创建日期**: 2026-04-15
- **最后更新**: 2026-04-15
- **维护人**: Codex
- **文档定位**: 开发/维护文档

---

## 目录

1. [文档目的](#文档目的)
2. [适用边界](#适用边界)
3. [入口说明](#入口说明)
4. [项目结构](#项目结构)
5. [系统概述](#系统概述)
6. [核心模块说明](#核心模块说明)
7. [主流程说明](#主流程说明)
8. [配置管理](#配置管理)
9. [点表管理](#点表管理)
10. [运行输出](#运行输出)
11. [能力边界](#能力边界)
12. [验证与审计](#验证与审计)
13. [相关文档索引](#相关文档索引)
14. [维护建议](#维护建议)

---

## 文档目的

本文档用于补齐 V1 软件的维护说明，帮助开发人员、工程人员和审计人员快速理解当前 V1 的入口、结构、配置、主流程、输出物和能力边界。

本文档重点回答以下问题：

1. V1 默认从哪里启动
2. V1 代码主要集中在哪些目录
3. V1 校准主流程如何组织
4. V1 配置文件和点表如何生效
5. V1 运行后会输出哪些文件
6. 当前 HEAD 对 CO2 / H2O 能力的明确边界是什么
7. 哪些工具可以安全地用于离线验证与审计

本文档不替代以下材料：

- 审计证据目录：`audit/v1_calibration_audit/`
- acceptance 摘要：`audit/v1_calibration_acceptance/`
- 在线工程验证材料：`audit/v1_calibration_acceptance_online/`
- V1/V2 行为契约：`docs/architecture/v1_to_v2_behavior_contract.md`

---

## 适用边界

### 1. 当前阶段规则

- `run_app.py` 仍然是 V1 默认入口，不允许擅自切换默认入口。
- V1 当前是生产基线与历史参考线，不是新增功能的默认承载面。
- 新功能、平台化能力和 V2 相关体验不要回接到 V1 UI。
- V1 运行时路径可能打开真实串口或真实设备，因此默认不能把“可运行”理解成“可直接真机执行”。

### 2. 真机限制

- 没有用户明确授权时，不执行任何 V1 real smoke / short run。
- 不执行 real compare / real verify / real manual operation。
- `run_v1_online_acceptance` 默认是 dry-run。
- 真机在线工程验证必须同时满足：
  - CLI 传入 `--real-device`
  - 环境变量设置 `ALLOW_REAL_DEVICE_WRITE=1`

### 3. 证据边界

- simulation / replay / suite / parity / resilience 可以作为工程证据。
- 以上证据不能当作 real acceptance 放行结论。
- 在线工程验证也只是工程验证证据，不等于真实放行结论。

---

## 入口说明

### 1. 入口总览

| 入口 | 用途 | 说明 |
|------|------|------|
| `python run_app.py` | V1 桌面 UI 默认入口 | 生产基线入口。脚本会把 `src/` 加入 `sys.path` 后启动 `gas_calibrator.ui.app.main()`。 |
| `$env:PYTHONPATH='src'; python -m gas_calibrator.tools.run_headless --config configs/default_config.json` | V1 无头运行入口 | 直接构建设备并执行 `CalibrationRunner`。属于 V1 runtime boundary，按配置可能打开真实串口。 |
| `python run_v1_postprocess.py` | V1 离线后处理 GUI | 根入口包装器，实际调用 `gas_calibrator.v2.scripts.v1_postprocess_gui`，用于 run 完成后的离线处理。 |
| `python run_v1_merged_sidecar.py --run-dir <completed_run_dir>` | V1 合并侧车入口 | 用于已完成 run 的离线工程 sidecar，可传多个 `--run-dir`。 |
| `$env:PYTHONPATH='src'; python -m gas_calibrator.tools.run_v1_online_acceptance --config configs/default_config.json` | V1 在线工程验证工具 | 默认 dry-run。只有在双门禁满足时才允许触发真机验证。 |
| `python tools/audit_v1_calibration.py` | V1 只读审计工具 | 汇总主流程、存储链路、trace 检查和 acceptance 摘要，适合离线核对当前 V1 状态。 |

### 2. 模块命令注意事项

- `run_app.py`、`run_v1_postprocess.py`、`run_v1_merged_sidecar.py` 这类根入口脚本会自行处理 `src/` 路径。
- 直接运行 `python -m gas_calibrator...` 时，当前仓库默认需要先设置 `PYTHONPATH=src`。
- 如果不先设置 `PYTHONPATH=src`，通常会报错：

```text
ModuleNotFoundError: No module named 'gas_calibrator'
```

---

## 项目结构

### 1. V1 相关目录结构

```text
gas_calibrator/
├── run_app.py                              # V1 默认入口
├── run_v1_postprocess.py                   # V1 离线后处理入口
├── run_v1_merged_sidecar.py                # V1 合并侧车入口
├── configs/
│   ├── default_config.json                 # 默认配置
│   ├── user_tuning.json                    # UI 可叠加的用户调参覆盖
│   ├── points_*.xlsx                       # 点表
│   └── overrides/                          # 特定工程 override
├── docs/
│   └── v1_software_guide.md                # 本文档
├── audit/
│   ├── v1_calibration_audit/               # V1 只读审计结果
│   ├── v1_calibration_acceptance/          # acceptance 摘要
│   └── v1_calibration_acceptance_online/   # 在线工程验证材料
├── src/
│   └── gas_calibrator/
│       ├── config.py                       # 配置加载与默认值
│       ├── logging_utils.py                # RunLogger 与导出
│       ├── h2o_summary_selection.py        # H2O 摘要策略
│       ├── data/
│       │   └── points.py                   # 点表解析与重排
│       ├── devices/                        # 设备驱动
│       ├── ui/
│       │   └── app.py                      # V1 Tk 桌面 UI
│       ├── workflow/
│       │   ├── runner.py                   # 主流程编排
│       │   └── tuning.py                   # 流程调参
│       └── tools/
│           ├── run_headless.py             # 无头运行
│           ├── run_v1_corrected_autodelivery.py
│           ├── run_v1_online_acceptance.py
│           └── run_v1_merged_calibration_sidecar.py
└── tests/
    ├── test_audit_v1_trace_check.py
    ├── test_runner_v1_writeback_safety.py
    ├── test_v1_writeback_fault_injection.py
    └── test_v1_online_acceptance_tool.py
```

### 2. 目录定位说明

| 目录/文件 | 定位 |
|------|------|
| `run_app.py` | V1 默认生产入口 |
| `src/gas_calibrator/ui/app.py` | V1 操作界面与运行控制 |
| `src/gas_calibrator/workflow/runner.py` | 主流程编排核心 |
| `src/gas_calibrator/data/points.py` | 点表解析和重排 |
| `src/gas_calibrator/config.py` | 配置加载、默认值、路径展开、能力边界 |
| `src/gas_calibrator/logging_utils.py` | 运行目录和导出物管理 |
| `src/gas_calibrator/tools/` | 无头运行、写回、在线验证、侧车等工具 |
| `audit/` | 只读审计与证据汇总 |

---

## 系统概述

### 1. 系统简介

V1 是当前仓库中的生产基线运行线，负责实际气体分析仪校准流程的桌面 UI 控制、主流程编排、数据采集、点位保存和系数写回闭环。

V1 当前更适合被理解为：

- 生产基线
- 历史参考
- 行为对照基准
- 工程验证基座

而不是：

- V2 的功能承载层
- 当前阶段的新平台化主战场
- 可直接替代 V2 的统一产品面

### 2. 当前主要能力

1. V1 桌面 UI 运行
2. 设备连接、自检与流程控制
3. 点表解析、温度分组与路由调度
4. CO2 主链 zero/span 业务闭环
5. 样本采集、点位汇总与导出
6. 系数写前快照、写入、GETCO 回读验证与回滚
7. 离线 trace 检查、fault injection 与在线工程验证工具

---

## 核心模块说明

### 1. UI 模块

**实现文件**: `src/gas_calibrator/ui/app.py`

**主要职责**:

- 加载配置和用户调参覆盖
- 编辑设备串口参数
- 预览点表与运行范围
- 启动/停止校准流程
- 展示设备状态、日志和运行摘要

**说明**:

- 当前 V1 UI 基于 `Tkinter`，不是 `PySide6`。
- 默认配置文件指向 `configs/default_config.json`。
- UI 在加载基础配置后，会尝试叠加 `configs/user_tuning.json`。

---

### 2. 主流程模块

**实现文件**: `src/gas_calibrator/workflow/runner.py`

**主要职责**:

- 点位调度
- 温度组编排
- CO2 / H2O 路由执行
- 稳态与门禁控制
- 采样和点位汇总
- 系数写回闭环
- 模式恢复和流程清理

**说明**:

- 这是 V1 最核心的编排文件。
- 当前工作区内该文件已有较多未提交改动，分析与验证时要结合当前 `git status` 一起看。

---

### 3. 配置管理模块

**实现文件**: `src/gas_calibrator/config.py`

**主要职责**:

- 读取 JSON 配置文件
- 补齐内置运行时默认值
- 展开 `paths` 中的相对路径
- 提供点分路径读取工具
- 暴露 V1 H2O zero/span 能力边界检查

**关键点**:

- `load_config()` 会把配置文件相对路径转成仓库绝对路径。
- 当前 HEAD 的能力边界常量明确写在这里：

```text
Current HEAD V1 only supports the CO2 main chain; H2O zero/span is NOT_SUPPORTED.
```

---

### 4. 点表解析模块

**实现文件**: `src/gas_calibrator/data/points.py`

**主要职责**:

- 从 Excel 点表读取校准点
- 解析温度、CO2、H2O、压力字段
- 支持点位向下继承
- 支持点位重排

**当前规则**:

- 默认按无表头 Excel 读取
- 默认跳过前两行
- 温度列和 CO2 列支持向下继承
- H2O 列支持解析湿度发生器温度、湿度、露点和 `mmol/mol`

---

### 5. 设备驱动模块

**实现目录**: `src/gas_calibrator/devices/`

**包含设备**:

- `gas_analyzer.py`：分析仪
- `pace5000.py`：压力控制器
- `paroscientific.py`：数字压力计
- `dewpoint_meter.py`：露点仪
- `humidity_generator.py`：湿度发生器
- `temperature_chamber.py`：温度箱
- `thermometer.py`：测温仪
- `relay.py`：继电器
- `serial_base.py`：串口基础能力

**说明**:

- 这些驱动属于真实设备边界。
- 任何直接打开这些设备的运行路径，都需要严格区分是否已获得真机授权。

---

### 6. 日志与导出模块

**实现文件**: `src/gas_calibrator/logging_utils.py`

**主要职责**:

- 创建 run 目录
- 写出样本级 CSV
- 写出点位级 CSV / XLSX
- 写出系数写回记录
- 提供可读化汇总输出

**核心类**:

- `RunLogger`

---

## 主流程说明

### 1. 流程总览

V1 当前主流程可以概括为：

```text
入口
→ 点表解析/重排
→ 温度分组编排
→ CO2/H2O 路由执行
→ 稳态/门禁
→ 样本采集
→ 点位保存
→ 系数写前快照/写入/回读/回滚
→ 模式恢复
→ 清理/后处理
```

### 2. 关键阶段说明

#### 1) 配置加载

- 读取 `configs/default_config.json`
- 补齐运行时默认值
- 展开路径字段
- UI 路径可额外叠加 `configs/user_tuning.json`

#### 2) 点表解析

- 优先使用 `paths.points_excel`
- UI 预览时会回退尝试 `points.xlsx`
- 按当前规则生成 `CalibrationPoint` 列表

#### 3) 启动门禁

- 检查设备配置中的 `enabled`
- 根据配置构建设备实例
- 可执行启动连通性检查与自检门禁

#### 4) 温度组与路由执行

- `workflow.route_mode` 控制主路线
- 常见取值：
  - `h2o_then_co2`
  - `h2o_only`
  - `co2_only`
- `selected_temps_c`、`selected_pressure_points`、`skip_co2_ppm` 用于控制运行范围

#### 5) 判稳与采样门禁

- 主流程在采样前会经过稳态判定
- 压力、露点、freshness 等门禁都会影响是否允许采样
- 相关阈值主要位于：
  - `workflow.pressure`
  - `workflow.sampling`
  - `workflow.stability`

#### 6) 样本采集与点位汇总

- 样本采集结束后写入样本级记录
- 点位汇总会生成点位级输出
- 当前导出明确保留：
  - `sample_ts`
  - `save_ts`

#### 7) 系数写回闭环

- 在 `collect_only=false` 且 `coefficients.enabled=true` 时进入拟合与写回路径
- 写回闭环包含：
  - 写前快照
  - 写入目标系数
  - GETCO 回读验证
  - 异常回滚

#### 8) 模式恢复与收尾

- 结束后恢复分析仪模式
- 清理连接
- 将 run 输出物写入日志目录

### 3. 流程参考材料

建议结合以下文档一起看主流程：

- `audit/v1_calibration_audit/02_v1_flow_map.md`
- `audit/v1_calibration_audit/03_point_storage_map.md`

---

## 配置管理

### 1. 默认配置文件

默认配置文件为：

```text
configs/default_config.json
```

### 2. 主要配置分区

| 配置分区 | 说明 |
|------|------|
| `devices` | 设备开关、串口、波特率和设备参数 |
| `workflow` | 运行范围、门禁、采样、稳定性和流程调参 |
| `paths` | 点表路径、输出目录等 |
| `coefficients` | 拟合、SENCO 次序、摘要策略与拟合参数 |
| `validation` | 离线验证相关开关 |
| `modeling` | 离线建模相关配置 |

### 3. 常用配置项

| 配置路径 | 作用 |
|------|------|
| `paths.points_excel` | 点表路径，默认相对路径是 `points.xlsx` |
| `paths.output_dir` | 输出目录，默认是 `logs` |
| `workflow.collect_only` | 只采集、不拟合、不写回 |
| `workflow.route_mode` | 路由模式 |
| `workflow.selected_temps_c` | 温度范围筛选 |
| `workflow.selected_pressure_points` | 压力点筛选 |
| `workflow.temperature_descending` | 温度顺序 |
| `workflow.skip_co2_ppm` | 跳过指定 CO2 点 |
| `workflow.missing_pressure_policy` | 压力缺失策略 |
| `workflow.h2o_carry_forward` | H2O 上下文继承策略 |
| `workflow.pressure` | 压力相关门禁与控制阈值 |
| `workflow.sampling` | 采样频率、缓存与 freshness 参数 |
| `workflow.stability` | 露点门禁和质量门禁 |

### 4. 设备配置分区

默认配置支持以下设备段：

- `pressure_controller`
- `pressure_gauge`
- `dewpoint_meter`
- `humidity_generator`
- `gas_analyzer`
- `gas_analyzers`
- `temperature_chamber`
- `thermometer`
- `relay`
- `relay_8`

### 5. 多分析仪默认配置

默认 `gas_analyzers` 配置通常包含：

- `ga01` ~ `ga08`
- `COM35` ~ `COM42`

说明：

- 这只是默认配置，不代表当前阶段允许直接打开这些真实 COM。
- 是否真机执行，仍然受任务授权约束。

---

## 点表管理

### 1. 点表来源

当前点表主要来自：

- `paths.points_excel`
- 工程专用点表文件，例如 `configs/points_*.xlsx`

### 2. 点表解析规则

点表解析模块当前遵循以下规则：

1. 默认按无表头 Excel 读取
2. 默认跳过前两行
3. 温度列支持向下继承
4. CO2 列支持向下继承
5. H2O 列支持解析：
   - 湿度发生器温度
   - 湿度发生器湿度
   - 露点
   - `mmol/mol`

### 3. 典型策略

| 参数 | 典型值 | 作用 |
|------|------|------|
| `missing_pressure_policy` | `require` | 压力缺失时报后续校验错误 |
| `missing_pressure_policy` | `carry_forward` | 压力缺失时沿用上一个有效压力 |
| `h2o_carry_forward` | `true/false` | 控制 H2O 文本上下文是否继承 |

---

## 运行输出

### 1. 输出目录

`RunLogger` 会在 `paths.output_dir` 下创建 run 目录，默认形式如下：

```text
logs/<run_id>/
```

### 2. 主要输出文件

| 文件 | 角色 |
|------|------|
| `samples_*.csv` | 样本级导出 |
| `point_XXXX*_samples.csv` | 单点样本明细 |
| `points_*.csv` | 点位级执行摘要 |
| `points_readable_*.csv` | 点位级可读摘要 |
| `points_readable_*.xlsx` | 点位级可读工作簿 |
| `coefficient_writeback_*.csv` | 系数写前/目标/回读/回滚链路 |
| `分析仪汇总_*.csv/.xlsx` | 汇总级导出 |

### 3. 当前存储特点

- 样本级记录与点位级汇总分离
- 点位导出具备可读化版本
- 写回闭环单独记录
- 当前导出同时保留 `sample_ts` 与 `save_ts`

---

## 能力边界

### 1. 已明确支持

- V1 默认桌面 UI 入口
- V1 无头运行入口
- CO2 主链 zero/span 业务闭环
- MODE 切换与恢复
- SENCO 写入与 GETCO 回读验证
- 写回异常回滚
- 点位采样与保存时间分离
- 离线 trace / writeback safety / fault injection 检查

### 2. 当前不支持或受限

- H2O zero/span 主业务闭环：`NOT_SUPPORTED`
- 未经授权直接执行 V1 真机 smoke / short run
- 默认执行 real compare / real verify / real manual operation
- 把 sidecar/offline 能力重新塞回 V1 UI

### 3. H2O 能力说明

当前 HEAD 的明确结论是：

- H2O 路由和 H2O 点位相关结构存在
- 但“存在 H2O 路由/点位”不等于“存在与 CO2 对等的 H2O zero/span 业务闭环”
- 当前 V1 对 H2O zero/span 的正式能力结论是 `NOT_SUPPORTED`

---

## 验证与审计

### 1. 推荐核对命令

```powershell
python tools/audit_v1_calibration.py
```

```powershell
python -m pytest -q tests/test_audit_v1_trace_check.py
```

```powershell
python -m pytest -q tests/test_runner_v1_writeback_safety.py
```

```powershell
python -m pytest -q tests/test_v1_writeback_fault_injection.py
```

```powershell
python -m pytest -q tests/test_v1_online_acceptance_tool.py
```

### 2. 验证用途说明

| 验证项 | 主要作用 |
|------|------|
| `test_audit_v1_trace_check.py` | 核对 trace、样本链路、门禁与导出结构 |
| `test_runner_v1_writeback_safety.py` | 核对写回安全、save_ts、H2O 边界说明 |
| `test_v1_writeback_fault_injection.py` | 核对写回异常恢复与回滚 |
| `test_v1_online_acceptance_tool.py` | 核对在线工程验证工具的双门禁和 CO2-only 限制 |

### 3. 在线工程验证边界

`run_v1_online_acceptance` 是受保护的工程验证工具，不是日常入口。其规则如下：

1. 默认 dry-run
2. 真机执行必须双门禁
3. 仅支持 CO2 相关受控验证
4. 输出的是工程验证证据，不是 real acceptance 放行结论

---

## 相关文档索引

### 1. 推荐阅读顺序

1. `docs/v1_software_guide.md`
2. `docs/v1/v1_device_protocols.md`
3. `docs/v1/v1_exception_handling.md`
4. `docs/v1/v1_stability_algorithms.md`
5. `docs/v1/v1_coefficient_writeback.md`
6. `docs/v1/v1_export_formats.md`
7. `docs/v1/v1_config_reference.md`
8. `docs/v1/v1_ui_workflow.md`
9. `docs/v1/v1_h2o_capability.md`
10. `audit/v1_calibration_audit/02_v1_flow_map.md`
11. `audit/v1_calibration_audit/03_point_storage_map.md`
12. `audit/v1_calibration_audit/04_risk_checklist.md`
13. `audit/v1_calibration_audit/06_trace_check.md`
14. `audit/v1_calibration_acceptance/06_acceptance_summary.md`
15. `audit/v1_calibration_acceptance_online/`
16. `audit/v1_v2_decouple/10_entrypoint_ownership.md`
17. `docs/architecture/v1_to_v2_behavior_contract.md`
18. `docs/v1_800ppm_ingress_smoke_checklist.md`

### 2. 文档分工说明

| 文档 | 作用 |
|------|------|
| 本文档 | V1 总体维护说明 |
| `v1_device_protocols.md` | 设备通信协议详细说明 |
| `v1_exception_handling.md` | 异常处理与容错路径 |
| `v1_stability_algorithms.md` | 稳态判定算法与参数 |
| `v1_coefficient_writeback.md` | 系数写回闭环协议 |
| `v1_export_formats.md` | 数据导出格式规范 |
| `v1_config_reference.md` | 配置模型完整映射 |
| `v1_ui_workflow.md` | UI交互流程与状态机 |
| `v1_h2o_capability.md` | H2O NOT_SUPPORTED原因分析 |
| `02_v1_flow_map.md` | 主流程源码映射 |
| `03_point_storage_map.md` | 点位与存储链路说明 |
| `04_risk_checklist.md` | 风险清单 |
| `06_trace_check.md` | 只读 trace 验证结论 |
| `06_acceptance_summary.md` | acceptance 摘要 |
| `v1_800ppm_ingress_smoke_checklist.md` | 特定 smoke 场景操作清单 |

---

## 维护建议

### 1. 建议做法

1. 先对齐文档、测试和审计材料，再改 V1 代码。
2. 涉及点位口径、导出结构、写回闭环的改动，优先补测试。
3. 如果只是做离线工程步骤，优先使用：
   - `run_v1_postprocess.py`
   - `run_v1_merged_sidecar.py`
   - `tools/audit_v1_calibration.py`
4. 不要为了“更完整”把离线 sidecar 能力重新塞回 `run_app.py`。

### 2. 真机任务注意事项

如果任务需要真机工程验证，建议在任务说明中明确写出：

- 授权范围
- 执行命令
- 输出目录
- 是否允许真实设备写入
- 结论是否仅限工程验证

---

## 文档更新记录

| 日期 | 版本 | 更新内容 |
|------|------|------|
| 2026-04-15 | v1.0 | 首次补齐 V1 软件维护手册，整理入口、结构、配置、流程、输出、边界和验证说明 |

---

**文档版本**: v1.0  
**最后更新**: 2026-04-15  
**维护说明**: 本文档应随 V1 入口、配置、流程边界和审计材料同步更新
