# V1.5 最终产品架构与 V1/V1.5/V2 收口设计

## 1. 执行结论

V1.5 是本仓库唯一最终产品。未来不再建设 V2 产品，不再讨论 V2 cutover。

- V1：冻结的生产 fallback、历史基线、故障恢复入口。
- V1.5：唯一产品、唯一未来规划、唯一正式校准和发布主线。
- V2：历史资产池；有价值能力迁移，无价值或重复能力删除。

这不是把 V2 改名成 V1.5。正确做法是保留成熟 V1.5 校准物理内核，把 V2 中
真正有价值的分析、报告、证书、仿真和展示能力拆出来，放入正确命名空间；
V2 平行运行内核、真机探针、产品壳和切换逻辑则退出。

## 2. 当前仓库审计事实

2026-07-28 收口前静态审计结果：

| 项目 | 当前值 | 结论 |
| --- | ---: | --- |
| `gas_calibrator.v1_5` Python 模块 | 27 | 已有正式产品骨架，但仍偏小 |
| `gas_calibrator.v1_5` Python 行数 | 10,989 | 主要集中于正式编排、QC、身份和新工作站 |
| `gas_calibrator.v2` Python 模块 | 403 | 明显形成了第二套庞大产品 |
| `gas_calibrator.v2` Python 行数 | 约199,935 | 远超 V1.5，维护成本和误解风险很高 |
| V2 `core/` | 135 模块、110,444 行 | 大量 builder、repository、gate、runner 和探针 |
| V2 `ui_v2/` | 67 模块、30,773 行 | 与 V1.5 最终工作站形成双产品壳 |
| V2 静态零引用模块 | 43 | 只能作为人工删除候选，不能直接等同安全删除 |
| V1.5 保护路径导入 V2 | 0 | 当前边界良好，必须继续保持 |

Gate 2 与最小 Gate 3 删除批次完成后：

| 项目 | 当前值 |
| --- | ---: |
| `gas_calibrator.v1_5` Python 模块/非空行数 | 36 / 13,312 |
| `gas_calibrator.validation` Python 模块/非空行数 | 221 / 118,698 |
| `gas_calibrator.v2` Python 模块/非空行数 | 347 / 148,685 |
| 本批删除的运行包内未收集测试副本 | 18模块 / 13,454行 |
| 本轮健康评估合并 | 2个V2模块 → 1个validation模块，运行模块净减少1个 |
| 本轮健康评估实现非空行 | 126 → 243（净增117，用于缺证据状态、边界约束和证据标记；不把模块减少冒充总代码减少） |
| 本轮旧算法UI删除 | 3个运行模块 / 121非空行；连同shell与快捷键清理，V2非空行减少129 |
| 本轮旧结果UI删除 | 2个运行模块（结果页、专属残差图）；连同feed/shell清理，V2非空行减少179 |
| 本轮实时分发收口 | 删除12个重复或无消费者的专用sink；只保留完整快照发布，V2非空行再减少53 |
| 本轮旧QC UI删除 | 3个运行模块（QC页、概览卡、拒绝原因图）；V2非空行减少362，V1.5证据合同增加128，两个命名空间净减少234 |
| 本轮 V2 仿真产品入口退役 | 13个运行模块 / 2,642非空行；V2 UI/headless/device-helper 启动链归零 |
| 本轮旧报告UI删除 | 4个运行模块（报告页、AI摘要、工件清单、导出栏）；V2非空行减少1,051，正式工件生成器和共享只读网关保留 |
| 本轮旧设备UI删除 | 4个运行模块（设备页、健康面板、状态表、指标卡）；V2非空行减少161，仿真控制器按独立批次拆解 |
| 本轮旧设备工作台生产器删除 | 2个运行模块（6,661行控制器、1,839行专属组件）/ 8,500非空行；预设、故障注入和设备动作未迁入V1.5，历史JSON工件保持只读兼容 |
| 本轮旧审阅展示层删除 | 2个运行模块（审阅面板、presenter）/ 2,904非空行；AppFacade历史证据聚合与纯工件范围合同保留 |
| 本轮旧UI编排与可编辑计划链删除 | 3个运行模块（AppFacade、PlanGateway、计划编辑页）/ 10,577非空行；ResultsGateway和WP2–WP6等独立合同改为直接测试，V1.5只读成熟队列不变 |
| 待迁移模块 | 254 |
| 提取唯一价值后删除 | 76 |
| 临时兼容包装 | 17 |
| V1.5 保护路径导入 V2 | 0 |

当前真正的问题不是“缺一个新功能”，而是版本所有权失控：

1. V2 的名字仍出现在主开发线、产品 UI、cutover 文档和真机探针中。
2. V2 的分析/证据能力与平行校准运行内核混在同一命名空间。
3. V1.5 已经有成熟正式流程，但用户可见产品能力尚未完全迁入。
4. 测试、历史门禁和治理代码数量庞大，部分没有进入默认 pytest 收集。
5. 多套入口和多套“未来主线”叙述使维护人员容易走错流程。

## 3. 版本处理决策

### 3.1 V1：冻结，不重构

保留：

- `run_app.py`
- `gas_calibrator.ui.app`
- `gas_calibrator.workflow.runner`
- 已验证设备驱动、点表解释、采样和安全写回行为

处理原则：

- 只修复明确生产缺陷；
- 不新增页面、证书管理、分析大屏或候选算法；
- 用作 V1.5 parity 基准和紧急 fallback；
- 最终发布 V1.5 后仍保留一个受控周期，不立即删除。

### 3.2 V1.5：唯一产品

V1.5 拥有：

- 产品入口和桌面工作站；
- 0613/0620/0621 成熟校准内核的正式调用边界；
- 初始化、身份、SENCO7/8 中性化和压力优先流程；
- 六通道操作体验；
- 证书指标、运行计划、QC、结果、报告、复核和发布状态；
- 候选算法的离线评审与受控启用；
- 真实设备授权、写入、读回、复验和回滚状态；
- 参观展示模式。

V1.5 不拥有设备无关的底层文件/数据库工具，也不复制通用设备驱动。

### 3.3 V2：迁移后退出

V2 不再拥有：

- 产品路线；
- 用户可见产品名称；
- 默认或候选生产入口；
- 真实 COM 工程探针；
- 平行校准状态机；
- 独立设备工厂；
- 独立采样、压力、温度、阀路和写系数服务；
- 独立 UI 壳；
- cutover 或替代 V1/V1.5 的文档。

历史 V2 包仅在迁移期存在。每个模块必须有去向和退出条件。

## 4. 最终代码所有权

尽量复用现有目录，不再创造一套“大架构”：

```text
src/gas_calibrator/
├── ui/                       # V1 冻结 UI
├── workflow/                 # V1 成熟行为基线
├── devices/                  # 共享设备驱动与协议
├── storage/                  # 共享数据库、证据、导入导出
├── modeling/                 # 离线拟合、候选算法、模型比较
├── validation/               # simulation/replay/parity/resilience/只读审计
├── v1_5/
│   ├── orchestration/        # 唯一 V1.5 application service 和运行编排
│   ├── ui/                   # 唯一最终产品壳
│   ├── qc_advanced/          # V1.5 产品 QC
│   ├── parameters/           # 参数、档案和版本治理
│   ├── certificate_metrics_registry.py
│   └── review_surface.py
└── v2/                       # 临时迁移区，最终删除
```

### 依赖方向

允许：

```text
V1.5 UI
  -> V1.5 orchestration
    -> mature queue runners / shared devices
    -> storage
    -> modeling
    -> validation
```

禁止：

```text
V1.5 -> V2
shared package -> V2
V2 -> 真实设备控制
UI -> 直接设备写入
报告/证书 -> 修改校准计算
```

## 5. V2 模块处置矩阵

| V2 区域 | 处理 | 目标 | 退出条件 |
| --- | --- | --- | --- |
| `core/*runner*`、`workflow_steps/`、`calibration_service` | 提取安全合同后删除 | V1.5 继续调用成熟 queue runner | V1.5 dry-run/real-run 共用同一 service 且 parity 通过 |
| `run001_*`、real-COM probe | 删除 | 若有唯一安全检查，迁入 V1.5 受控执行层 | 不再有 V2 COM 入口和配置 |
| `ui_v2/` 壳、导航、run controller | 删除 | V1.5 UI | 证书、报告、审核、展示所需组件迁移完成 |
| `ui_v2` 图表/空状态/可折叠面板 | 按实际消费者迁移或删除 | `v1_5/ui` | 不保留零调用旧组件；V1.5需要时在统一状态源上实现 |
| `certificate_metrics_registry` | 已迁移并删除包装 | `v1_5` | V1.5 是唯一实现和导入路径 |
| certificate page / visitor page | 重构迁移 | V1.5 单一产品壳 | 使用 V1.5 状态源且无控制权限 |
| `analytics/` | 按用途迁移 | `v1_5/qc_advanced` 或 `validation` | 生产展示与模拟分析分开 |
| `qc/` | 去重后迁移 | `v1_5/qc_advanced` | 与现有 QC 不再重复 |
| `algorithms/` | 迁移或删除 | `modeling` | 仅离线候选算法，无隐式生产选择 |
| `sim/` | 迁移 | `validation` | suite 名称不再带 V2 产品语义 |
| `storage/` | 迁移/兼容包装 | `storage` | 共享实现单一来源，V2 wrapper 可删 |
| `utils/` | 兼容包装后删除 | `gas_calibrator.utils` | 全仓使用正式共享命名空间 |
| `adapters/` | 逐个判断 | `validation`、`v1_5` 或删除 | 不再以 V1/V2 对比为长期产品结构 |
| `intelligence/` | 可选迁移 | V1.5 离线复核 | 不参与控制、拟合选择或 release 决策 |
| `scripts/` | 迁移有效 CLI，删除探针/cutover | `tools` 或 `validation` | 根入口和帮助文档只有 V1/V1.5 |
| `src/gas_calibrator/v2/tests` | 删除 | 正式测试只能位于 `tests/` | 默认 pytest 不再携带未收集测试副本 |
| V2 cutover 文档 | 历史化 | 文档归档 | 顶部明确 superseded |

## 6. V1.5 软件产品设计

### 6.1 一个产品壳，四种视图

四种视图不是四套 UI，也不是权限系统：

1. **操作员视图**
   - 当前运行阶段；
   - 六台分析仪实时/模拟状态；
   - 温度、压力、露点/流量、气体源；
   - 当前点、目标值、稳定性、剩余时间；
   - 唯一下一动作；
   - 暂停、安全停止和异常处理。

2. **工程师抽屉**
   - 原始协议帧；
   - COM/设备身份映射；
   - 稳定窗口；
   - 温压流时间对齐；
   - QC 拒绝原因；
   - 旧值/候选值/读回值；
   - 诊断日志。

3. **审核者视图**
   - 点表和算法档案；
   - 证书/气瓶批次；
   - 拟合输入和排除点；
   - 不确定度预算；
   - 操作者/审核者/批准者记录；
   - 工件哈希和 release gate。

4. **参观展示模式**
   - 只读、脱敏；
   - 显示校准原理、六通道进度、环境条件、质量指标和追溯链；
   - 不显示控制按钮、端口、序列号、文件路径或授权入口；
   - 不生成假实时数据，离线时必须标注“演示/历史回放”。

所有视图读取同一个 `WorkstationSnapshot`，不能各自拼状态。

### 6.2 页面结构

最终导航只保留：

1. 校准运行
2. 设备与初始化
3. 校准计划
4. 质量控制
5. 结果与拟合
6. 证书与标准器
7. 报告与归档
8. 系统审计

“V2 分析”“V2 驾驶舱”“V2 设备工作台”全部从用户界面消失。

### 6.3 状态模型

建议唯一顶层状态：

```text
IDLE
PRECHECK
INITIALIZING
CONDITIONING
CO2_RUNNING
H2O_RUNNING
FIT_REVIEW
WRITE_REVIEW
WRITING
READBACK_VERIFY
POST_VERIFY
ARCHIVE_REVIEW
COMPLETED
HELD
SAFE_STOPPED
```

每个状态同时具备：

- 进入条件；
- 可执行动作；
- 禁止动作；
- 超时；
- 安全停止；
- 证据输出；
- 恢复/重跑策略。

UI 不自行改变状态，只向 orchestration service 提交命令。

## 7. 从初始化到证书的完整流程

### 阶段 A：批次建立

1. 选择1至6台设备。
2. 绑定 SN、device_code、协议 ID 和 COM。
3. 绑定配置、点表、算法档案和操作者。
4. 绑定气瓶、零气、干气、露点仪、压力计和数字测温仪资料。
5. 生成不可变 run manifest。

### 阶段 B：初始化

1. 查询当前 GETCO1-9。
2. 保存 epoch-0 快照。
3. SENCO7/SENCO8 中性化。
4. 身份和通信自检。
5. 参考仪器新鲜度和单位检查。
6. 初始化失败时 hold，不进入正式采样。

### 阶段 C：压力优先

1. 使用成熟 pressure-first SENCO9 流程。
2. 保留压力设定、实际读数、稳定窗口和候选系数。
3. 独立审核后才允许写入。

### 阶段 D：CO2 开放流通

1. CO2 零气独立锚定。
2. 按45点成熟队列运行。
3. 每点保存目标、温度真值、压力、流量/露点、六通道原始帧、过滤值和 QC。
4. 不允许用补跑点静默覆盖原失败点。

### 阶段 E：H2O 开放流通

1. H2O 干气/低水点独立于13个湿点。
2. 按13点成熟队列运行。
3. 露点、流量、恢复时间和吸附/解吸滞后独立判断。
4. CO2 的零点结论不能替代 H2O 干气证据。

### 阶段 F：拟合与写入

1. 生产默认只使用已批准算法档案。
2. 明示拟合输入、排除点、权重、残差和不确定度。
3. 写入前显示旧值、候选值、变化量、设备、算法、证据哈希。
4. 写入必须双重确认。
5. 逐项读回；差异超限立即 hold/rollback。

### 阶段 G：复验、归档和证书

1. 写后运行规定复验点。
2. 生成设备级和批次级结论。
3. 固化原始数据、执行摘要、诊断分析和正式分析。
4. 证书资料不足可生成草稿，但不能正式签发。
5. 归档、数据库导入和证书 release 分别审核。

## 8. 数据与证据设计

### 单一运行标识

一个批次只使用一个不可变 `run_id`，所有对象通过它关联：

- 设备身份；
- 点表和算法档案；
- 证书与气体批次；
- 原始帧和采样；
- QC 决策；
- 系数快照、写入、读回；
- 报告、归档和数据库账本。

### 证据分层

| 层级 | 用途 | 是否能放行 |
| --- | --- | --- |
| simulated | UI/流程/异常回归 | 否 |
| replay | 历史数据只读重放 | 否 |
| engineering_probe | 极小范围真实读取/验证 | 否 |
| validation_run | 受控真机验证 | 仅限已声明范围 |
| real_acceptance | 正式台架验收 | 可以进入相应 release 审核 |

任何界面和报告都必须展示证据层级，不依赖文件夹名称猜测。

## 9. 算法与科学边界

1. 校准系统必须先满足守恒、量纲、时间同步和可追溯性，再讨论拟合优度。
2. CO2 与 H2O 使用不同低端锚点和不同滞后/吸附解释。
3. 压力、温度、流量和水汽路径的交互项必须在离线候选模型中评审，不得自动进入生产。
4. 训练/拟合、模型选择和设备写入必须是三个独立状态。
5. 候选算法永远不能因为离线 RMSE 更低就自动替换生产档案。
6. 任何新算法都必须与成熟45/13历史重放、边界点、残差结构和写后复验同时比较。

## 10. 验证体系

### 每次变更

- 单元测试；
- 边界导入测试；
- dry-run；
- 受影响的 parity/resilience/UI 测试；
- `git diff --check`；
- 保护文件差异检查。

### 每个迁移批次

- 新旧导入对象身份相同；
- V1.5 不导入 V2；
- 原 V2 模块仅薄包装；
- 调用者迁移完成；
- 删除旧模块后全收集无错误。

### 发布冻结

- 全量 pytest；
- smoke/regression/nightly/parity/resilience；
- 1920×1080 UI、键盘、缩放和中文化；
- 打包后从真实发布物启动；
- 受控真机 acceptance；
- 数据库、证书、写入、rollback 和审计包核对。

## 11. 四个收口阶段

### Gate 1：产品身份收口

- 长期规则明确 V1.5 唯一终版；
- UI 去除 V2 产品名称；
- V2 README 和 cutover 文档标记废止；
- 禁止新增 V2 模块。

### Gate 2：有价值资产迁移

- 证书注册表、证书编辑页和参观展示页已迁入 V1.5；
- V1.5 `WorkstationSnapshot` 已成为计划、QC、结果、报告、审核、设备、算法和参观展示的统一只读合同；
- V1.5 计划/QC/结果/报告/审核/设备/算法摘要使用一个声明式只读页面实现，不复制 V2 controller；
- 计划只预览成熟 45/13 队列，禁止编辑点表；QC 对真实样本稳定性和设备回读保持 `not_evaluated`；
- 设备页只展示六个配置通道槽位，连接/身份/健康/实时帧保持未评估；不扫描 COM；
- 算法页锁定 `legacy_ratio_production`，`absorption_ratio_shadow` 保持离线候选和晋级阻断；
- GUI与CLI已共用一个“执行一次、写出一次”的薄 application service，成熟 runner 未修改；
- V2 `devices_page` / `algorithms_page` / `qc_page` / `results_page` / `reports_page` 页面壳已转为“提取后随旧壳删除”处置；
- `algorithms_page`、`algorithm_compare_table` 和 `winner_badge` 已删除；旧shell导航、菜单、刷新回调和快捷键已解除；
- `results_page` 与专属 `residual_chart` 已删除；底层结果快照与正式工件未删除；
- `qc_page`、`qc_overview_panel` 和 `qc_reject_reason_chart` 已删除；正式QC工件及其共享只读网关未删除；
- V1.5 QC只读快照已声明点级证据合同：唯一权威为成熟runner的 `execution_rows` / `execution_summary` / `formal_analysis`，dry-run期间保持 `not_evaluated`，策略版本与阈值配置哈希必须可追溯，UI禁止编辑阈值；
- `reports_page`、`ai_summary_panel`、`artifact_list_panel` 和 `export_bar` 已删除；共享报告/工件生成器、结果网关和审阅数据保持原位；
- V1.5 报告只读快照已声明统一合同：角色仅允许 `execution_rows` / `execution_summary` / `diagnostic_analysis` / `formal_analysis`，导出状态仅允许 `ok` / `skipped` / `missing` / `error`，权威来源为成熟 V1.5 runner 工件；dry-run 正式签发保持 `not_evaluated`，UI 不提供导出、签发或批准动作；
- `devices_page`、`analyzer_health_panel`、`device_status_table` 和 `metric_card` 已删除；五个旧 V2 产品页面壳全部退出；
- V1.5 设备只读快照已声明安全合同：真实运行状态仅以成熟 V1.5 runner 为权威，不扫描 COM，不采信外部传入的端口/序列号/健康状态，不提供仿真预设、故障注入、气路控制或设备配置动作；初始化由成熟 V1.5 流程负责 MODE2、1 Hz、SENCO7/SENCO8 中性化及读回证据；
- 页面壳删除进度为 `5/5`，当前 `extracted_page_shell_count=0`；
- 两个纯离线健康评分器已由 V2 合并迁入 `gas_calibrator.validation.analyzer_health`，V2 聚合入口仅重新导出同一函数对象；
- 健康评分不再把零样本、零运行、零帧或零点数误判为健康，缺少拟合结果的10分惩罚已恢复，漂移历史不足保持 `not_evaluated`；
- 设备工作台控制器审计确认6661行快照/预设/故障注入/设备动作高度耦合，没有适合直接迁入V1.5的独立纯函数；共享审阅链已改为只读历史 `workbench_action_report.json` / `workbench_action_snapshot.json`，控制器与1839行专属组件已经删除；
- `ReviewCenterPanel` 与 `review_center_presenter` 在旧 shell 删除后仅互相调用且没有运行时消费者，已删除；`review_center_artifact_scope` 纯工件范围合同及导出索引继续保留；
- AppFacade 在旧 shell、设备工作台和审阅面板删除后已无任何运行时构造方，PlanGateway与可编辑计划页也只剩测试消费者；三者已删除，ResultsGateway、离线治理构建器、识别范围及WP2–WP6合同改为直接验证；
- 14 个历史 Run001/real-COM/cutover 单用途 CLI 在运行时、测试和当前操作文档中均无模块调用方，已删除 1167 行非空代码；底层 no-write/准入/证据合同与测试保持原位，`query_only_com_sanity_probe` 待迁移验证入口明确保留；
- 四个失去入口的 V2 probe core 已完成函数级审计并删除；双重解锁、操作员确认、no-write、V1 fallback、禁止扩大范围和非验收标记收敛为无 V2 依赖的 `gas_calibrator.validation.engineering_probe_admission` 纯准入合同，合同本身不打开 COM 或执行探针；
- `run001_r1_conditioning_only_probe` 在A1R/A2 core删除后已无任何调用方，其旧端口映射、继电器conditioning、心跳/压力时序和重复准入逻辑不属于V1.5最终产品，已删除；仍由 `query_only_com_sanity_probe` 调用的R0/query-only只读链完整保留且本批未执行；
- `sim.protocol` 的“静态零引用”经审计确认为 `_EXPORTS` 延迟导入造成的假阴性：`run_simulated_compare`与simulation suite仍调用它，协议成功/故障、fixture replay及suite测试仍覆盖它；模块清单现解析延迟导出表，该活跃链本批不删除；
- `analytics.service` 与 `analytics.measurement.service` 的“静态零引用”同样是 `_LAZY_EXPORTS` 相对路径延迟导入造成的假阴性：`v1_postprocess_runner` 会在通用与逐帧后处理阶段分别加载两个服务，共享一次 feature build 后逐项隔离生成分析工件；删除会破坏现存后处理和报告导出，本批不删除、不修改计算公式，只把清单解析扩展到 `_LAZY_EXPORTS` 及相对模块路径；
- `storage.profile_store` 经动态导出、调用方和数据合同审计确认只剩 V2 存储包重导出及专属测试，没有产品运行消费者；其唯一附加职责是把已共享的 `JsonProfileRepository` 文档转换成已退役的 V2 `CalibrationPlanProfile`。包装、两个包导出和专属测试已经删除；共享仓库的保存/加载/列表、默认项、删除、导入导出、原子写入和路径防护完整保留。清单同时补充 `_EXPORT_MAP` 相对路径解析，避免以后把包重导出误判为零引用；
- `ui_v2.widgets.collapsible_section` 没有包导出、Python调用方、测试构造方或V1.5页面依赖，只是从未接入产品的67行通用Tk展示组件；其折叠状态不承载数据、阈值、i18n或校准状态，已直接删除。工程视图“高频优先、低频折叠”的设计原则继续保留，但未来如确有需求必须在V1.5统一状态源上实现，不恢复旧V2组件；
- `scripts.audit_historical_frame_parity` 只包装已迁入共享validation的逐帧算法，但此前仍是唯一CLI入口；直接删除会丢失人工复核运行方式。本批把完全相同的27行CLI合同迁入 `gas_calibrator.tools.audit_historical_frame_parity`，保留 `--catalog`、`--fixture`、`--output-dir`、三类工件输出及PASS=0/FAIL=2退出码，再删除V2脚本；算法、工件schema和只读边界均未复制或修改；
- V1.5证书注册表只负责人工录入、修订和复核状态，不能替代本地照片/PDF/DOCX的只读发现、去重、角色覆盖和缺口清点；因此证书证据普查不能删除。本批把950行核心、69行CLI、合同JSON、测试和操作文档作为完整单元从V2等量迁入 `validation`、`tools`、根 `configs/metrology` 和正式文档目录，不保留反向兼容壳；证书仍为非阻断治理信息，不接校准输入、拟合、数据库或设备；
- `LiveStateFeed` 曾先收口为单一完整快照发布，现已随旧 V2 产品 shell 一并删除；后续迁移审计曾由 `ResultsGateway` 承担离线聚合读取，Gate 42 已进一步收口为五个窄文件网关和纯工件合同，已无 V2 产品入口；
- V2 `run_v2`、`test_v2_device`、`test_v2_safe`、`ui_v2.app`、`ui_v2.shell`、运行控制页以及只服务于该 shell 的 feed/shortcut/run controller、启动 splash、诊断包和 UI runtime preflight 已全部删除；历史 Step 2 工件明确输出 `launcher_state=retired`，不再给出可执行 V2 仿真命令；
- 历史逐帧 parity 已迁入 `validation`；
- simulation、replay、resilience 继续按调用关系迁入 `validation`；
- 通用存储/转换迁入 shared；
- 候选算法迁入 modeling。

当前验证基线：

- 三组定向回归分别通过：健康评估与模块清单13项，分析/测量/UI兼容49项，成熟队列/算法lineage/物理锚点/逐帧/工作站/UI 146项（测试集合有重叠，不累计计数）；
- 本轮删除批次新增验证：旧shell/导航/快捷键/删除台账11项，V1.5统一快照/算法页/产品边界20项，中文i18n与1920×1080 shell布局5项，成熟算法/逐帧/工作站49项；
- 结果页删除批次新增验证：feed/shell/快捷键/删除台账/1080p共15项，中文与V1.5产品边界24项，逐帧/工作站/健康评估14项；
- 单一快照分发批次新增验证：完整快照与单次渲染5项，中文/1080p/V1.5产品边界28项，逐帧/工作站/健康评估14项，模块清单6项；
- QC页面收口批次新增验证：V1.5 QC合同与模块清单16项，旧壳导航/路由5项，应用/1080p布局5项，V2 QC数据与报告4项，成熟QC工件10项，V1.5 UI/边界/逐帧31项；
- V2 仿真启动链退役批次验证：入口退役/清单/历史就绪度/V1.5边界57项，保留设备与报告资产定向回归13项；旧 V2 启动器和 shell 专属模块13个、非空代码2642行被删除；
- V2 报告页面收口批次验证：V1.5报告合同/UI/边界与模块清单27项，AppFacade共享报告合同6项，设备页1080p布局1项；共享结果/运行清单/离线工件46项完成回归，其中中文“方法确认覆盖项”稳定标签已恢复；
- V2 设备页面收口批次验证：V1.5设备安全合同/UI/产品边界/模块清单28项通过；SENCO7/SENCO8初始化、中性化、前置门禁和受控复核37项通过；
- V2 设备工作台生产器退役批次验证：AppFacade/审阅中心历史工件回放2项、软件验证合同2项、WP6合同130项、ResultsGateway稀疏/旧工件兼容3项通过；活跃代码中控制器、组件和动作入口引用归零；
- V2 审阅展示层退役批次验证：保留的AppFacade历史证据、工件范围、治理导出、阶段桥和closeout合同47项通过；活跃代码中旧面板和presenter引用归零；
- V2 AppFacade与可编辑计划链退役批次验证：ResultsGateway 37项、WP2–WP5及治理合同23项、WP6/compact/工件范围合同226项、V1.5产品边界与清单32项通过；
- 历史 Run001/real-COM CLI 退役批次：14 个模块、1378 个物理行/1167 个非空行删除；保留核心合同149项、V1.5产品边界/工作站/清单38项通过；当前 V2 为333个模块、147518个非空行，静态零引用降至19项，V1.5禁止导入V2违规仍为0；
- probe core 收口批次：四个 V2 core 删除6349个物理行/5973个非空行，新增共享纯准入合同225个非空行，运行时代码净减少5748个非空行；共享准入合同13项、保留A1/cutover核心149项、V1.5产品边界/工作站/清单51项通过；当前 V2 为329个模块、141545个非空行，静态零引用为16项，V1.5及共享validation导入V2违规仍为0；
- R1 conditioning core退役批次：删除1个V2模块、2088个物理行/1936个非空行；共享准入/模块清单/边界20项、保留A1/cutover核心149项、V1.5产品边界/工作站/清单51项通过，R0/query-only三层只导入检查通过；当前V2为328个模块、139609个非空行，静态零引用为15项，V1.5及共享validation导入V2违规仍为0；
- 动态仿真引用审计批次：清单/统一JSON写入17项、simulated-compare CLI 3项、协议成功/故障2项、suite smoke/parity 2项通过；`sim.protocol`内部引用数修正为1，当前V2仍为328个模块、139609个非空行，真实静态零引用降为14项；
- analytics服务动态引用审计批次：模块清单、两个服务、延迟包导入和V1后处理适配器共26项通过；两个服务内部引用数均修正为1，当前V2仍为328个模块、139609个非空行，真实静态零引用降为12项，保护路径导入V2违规仍为0；
- profile store退役批次：删除1个V2运行模块、103个物理行/84个非空行及其76行专属测试；共享仓库/清单/存储包/计划模型21项、存储与V1后处理23项、V1.5产品边界51项通过；当前V2为327个模块、139521个非空行，真实静态零引用为11项，保护路径导入V2违规仍为0；
- collapsible section退役批次：删除1个V2 UI模块、67个物理行/53个非空行；清单、中文化、V2保留页面及V1.5 UI/产品边界25项、V1.5工作站与保护边界51项通过；当前V2为326个模块、139468个非空行，真实静态零引用为10项，保护路径导入V2违规仍为0；
- 历史逐帧CLI迁移批次：27行CLI从V2等量迁入共享tools，新增参数/退出码回归而不增加第二套算法；CLI、逐帧、兼容桥和清单11项、V1.5工作站与保护边界51项通过，实际只读重放45/45点、22197/22197原始帧、2698正式采样帧且字段差异0、源修改0；当前V2为325个模块、139441个非空行，真实静态零引用为9项，保护路径导入V2违规仍为0；
- 证书证据普查迁移批次：1019行核心与CLI从V2等量迁入共享validation/tools，合同和文档同步退出V2；证书核心、共享文件写入、V1.5注册表和清单34项通过，新CLI对受控夹具完成只读枚举并正确报告 `CENSUS_COMPLETE_WITH_GAPS`，不把缺证书升级为校准阻断；当前V2为323个模块、138422个非空行，真实静态零引用为8项，保护路径导入V2违规仍为0；
- 证书运行资料准入迁移批次：747行核心与CLI从V2等量迁入共享validation/tools，合同、业主确认资料、文档和测试同步退出V2；V2只保留47行仿真套件适配器。证书/文件治理及V1.5边界67项、导出韧性4项通过，regression套件22/22、parity套件1/1通过；桌面与 `D:\手册` 的27个锁定文件全部通过文件名与SHA-256只读复核，迁移前后执行行逐字节一致、JSON归一化时间戳后一致；45/13成熟工作站dry-run再次通过且证书启动门禁为non-blocking。准入结论仍只允许离线程序推进，正式证书门禁、真机执行和promotion保持阻断。当前V2为321个模块、137675个非空行，真实静态零引用为7项，保护路径导入V2违规仍为0；
- regression scoreboard孤立单元退役批次：删除零调用CLI、只被该CLI/专属测试使用的scoreboard核心和golden dataset registry，共3个V2运行模块、1701个物理行/1567个非空行及174个非空行专属测试。该scoreboard仍要求已经退役的V2工作台、审阅中心和AI工件，继续保留会制造虚假缺口；权威门禁由suite summary、parity、resilience、直接schema测试和45/13成熟dry-run继续承担。删除后相关suite/清单18项、parity套件1/1和45/13 dry-run通过；当前V2为318个模块、136108个非空行，真实静态零引用为6项，保护路径导入V2违规仍为0；
- RS-485旧对齐链退役批次：删除只调用旧A2.14/Run001比较器的CLI及其核心，共2个V2运行模块、600个物理行/539个非空行。三个必需输入工件在当前产品树中均无生产器，核心还将历史COM30写死为P3成功依据，而当前受控配置的压力表/压力控制器为COM22/COM23；继续保留会把历史工程口径误认为当前协议权威。PACE、Paroscientific、继电器驱动、映射及默认配置仍是唯一可执行协议合同，本批均未修改。删除前旧专属测试7项通过，删除后清单/驱动/配置/保护边界126项、导出韧性/summary parity 6项、parity套件1/1及45/13 dry-run通过；当前V2为316个模块、135569个非空行，真实静态零引用为5项，保护路径导入V2违规仍为0；
- Step 3A R0入口审计批次：`query_only_com_sanity_probe`确认是唯一受控人工命令入口，不是普通Python调用链，因此静态零引用属于入口型假阳性；其query-only核心和只读设备辅助层共同标记为临时 `retain_step3a_r0`，退出条件是Step 3A正式关闭，禁止迁入V1.5产品运行时。清单v4同时识别Python包初始化器的隐式执行关系：原始静态零引用5项全部得到解释（4个隐式包初始化器、1个人工入口），未解释项为0。旧包安全合同18项离线基线通过，正式tests新增8项覆盖CLI/env/operator三重记录、控制/写入能力拒绝、dry admission不开COM且promotion blocked、import-only自检；R0/驱动/配置/保护边界134项、parity套件1/1及45/13 dry-run通过。V2模块与非空行数保持316/135569，保护路径导入V2违规仍为0；
- R0.1只读辅助层收口批次：删除1062行的旧 `run001_r0_1_reference_read_probe`，移除其standalone准入、工件writer、历史COM30/COM27白名单和可独立执行路径；现行R0入口实际需要的压力表P3只读诊断、温度箱寄存器只读诊断和默认温度箱client factory收口为770行 `query_only_readers`，该模块不拥有CLI、准入、工件writer或standalone executor。所有启用端口现在必须由受控配置显式提供，缺端口在准入阶段fail-closed，不再回退历史COM号；读取命令和底层驱动未改。旧18项mock安全合同、新正式R0/清单15项、R0/驱动/配置/保护边界135项、导出韧性/summary parity 6项、parity套件1/1及45/13 dry-run通过；V2模块数仍为316，非空行135500（净减69），5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- R0 P3瞬时状态语义批次：不改Paroscientific驱动或P3读取命令，把“无持久/控制写入”“发送只读查询字节”“P3可能终止P4/P7连续输出”拆成独立证据。query-only schema升级至v2：新增 `serial_query_command_bytes_sent`、`persistent_device_state_write_sent=false`、`query_only_state_neutral=false`、`pressure_p3_may_cancel_continuous_output=true`、实际P3发送/连续输出取消状态及state-effect分类；`any_write_command_sent=false`明确只表示持久或控制写入，不再隐含未发送查询字节。operator confirmation新增“query-only并非完全状态中性”和“P3可能终止连续输出”两项强制确认，旧确认缺任一项即fail-closed；dry admission仍明确没有发送P3。正式R0测试12项、工程探针/准入治理52项、R0/驱动/配置/保护边界138项、导出韧性/summary parity 6项、parity套件1/1及45/13 dry-run通过；V2仍为316模块/135551非空行，本批为消除安全歧义增加51行，不增加模块，保护路径导入V2违规仍为0；
- 分析仪身份合同收口批次：逐字段核对确认，V1.5 已由只读 GETCO 身份快照、MODE2/stream 运行身份、冻结的端口绑定配置、历史逐帧 ID 审计和正式数据库重复身份门禁共同接管旧 `run001_a1_analyzer_id_truth` / `run001_a1_analyzer_mapping` 职责；COM/GA仍仅是运输标签，现场标签不能覆盖MODE2身份，映射不得自动写回正式配置。为避免重复身份直到正式入库才被发现，现有 V1.5 GETCO readiness 前移一对一门禁：同一运行身份出现在多条记录或同一串口出现多条映射，均在压力、CO₂、H₂O及系数阶段前fail-closed。删除2个仅有专属测试调用的V2模块、598个物理行/547个非空行及6个旧专属测试；删除前6项通过，迁移后身份/初始化/数据库/逐帧/清单39项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为314模块/135004非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 串口助手等效探针退役批次：审计确认 `run001_a1_serial_assistant_probe` 并非只读记录器，而是可独立打开真实COM、发送 `READDATA`，并可选发送 `MODE/SETCOMWAY` 的第二探针入口；其COM35/37/41/42、历史设备ID、10 Hz及“现场已测试”均为无原始转录绑定的硬编码基线，不能作为当前协议或验收权威。有效行为已由正式 `GasAnalyzer` 驱动的CRLF/115200/ACK噪声解析、V1.5 analyzer runtime setup、GETCO身份前后校验、逐帧审计和受控R0只读入口覆盖；本批未复制任何实现。删除1个V2运行模块、553个物理行/503个非空行及5个专属mock测试；删除前5项通过，删除后串口/身份/MODE2/逐帧/R0/清单88项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为313模块/134501非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- MODE2第二初始化器退役批次：逐命令核对确认，旧 `run001_a1_analyzer_mode2_setup` 只负责 `MODE=2` 与主动发送，现行 V1.5 analyzer runtime setup 还包含通信静默、MODE2、FTD、AVERAGE1/2、恢复主动发送的完整顺序，并强制至少1秒命令间隔、SN及初始身份一致、连续MODE2帧、ACK等待后复读、真实上传频率验证和有限重试；SENCO、设备ID、采样、拟合及气路动作均明确禁止。旧模块的“先写临时工件、命令中增量刷新、中断保留部分记录”是未接入当前产品的审计韧性，不属于校准计算或放行权威；本批不为它保留1045行第二初始化器，也未修改现行V1.5初始化实现。删除1个V2运行模块、1045个物理行/958个非空行及26个专属mock测试；删除前旧测试26项、V1.5替代链93项通过，删除后替代链与清单99项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为312模块/133543非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 分析仪第二诊断入口退役批次：审计确认，旧 `run001_a1_analyzer_diagnostics` 可依据历史配置或任意显式COM列表直接打开真实串口、监听主动上传并可选发送 `READDATA`，却没有现行Step 3A要求的双重解锁、operator confirmation和统一准入记录；其端口发现结果也不执行V1.5的一对一重复端口/重复身份门禁，不能作为当前校准放行依据。有效合同已分别由V1.5 analyzer runtime setup的MODE2/FTD/AVERAGE/连续帧与上传频率验证、GETCO前后运行身份核验、一对一identity readiness、历史逐帧ID审计及唯一受控R0 query-only入口覆盖；本批未复制第二套诊断实现。删除1个V2运行模块、558个物理行/510个非空行及11个专属mock测试，同时移除其跨边界驱动导入白名单；删除前旧测试11项通过，删除后替代链与边界89项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为311模块/133033非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- V2切换候选表退役批次：旧 `cutover_candidate_worksheet` 是2026-04遗留的V2真机dry-run准备模型，固定引用提交 `cba08beb`、已退役的V2 headless/route-trace/skip0工件和静态红黄绿种子，并可在这些静态项绿灯时输出“可进入V2真机dry-run准备”；这与V1.5唯一最终产品方向不再相容。其V1冻结检查只服务于该孤立模型，Git命令失败还会静默返回空变更，不能升级为发布权威；当前长期规则、模块清单protected-import检测、V1.5 final-product boundary和桥接白名单测试提供更直接的fail-closed治理。本批未迁移过期的切换建议，删除1个V2模块、549个物理行/508个非空行、7个专属测试以及4个共381行的静态V2切换/回退工件；删除前旧基线7项通过，删除后治理与边界74项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为310模块/132525非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 孤立信息窗口退役批次：逐模块调用图确认，旧 `build_info_loader`、`release_notes_loader`、`app_info`、关于、许可证和版本说明窗口均已随V2 shell退出运行链，只剩8个专属UI/loader测试保活；其中产品身份仍写死为“气体校准 V2 驾驶舱 / gas-calibrator-v2 / 0.6.0-demo”，迁入V1.5会重新制造第二套产品身份和版本源。V1.5工作站及参观展示页没有引用这些模块，本批不复制其实现；删除6个V2模块、165个物理行/131个非空行、8个专属测试，并从中英文locale同时删除只服务于这些窗口的44行对称文案。删除前旧组及i18n基线12项通过，删除后V1.5 UI、中文默认、locale一致性、清单与边界26项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过；当前V2为304模块/132394非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 孤立运行反馈组件退役批次：调用图确认 `busy_overlay`、`error_banner`、`notification_center` 和 `log_panel` 均无运行调用，只由4个专属Tk测试保活；它们原本依赖已删除的V2 shell状态源，单独保留不会提升V1.5运行安全。V1.5工作站已有持续状态栏、运行中/通过/失败状态、入口阻断与执行失败弹窗，证书页也保留字段校验、保存失败和加载失败可见反馈；正式runner日志与证据工件没有依赖这些V2控件。本批删除4个V2模块、148个物理行/123个非空行、4个专属测试，以及随之成为零调用的通知级别翻译函数；中英文locale同步删除30行专属文案。删除前旧组件/i18n/V1.5反馈基线14项通过，删除后V1.5反馈、剩余UI组件、中文默认、locale一致性与边界28项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过；当前V2为300模块/132269非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 孤立偏好设置双链退役批次：调用图确认 `preferences_dialog` 与 `preferences_store` 均无运行调用，只由3个专属测试保活；旧窗口仅保存最后配置路径、仿真默认、自动开始供气和截图格式，旧JSON存储还保留已退出工作台的窗口几何、日志分隔条、频谱质量默认值、布局/视图/显示档案及预设偏好。V1.5工作站已经直接拥有正式配置、CO₂ 45点队列、H₂O 13点队列、证据输出目录和可选证书路径，并将其交给唯一V1.5 application service；因此本批未迁入仿真默认值、旧显示偏好、频谱默认值或第二套配置持久化源。删除2个V2模块、147个物理行/131个非空行、3个专属测试，中英文locale同步删除38行只服务于旧偏好/通用对话框的对称文案；删除前旧偏好/i18n/V1.5设置基线13项通过，删除后V1.5设置、证书/展示UI、中文/i18n、清单与边界31项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为298模块/132138非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 孤立会话/恢复工具退役批次：调用图确认 `crash_recovery`、`recovery_store`、`recent_runs_store`、`route_memory` 与 `runtime_paths` 均无产品运行调用，只由5个专属测试保活；`recovery_store` 的唯一V2内部调用也只来自同组 `crash_recovery`。旧“崩溃恢复”只保存页面名、阶段文字、进度百分比和最近20行日志，不能恢复气路实际状态、稳定判据、采样闭包、设备读回或写入边界，迁入V1.5反而会造成可续跑错觉；旧运行路径还固定使用 `GasCalibratorV2`、已退出的plan profile和偏好文件目录。V1.5继续以显式输出目录、不可变运行目录及统一只读 `WorkstationSnapshot` 为证据权威，本批未改变正式运行工件或增加第二套历史/恢复源。删除5个V2模块、213个物理行/178个非空行、5个专属测试，中英文locale同步删除20行最近运行/页面恢复专属文案；删除前旧组/i18n/V1.5快照基线28项通过，删除后V1.5工作站/快照、剩余UI、中文/i18n、清单与边界41项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为293模块/131960非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 孤立展示组件退役批次：调用图确认 `styles`、`empty_state`、`route_progress_timeline` 与 `timeseries_chart` 均无产品运行调用；后三者只由各自1个Tk测试保活，`styles` 也只是对仍保留 `theme.tokens`/`theme.ttk_theme` 的6行转发壳，唯一引用来自证书页测试。V1.5已有独立主题、明确的未开始/等待状态、45/13路径进度和六通道状态卡；旧折线图没有单位、时间轴、质量标记或校准判定输入，不能作为科学测量展示迁入最终产品。本批删除4个V2模块、236个物理行/203个非空行、3个专属控件测试；证书页测试改为直接调用实际主题实现，中英文locale同步删除34行专属文案。删除前旧组/i18n/V1.5 UI基线22项通过，删除后V1.5 UI/快照、证书/展示UI、实际主题、中文/i18n、清单与边界38项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为289模块/131757非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 孤立诊断脱敏助手退役批次：调用图确认 `ui_v2.diagnostics.redact_helpers` 无任何运行调用，只由1个专属测试保活；其原服务对象V2诊断包导出器和shell均已删除。现行V2运行清单由 `core.run_manifest.safe_serialize` 独立遮蔽敏感键，V1.5数据库证据链由共享 `mask_dsn` 遮蔽口令，受控生产迁移结果也验证不回显DSN秘密；旧助手无条件遮蔽COM号和本地路径，而这些字段在正式校准工件中属于必要溯源信息，因此未迁入共享层。本批删除1个V2模块、38个物理行/29个非空行、1个专属测试，中英文locale同步删除6行已失效诊断包菜单/日志文案；删除前旧助手/现行安全合同基线3项通过，删除后运行清单脱敏、DSN遮蔽、i18n、导出韧性、清单与边界24项及受控数据库秘密不泄露1项通过，full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为288模块/131728非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 旧审阅扫描合同子层退役批次：对三个大型零运行引用审阅模块按风险拆分后，本批只删除最独立的 `ui_v2.review_center_scan_contracts`；它仅负责旧V2/V1.2 suite、parity、resilience、workbench和state-transition文件族扫描预算及显示适配，没有产品运行消费者。共享 `phase_evidence_display_contracts`、`reviewer_summary_builders`、`reviewer_summary_packs` 继续保留阶段术语、紧凑摘要和simulation-only证据边界；`review_center_artifact_scope` 与 `review_scope_export_index` 仍保留，等待分别核对来源消歧、历史索引和字段映射语义。本批删除1个V2模块、303个物理行/258个非空行、1个专属测试文件及混合测试中的6项旧适配器断言，共删除47项测试；删除前审阅扫描/共享摘要基线328项通过，删除后共享审阅/边界定向303项、full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为287模块/131470非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 旧审阅导出索引退役批次：调用图确认 `ui_v2.review_scope_export_index` 没有任何产品运行导入，唯一导入者是混合测试文件；其所谓历史索引只是旧UI导出目录中每次整体重写的 `index.json`，不是原子追加、不可变运行证据或V1.5正式lineage权威。审阅显示水合、阶段桥接及各类准入工件条目均由共享构建器独立实现并有现行合同测试，V1.5继续以不可变运行目录、`WorkstationSnapshot`、正式证据索引和共享lineage合同为权威，因此没有迁入第二套索引写入器。软件验证清单中的陈旧可见面从已删除的 `review_scope_export_index` 改为仍存在的 `review_scope_manifest`。本批删除1个V2模块、342个物理行/300个非空行，混合测试删除6项旧索引专属用例并保留9项工件范围/manifest用例；删除前相同基线226项通过，删除后220项及软件验证/结果网关扩展合同258项通过，full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为286模块/131170非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 旧审阅工件范围层退役批次：对 `ui_v2.review_center_artifact_scope` 的来源消歧、direct/source-scan/missing排序、范围分母、external/reference-only和manifest渲染逐项审计后，确认它只是已删除V2审阅中心的本地磁盘扫描/展示适配器，没有产品运行导入。其“磁盘存在”和临时scope分母不能替代V1.5四类工件角色、统一导出状态、不可变运行目录与正式lineage；source scan还只受文件/条目数量预算约束，不具备真实校准证据准入含义。共享compact-summary核心及实际 `results_payload`、`reports`、`historical_artifacts` 消费者继续保留，旧 `review_scope_manifest`/`artifact_scope_view` 可见面、零调用formatter适配器和中英文专属locale同步退出。本批删除1个V2模块、1146个物理行/1062个非空行，并清除其余212个非空Python适配行；删除两个专属测试文件及混合测试中的11项适配断言，共删除86项旧测试。删除前同范围252项通过，删除后166项通过，full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为285模块/129896非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 证书页/主题兼容单元退役批次：逐字段对照确认，V1.5正式证书编辑页完整保留25个可更新字段、修订历史、提交复核、日期/不确定度校验和中文默认体验；V1.5注册表继续强制 `calibration_input_connected=false`、`coefficient_write_allowed=false`、`device_io_allowed=false`，工作站在未配置证书时仍以non-blocking门禁执行成熟45/13 dry-run。旧 `ui_v2.pages.certificate_metrics_page` 只是5行身份转发，`theme.tokens`/`theme.ttk_theme` 只由三个专属测试互相保活，没有独占证书、科学计算或设备语义。本批整体删除3个V2模块、115个物理行/101个非空行、3个专属测试和中英文重复证书文案，并新增防复生边界；删除前同范围36项、删除后34项通过，full-flow/多分析仪/导出韧性/保护边界130项、parity套件1/1及45/13 dry-run通过。当前V2为282模块/129795非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- UI兼容出口退役批次：调用图确认 `ui_v2.pages.visitor_showcase_page`、`ui_v2.utils.screenshot` 和 `ui_v2.widgets.scrollable_page_frame` 均为5行V1.5身份转发，除兼容断言和旧V2展示测试外没有产品运行消费者。参观页的中文只读展示、非真机证据标记、六通道仿真曲线、1920×1080布局和全屏切换，截图失败回退工件，以及页面溢出时滚动条可达性均改由实际V1.5实现直接回归；旧V2中仍写有“V2展示界面”的中英文重复文案同步退出。本批删除3个V2模块、15个物理行/9个非空行和2个V2测试文件，同时把2项有效行为测试迁入V1.5并新增1项实际滚动骨架测试；删除前同范围35项、删除后34项通过，full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为279模块/129786非空行，兼容包装由16项降至13项，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 证书注册表包装退役批次：调用图确认 `v2.core.certificate_metrics_registry` 只重新导出V1.5的schema、边界、规范化函数、空注册表和注册表类，唯一调用者是身份兼容断言。实际V1.5实现独立完成schema与边界漂移拒绝、临时文件写入、`fsync`和原子替换、修订快照、审计事件、日期/数值校验及提交复核必填项，并将校准输入、系数拟合/写入、设备I/O、数据库写入和正式证据刷新全部保持为false；工作站对缺失、损坏或未配置证书仍只产生non-blocking警告。本批删除1个V2模块、17个物理行/15个非空行及1项身份断言，并把旧路径加入防复生边界；删除前同范围36项、删除后35项通过，full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为278模块/129771非空行，兼容包装由13项降至12项，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 历史逐帧适配器退役批次：调用图确认 `v2.adapters.historical_frame_parity_audit` 只重新导出共享validation的默认目录、逐帧审计和工件writer，唯一消费者是身份断言；正式 `gas_calibrator.tools.audit_historical_frame_parity` 已直接调用共享实现并独立覆盖参数传递与PASS=0/FAIL=2退出码。实际0620历史证据回归继续确认45/45接受点、22197/22197原始帧解析与端口身份、2698正式采样帧字段差异0、182个源文件哈希和源修改0，同时保持CO₂零气与H₂O干气点分离及pressure-first SENCO9口径。本批删除1个V2模块、15个物理行/13个非空行、1项身份断言和1条陈旧跨边界白名单豁免，并把旧路径加入防复生边界；删除前18项、删除后17项逐帧/CLI/边界回归通过，full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为277模块/129758非空行，兼容包装由12项降至11项，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 转换子模块包装退役批次：调用图确认 `v2.utils.converters` 没有V2内部运行调用，唯一消费者是共享命名空间身份断言；现行V2的8处内部调用和2处测试调用均经必须保留的 `v2.utils` 包入口取得同一组共享函数。逐函数核对 `as_float`、`as_int`、`as_bool`、`parse_first_float`、`parse_first_int`、`safe_get`、`clamp` 和 `format_number` 后，确认数值/整数/布尔解析、首个数值提取、嵌套取值、钳位、默认值及单位格式语义全部由 `gas_calibrator.utils` 唯一实现；删除前后同一54项转换合同、身份、清单和边界测试均通过。本批删除1个V2模块、23个物理行/21个非空行及1项子模块身份断言，并把旧路径加入防复生边界；full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为276模块/129737非空行，兼容包装由11项降至10项，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 初始化/就绪事件兼容组退役批次：调用图确认 `v2.storage.import_v1_5_initialization`、`v2.storage.v1_5_initialization` 和 `v2.storage.import_v1_5_readiness_events` 没有产品运行消费者，只被兼容清单、身份断言和三组行为测试引用；现行初始化数据库、runtime setup及就绪事件实现和CLI均已由 `gas_calibrator.v1_5` 唯一拥有。行为测试没有删除，而是直接改为验证正式V1.5命名空间，删除前后同一32项preview/apply、SN子集与来源、幂等、acknowledgement/allow-write门禁、未成熟结果拒绝及批量事件事务回滚合同全部通过；另有初始化预检/就绪/正式runner/pre-gas/状态聚合108项和full-flow/多分析仪/导出韧性/保护边界129项通过。本批删除3个V2模块、78个物理行/62个非空行，并把旧路径加入防复生边界；parity套件1/1及45/13 dry-run通过，验证数据库仅为pytest临时SQLite。当前V2为273模块/129675非空行，兼容包装由10项降至7项，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 共享存储窄适配器退役批次：调用图确认 `v2.storage.coefficient_store`、`v2.storage.importer`、`v2.storage.queries` 和 `v2.storage.sidecar_index` 均只重新导出共享存储类型；V2的 `import_run` 与 `exporter` 早已直接调用共享importer/query，保留自身离线诊断和报告语义，不随包装删除。V2存储包级兼容API继续保留，但 `_EXPORT_MAP` 已直接延迟加载 `gas_calibrator.storage`；结果网关、sidecar测试支撑和初始化测试的子模块导入同步转向共享命名空间。删除前后同一79项包级身份、系数保存/审批/部署/回滚、工件导入与幂等、历史查询、sidecar文件/SQLite后端和结果网关合同全部通过。本批删除4个V2包装文件共40个物理行/24个非空行；为保持包级延迟兼容增加15个物理行/15个非空行，净减少25个物理行/9个非空行，并把旧路径加入防复生边界；full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过，全部存储验证只使用临时SQLite和离线工件。当前V2为269模块/129666非空行，兼容包装由7项降至3项，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 通用工具包入口退役批次：调用图确认 `v2.utils` 只剩单个23行身份转发文件，8个实际消费模块分布在离线配置、编排器、设备管理、点解析、稳定性、路由规划、采样服务和湿度发生器服务，另有40项参数化转换测试及1项共享身份断言。全部消费者与测试已直接改用 `gas_calibrator.utils`，共享转换文档示例也不再指向旧V2路径；`as_float`、`as_int`、`as_bool`、首个数值提取、嵌套取值、钳位、缺失默认值和单位格式逻辑均未改。删除前后同一105项转换与消费者合同通过，本批删除1个V2模块、23个物理行/21个非空行；对8个触碰文件执行Ruff导入块整理增加16个物理行/9个非空行，净减少7个物理行/12个非空行，并把旧包路径加入防复生边界；full-flow/多分析仪/导出韧性/保护边界129项、parity套件1/1及45/13 dry-run通过。当前V2为268模块/129654非空行，兼容包装由3项降至2项，仅剩共享数据库与模型包装；5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 共享数据库/模型包装退役批次：调用图和类型身份核对确认 `v2.storage.database`、`v2.storage.models` 只转发共享数据库管理、配置/UUID工具及ORM模型；V2包级延迟出口、后处理runner、两层analytics feature builder和现有测试调用已直接转向 `gas_calibrator.storage`，共享schema、事务、外键、UUID、脱敏和存储实现均未修改。完整基线在首次删除后捕获 `analytics.measurement.feature_builder` 的深层旧相对导入并在同批修正，随后同一77项包级/导入/analytics/初始化合同通过，另有1项受控PostgreSQL staging测试因未配置 `V1_5_POSTGRES_STAGING_DSN_TEST` 按合同早期skip；30项数据库事务/授权/preflight、129项full-flow/多分析仪/导出韧性/保护边界、parity套件1/1及45/13 dry-run均通过，数据库验证仅使用pytest临时SQLite。本批删除2个V2包装文件共31个物理行/24个非空行；包级出口、调用迁移和Ruff导入整理增加19个物理行/20个非空行，净减少12个物理行/4个非空行，并把旧路径加入防复生边界。当前V2为266模块/129650非空行，兼容包装由2项降至0项，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- V2存储包级共享门面退役批次：调用图确认 `v2.storage.__init__` 仍把共享数据库、导入器、查询、系数和sidecar类型重新包装成V2包级API，两个V2核心服务及存储测试仍通过该门面调用；`exporter` 与 `import_run` 则分别保留诊断/H₂O报告和V2后处理导入语义，尚不能随包级门面删除。两个核心服务和测试已直接改用 `gas_calibrator.storage`，包根现只作空容器并明确要求产品适配器从具体子模块导入，不再延迟导出任何共享类型或 `StorageExporter`。删除前61项、删除后58项同粒度存储/导入/导出/后处理/服务/边界测试通过，减少的3项是已失效的包根身份转发断言；另有129项full-flow/多分析仪/导出韧性/保护边界、parity套件1/1及45/13 dry-run通过。包根减少45个物理行/43个非空行，两个核心服务的显式共享导入增加2个物理行/2个非空行，V2净减少43个物理行/41个非空行；当前V2仍为266模块/129609非空行，兼容包装保持0，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 无产品调用的V2数据库导出器退役批次：调用图确认 `v2.storage.exporter` 没有产品运行或外部源码调用，仅由2处测试引用；其run/sensor bundle只是从共享数据库重建 `summary.json`、`points.csv`、`samples.csv`、`qc_report.json`、manifest及历史查询副本，而V1/V1.5已保留原始执行工件，共享 `HistoryQueryService` 仍提供全部只读历史查询。科学审计进一步确认，旧“H₂O Calibration Report”只有样本均值/标准差，没有参考真值、示值误差、不确定度、溯源链、CO₂零气与H₂O干气锚点区分、合格判定或签发门禁，却被V2计划标作正式校准报告，存在误导风险；V1.5正式报告链已从冻结证据包生成参考证书、不确定度、QC、release decision、逐台校准/复验证书及哈希清单，是真正的最终产品所有者。本批删除482行导出器和100行陈旧V2报告设计文档，V2报告计划同步改为 `retired_v2_no_formal_exporter`，测试保留共享导入、查询、传感器、系数和V1.5正式报告覆盖；删除前后同一59项存储/报告计划/run manifest/plan compiler/V1.5报告/证书/边界测试通过，另有129项full-flow/多分析仪/导出韧性/保护边界、parity套件1/1及45/13 dry-run通过。V2净减少1个模块、480个物理行/445个非空行，当前为265模块/129164非空行；兼容包装保持0，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；
- 最后一个V2 Python存储导入适配器退役批次：调用图确认 `v2.storage.import_run` 的唯一产品调用者是离线 `v1_postprocess_runner`，独立CLI没有外部源码调用；其真正需要保留的合同仅是raw/enrich/all分段、schema初始化、operator/batch/artifact目录透传、结果摘要和数据库释放。后处理runner现直接延迟加载共享 `DatabaseManager`、`StorageSettings` 与 `ArtifactImporter`，以私有窄函数保留上述合同和SQLAlchemy缺失时的降级边界，删除了第二套参数解析、存储配置覆盖及V2 CLI。删除前后同一48项临时SQLite导入、幂等、缺失工件跳过、analytics bootstrap、失败回退、共享身份和边界测试通过，另有150项full-flow/多分析仪/导出韧性/保护边界增强回归、parity套件1/1及CO₂ 45点/H₂O 13点成熟dry-run通过；全仓仍可收集5075项。V2减少1个模块，净减少91个物理行/74个非空行，当前为264模块/138256物理行/129090非空行；兼容包装保持0，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0。`v2.storage` 现在只保留SQL迁移历史资产，不再提供Python运行API；
- V2存储迁移资产所有权收口批次：逐个审计确认001—004不是可删除的孤立草稿，而是PostgreSQL累计升级历史，依次保存初始存储schema、sensor维表与run/point元数据、V1.5正式SN/设备码回填和身份别名历史；所有SQL均无应用运行调用。四个文件保持原DDL与顺序迁入 `gas_calibrator.storage.migrations`，仅增加共享所有权及受控部署注释；共享README明确禁止应用启动时自动执行、禁止未经数据库变更评审应用到真实库。审计同步发现并锁定为独立P1的ORM—PostgreSQL差异：run mode字段长度、`co2_group`长度、mode/profile复合索引列、fact表 `sensor_id` 删除行为以及部分服务端默认值；本批没有借搬迁修改任何表语义。删除V2 storage说明性包根和1项已失效包初始化测试，新增4项所有权、迁移顺序、终态覆盖、已知差异及临时SQLite建库测试；变更前68项、变更后70项存储/身份/后处理/边界验证通过，另有149项V1.5核心回归、parity 1/1、CO₂ 45点/H₂O 13点成熟dry-run通过，全仓可收集5077项。V2减少1个模块、9个物理行/6个非空行，当前为263模块/138247物理行/129084非空行；保护路径导入V2违规仍为0，未连接或迁移真实数据库；
- 旧离线后处理GUI与目录watcher退役批次：调用图确认 `v1_postprocess_runner` 仍由仓库级no-500历史工具调用，两层analytics也由runner真实延迟加载，因此runner、QC/refit、analytics、本地SQLite和显式下载器全部保留；旧 `v1_postprocess_gui` 则只有自己的CMD启动器和3项测试，界面文字已损坏、仍使用V1旧产品身份，并把“下发到分析仪”默认设为true，和V1.5唯一正式工作站及默认no-write边界冲突。`v1_sidecar_watcher` 没有产品调用、正式入口或文档引用，只由4项自测保活，且会在原run目录回写 `sidecar_status.json`，不符合不可变运行证据方向。本批删除两个V2模块、旧CMD启动器和7项专属测试，没有改runner默认 `download=False`、下载器实现或任何计算公式；删除前同范围62项通过，删除后保留链57项、直接analytics 12项及V1.5核心149项通过，parity 1/1、CO₂ 45点/H₂O 13点成熟dry-run通过，全仓可收集5070项。V2减少2个模块、653个物理行/547个非空行，当前为261模块/137594物理行/128537非空行；保护路径导入V2违规仍为0，未连接设备、真实数据库或改写既有运行目录；
- no-500可执行桥与悬空旧GUI根入口退役批次：调用图确认 `tools.run_v1_no500_postprocess` 没有产品源码调用，现行入口清单已把它标为历史V1参考，正式V1.5校准不能由它启动；有效科学语义早已由 `tools._no500_filter` 独立承载，并被现行corrected-autodelivery直接使用。本批把旧桥的2项测试改为纯函数3项合同测试，逐项锁定仅删除500 hPa、ambient/sealed容差差异及空输入行为，不改正式校准点集或500 hPa科学判定；删除226行旧工具、29行因上一批GUI退役而悬空的根启动器，并移除V2 adapters的1行懒导出，运行源码共减少256个物理行/212个非空行。变更前相关100项中98项通过、2项准确暴露悬空GUI导入，收口后相关99项、入口/边界复验52项、analytics 12项及V1.5核心149项均通过；入口清单重新生成后共有570个入口、历史V1参考由4降至3，全仓可收集5069项。parity 1/1及CO₂ 45点/H₂O 13点成熟dry-run通过，仍使用 `v1_5_legacy_ratio_0613_0620_0621` 内核且证书门禁为non-blocking。V2模块数保持261，仅因懒导出删除降至137593个物理行/128536个非空行；`v1_postprocess_runner`、两层analytics、本地SQLite和显式下载器本批不删除，默认 `download=False` 不变，下一批按整个后处理链联合审计；未连接设备、真实数据库或改写既有运行目录；
- 无产品入口的V1平行后处理runner退役批次：联合调用图确认 `v2.adapters.v1_postprocess_runner` 在no-500桥、旧GUI和根启动器退出后没有任何产品源码调用、文档入口或包级导出，只剩模块级人工CLI和20项自身/私有辅助测试；它会在一次隐式流程中默认执行两层analytics，并可选重做旧V2 QC/refit、导入数据库和显式下载系数，与V1.5正式候选系数、QC、报告、证据角色和写前门禁形成第二套平行编排。其内部能力逐项拆分审计后没有随runner误删：`analyzer_coefficient_downloader` 仍被保留的merged sidecar显式调用；`offline_refit_runner` 仍有独立CLI和包出口；两层analytics仍可作为独立诊断API运行；共享storage继续唯一拥有schema与导入器。本批仅删除runner一个模块及14项runner、4项私有存储包装、1项SQLAlchemy降级包装和1项旧共享身份测试，共减少1474个物理行/1314个非空行；删除前同范围71项、删除后保留能力51项通过，另有入口/边界56项、共享存储43项通过且1项无PostgreSQL staging DSN按合同skip、独立analytics 12项及V1.5核心148项通过。parity 1/1、CO₂ 45点/H₂O 13点成熟dry-run及全仓5049项收集通过，仍使用 `v1_5_legacy_ratio_0613_0620_0621` 内核，证书门禁保持non-blocking；入口清单仍为570项，旧V2参考树由1179降至1178。当前V2为260模块/136119物理行/127222非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；未调用系数下载、COM、真实数据库或设备；
- 旧离线筛选重拟合三模块退役批次：调用图确认 `adapters.offline_refit_runner`、`core.refit_filtering` 和 `config.offline_modeling` 只被adapter懒出口及3项专属测试保活，没有产品源码或正式文档调用。科学审计确认该链不能迁入V1.5：当参考列缺失时会把分析仪自身 `ppm_CO2`/`ppm_H2O` 输出回退成真值，未指定分析仪时会把多台数据合并拟合却标记为第一台身份；固定9参数模型只要求9点，没有rank/condition放行阻断，删点后仍在同一训练集比较RMSE并据此给出“推荐”；同时缺少open-flow/fit角色、证书、独立压力通道、CO₂认证零气和H₂O露点/干气锚点合同，旧默认分箱也不等于当前正式45/13点集。正式 `formal_candidate_coefficients` 已独立拥有逐台身份、fit准入、零气/干气分离、露点参考真值、rank/condition、共同源异常、残差质量和no-write写前门禁，因此没有迁入旧算法或另造兼容层。本批删除3个V2模块、包级配置/core/adapter出口及3项专属测试，共减少942个物理行/818个非空行；删除前同范围75项、删除后72项正式候选/包边界回归通过，候选系数至写前门禁扩展90项及V1.5核心148项通过。parity 1/1、CO₂ 45点/H₂O 13点成熟dry-run及全仓5046项收集通过，仍使用 `v1_5_legacy_ratio_0613_0620_0621` 内核，证书门禁保持non-blocking；入口清单仍为570项，旧V2参考树由1178降至1175。当前V2为257模块/135177物理行/126404非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；未连接设备、数据库或执行任何系数写入；
- 两层孤立analytics退役批次：调用图确认通用和逐帧 `service`、`feature_builder`、`exporter` 及10个mart在平行后处理runner退出后没有产品源码调用，只由13个专属测试和包级懒出口保活；唯一真实消费者是 `adapters.results_gateway` 对 `analytics.sidecar_views` 的只读旁路索引摘要，因此包根和该摘要明确保留。科学审计确认旧分析不能进入V1.5正式结论：它跨不同气体、温度和压力点直接比较分析仪输出均值，把量纲不同的CO₂、H₂O与RMSE绝对差相加为drift score；控制图把不同工况混为一组并用总体方差在两点时即称为可用；measurement quality采用无验证依据的55/25/20权重，drift/anomaly采用固定1%/2%/10%阈值，context attribution还把950—1050 hPa和0—35 ℃写成合法工况，必然误报正式压力扫点及低温校准。字符串关键词故障归因和依赖已退役AI/postprocess工件的traceability同样只能形成启发式展示，不能成为计量证据。共享 `validation.analyzer_health` 已唯一保留有缺证据即不评分的analyzer/instrument health，V1.5正式QC、候选系数和 `core.offline_artifacts` 的现行治理摘要均未修改。本批删除20个V2模块、13个专属测试文件和1项旧包重导出断言，包根只导出sidecar摘要，源码净减少1865个物理行/1646个非空行；删除前同范围26项通过，删除后保留链23项、结果网关37项、正式QC/候选/工件韧性56项及V1.5核心148项通过。parity 1/1、CO₂ 45点/H₂O 13点成熟dry-run及全仓5030项收集通过，内核仍为 `v1_5_legacy_ratio_0613_0620_0621`，证书门禁保持non-blocking，入口清单仍为570项。当前V2为237模块/133312物理行/124758非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；未打开COM、控制气路、连接真实数据库或执行系数写入；
- 实时旁路索引与自动Copilot退役批次：全源码调用图确认共享 `storage.sidecar_index.SidecarIndexStore` 没有任何运行构造方，只由 `analytics.sidecar_views`、`intelligence.review_copilot` 和专属测试消费；`ResultsGateway` 本身没有产品构造方，但仍承担大量历史文件工件兼容合同。旧链允许调用方直接写入任意 `risk_score/risk_level/model release_status/evaluation_metrics`，只做类型整理而没有模型哈希、训练/验证数据集、同工况计量依据、证书溯源或放行签名；Copilot随后把这些内容格式化为“风险摘要、证据缺口、复验建议、model release/rollback”，即使标记reviewer-only也容易形成伪权威。本批保留历史兼容最小面：`ResultsGateway` 只读取已落盘的 `sidecar_index_summary.json`、`review_copilot_payload.json`、`model_governance_summary.json` 或同名analytics_summary段，不再即时构造SQLite/file索引或自动生成结论。删除analytics最后两个模块、`review_copilot`、共享sidecar索引实现和4项专属测试，清理测试支撑中的虚构risk-model数据；V2源码净减少328个物理行/301个非空行，另删除共享索引412个物理行/374个非空行，运行源码合计减少740个物理行/675个非空行。删除前旁路/网关/closeout同范围92项、删除后持久工件兼容链88项通过；剩余intelligence/QC/算法/服务及存储边界28项、V1.5核心148项通过。parity 1/1、CO₂ 45点/H₂O 13点成熟dry-run及全仓5026项收集通过，内核仍为 `v1_5_legacy_ratio_0613_0620_0621`，证书门禁保持non-blocking，入口清单仍为570项。当前V2为234模块/132984物理行/124457非空行，5项原始静态零引用全部可解释，保护路径导入V2违规仍为0；未打开COM、控制气路、连接真实数据库或执行系数写入；
- 聚合结果网关退役批次：方法级调用图确认 `adapters.results_gateway` 的约5060行编排没有任何存活产品构造方，只由自循环测试保活；V1.5计划/QC/结果/报告/复核/设备/算法页直接消费统一 `WorkstationSnapshot`，历史运行目录则由 `scripts.historical_artifacts` 直接调用识别范围、不确定度、方法确认、软件验证和WP6五个窄文件网关。由此删除聚合网关、UI专属artifact-registry包装及其专属测试，并把软件验证traceability从已删除results/reports表面改为persisted/historical artifacts。清单随后暴露的工程隔离门禁条目包装、工程隔离门禁仓库和Step 2收官仓库也只有聚合网关调用；实际evaluator/builder仍由offline governance和历史脚本直接使用，故三个空壳一并删除。compact summary预算收口为唯一`historical`表面，不再保留已退役results_gateway/review_center预算。V2源码净减少5604个物理行/5455个非空行，当前为229模块/127380物理行/119002非空行；清单5项原始静态零引用全部可解释，保护路径导入V2违规仍为0。窄网关/历史目录16项、摘要/治理/阶段合同306项及WP6 114项通过；未打开COM、控制气路、连接真实数据库或执行系数写入；
- 两个孤立阶段工件展示包装退役批次：方法级调用图确认 `stage_admission_review_pack_artifact_entry` 和 `stage3_standards_alignment_matrix_artifact_entry` 没有产品源码、离线治理脚本或历史工件读取方，只由 `test_acceptance_governance.py` 中自身展示断言保活；真正的 `stage_admission_review_pack` 与 `stage3_standards_alignment_matrix` 构建器仍由 `build_offline_governance_artifacts` 直接调用并生成JSON/Markdown。因此本批只删除两个展示转换器及其自循环断言，不删除真实工件、标准家族/证据类别映射或阶段边界合同。V2源码减少476个物理行/451个非空行，当前为227模块/126904物理行/118551非空行；真实工件链和边界40项、V1.5成熟路径专项25项、V1.5正式门禁148项及parity 1/1通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行系数/设备ID写入；
- 阶段桥/工程隔离重复展示包装退役批次：调用图确认 `phase_transition_bridge_reviewer_artifact_entry` 与 `engineering_isolation_admission_checklist_artifact_entry` 同样没有运行时消费者，只在三份测试中把保留工件再次转换为旧展示入口；真正的 `phase_transition_bridge_reviewer_artifact`、`engineering_isolation_admission_checklist`、governance handoff合同和 `build_offline_governance_artifacts` 仍直接生成、持久化并验证JSON/Markdown。测试已删除仅针对包装字段拼装的自循环断言，继续直接验证真实工件的中文标题、角色、阶段、非验收边界和输出路径。V2源码减少231个物理行/217个非空行，当前为225模块/126673物理行/118334非空行；真实治理链、合同与产品边界94项、V1.5正式门禁148项及parity 1/1通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run保持生产内核、profile和non-blocking证书门禁不变，未打开COM、控制气路、连接真实数据库或执行写入；
- Stage 3计划展示包装退役批次：调用图确认 `stage3_real_validation_plan_artifact_entry` 没有产品运行、离线治理或历史读取消费者，只有一组测试把正式计划再次解析成旧卡片；它同时错误拥有七类真实验证标签，使工程隔离评估器和标准对齐矩阵反向依赖展示层。本批把 `reference_instrument_enforcement`、traceability、不确定度、真机acceptance、真实重复性/漂移、pass/fail和异常复测标签归回正式 `stage3_real_validation_plan` 合同，两个消费者改为直接依赖该唯一所有者；正式JSON/Markdown计划、工件目录、离线治理导出、标准家族映射和非真实验收边界全部保留。V2源码净减少355个物理行/332个非空行，当前为224模块/126318物理行/118002非空行；Stage 3/治理/清单/成熟合同166项、V1.5正式门禁148项、parity 1/1和全仓4960项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- 阶段桥presenter收口批次：调用图确认 `phase_transition_bridge_presenter` 不生成独立工件或阶段结论，只把正式桥的 `reviewer_display` 再转换成面板section；离线治理脚本先调用presenter、再调用reviewer artifact，而reviewer artifact内部又调用一次presenter，形成同一桥的重复展示状态源。本批把engineering-isolation/real-acceptance准备文本和非验收警示归入正式 `phase_transition_bridge` reviewer display，由 `phase_transition_bridge_reviewer_artifact` 唯一生成section与Markdown，离线治理直接复用该artifact的section。阶段状态判定、JSON/Markdown文件名、summary/manifest字段、Stage 3计划和真实证据边界全部保留。V2源码净减少161个物理行/135个非空行，当前为223模块/126157物理行/117867非空行；治理/清单/成熟合同280项、V1.5正式门禁148项、parity 1/1和全仓4960项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- 四个replacement快捷包装器退役批次：调用图确认 `verify_v1_v2_h2o_only_replacement`、`verify_v1_v2_skip0_co2_only_diagnostic_relaxed`、`verify_v1_v2_skip0_co2_only_replacement` 和 `verify_v1_v2_skip0_replacement` 均没有运行调用者、包级入口或独占算法，只给底层 `compare_v1_v2_control_flow` 增加固定preset和 `--skip-connect-check`，并由各自两项参数转发测试自保活。删除后首次回归捕获到底层报告仍生成四条已删除命令，已改为指向存活比较器的显式preset调用，并新增四类command-hint合同；底层compare、simulation/replay/parity、历史工件schema和固定profile全部保留，真实compare仍未运行且不获得放行。V2源码净减少80个物理行/40个非空行，当前为219模块/126077物理行/117827非空行；比较/replay/清单/成熟合同103项、V1.5正式门禁148项、parity 1/1和全仓4953项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- workflow-step碎片合并批次：审计确认 `precheck`、`startup`、`temperature_group`、`h2o_route`、`co2_route`、`sampling` 和 `finalize` 七个19—48行子模块并非孤立代码，仍由 `calibration_service` 通过 `workflow_steps` 包入口使用；但仓库内没有任何直接子模块导入，七个文件主要重复类型/import骨架。本批把七个步骤类原样归并进现有 `workflow_steps/__init__.py`，保持 `from gas_calibrator.v2.core.workflow_steps import <Step>` API、步骤顺序、runner调用、采样/QC、状态更新和event bus发布不变，并增加单一包所有者与旧路径防复生合同。V2减少7个模块，净减少38个物理行/26个非空行，当前为212模块/126039物理行/117801非空行；步骤链/路由/清单/成熟合同98项、V1.5正式门禁148项、parity 1/1和全仓4954项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- 领域执行模型碎片合并批次：五个历史证据文件网关继续保持窄职责，本批不做聚合；审计确认 `domain.point_models` 与 `domain.run_models` 只定义四个同属执行上下文的微型数据类，产品公共API本来已由 `gas_calibrator.v2.domain` 导出，且没有V1.5成熟runner或V2设备控制链的直接子模块消费者。本批将四个定义按原字段、默认值和枚举归位到 `domain/__init__.py`，测试统一改用既有包级公共入口，并增加唯一所有者和旧路径防复生合同；校准点目标、运行状态、采样时间及物理流程均未修改。V2减少2个模块，净减少9个物理行/8个非空行，当前为210模块/126030物理行/117793非空行；领域/QC/清单/成熟合同85项、V1.5正式门禁148项、parity 1/1和全仓4956项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- AI解释上下文碎片合并批次：审计否决了整体删除 `intelligence` 的初始候选，因为 `calibration_service`、算法引擎和QC pipeline仍保留可选消费者；AI解释继续不得参与设备控制、系数拟合选择或正式放行。本批只把 `fit_context`、`qc_context`、`run_context` 三个45—58行builder子模块按原数据类、归一化规则和中文动作标签归并进既有 `context_builders/__init__.py`，三个解释器改为依赖包级公共API，并增加唯一所有者与旧路径防复生合同。V2减少3个模块，净减少11个物理行/9个非空行，当前为207模块/126019物理行/117784非空行；AI/QC/算法/清单/成熟合同100项、V1.5正式门禁148项、parity 1/1和全仓4957项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- 孤立V2复核格式器退役批次：调用图、动态入口、配置和正式消费者审计确认，1690行 `review_surface_formatter.py` 在reports页、review center和ResultsGateway退出后已无任何源码调用，只由专属及交叉显示测试保活；V1.5拥有独立 `review_surface.py`，共享taxonomy、fragment、compact-summary builders和阶段证据合同也不依赖该格式器。本批删除旧格式器、专属测试及交叉测试中21项只验证已退出显示层的断言，保留并继续验证persisted taxonomy/fragment keys、共享摘要构建器、中文/英文合同和V1.5正式复核模型；同时删除清单分类器中的失效专名分支并加入旧路径防复生门禁。V2减少1个模块，净减少1690个物理行/1592个非空行，当前为206模块/124329物理行/116192非空行；共享显示合同/V1.5复核面/清单/成熟合同370项、V1.5正式门禁148项、parity 1/1和全仓4936项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- AI建议器碎片合并批次：206模块低引用闭包复核未发现新的安全孤立单元，历史只读入口、simulation suite、校准服务、算法引擎、QC及阶段治理消费者均继续保留；本批只处理 `algorithm_advisor` 与 `anomaly_advisor` 两个同类AI建议器。两者仍被算法引擎和 `AIRuntime` 使用，不能删除；审计确认确定性算法排序及 `best_algorithm` 仍只由 `valid/R²/RMSE` 决定，advisor输出仅进入 `ai_recommendation`、message/warnings，不得改变正式系数、QC或放行。本批把两个类按原fallback文本、排序公式、异常聚合和公共API归并进既有 `advisors/__init__.py`，增加唯一所有者与旧路径防复生合同。V2减少2个模块，净减少9个物理行/7个非空行，当前为204模块/124320物理行/116185非空行；AI/算法/清单/成熟合同95项、V1.5正式门禁148项、parity 1/1和全仓4937项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，未打开COM、控制气路、连接真实数据库或执行写入；
- 平行V2温度补偿闭包退役批次：全源码调用图确认 `v2.calibration.temperature_compensation`、其包入口和 `v2.export.temperature_compensation_export` 没有产品运行消费者，只由2项专属测试自保活；V1/V1.5正式runner、温度通道复核和merged sidecar始终直接使用共享 `calibration.temperature_compensation_fit` 与 `export.temperature_compensation_export`。删除前对正常三次拟合、两点降阶、单点、非法值和空输入5组结果逐字段比较，V2与共享拟合完全一致；共享实现继续唯一保留中性 `(A,B,C,D)=(0,1,0,0)` 回退、最多三次拟合、安全降阶、SENCO7/SENCO8格式化、观测/结果工件及正式导出。本批删除3个V2源码模块、2项孤立自测并清理V2导出包重导出，旧路径加入防复生门禁；V2净减少338个物理行/292个非空行，当前为201模块/123982物理行/115893非空行。删除前温补及正式链55项、删除后共享温补/正式链/产品导出/边界62项与导出专项33项均通过，V1.5正式门禁148项、parity 1/1和全仓4935项收集通过，入口清单仍为570项。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking；未修改 `run_app.py`、成熟runner、默认配置、设备层或V1.5 full-flow，未打开COM、控制气路、连接真实数据库或执行系数/设备ID写入；
- AI解释器碎片合并批次：低引用审计首先否决删除6672行 `run001_a2_no_write` 的候选，因为artifact service、finalization runner、orchestrator、no-write guard和气路服务仍保留A2工程探针的终态证据及安全合同；本批不撕裂Step 3A边界。随后确认 `fit_explainer`、`qc_explainer`、`run_explainer` 三个活跃模块的所有源码和测试消费者本来只经 `gas_calibrator.v2.intelligence.explainers` 公共入口调用，没有直接子模块依赖。本批把三个类按原提示词路径、Mock/LLM fallback、QC规则文本、缓存键、上下文builder和公共方法归并进现有 `explainers/__init__.py`，增加唯一所有者及旧路径防复生合同；第一次格式展开导致源码反增58行后立即收回，最终同时实现模块与代码体积下降。V2减少3个模块，净减少16个物理行/13个非空行，当前为198模块/123966物理行/115880非空行；删除前AI/QC/算法/边界34项、归并后36项通过，V1.5正式门禁148项、parity 1/1和全仓4937项收集通过，入口清单仍为570项，旧子模块引用为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking且证据仍为dry-run only；未修改 `run_app.py`、成熟runner、默认配置、设备层或V1.5 full-flow，未打开COM、控制气路、连接真实数据库或执行系数/设备ID写入；
- AI提示词资产收口批次：四份历史模板逐一核对运行时文件路径、静态导入、目录遍历、资源API和仓库打包配置后，确认 `algorithm_recommend.txt` 与 `report_summary.txt` 分别由保留的Fit/Run解释器直接读取，必须保留；`anomaly_diagnosis.txt` 与 `qc_explain.txt` 从未被提示词加载链使用，其中前者同名字符串只属于运行结果工件名，不是模板引用。`prompts/__init__.py` 的 `PROMPTS_DIR/load_prompt()` 同样没有消费者，仓库也没有要求该资源目录成为Python包的打包清单。本批把目录收口为只含两份活跃模板的纯资源目录，删除1个无用Python模块和2份废弃模板，并增加显式两文件资产清单、活跃模板实际加载及旧包装防复生合同。V2运行源码减少13个物理行/6个非空行，另减少35个物理行/27个非空行模板资源，当前为197模块/123953物理行/115874非空行、263个V2文件；专项37项、V1.5正式门禁148项、parity 1/1和全仓4938项收集通过，入口清单仍为570项，未解释静态零引用为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking且证据为dry-run only；未修改 `run_app.py`、成熟runner、默认配置、设备层或V1.5 full-flow，未打开COM、控制气路、连接真实数据库或执行系数/设备ID写入；
- 五个只读历史证据网关归并批次：调用图确认 `method_confirmation_gateway`、`recognition_scope_gateway`、`software_validation_gateway`、`uncertainty_gateway` 和 `wp6_gateway` 各自只有历史工件CLI一个源码消费者，且既有 `gas_calibrator.v2.adapters` 包入口已经是公共所有者；五类职责均为按固定文件名读取persisted JSON并原样组织reviewer payload，不拥有设备控制、校准算法、拟合、正式放行或写入。为保持 `adapters` 普通导入轻量，本批把五个类归并到包入口后仍在构造实例时才延迟导入对应repository；文件名、缺失文件异常、payload键、旧工件读取语义和历史CLI均保持不变。新生成工件的 `generated_by_tool` 从已删除的子模块路径统一改为真实存在的 `gas_calibrator.v2.adapters`，已存历史值仍按原JSON透传；新增唯一包所有者及五条旧路径防复生合同。V2减少5个模块，净减少74个物理行/70个非空行，当前为192模块/123879物理行/115804非空行、258个V2文件；归并基线136项、归并后137项、V1.5正式门禁148项、parity 1/1和全仓4939项收集通过，入口清单仍为570项，原始静态零引用2项均已解释、未解释项为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，安全标志为no-write/不开COM/不控气路/不写系数和设备ID，且证据明确不是real acceptance；未修改 `run_app.py`、成熟runner、默认配置、设备层或V1.5 full-flow；
- AI运行时组装碎片归并批次：192模块低引用交叉筛选首先排除60行 `coefficient_service` 和Step 3A模块，前者仍在正式系数候选链，后者仍承载工程探针安全合同；本批只处理49行 `intelligence.runtime`。调用图确认 `AIRuntime` 只有校准服务一个源码消费者、没有直接子模块导入者，公共API本来已由 `gas_calibrator.v2.intelligence` 导出；该类只按AIConfig组装LLM client、Summarizer、QCExplainer和两个advisor，不参与确定性算法排序、QC判定、系数、设备或正式放行。本批将类按原字段、默认配置、client参数、组件构造顺序和 `feature_enabled` 语义归并进现有包入口，删除独立runtime模块并增加唯一所有者与旧路径防复生合同。V2减少1个模块，净减少4个物理行/5个非空行，当前为191模块/123875物理行/115799非空行、257个V2文件；归并前63项、归并后64项、V1.5正式门禁148项、parity 1/1和全仓4940项收集通过，入口清单仍为570项，旧runtime引用为0，未解释静态零引用为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书门禁保持non-blocking，安全标志为no-write/不开COM/不控气路/不写系数和设备ID，且证据明确不是real acceptance；未修改 `run_app.py`、成熟runner、默认配置、设备层或V1.5 full-flow；
- 证书准入仿真适配碎片归并批次：191模块交叉筛选排除了正式系数服务、A2 hooks、CLI安全锁和R0入口；本批只处理54行 `sim.certificate_operational_admission`。调用图确认该函数只有simulation suite一个源码消费者和一组共享证书测试消费者，且消费者原本均可通过 `gas_calibrator.v2.sim` 公共入口访问；真实证书合同、业主确认资料、SHA-256锁定复核、准入判断和工件写入均由共享 `gas_calibrator.validation.certificate_operational_admission` 唯一拥有。本批只把V2离线报告适配函数归并进现有 `sim` 包入口，并把共享validation依赖保留为函数调用时延迟导入；`PASSED_WITH_OWNER_ATTESTATION`、operational gate、strict original gate、offline progress、五类工件角色和失败判断均未修改。V2减少1个模块，净减少13个物理行/10个非空行，当前为190模块/123862物理行/115789非空行、256个V2文件；共享证书及边界20项、完整regression 22/22、V1.5正式门禁148项、parity 1/1和全仓4941项收集通过，入口清单仍为570项，旧子模块引用为0，未解释静态零引用为0。regression明确保持strict original gate=false、ready_for_real_execution=false、promotion_state=dry_run_only和ready_for_promotion=false；CO₂ 45点/H₂O 13点成熟dry-run继续使用生产内核，证书启动门禁仍为non-blocking。未修改 `run_app.py`、成熟runner、默认配置、设备层、V1.5 full-flow或共享证书判断，未打开COM、控制气路、连接真实数据库或执行写入；
- 仿真设备规格模型碎片归并批次：190模块交叉筛选继续排除正式系数服务、独立replay CLI、A2 hooks和R0安全入口；本批只处理147行 `sim.devices.models`。调用图确认十个冻结data class只由 `sim.devices` 包入口重新导出，唯一源码消费者是场景目录，各fake设备、device factory和测试均使用包级API；这些类型只描述仿真协议、通道数、单位、故障、跳过策略和设备矩阵，不拥有真实COM、气路、系数或放行能力。本批将十个类按原字段、默认factory、冻结语义、嵌套fault序列化、device_overrides deep-copy和 `to_dict()`结果归并进现有 `sim.devices` 包入口，并将fake设备导入保持在类型定义之后；新增唯一所有者与旧路径防复生合同。V2减少1个模块，净减少10个物理行/12个非空行，当前为189模块/123852物理行/115777非空行、255个V2文件；归并前68项、归并后完整相关回归69/69、smoke 6/6、V1.5正式门禁148项、parity 1/1和全仓4942项收集通过，入口清单仍为570项，旧模型路径引用为0，未解释静态零引用为0。smoke中的全流程、继电器卡滞、温度参考陈旧、压力参考降级、动态合同和summary parity均保持预期；CO₂ 45点/H₂O 13点成熟dry-run继续使用生产内核和profile。未修改 `run_app.py`、成熟runner、默认配置、真实设备层或V1.5 full-flow，未打开COM、控制气路、连接真实数据库或执行写入；
- AI解释服务碎片归并批次：189模块低引用交叉筛选继续排除60行正式系数服务、独立replay CLI、A2 hooks、CLI安全锁和R0入口；本批只处理132行 `core.services.ai_explanation_service`。调用图确认 `AIExplanationService` 的源码消费者本来只经 `gas_calibrator.v2.core.services` 包入口使用，且服务仅在AI启用且存在运行目录时读取既有结果、生成 `anomaly_diagnosis.txt/json` 和摘要工件；失败点、告警类别、最多100条I/O日志解析、异常记录后非阻断返回等语义均保持不变，不拥有设备控制、系数、校准输入或正式放行权限。本批将类归并进既有services包入口，删除独立模块，并增加唯一所有者和旧路径防复生合同。V2减少1个模块/1个文件，净减少9个物理行/11个非空行，当前为188模块/123843物理行/115766非空行、254个V2文件；归并前31项、归并后32项、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4943项收集通过，入口清单仍为570项，旧子模块引用为0，2项原始静态零引用均已解释且未解释项为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 基础领域模型碎片归并批次：188模块低行数审计首先排除17行 `core.runners.route_run_result`，因为runners是无 `__init__.py` 的命名空间包，强行新增入口既不减模块又会改变导入拓扑；正式系数服务、replay CLI和Step 3A安全模块继续不动。本批只处理 `domain.enums`、`domain.result_models`、`domain.sample_models` 和 `domain.qc_models` 四个基础类型文件。它们只定义4个Enum和6个dataclass，无文件I/O、设备、拟合、系数或放行副作用，十个类型本来均由 `gas_calibrator.v2.domain` 公共入口导出。本批按枚举、原始样本、结果、QC的依赖顺序归并到包入口，保持全部字段、默认factory、枚举值、Optional/前向类型和类身份语义；算法模型及QC pipeline改用包级API，其他领域模块导入放在基础类型定义之后以避免循环。新增十个类型唯一所有者及四条旧路径防复生合同。V2减少4个模块/4个文件，净减少22个物理行/18个非空行，当前为184模块/123821物理行/115748非空行、250个V2文件；归并前71项、归并后72项、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4944项收集通过，入口清单仍为570项，旧子模块引用为0，2项原始静态零引用均已解释且未解释项为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 算法结果类型与注册表碎片归并批次：184模块低行数审计继续排除无法减少模块的 `core.runners.route_run_result`、导出包入口、正式系数服务、replay CLI和Step 3A安全模块；本批只处理31行 `algorithms.result_types` 与39行 `algorithms.registry`。前者只定义 `ValidationResult`、`ComparisonResult` 并转发领域 `FitResult`，后者只维护进程内算法类/实例注册和默认算法清单；两者无文件I/O、设备、系数写入或正式放行副作用，公共API本来已由 `gas_calibrator.v2.algorithms` 导出。本批把两个结果dataclass和注册表归并到算法包入口，`FitResult` 仍由 `domain.algorithm_models` 唯一拥有；基础结果类型先定义、`AlgorithmBase`及具体算法再延迟导入，以保持循环安全。线性、多项式、AMT、稳健算法、反向验证、排名指标、缓存键和默认注册顺序均未修改。新增三个归并类型唯一所有者及两条旧路径防复生合同。V2减少2个模块/2个文件，净减少3个物理行/5个非空行，当前为182模块/123818物理行/115743非空行、248个V2文件；归并前28项、归并后29项、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4945项收集通过，入口清单仍为570项，旧子模块引用为0，1项原始静态零引用已解释且未解释项为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 编排上下文碎片归并批次：182模块低行数审计确认 `core.orchestration_context` 只定义一个冻结dataclass，字段用于汇集config、session、state/event/result/log/device/stability依赖及stop/pause event，唯一方法只是只读转发 `result_store.data_writer`；它不执行设备控制、文件I/O、系数、拟合或放行。本批将 `OrchestrationContext` 归入已有lazy `gas_calibrator.v2.core` 包，全部具体类型只在 `TYPE_CHECKING` 下导入，避免把设备和状态模块变成eager import；13个服务/仿真/编排源码消费者及13个测试消费者改用包级API，字段、冻结语义、构造顺序和data-writer身份保持不变。新增唯一所有者和旧路径防复生合同。V2减少1个模块/1个文件，物理行净减少2、非空行保持不变，当前为181模块/123816物理行/115743非空行、247个V2文件；归并前直接消费者112项、归并后113项、快速核心17项、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4946项收集通过，入口清单仍为570项，旧子模块引用为0，1项原始静态零引用已解释且未解释项为0。测试执行中先后暴露了不存在测试文件参数、两次长组超时和一次owner测试插入位置错误；均未触及产品逻辑，最终按普通服务41项、编排/simulation/calibration 52项及压力/阀路20项完整通过收口。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 解释领域模型碎片归并批次：181模块审计继续排除无模块减量的入口文件、实际算法公式、控制服务、正式系数服务、replay CLI和Step 3A安全模块；本批只处理85行 `domain.explanation_models`。该文件只定义 `Recommendation`、`AlgorithmRecommendation`、`PointRejection` 和 `RunExplanation` 四个dataclass及确定性英文解释/报告渲染，唯一产品源码消费者是 `AlgorithmEngine`，不会选择正式系数、改变QC、设备控制或放行；四类公共API本来已由 `gas_calibrator.v2.domain` 导出。本批把四个类型归并到domain包入口，保持字段、默认factory、排名/原因顺序、QC两位小数、报告行顺序和所有 `explain()/to_report()` 输出；算法引擎只改用包级导入。新增四类唯一所有者及旧路径防复生合同。V2减少1个模块/1个文件，净减少5个物理行/4个非空行，当前为180模块/123811物理行/115739非空行、246个V2文件；归并前23项、归并后24项、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4947项收集通过，入口清单仍为570项，旧子模块引用为0，1项原始静态零引用已解释且未解释项为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 运行模式领域模型碎片归并批次：180模块审计继续排除无模块减量的入口文件、实际算法/控制实现、正式系数服务、replay CLI和Step 3A安全模块；本批只处理102行 `domain.mode_models`。该文件只定义 `RunMode` 枚举、固定别名归一化、中文标签和冻结 `ModeProfile`，源码消费者仅为plan model与plan compiler；它不启动校准模式、设备、气路或报告写入，公共API本来已由 `gas_calibrator.v2.domain` 导出。本批将枚举、别名表、归一化/标签函数及profile归并到domain包入口，保持所有17个别名、未知值default回退、route trim/lower、`co2_only/h2o_only/h2o_then_co2`推导、正式报告显式覆盖、默认仅自动校准启用以及 `to_dict()`省略规则；plan model与compiler只改包级导入。新增类型/函数唯一所有者和旧路径防复生合同。V2减少1个模块/1个文件，净减少6个物理行/5个非空行，当前为179模块/123805物理行/115734非空行、245个V2文件；归并前25项、归并后26项、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4948项收集通过，入口清单仍为570项，旧子模块引用为0，1项原始静态零引用已解释且未解释项为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 路由执行结果模型碎片归并批次：179模块审计重新处理此前因 `core.runners` 没有包入口而排除的17行 `route_run_result`，没有为减文件强行新增聚合层，而是确认其唯一类型 `RouteRunResult` 本来只依赖 `core.models.CalibrationPoint`，且只被CO₂/H₂O runner及两个测试消费者使用。该类型仅承载成功、完成点、采样点、跳过点、停止和错误状态，无设备、气路、文件、数据库、拟合、系数或放行副作用。本批将其收归已有 `core.models`，保持可变dataclass语义、全部八个字段、五个独立列表default factory以及 `stopped/error` 默认值；两个runner只改导入，不改任何执行分支。新增唯一所有者与旧路径防复生合同。V2减少1个模块/1个文件，净减少7个物理行/5个非空行，当前为178模块/123798物理行/115729非空行、244个V2文件；归并前28项、归并后29项、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4949项收集通过，入口清单仍为570项，旧导入引用为0，1项原始静态零引用已解释且未解释项为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 进程内事件总线碎片归并批次：178模块审计排除压力选择物理语义、CSV原子落盘、实际拟合算法、正式系数服务、replay CLI和Step 3A安全模块，本批只处理61行 `core.event_bus`。该文件只定义9值 `EventType`、冻结 `Event` 和使用 `RLock` 的进程内 `EventBus`，不执行设备、气路、文件、数据库、拟合、系数或放行；三项API本来已由lazy `gas_calibrator.v2.core`公共入口转发。本批把三项定义原样归入core包，保持枚举值、时间戳生成时机、去重订阅、身份退订、锁内快照/锁外回调和clear语义；11个源码消费者与20个测试消费者只改包级导入，`OrchestrationContext`直接引用同一所有者。新增三项唯一所有者和旧路径防复生合同。V2减少1个模块/1个文件，净减少6个物理行/5个非空行，当前为177模块/123792物理行/115724非空行、243个V2文件；归并前108项、归并后109项、核心服务/编排/状态导入循环检查、导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4950项收集通过，入口清单仍为570项，旧导入引用为0，1项原始静态零引用已解释且未解释项为0。新增实现全规则Ruff和机械导入排序检查通过；全文件Ruff另暴露 `orchestrator.py` 既有未用导入及 `status_service.py` 既有不可达未定义名，均不在本批顺带修改，列为发布前P1静态债务。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 核心编排/状态静态债务关闭批次：Gate 67记录的12项Ruff债务经AST与调用检索逐项复核，`orchestrator.py` 中标准库、工具函数、异常、报告导出、压力等待结果和稳定性枚举共11个导入均无符号引用，也不独占任何注册/初始化职责，本批直接移除而不改编排方法。`status_service.py` 的 `return str(value)` 并非普通冗余：HEAD与blame确认它从初始导入起就错位在 `_progress_point_key()` 必然返回之后，使 `_json_safe_value()` 遇到Path之外的未知证据对象时静默返回`None`。本批把该语句恢复到JSON安全转换的最终兜底，并新增真实 `route_trace.jsonl` 测试，锁定自定义证据对象导出为稳定字符串而不是 `null`。模块/文件数保持177/243，V2源码净减少10个物理行/10个非空行，当前为123782物理行/115714非空行；修复前34项、修复后35项编排/状态/集成/边界测试通过，两个源文件和新增测试全规则Ruff归零。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4951项收集通过，入口清单仍为570项，兼容包装与保护路径导入违规均为0；Gate 67两组静态债务已关闭。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 路由上下文状态模型碎片归并批次：177模块审计确认79行 `core.route_context` 只定义可变内存 `RouteContext`，唯一源码导入者是编排器，五个测试/支撑文件直接构造它；其本身只依赖 `core.models` 已有的 `CalibrationPhase`、`CalibrationPoint`、`Any/Optional` 和dataclass field，不含设备、气路、文件、数据库、拟合、系数或放行副作用。本批将其收归已有 `core.models`，保持全部8个字段、独立 `route_state` default factory、route trim/lower、retry非负约束、source/active默认继承、current-point联动、route-state增量合并和clear原地清空语义；编排器与测试只改导入，CO₂/H₂O/温度组runner调用对象和顺序均不变。新增唯一所有者及旧路径防复生合同。V2减少1个模块/1个文件，净减少6个物理行/4个非空行，当前为176模块/123776物理行/115710非空行、242个V2文件；归并前33项、归并后34项、机械修复后14项快速复验和变更文件全规则Ruff通过。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4952项收集通过，入口清单仍为570项，旧导入为0，兼容包装与保护路径导入违规均为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 运行会话状态模型碎片归并批次：176模块低行数审计继续排除实际拟合算法、CSV原子落盘、正式系数服务、Step 3A安全钩子和独立CLI，本批只处理114行 `core.session`。调用图和配置反向依赖审计确认 `RunSession` 仅保存单次运行的时间、阶段、点位、进度、启用设备名称、输出目录路径、告警和错误，并提供确定性序列化；它不创建目录、不读写文件、不打开设备、不控制气路、不拟合或写入系数，也不参与正式放行。配置类型改为 `TYPE_CHECKING` 导入后，将类收归已经统一承载 `CalibrationPhase`、`CalibrationPoint`、`RouteRunResult` 和 `RouteContext` 的 `core.models`；全部字段、时间戳格式、启用设备筛选、Path构造、dataclass序列化和未知点位兜底保持不变，公共 `gas_calibrator.v2.core.RunSession` 仍解析为同一类对象。新增唯一所有者及旧路径防复生合同。V2减少1个模块/1个文件，净减少5个Python物理行/4个非空行，当前为175模块/123771物理行/115706非空行、241个V2文件；归并前117项直接消费者、归并后含边界门禁125项和变更文件全规则Ruff通过。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4953项收集通过，入口清单仍为570项，旧导入为0，兼容包装、保护路径导入违规和未解释静态零引用均为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- Conditioning类型解析债务关闭批次：Gate 70全V2静态审计的19项F821全部集中在5452行 `core.services.conditioning_service`，均为从编排器抽取服务后未补回的 `CalibrationPoint` 和 `Iterable` 类型符号。由于模块使用延迟注解，47项既有直接消费者基线仍通过，但 `typing.get_type_hints()` 可稳定复现 `NameError: CalibrationPoint is not defined`，证明它不是纯显示告警。本批只从无反向依赖的 `core.models` 补回 `CalibrationPoint`，并从 `collections.abc` 补回 `Iterable`；不修改任何conditioning分支、压力阈值、时间预算、SENCO9压力优先、阀路或设备调用。新增门禁遍历 `ConditioningService` 全部67个带注解方法并要求类型提示可完整解析。V2模块/文件保持175/241，源码增加2个导入物理行/2个非空行，当前为123773物理行/115708非空行；直接消费者与新门禁48项、变更文件全规则Ruff和全V2 F821专项门禁通过，F821由19降为0，全V2剩余静态债务由145项/34文件降至126项/33文件。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4954项收集通过，入口清单仍为570项，兼容包装、保护路径导入违规和未解释静态零引用均为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 重复字典键语义收口批次：Gate 71剩余4项F601分布在 `core.run001_a2_no_write` 与 `export.ratio_poly_report`。两项正压预密封最大/最小压力和两项CO₂通大气conditioning工件路径前后表达式完全相同；`PointIntegrity` 的简单“有帧/无帧”值会被后面的fleet完整性函数覆盖。为同时保持当前最终值与Python字典首次插入顺序，本批删除后出现的相同压力/路径键，并把精确 `PointIntegrity` 计算移到首次位置后删除覆盖项；不改变证据字段值、JSON顺序、Excel列序、压力计算或A2 no-write逻辑。修复前报告/summary/parity相关13项通过；正压证据的压力键位置48/49、153键总数以及最小A2包23个文件中readiness/manifest/summary的关键路径位置和值均已冻结。修复后新增AST literal-key唯一性、压力键值/顺序和 `PointIntegrity` 值/列序门禁，相关15项通过；前后压力位置/值/总键数与23文件工件路径位置和值完全一致。V2模块/文件/源码行数保持175/241/123773/115708，F601由4降为0，全V2剩余静态债务由126项/33文件降至122项/32文件；`run001_a2_no_write` 未修改时序段仍有2项既有F841，未混入本批。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4956项收集通过，入口清单仍为570项，兼容包装、保护路径导入违规和未解释静态零引用均为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 重复定义与WP6导入顺序收口批次：Gate 72剩余6项F811包括配置模型的 `Path` 重复绑定、`artifact_compatibility` 两个同名工件函数和 `wp6_builder` 三个同名导入/构建函数。AST比较确认五组函数前后实现不完全相同，但Python模块最终只暴露后定义；源码调用、别名、默认参数、注册表和顶层执行审计均未发现第一份函数对象被捕获。修改前对当前后定义运行的WP6 114项、配置/历史工件21项基线通过，但干净解释器直接导入 `wp6_builder` 会因未使用的 `recognition_readiness_artifacts` 反向导入触发循环导入，证明测试顺序曾掩盖真实导入缺陷。本批冻结五个最终实现AST SHA-256后，机械删除五份第一定义，移除配置模块无效顶层 `Path` 和WP6未使用反向导入；写回后五个保留实现AST哈希逐项完全一致。新增每个公共函数唯一绑定、配置Path单一导入和干净解释器WP6导入门禁；修复后138项直接消费者通过，单独导入不再依赖测试顺序。V2模块/文件保持175/241，净减少421个物理行/406个非空行，当前为123352物理行/115302非空行；F811由6降为0，F401同步减少2项，全V2剩余静态债务由122项降至114项/32文件。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4959项收集通过，入口清单仍为570项，兼容包装、保护路径导入违规和未解释静态零引用均为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，V1 fallback保留，且证据明确不是real acceptance；
- 固定展示文本f-string卫生收口批次：Gate 73剩余6项F541逐项求值与下游调用审计确认，`human_governance_artifacts` 的1条placeholder明细和 `recognition_readiness_artifacts` 的5条固定reviewer action均不含占位符、花括号转义或运行期拼接依赖。本批只删除6个冗余 `f` 前缀，字符内容、中文文案、route插值分支、证据边界和工件结构均不变；新增AST门禁排除格式说明符节点后禁止目标文件重新出现无插值f-string，并用实际builder输出和五维reviewer action精确文本断言冻结结果。相关17项测试和全V2 F541专项Ruff通过，F541由6降为0；V2模块/文件/源码行数保持175/241/123352/115302，全V2剩余静态债务由114项/32文件降至108项/31文件，仅余F401 70项、E402 24项和F841 14项。全仓可收集4962项，清单兼容包装、保护路径导入违规和未解释静态零引用仍为0；本批不改校准、设备、气路、数据库、系数、设备ID、成熟runner或 `run_app.py`；
- 未使用局部变量与WP6私有重复实现收口批次：Gate 74剩余14项F841逐项按右值副作用、调用关系和证据用途审计。10项属于无副作用的死归一化、死列表、死时间戳或死提示变量，删除后不改变路由点序、传感器重试、正压预密封诊断或工件输出；2项暴露真实只读治理缺口，现将方法确认运行集的精简route/measurand/status/reviewer边界写入软件验证证据索引，并将已合并的 `defer_to_stage3_real_validation` 清单写回Stage 3计划raw工件，继续明确reviewer-only、非真实acceptance。WP6剩余2项位于7个被同名后实现覆盖的私有函数中；删除连续525行旧定义前冻结七个保留实现AST SHA-256，删除后哈希逐项完全一致，并把私有函数纳入单定义防复生门禁。修复前236项、修复后240项直接测试通过，F841由14降为0；V2模块/文件保持175/241，净减少525个物理行/499个非空行，当前为122827物理行/114803非空行，全V2静态债务由108项/31文件降至94项/25文件，仅余F401 70项和E402 24项。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4963项收集通过，入口清单仍为570项，兼容包装、保护路径导入违规和未解释静态零引用均为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，且全部证据仍明确不是real acceptance；
- 未使用导入与显式再导出收口批次：Gate 75剩余70项F401分布在23个V2历史模块。本批没有执行批量 `--fix`，而是逐项检查AST符号使用、仓库直接导入消费者、模块别名属性访问、`__all__`、来源模块顶层执行副作用和干净解释器导入。初次精确删除后，干净解释器立即暴露 `recognition_readiness_artifacts.STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME` 是artifact compatibility、offline artifacts和WP6测试依赖的模块属性再导出；该名称恢复为显式同名再导出，其他69项均无支持中的再导出、注册或加载副作用用途。新增门禁要求23个受影响模块的导入绑定必须被使用或显式再导出，固定closeout Markdown文件名身份，并在干净解释器一次导入全部23模块。相关运行/工具、治理/工件、smoke/parity/nightly共414项直接测试通过，其中nightly单测独立运行149秒通过；F401由70降为0。V2模块/文件保持175/241，净减少61个物理行/61个非空行，当前为122766物理行/114742非空行；全V2静态债务由94项/25文件降至24项/3文件，仅余E402。导出/工件韧性8项、V1.5正式门禁148项、parity 1/1和全仓4966项收集通过，入口清单仍为570项，兼容包装、保护路径导入违规和未解释静态零引用均为0。CO₂ 45点/H₂O 13点成熟dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking；安全字段确认no-write、不开COM、不控气路、不写系数/设备ID、不改成熟runner或 `run_app.py`，且证据明确不是real acceptance；
- 后置导入顺序与公共身份收口批次：Gate 77对最后24项E402逐文件审计。4个识别就绪构建器只依赖标准库，13个服务子模块依赖 `core` 公共上下文而不反向依赖 `core.services` 聚合包，7个仿真假设备子模块也不依赖包内规格定义，因此三处后置导入均非循环依赖隔离，可安全上移到模块导入区。本批只移动既有导入，不改变类、函数、常量、仿真注册、设备协议或校准执行逻辑；新增门禁要求导入位于运行期定义之前、6个代表性公共再导出保持对象身份，并在干净解释器中按正序/逆序加载三组模块。回归同时发现WP6合同仍以源码字符串要求已删除的私有死别名，现改为验证 `historical_artifacts` 直接复用共享payload提取器对象，不重新引入F401。受影响服务、识别就绪、WP3/WP5/WP6及仿真设备290项通过，E402由24降为0；F811、F541、F841、F401、E402、F821、F601七类收敛静态门禁现均为0。V2保持175模块/122766物理行，非空行因服务导入格式化为114745；全仓可收集4969项，库存兼容包装、保护路径导入违规和未解释静态零引用均为0。V1.5正式门禁148项、导出/工件韧性8项、parity 1/1、nightly单测和CO₂ 45点/H₂O 13点成熟dry-run通过，入口清单仍为570项；dry-run继续使用 `v1_5_legacy_ratio_0613_0620_0621` 与 `legacy_ratio_production`，证书启动门禁保持non-blocking，且明确不是real acceptance；本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner或 `run_app.py`；
- 领域服务聚合门面退役批次：Gate 78从最新30个 `delete_after_extraction` 候选中逐个比较物理语义、调用数和运行风险，排除压力选择、workflow steps与系数服务后，选择53行 `domain.services` 包级再导出门面。调用图确认该文件不含业务实现、无测试直接消费者、无仓库外源码消费者；唯一运行引用是 `offline_artifacts` 对频谱摘要builder的包级导入，其余仿真与测试本来已直连七个实际服务模块。干净解释器基线证明旧门面会为一次离线工件导入同时加载7个服务模块；本批将唯一消费者改为直连 `spectral_quality_engine` 后删除聚合 `__init__.py`，子模块继续作为namespace package按原完整路径导入。新增门禁锁定门面文件不复生、频谱builder对象身份不变，并要求干净解释器只加载频谱实际所有者而不加载另外6个大型分析服务。相关频谱、离线工件、动态计量、系统辨识、分析仪asset/bench/uncertainty/envelope基线86项，修改后87项通过；V2由175降至174模块，`delete_after_extraction`由30降至29，源码净减少53个物理行/52个非空行，当前为122713/114693。全仓可收集4970项，七类静态门禁、兼容包装、保护路径导入违规和未解释静态零引用均为0。V1.5正式门禁148项、导出/工件韧性8项、parity 1/1、nightly单测及CO₂ 45点/H₂O 13点成熟dry-run通过，入口清单仍为570项；dry-run内核和profile不变，证书启动门禁保持non-blocking且明确不是real acceptance。本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 系数报告工件服务归并批次：Gate 79比较最小候选后，确认 `pressure_selection` 仍由8个运行模块使用并承载ambient/sealed物理语义，`workflow_steps` 仍控制预检、初始化、温箱、CO₂/H₂O路由和采样顺序，均不得删除；60行 `coefficient_service` 则只由orchestrator构造一次并委托一次，而同一系数报告本来已由 `ArtifactService` 负责角色、状态和工件编排，实际拟合与Excel生成仍由正式系数及 `ratio_poly_report` 所有。迁移前冻结 `export_coefficient_report` 完整AST SHA-256，方法原样移入 `ArtifactService` 后哈希仍为 `c5df9f59ddd7e438dff230e138e1095c7f0cbb88e308e1a57ee2a38a957e773a`；同时删除第二个服务对象、orchestrator委托方法、公共再导出及旧模块，工件回调改为直接调用唯一所有者。测试迁为artifact coefficient export合同并新增旧模块/公共符号防复生门禁；编排、ratio-poly报告、离线工件、manifest、导出韧性和导入相关48项通过。V2由174降至173模块，`delete_after_extraction`由29降至28，运行源码净减少19个物理行/13个非空行，当前为122694/114680；全仓可收集4971项，七类静态门禁、兼容包装、保护路径导入违规和未解释静态零引用均为0。V1.5正式门禁148项、导出/工件韧性8项、parity 1/1、nightly单测及CO₂ 45点/H₂O 13点成熟dry-run通过，入口清单仍为570项；拟合算法、报告字段、压力优先流程、dry-run内核/profile和non-blocking证书门禁均未改变，且证据明确不是real acceptance。本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 算法运行合同所有权归并批次：Gate 80逐类型审计191行 `domain.algorithm_models`。六个类中只有 `AlgorithmSpec` 和 `FitResult` 被V2算法运行源码消费；`FitPoint`、`FitDataset`、`FitInput` 与 `CoefficientSet` 没有任何运行消费者，只由旧domain再导出和三项专属自测形成自循环，正式V1.5候选系数、ratio-poly拟合及共享存储均使用各自独立合同。迁移前冻结两个活跃类AST SHA-256，原样移入唯一运行所有者 `v2.algorithms` 后分别保持 `d77606a00e8284ad00739f46777d78212afd2959219b1801fb52d21ec2c7b2f9` 与 `eca472c558f05c5fef6cd63f8fddc937960d914b91137e3f80ac6f66af2b252c`；旧domain模块、六个domain公共出口和四个死合同删除。算法模型测试迁入算法包，新增旧模块/旧出口防复生、base/linear公共对象身份及algorithms/domain正逆序干净解释器门禁。算法与domain相关40项通过；V2由173降至172模块，`delete_after_extraction`由28降至27，运行源码净减少143个物理行/119个非空行，当前为122551/114561；全仓仍可收集4971项，说明测试覆盖未因死合同退出而缩水，七类静态门禁、兼容包装、保护路径导入违规和未解释静态零引用均为0。V1.5正式门禁148项、导出/工件韧性8项、parity 1/1、nightly单测及CO₂ 45点/H₂O 13点成熟dry-run通过，入口清单仍为570项；实际拟合实现、CO₂零气/H₂O干气锚点、压力优先流程、系数/序列化结果、dry-run内核/profile和non-blocking证书门禁均未改变，且证据明确不是real acceptance。本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 计划profile合同所有权归并批次：Gate 81联合审计409行 `domain.plan_models`、`config.models.WorkflowConfig` 与 `core.plan_compiler`。七个plan类型全部是活跃编译输入，不能像死合同一样删除；但生产源码只有 `plan_compiler` 一个消费者，旧domain包根及三个测试文件形成第二入口。`WorkflowConfig` 负责有效运行配置，profile类型负责可序列化编译输入，二者字段虽有映射但物理职责不同，本批明确不做字段强合并。迁移前14项行为基线通过并逐类冻结AST SHA-256：`AnalyzerSetupSpec=836c7de0ee69bbb9b8f9ae33da9578ffcf5cdc4f674e6752764218a3a279298c`、`TemperatureSpec=e4a1f586c227e08ef9f012311a0d82a6c0c985f16230991bbe3a54f9338e4f73`、`HumiditySpec=93b8b926520b5330c72d8a509448de8969ac6031d172bd56d7732577e6fa2572`、`GasPointSpec=d6c8c993437a2b31d013838f4c0f28f40d2631f199eadfceea89836e722917f8`、`PressureSpec=a61cdac869093700e3f76aa187e81b5265dc91a842476832ad1ed574a7884d48`、`PlanOrderingOptions=359afacebf2aa020c8aa44dcccbe75814bf23a874a7bb93cba908f0b0220d0ec`、`CalibrationPlanProfile=eb87bea33badce20adccc2e8cf23cff39947e25115325e058c1ba9a34edeeb6d`；七类原样迁入唯一编译所有者后哈希逐一不变。旧模块及domain包根七个公共出口删除，测试迁至plan profile/编译所有者并新增旧文件/旧出口防复生和domain/config/compiler正逆序干净解释器门禁。计划/编译/导入相关19项及profile、受控状态机、route planner、point parser、calibration service和报告计划扩大回归36项通过。V2由172降至171模块，`delete_after_extraction`由27降至26，运行源码净减少32个物理行/28个非空行，当前为122519/114533；全仓可收集4972项，七类静态门禁、兼容包装、保护路径导入违规和未解释静态零引用均为0。V1.5正式门禁148项、导出/工件韧性8项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run通过，入口清单仍为570项。运行点生成、H₂O carry-forward、ambient/sealed压力选择、CO₂零气/H₂O干气锚点、温度/压力/路由顺序、序列化payload、dry-run内核/profile和non-blocking证书门禁均未改变，证据明确不是real acceptance；本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- V2包根产品启动门面退役批次：Gate 82重新排序剩余26个 `delete_after_extraction` 候选，冻结承载ambient/sealed压力物理语义的 `pressure_selection`、workflow/route/device/采样/拟合链和六个独立科学服务，只选择最小的29行 `gas_calibrator.v2.__init__`。调用图确认其两个函数仅延迟委托 `v2.entry.create_calibration_service` 与 `v2.entry.run_calibration`，生产源码没有调用者，唯一直接消费者是一份integration测试；`2.0.0-alpha` 包根版本号也没有V2消费者，data writer、run manifest与result store使用的是上层 `gas_calibrator.__version__`。迁移前包根/entry/core导入与仿真集成9项通过；本批让integration测试直连唯一实现所有者，删除包根文件并把 `gas_calibrator.v2` 转为无产品API的namespace package，所有 `gas_calibrator.v2.*` 子包、`v2.entry` 和core延迟导入继续可用。新增门禁锁定包根文件、两个产品启动属性及alpha版本属性不得复生，同时确认namespace搜索路径存在且导入包根不加载entry。定向17项和entry runtime hooks、manifest、data writer、result store扩大回归26项通过。V2由171降至170模块，`delete_after_extraction`由26降至25，源码净减少29个物理行/17个非空行，当前为122490/114516；全仓仍可收集4972项，旧facade引用、七类静态门禁、兼容包装、保护路径导入违规和未解释静态零引用均为0。V1.5正式门禁148项、导出/工件韧性8项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run通过，入口清单仍为570项；V1.5成熟内核/profile、压力优先、CO₂零气/H₂O干气锚点及non-blocking证书门禁均未改变，证据明确不是real acceptance。本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- V2配置包级再导出门面退役批次：Gate 83逐项审计 `v2.config.__init__` 的25个静态再导出、24处V2运行源码消费、45处测试消费及1处V2目录外sidecar消费；25个对象与唯一实现 `v2.config.models` 的身份全部一致，迁移前模型文件SHA-256为 `14CFCB5081E11FEEFC35A8A6C68BB0F6DA2B88413E15918C2307DB5A91B48CF2`。本批机械改为从唯一实现模块直接导入并删除60行包级门面，`v2.config` 保持可搜索的namespace package；全仓收集首次发现遗漏的sidecar旧入口后已一并修正，而两个脚本中指向共享 `gas_calibrator.config.load_config` 的相对导入经语义复核后原样保留。模型文件只更新文档示例路径，内存归一化还原该行后SHA-256与迁移前完全一致，证明1849行配置模型实现、字段、默认值和校验行为未改。配置/core定向48项、直接模型消费者330项通过且2项跳过，sidecar/validation扩大回归24项通过；V2由170降至169模块，`delete_after_extraction`由25降至24，运行源码净减少60个物理行/58个非空行，当前为122430/114458。全仓可收集4973项，旧配置门面引用、七类静态门禁、兼容包装、保护路径导入违规和未解释静态零引用均为0；V1.5正式门禁148项、导出/工件韧性8项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过，入口清单仍为570项。成熟内核 `v1_5_legacy_ratio_0613_0620_0621`、`legacy_ratio_production` profile、压力优先、CO₂零气/H₂O干气锚点及non-blocking证书门禁均未改变，且证据明确不是real acceptance。本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 概念性频域诊断退役批次：Gate 84先按代码量审计剩余24个 `delete_after_extraction` 候选，确认145行 `pressure_selection` 承载ambient/sealed压力物理语义、176行 `workflow_steps` 是七个活跃步骤的唯一实现、400行 `domain` 包根也已承载运行模式/采样/QC/结果合同，三者均冻结不删。随后审计435行 `spectral_quality_engine` 及其配置、离线工件、目录和中英文显示枝杈：它对运行时间序列做Welch功率谱分析而非分析仪光学吸收谱，却以“谱质量”展示；CO₂、H₂O、压力、温度等不同量纲通道共用固定0.01 Hz阈值和经验评分，没有按校准点/设定值阶段分段，设定值切换可能被误报为低频漂移，而常量序列又直接得到1.0稳定性分数，无法区分真实稳定与传感器卡死。该功能默认关闭、无V1.5或正式验收消费者、没有经真实样本/故障注入/通道专属判据验证，继续保留会制造伪科学展示和额外工件角色。本批不另造替代算法，删除频域引擎及专属测试，移除离线生成/摘要/manifest/目录、中英文文案和四个配置字段；历史配置中的旧字段仍由 `from_dict` 安全忽略，新增门禁要求旧模块、builder、配置属性和工件不得复生。修改面基线153项、修改后145项通过；V2由169降至168模块，`delete_after_extraction`由24降至23，运行源码净减少531个物理行/493个非空行，当前为121899/113965。全仓可收集4965项，删除的8项均为退役算法/工件自测；七类静态门禁、兼容包装、保护路径导入违规和未解释静态零引用仍为0。V1.5正式门禁148项、导出韧性/parity/suite合同8项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过，入口清单仍为570项；成熟内核/profile、压力优先、CO₂零气/H₂O干气锚点、现行稳定性/QC链和non-blocking证书门禁均未改变，且证据明确不是real acceptance。本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 最终validation归属与直接删除停止边界批次：Gate 85对Gate 84剩余23个 `delete_after_extraction` 逐项复读运行调用、测试、nightly用例和外部源码消费者，确认不存在可直接删除单元。1项 `config.models` 同时服务仿真闭包并被merged sidecar窄用，目标归为 `gas_calibrator.validation.simulation.config`，退出条件是sidecar先改用非V2窄合同；13项配置以外的calibration service、entry、orchestrator、device factory/manager、plan/point/route、sampling/stability/workflow和domain/pressure合同共同组成离线仿真执行闭包，目标归为 `gas_calibrator.validation.simulation`，必须整闭包迁移并保持parity/nightly后才能删除旧路径；`no_write_guard`、A1 dry-run和A2 no-write 3项归为 `gas_calibrator.validation.engineering_probe`，必须保持no-write、promotion blocked并等待Step 3A关闭或等价验证迁移；6项动态计量、系统辨识、分析仪档案、台架就绪、动态不确定度和工况包络合同仍由nightly直接执行，归为 `gas_calibrator.validation.metrology`，迁移前必须逐项完成科学复核且继续标记diagnostic-only/not-real-acceptance。清单生成器现输出四组目标命名空间、各自退出条件、`direct_deletion_phase_status=stopped_no_reviewed_delete_candidates` 和23项最终validation闭包；`delete_after_extraction`由23归零，`migrate_to_validation`由31增至54，自动删除仍为false。V2运行源码保持168模块/121899物理行/113965非空行，本批只修正治理清单和增加1项分组完整性测试；清单/入口/跨边界62项、V1.5正式门禁148项、导出韧性/parity/suite合同8项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过，全仓可收集4966项，入口清单仍为570项。成熟内核/profile、压力优先、CO₂零气/H₂O干气锚点、设备/采样/QC/拟合实现和non-blocking证书门禁均未改变；保护路径导入违规、兼容包装和未解释静态零引用均为0。本批未打开COM、控制气路、连接真实数据库、写系数/设备ID，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；直接删除阶段到此停止，后续只能按四个闭包做等价迁移，不能再按文件大小删模块；
- merged sidecar配置合同解耦批次：Gate 86确认 `run_v1_merged_calibration_sidecar` 只为比值多项式报告使用 `coefficients` 子树，却通过 `AppConfig` 构造整套V2设备、流程、阀门、QC、存储和AI配置。报告生成、下载计划和真实下载实现分别审计后，确认设备系数下载器本来就直接读取显式JSON目标，不依赖 `AppConfig`；本批因此只在 `ratio_poly_report` 增加报告所有者自己的窄加载函数，复用原有 `CoefficientsConfig.from_dict`、默认值、H₂O选择规则和缺文件异常，不复制第二套配置类。sidecar改为调用该窄入口并保持 `enabled/auto_fit=True` 的既有行为，拟合公式、报告字段、下载顺序和所有写前门禁均未改。定向配置/sidecar/报告/下载/清单43项通过；清单证明 `config.models` 的仓库外源码引用由1降为0，V2内部调用27处、测试调用45处仍在，因此模块继续归 `gas_calibrator.validation.simulation.config`，只把退出条件收口为整闭包parity/nightly迁移且V2调用归零，不执行迁移或删除。V2仍为168模块，未新增配置类、模块、入口、状态源或工件角色；全仓可收集4968项，V1.5正式门禁148项、导出韧性/parity/suite合同8项、入口/边界33项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过，入口清单仍为570项。成熟内核 `v1_5_legacy_ratio_0613_0620_0621`、`legacy_ratio_production` profile、压力优先、CO₂零气/H₂O干气锚点和non-blocking证书门禁均未改变；所有套件继续标记dry-run/simulated、禁止promotion且不是real acceptance。本批未打开COM、控制气路、连接真实数据库、执行系数/设备ID写入，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 稳定性配置首个validation所有权迁移批次：Gate 87先对 `config.models` 的27个V2运行消费者和45个测试消费者按导入对象建立依赖图，冻结AI、QC、设备、系数、完整AppConfig和带私有归一化函数的配置，只选择无V2私有依赖、由 `WorkflowConfig` 聚合且仅被 `stability_checker` 直接消费的五类稳定性配置。`TemperatureStabilityConfig`、`HumidityStabilityConfig`、`PressureStabilityConfig`、`SignalStabilityConfig` 与 `StabilityConfig` 原样迁入唯一所有者 `gas_calibrator.validation.simulation.config`；迁移前后五个完整类AST SHA-256分别保持 `9e81768d3892168f13c7b20f60690cfae85628754fbd6ebbf426f003be3c341c`、`50a283e94637759658a7d2027de8353a5970ae3341c1c69f57c63b6ee501c002`、`805c9c1771faca898a3ba2620b2336323b25ce376319567f8b3fc4eb0e5e98be`、`abafc163dec42ce3518ee45d6dab9337c438d7a1f554be83051daafbc191781a` 和 `b6b086fc2074dfc7a1377ea6a1fe721bc1b56c0352ddd67c366a961b1fb38078`。V2旧路径只显式再导出同一对象供 `WorkflowConfig` 和历史调用兼容，不保留第二份类定义；`stability_checker`与其测试已直连validation所有者，身份测试锁定五类新旧路径均为同一对象。配置消费者扩大回归332项通过、2项按环境合同跳过；迁移/边界/卫生75项、V1.5正式门禁148项、导出韧性/parity/suite合同8项、入口/边界33项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过，全仓可收集4969项，入口清单仍为570项。清单显示 `config.models` 的V2内部消费者27→26、测试消费者45→44、仓库外消费者保持0；V2仍为168模块，自动删除仍为false。迁移没有改变温度、露点、压力或信号稳定阈值、窗口、浸泡/超时、温箱指令偏移、分析仪腔体温度门禁、压力优先顺序、CO₂零气/H₂O干气锚点或任何设备行为；新所有者明确不授权设备I/O、气路控制、系数写入或real acceptance。本批未打开COM、连接真实数据库、执行系数/设备ID写入，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 可选诊断AI配置validation所有权迁移批次：Gate 88联合审计AI与QC配置后冻结 `QCConfig`，因为它直接控制采样数、异常值比例、漂移/质量阈值和路径规则；本批只迁移默认关闭、provider为mock且所有feature受总开关约束的 `AIFeaturesConfig` 与 `AIConfig`。两个类原样迁入唯一所有者 `gas_calibrator.validation.simulation.config`，迁移前后完整类AST SHA-256分别保持 `b36cabafe318ba7c1ca77d3f9928bf9aa79193379169dd56ca84d729c6198dba` 与 `f68ac63b19a4b4809ac7f386d53994f56e8130a083460141f0fa60720faddba3`；V2旧路径只显式再导出同一对象，AI运行消费者和专属测试改为直连validation所有者，不保留第二份定义。AI/QC/算法/entry/config定向59项、配置消费者扩大回归333项通过且2项按环境合同跳过，静态边界/导入卫生48项、V1.5正式门禁148项、导出韧性与suite合同11项、入口清单37项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过；全仓可收集4970项，入口清单仍为570项。清单显示 `config.models` 的V2内部消费者26→21、测试消费者44→42、仓库外消费者保持0；V2仍为168模块，自动删除仍为false。迁移没有启用AI、增加provider或功能，也没有改变AI超时/mock fallback、QC阈值、采样/拟合/路由、成熟内核/profile、压力优先、CO₂零气/H₂O干气锚点或non-blocking证书门禁；AI继续只能给出可选诊断说明，不参与设备控制、系数选择或正式放行。本批未打开COM、控制气路、连接真实数据库、执行系数/设备ID写入，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 离线路径配置validation所有权迁移批次：Gate 89审计剩余配置对象及其直接调用者后，冻结会启用真实持久化的 `StorageConfig`、改变运行模式的 `FeaturesConfig`、改变候选拟合选择的 `AlgorithmConfig`，并继续冻结设备、QC、系数、Workflow和完整 `AppConfig`；只选择不含行为开关、物理阈值或I/O能力的 `PathsConfig`。该类只定义校准点文件、输出目录和日志目录三个字符串路径，原样迁入唯一所有者 `gas_calibrator.validation.simulation.config`，迁移前后完整类AST SHA-256保持 `15e32083e8aff166265c1c8916ed8531b1e8ab1643be29f2b1762378b7b76ded`；默认值、自定义 `from_dict` 结果和 `AppConfig.paths` 对象身份全部保持一致，V2旧路径只使用同一对象。路径/entry/integration/result-store定向37项、配置消费者扩大回归334项通过且2项按环境合同跳过，静态边界/导入卫生48项、V1.5正式门禁148项、导出韧性与suite合同11项、入口清单37项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过；全仓可收集4971项，入口清单仍为570项。由于该类原本没有独立模块导入方，清单如实保持 `config.models` 的V2内部消费者21、测试消费者42、仓库外消费者0不变；收敛成果是V2中的一个独立类定义退出而不是伪造调用者下降。V2仍为168模块，自动删除仍为false。迁移没有改变路径解析、运行模式、算法/QC/采样/拟合/路由、成熟内核/profile、压力优先、CO₂零气/H₂O干气锚点或non-blocking证书门禁；本批未打开COM、控制气路、连接真实数据库、执行系数/设备ID写入，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 仿真特性配置与冗余V2标签收口批次：Gate 90逐字段审计 `FeaturesConfig`，确认真正决定V2 validation service或V1 fallback的是 `create_runner(..., use_v2=...)` 的显式函数参数；`config.features.use_v2` 没有进入任何条件判断、设备控制、数据处理或放行逻辑，只被legacy adapter赋值、单测读取和manifest展示各一次。保留的 `simulation_mode` 仍由entry、calibration service和orchestrator实际消费，`debug_mode`仍作为运行元数据，因此两个字段及默认值不变并迁入唯一所有者 `gas_calibrator.validation.simulation.config`；删除冗余 `use_v2` 配置字段和legacy adapter赋值，历史JSON中的同名键由 `from_dict` 安全忽略。迁移前完整类AST SHA-256为 `052ea4aabc67c2aa6fe65d61481f0d38f6518031e45b063b47de2f31b5044150`；仅机械去除废弃字段及对应解析参数后的预期AST与迁移后AST SHA-256均为 `b2f59a160cba30f2b9bb5d3ce57c357ef874cd4c87b612d907d1d7c0af6e0cc0`。manifest保留历史 `use_v2=true` schema键，但改由V2 validation manifest构建器自身声明运行身份，不再从无行为意义的配置标签推断；新增V1 fallback门禁证明即使历史配置带 `use_v2:true`，只要显式函数参数为false仍调用旧runner。feature/entry/manifest/fallback定向52项、配置消费者扩大回归340项通过且2项按环境合同跳过，静态边界/导入卫生48项、V1.5正式门禁148项、导出韧性与suite合同11项、入口清单37项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过；全仓可收集4974项，入口清单仍为570项。由于该类仍由 `AppConfig` 聚合，清单保持 `config.models` 的V2内部消费者21、测试消费者42、仓库外消费者0不变；V2仍为168模块，自动删除仍为false。本批没有改变V1 fallback选择、simulation/debug语义、算法/QC/采样/拟合/路由、成熟内核/profile、压力优先、CO₂零气/H₂O干气锚点或non-blocking证书门禁；未打开COM、控制气路、连接真实数据库、执行系数/设备ID写入，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 无消费者算法配置退役批次：Gate 91联合审计 `AlgorithmConfig`、`AppConfig.algorithm` 和 `AlgorithmEngine`。四个配置字段 `default_algorithm`、`candidates`、`auto_select`、`validation_tolerance` 在运行源码中均没有消费者，也没有通过动态 `getattr`、条件分支、manifest或工件构建器影响拟合；实际引擎接口由调用方显式传入算法名或候选列表，`compare`继续固定按valid、R²和RMSE排序，V1.5生产算法快照独立明确 `auto_select=false`，不依赖该V2配置。迁移只会保留误导性死旋钮，因此本批没有把类搬入validation，而是删除 `AlgorithmConfig` 定义、`AppConfig.algorithm`字段和对应 `from_dict`装配；历史JSON中的 `algorithm`块继续安全忽略。退役前类AST SHA-256为 `89cd7e6b9d0ef283618f2d29053eb3f45edaa0816f3e90450dff2319b353153d`，默认值和自定义解析已冻结；退役后定义数为0，唯一同名字符串是防复生断言。算法/config/manifest/integration定向56项、配置消费者扩大回归342项通过且2项按环境合同跳过，静态边界/导入卫生48项、V1.5正式门禁148项、导出韧性与suite合同11项、入口清单37项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过；全仓可收集4975项，入口清单仍为570项。由于死类只由 `AppConfig` 内部聚合，清单保持 `config.models` 的V2内部消费者21、测试消费者42、仓库外消费者0不变；V2仍为168模块，自动删除仍为false。本批没有修改 `AlgorithmEngine`、linear/polynomial/AMT/robust算法、V1.5成熟拟合或模型选择，没有改变QC、采样、路由、压力优先、CO₂零气/H₂O干气锚点、成熟内核/profile或non-blocking证书门禁；未打开COM、控制气路、连接真实数据库、执行系数/设备ID写入，也未修改成熟runner、设备实现、默认配置或 `run_app.py`；
- 仿真存储fail-closed与旧入口辅助函数退役批次：Gate 92逐字段审计 `StorageConfig`、共享 `StorageSettings`、`DatabaseManager.from_config`、entry旁置配置合并和finalization自动导入链。`enabled/backend/host/port/database/user/password/pool_size/echo/dsn/timescaledb/auto_import`均有连接、建库、连接池或自动导入消费者，不能当作死配置迁入validation或删除；`schema`当前没有运行读者，但同时存在于共享存储公共构造合同，本批按共享数据库冻结边界保留，等待独立兼容性处置。审计同时发现：`src/gas_calibrator/v2/configs/storage_config.json`会被同目录仿真配置自动合并成PostgreSQL+`auto_import=true`，数据库不可达时仅降级warning，但本机数据库可用时可能产生真实落库。现改为只要最终 `features.simulation_mode=true`，配置对象和原始快照都显式写入 `storage.enabled=false`；主配置显式 `enabled=false` 时也不再允许旁置文件反向启用，而非仿真、未显式决定存储的离线入口仍保持原有sidecar opt-in和SQLite相对路径解析。`_resolve_config_paths`、`_load_storage_config`、`_load_ai_config`三个已被 `_resolve_raw_config_paths` 与 `_merge_optional_section`完全替代且全仓零调用的旧辅助函数同步删除，没有复制第二套加载器。新增4项门禁分别覆盖仿真旁置PostgreSQL强制禁用、显式禁用优先、非仿真显式opt-in保持和禁用状态下不调用数据库实现；配置/entry/artifact/calibration定向41项及清单合并回归48项、导出韧性与suite合同11项、入口与生产映射52项、V1.5正式门禁149项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过；全仓可收集4979项。清单保持168个V2模块、23项最终validation闭包、0项保护路径导入违规、0项未解释静态零引用及自动删除false。本批没有创建数据库引擎或session、没有执行schema/import/write，也没有修改成熟runner、设备层、路由、采样、拟合、QC、默认配置、共享存储实现、V1.5 full-flow或 `run_app.py`；0613/0620/0621成熟内核、压力优先、CO₂零气/H₂O干气锚点和non-blocking证书门禁均保持；
- 旁置存储显式启用与V2死schema字段退役批次：Gate 93把Gate 92遗留的两项P1收口。`_merge_optional_section`增加可选的候选载荷准入谓词，且只对 `storage_config.json` 要求解析结果明确 `enabled=true`；旁置文件缺少该值或为false时不再进入原始配置，主配置中内嵌的SQLite/PostgreSQL/DSN后端推断保持原样。仓库自带旁置模板新增 `enabled=false`，使默认状态可读且必须由操作者显式改为true；仿真模式仍在最终配置和快照层二次强制false。调用图再次确认V2 `StorageConfig.schema`只有定义与解析、没有任何属性读者或建schema消费者，因此删除该V2字段和解析装配，历史JSON继续安全忽略；共享 `StorageSettings.schema`、`DatabaseManager`、迁移和正式数据库合同完全未动，V2配置转换为共享设置时仍得到共享默认 `public`。新增2项测试覆盖“无显式启用的旁置文件被忽略”和“历史schema字段不复生”，既有显式禁用、显式opt-in、SQLite相对路径及artifact禁用短路继续通过。扩大配置消费者回归首次出现两个与存储零依赖的线程等待超时；诊断实测完整fake流程约1.77秒完成、无残留线程或I/O，其中一项单独复跑即通过，另一项原测试以2秒硬卡整套启动/采样/QC/导出/安全停机。仅将该测试等待窗口由2秒调整为5秒且不减少任何事件断言，重跑320项通过、2项按既有环境合同跳过。最终配置/entry/artifact/calibration定向43项、清单合并回归50项、V1.5正式门禁149项、入口与生产映射52项、导出韧性与suite合同11项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过；全仓可收集4981项。清单保持168个V2模块、23项最终validation闭包、0项保护路径导入违规和自动删除false。本批未创建数据库engine/session、未执行schema/import/write，未打开COM或控制气水路，也未修改共享存储、成熟runner、设备层、默认配置、V1.5 full-flow、拟合/QC/采样/路由或 `run_app.py`；0613/0620/0621成熟内核、压力优先、CO₂零气/H₂O干气锚点和non-blocking证书门禁不变；
- 存储配置单一公共契约收敛批次：Gate 94逐字段对照V2 `StorageConfig` 与共享 `StorageSettings`，确认启用判定、后端归一化、同步URL、旁置解析和十二区公共字段存在两份平行实现。若把两个类直接做成同一dataclass，会把数据库专用的schema与异步驱动字段注入V2运行配置快照并无意义改变配置指纹；因此新增轻量共享所有者 `gas_calibrator.storage.settings`：`StorageConfig`唯一拥有公共字段、解析、启用判定和同步URL，`StorageSettings`只增加既有数据库合同需要的 `schema/async_driver/async_url`。V2旧导入现在与共享 `StorageConfig` 为同一对象，`AppConfig.storage`的字段集合、默认值、历史schema忽略、`database_enabled`和快照形状不变；`gas_calibrator.storage.database`继续原对象身份再导出 `StorageSettings/load_storage_config_file`，所有V1.5及共享数据库调用无需改入口。共享schema默认值与载荷解析、异步驱动自定义、PostgreSQL凭据转义、SQLite内存/路径URL和DSN转换均保持原行为。独立子进程门禁证明只导入轻量设置模块不会加载SQLAlchemy；单元/配置/entry定向65项通过，42个配置消费者文件分四批343项通过、2项按既有环境合同跳过，先前大批同进程出现的7项线程时限失败均在单文件10项及分批回归中通过，未修改运行实现或放宽断言。最终清单/所有权定向37项、V1.5正式门禁148项、入口/生产映射/静态边界63项、导出韧性与suite合同13项、parity 1/1、nightly 24/24及CO₂ 45点/H₂O 13点成熟dry-run全部通过；全仓可收集4986项。清单仍保持168个V2模块、23项最终validation闭包、0项保护路径导入违规和自动删除false。本批未创建数据库engine/session、未执行schema/import/write，未打开COM或控制气水路，也未修改成熟runner、设备层、默认配置、V1.5 full-flow、拟合/QC/采样/路由或 `run_app.py`；0613/0620/0621成熟内核、压力优先、CO₂零气/H₂O干气锚点和non-blocking证书门禁不变，全部suite证据继续是dry-run/simulated且promotion blocked，不是real acceptance；
- QC配置validation所有权迁移批次：Gate 95先逐字段追踪 `QCConfig/QCRuleConfig`，确认 `min_sample_count`、`max_outlier_ratio`、`spike_threshold`、`drift_threshold`、`quality_threshold`、默认规则、气路/模式映射和自定义规则全部有采样质量、异常值、漂移、点级放行或规则注册消费者，任何删除、默认值调整或拆分迁移都会改变QC物理口径，因此全部冻结。本批只把两个互相依赖且不引用V2私有类型的dataclass作为一个不可拆分单元原样迁入 `gas_calibrator.validation.simulation.config`；V2旧路径继续再导出同一对象，`AppConfig.qc`、配置快照和历史导入兼容不变，五个QC运行模块改为直连validation所有者。迁移前后完整类AST SHA-256分别保持 `8bbe8f039577425c0f08597b56d5fd8825a9ebb675790c1c4c06f0ff3e928035` 和 `f1edf28737080ad318ec476df2c47e9aae3136aca07a2b9f5db1bf0c3fb4e5da`。新增身份及完整自定义解析测试锁定两个类、五个数值阈值和四组规则字段；QC/config/service聚焦47项通过。扩大配置消费者按稳定排序分块执行，41个非线程文件336项通过、2项按既有环境合同跳过；`CalibrationService`八项因Windows同进程累计调度出现三个既有wait超时，逐项隔离后8/8通过且不修改运行实现、不延长等待、不减少断言。迁移没有改变异常值检测公式、规则模板、点级判定、运行评分、报告或拟合准入。最终门禁为：正式标记148项、入口/生产边界63项、resilience/suite 13项、parity 1/1、nightly 24/24、测试收集4987项均通过；成熟0613/0620/0621内核按45个CO2点和13个H2O点完成no-write dry-run，两个气路均通过。处置清单保持168个模块、23个validation收口模块、0个受保护路径违规和0个未解释静态零引用模块，`config.models` 的V2内部引用由21降至16，自动删除仍为false。本批不打开COM、不控制气路、不写数据库/系数/设备ID，不修改成熟runner、默认配置或 `run_app.py`；全部证据仅为simulation/replay/dry-run，不是real acceptance；
- 采样配置validation所有权迁移批次：Gate 96比较剩余采样、压力、预检、工作流、设备、阀门和系数配置后，冻结直接参与压力放行与启动预检的 `PressureControlConfig/PrecheckConfig`，也冻结依赖设备、路由或拟合语义的更大配置，只选择没有V2私有依赖的三字段值对象 `SamplingConfig`。`interval_s/count/discard_first_n` 的字段、默认值和 `from_dict` 解析原样迁入唯一所有者 `gas_calibrator.validation.simulation.config`；即使 `discard_first_n` 当前没有独立属性消费者，也作为历史配置和快照合同保留，不借迁移删除。V2旧路径继续再导出同一对象，`WorkflowConfig.sampling`、`AppConfig.from_dict`、配置快照和调用入口均不变；迁移前后完整类AST SHA-256保持 `57df94e8d7d91bee694539e76c7c29a2320b5184f9474d1a82a1302a4ff9e81b`。新增身份、默认值及自定义解析测试后，配置/采样聚焦32项、41个稳定配置消费者324项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离全部通过；正式标记148项、入口/最终产品边界/模块清单66项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集4988项全部闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。处置清单保持168个模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，`config.models` 仍有16个V2内部消费者和43个测试消费者，因此自动删除继续为false。本批没有改变实际采样循环、QC、压力优先、预检、路由、拟合、CO₂零气/H₂O干气锚点或任何设备行为；未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`。全部suite证据保持 `promotion_state=dry_run_only`、`ready_for_promotion=false`，不是real acceptance；
- 通用预检配置validation所有权迁移与事件测试隔离批次：Gate 97先区分 `workflow.precheck`、`workflow.sensor_precheck` 和 `workflow.startup_pressure_precheck` 三个不同合同，确认 `PrecheckConfig.enabled/device_connection/pressure_leak_test/sensor_check` 分别控制通用预检跳过、设备健康、压力泄漏和传感器检查分支，全部有实际消费者且不能删除或合并。本批将无V2私有依赖的四字段 `PrecheckConfig` 原样迁入唯一所有者 `gas_calibrator.validation.simulation.config`；V2旧路径继续再导出同一对象，`WorkflowConfig.precheck`、配置快照、默认全开和自定义解析行为不变。迁移前后完整类AST SHA-256保持 `4196fc44fecc5f275cb1b68d44c69c2e8867e489bd1628beb5b3d5eb117459d1`。扩大同进程回归发现工作流事件测试在5秒时已完成预检/采样并进入finalizing、无错误，但因该测试同时执行与目标无关的全量导出而约7秒后才完成；只读对照将全量导出隔离后同一事件流程约0.16秒完成。测试因此把输出/日志收口到 `tmp_path` 并仅在事件合同中替换 `_export_all_artifacts`，保留原5秒时限及全部事件断言；实际导出、失败隔离和nightly工件仍由独立13项resilience/parity/suite合同完整执行并全部通过。配置消费者随后按四批重跑325项通过、2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；正式标记148项、入口/最终产品边界/模块清单66项、parity 1/1、nightly 24/24和全仓收集4989项闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁仍为non-blocking。清单保持168个模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，`config.models` 仍有16个V2内部消费者和43个测试消费者，自动删除继续为false。压力控制配置及其物理阈值本批冻结未迁移；本批未改变预检顺序、设备健康判定、泄漏/传感器实现、采样、QC、压力优先、路由、拟合、CO₂零气/H₂O干气锚点或生产导出，未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`。全部suite证据保持 `promotion_state=dry_run_only`、`ready_for_promotion=false`，不是real acceptance；
- 压力容差配置validation所有权迁移与误导性死旋钮退役批次：Gate 98逐字段追踪 `PressureControlConfig` 后确认，`setpoint_tolerance_hpa` 被编排器泄漏预检以及压力控制服务的ready/in-limits判定实际消费，必须保留；原 `ramp_rate_hpa_per_s`、`max_pressure_hpa`、`min_pressure_hpa` 则只有定义和解析，没有压力控制、报告、工件或放行消费者，继续暴露会错误暗示系统已执行爬升率和上下限安全保护。实际压力点与稳压政策仍由 `workflow.pressure.*` 提供，V1.5正式开流runner的 `--max-open-flow-pressure-hpa` 是另一条独立生产安全合同且本批完全未动。因此本批把有效的一字段 `PressureControlConfig` 迁入唯一所有者 `gas_calibrator.validation.simulation.config`，V2旧路径只再导出同一对象，并退役三个没有执行语义的字段；历史JSON中的同名键继续安全忽略。原四字段类AST SHA-256为 `2a17cff0f435a491a7661d27334cf1d7932fb824d4825c787bcdec8e4bdacf36`，机械去除三个死字段后的预期AST与迁移后AST SHA-256均为 `a0d266cb40f670c5c38363b74ff580f1621b1356a7da55a1cf55e6a49ea3d8e9`。配置/压力/ready/编排/manifest聚焦72项、42个配置消费者分四批326项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；正式标记148项、入口/最终产品边界/模块清单66项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集4990项全部闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁仍为non-blocking。处置清单保持168个模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用；`config.models` 仍有16个V2内部消费者和43个测试消费者，自动删除继续为false。本批没有改变实际压力算法、压力优先/SENCO9顺序、压力点、泄漏/ready判定公式、预检、采样、QC、路由、拟合、CO₂零气/H₂O干气锚点或生产导出；未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`。全部suite证据保持 `promotion_state=dry_run_only`、`ready_for_promotion=false`、`simulated_readiness_only=true`，不是real acceptance；
- 工作流聚合配置逐字段审计与无行为V2启动连接镜像退役批次：Gate 99没有整体迁移高风险 `WorkflowConfig`，而是逐项追踪33个字段。除 `startup_connect_check` 外，其余32项均有计划编译、路由排序、压力/稳态/采样/QC、分析仪初始化、运行模式、报告或证据消费者，全部冻结不动。V1原始字典 `workflow.startup_connect_check` 仍被生产UI、headless/short-run工具及V1/V2控制流对比脚本读取，是真实且必须保留的连接检查合同；但V2 dataclass中的同名布尔字段没有任何运行读取者，只被通用序列化带入配置快照，而且会把V1的 `{enabled, retries, ...}` 嵌套结构错误压扁成单一布尔值，形成V2已执行启动连接门禁的误导。本批仅删除V2 `WorkflowConfig` 中该字段、临时解析变量和构造参数；历史V2 JSON键由 `AppConfig.from_dict` 安全忽略，原始字典保持不变，V1配置、连接检查实现、对比脚本及所有历史JSON均未修改。原33字段类AST SHA-256为 `182bacd61354de81341e41f62eaf3ecbaede22dd3b18c11911d4db1b4fe2c39f`，机械删除无行为镜像后的预期AST与变更后AST SHA-256均为 `45e4ffe1a353a5e78852f6825551842cf1bfe8a1b3acb161b5bd672cd3893788`。配置/manifest/控制流/V1连接检查聚焦门禁由变更前62项增至变更后63项并全部通过；41个非CalibrationService配置消费者分四批331项通过、2项按既有环境合同跳过，`CalibrationService` 8项隔离通过；正式标记148项、入口/最终产品边界/模块清单66项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集4991项全部闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁仍为non-blocking。处置清单保持168个模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用；`config.models` 仍有16个V2内部消费者和43个测试消费者，自动删除继续为false。本批没有改变启动连接检查本体、设备发现、预检、压力优先/SENCO9、压力点、稳态、采样、QC、路由、拟合、CO₂零气/H₂O干气锚点、运行报告或生产导出；未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`。全部suite证据保持 `promotion_state=dry_run_only`、`ready_for_promotion=false`、`simulated_readiness_only=true`，不是real acceptance；
- 设备配置逐字段审计与参考温度计V2证据链闭合批次：Gate 100逐项核对 `SingleDeviceConfig` 的22个字段、`DeviceConfig` 的9个设备角色、设备工厂构造签名、分析仪初始化读取、设备管理注册表、采样服务和run manifest。全部字段均有通信构造、启停/身份、分析仪MODE/FTD/AVERAGE、快照描述或兼容别名消费者，不能删除或泛化迁移；两个配置类继续冻结在V2配置闭包。审计发现 `thermometer` 已被配置解析、设备工厂和采样服务支持，但设备管理注册表、编排器创建阶段和manifest设备快照均漏掉它，导致仅靠配置声明时箱内铂电阻数字测温仪无法进入V2状态与证据链。本批只补三处既有映射：`DeviceManager`按 `DeviceType.THERMOMETER` 注册配置项，编排器在simulation创建阶段通过既有工厂实例化温度计，run manifest记录其端口、启用状态、串口参数和描述；真实温度计驱动仍保持lazy import，`src/gas_calibrator/devices`完全未改。新增两项测试锁定注册表角色和simulation创建，manifest既有测试增加参考温度计快照断言；设备/config/manifest/采样/编排聚焦门禁由变更前66项增至变更后68项并全部通过。41个非CalibrationService配置消费者分四批333项通过、2项按既有环境合同跳过，`CalibrationService` 8项隔离通过；正式标记148项、入口/最终产品边界/模块清单66项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集4993项全部闭合。nightly覆盖温度计健康、陈旧和无响应场景；成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，V1正式流程既有 `thermometer_truth_required=true` 合同和证书non-blocking启动门禁保持不变。处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批没有把V2参考状态提升为真实放行门禁，没有改变温箱控制、参考温度判定公式、设备失败策略、预检、压力优先/SENCO9、压力点、稳态、采样/QC算法、路由、拟合、CO₂零气/H₂O干气锚点或生产导出；未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改成熟runner、默认配置、V1.5 full-flow或 `run_app.py`。全部suite证据保持 `promotion_state=dry_run_only`、`ready_for_promotion=false`、`simulated_readiness_only=true`，不是real acceptance；
- 阀路配置逐字段审计与无行为历史镜像退役批次：Gate 101逐项核对 `CO2GroupConfig`、`ValveConfig`、默认配置、`ValveRoutingService`、CO₂/H₂O route runner和run manifest。真实V2阀路决策始终通过 `_cfg_get("valves...")` 读取原始配置；默认配置恰好包含 `co2_path`、`co2_path_group2`、`gas_main`、`h2o_path`、`flow_switch`、`hold`、`relay_map`、`co2_map`、`co2_map_group2` 九个键，这九项全部有阀集合、物理继电器映射、总阀/H₂O支路、CO₂组别路径或气瓶源阀消费者，并继续由 `ValveConfig` 原样进入manifest证据快照。`group_a`、`group_b`、`valve_mapping` 和为前两者服务的 `CO2GroupConfig` 则在运行源码、默认配置、路由、报告与放行中均无消费者；继续保留会形成另一套看似可配置但实际上不控制气路的平行语义。本批只退役这三个历史镜像字段和一个死类，历史JSON中的旧键由 `AppConfig.from_dict` 安全忽略；阀驱动、原始 `valves` 配置、全部阀号/气瓶映射、气路顺序和物理动作均未修改。原 `CO2GroupConfig` 与 `ValveConfig` AST SHA-256分别为 `01df22460f4cf94111d8c6cca2b82979384055acccefcdfd3f1051124a17eb98`、`546d50bd3ec939ce798377fbe0646f73f7248555886a0730a3a9068aabffbb0a`；退役后前者不存在，收口后的九字段 `ValveConfig` AST SHA-256为 `7e87abc4744b8f26c24517ff92b6fb9c67e37a5de2b22f4d44e5ca5ab558d923`。新增两项测试锁定旧键安全忽略、九字段完整解析及manifest快照；配置/阀路/manifest聚焦39项、直接与间接配置消费者347项通过且2项按既有环境合同跳过，`CalibrationService` 8项隔离通过；正式标记148项、入口/最终产品边界/模块清单66项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集4995项全部闭合。nightly继续覆盖双CO₂组映射、H₂O切换、继电器卡滞、参考温度计异常和导出韧性；成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁仍为non-blocking。处置清单保持168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批没有改变预检、压力优先/SENCO9、压力点、稳态、采样/QC、拟合、CO₂零气/H₂O干气锚点或生产导出；未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改阀驱动、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`。全部suite证据保持 `promotion_state=dry_run_only`、`ready_for_promotion=false`、`simulated_readiness_only=true`，不是real acceptance；
- 系数配置逐字段审计与H₂O干气锚点门禁对齐批次：Gate 102逐项追踪 `CoefficientsConfig`、`CoefficientSummaryColumnConfig`、`H2OSummarySelectionConfig`、默认配置、V1成熟runner、V2 `ArtifactService` 和ratio-poly报告。原始配置中的 `order` 与 `signal_keys` 仍由V1 AMT拟合实际读取，必须保留；V2 dataclass中的同名属性却没有任何属性消费者，V2正式只支持 `ratio_poly_rt_p` 并从 `ratio_degree`、温度偏移、截距、简化策略、报告列和H₂O选点配置构造候选报告。本批因此只退役两个V2无行为镜像，历史输入字典及V1原始配置完全不变。科学审计同时发现V2报告复制的简化H₂O选点器虽然能选H₂O相和指定CO₂零点，却没有执行既有的露点证据/含水量上限门禁；这会把温度匹配但实际不够干的CO₂零气误当作H₂O低端干气锚点。本批删除该重复筛选逻辑，改为调用共享 `select_corrected_fit_rows_with_diagnostics`，继续明确CO₂零气与H₂O干气锚点物理含义不同：-20 °C下露点约-90 °C的干点进入H₂O拟合，露点-10 °C的湿零点被默认质量门禁拒绝；显式关闭门禁时仍保留历史选择行为。`CoefficientSummaryColumnConfig.temperature/pressure` 以及全部H₂O selection字段因仍属于V1原始配置或跨报告合同而冻结不删。配置/ratio-poly/正式纠正报告聚焦78项、V2配置与报告定向41项、V1.5正式门禁148项、入口/最终产品边界/模块清单67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集4997项通过；成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。处置清单保持168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批没有改变拟合公式、拟合输入列、压力优先/SENCO9、点计划、稳态、采样/QC、气路、V1报告或生产导出；未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`。全部工件均为simulation/replay/dry-run证据，不是real acceptance；
- 气体级温压列V2无行为镜像退役与来源边界审计批次：Gate 103关闭Gate 102登记的双配置键P1，逐项核对 `CoefficientSummaryColumnConfig.temperature/pressure`、`CoefficientsConfig.report_temperature_key/report_pressure_key`、默认配置、成熟V1 ratio-poly runner、共享validation parity、V2报告和merged sidecar。成熟V1继续从原始 `summary_columns` 读取 `thermometer_temp_c/BAR`，并结合 `ratio_poly_fit.pressure_source_preference=reference_first` 选择温压拟合输入，这四个原始键均为生产合同，完全不动；V2 dataclass中的气体级 `temperature/pressure` 没有属性消费者，V2报告始终只使用报告级 `Temp/P_fit`，继续保留前者只会把未执行的每气体选择能力写入配置快照。本批因此仅退役两个V2镜像，历史JSON旧键由 `AppConfig.from_dict` 安全忽略，原始字典保持不变；V2每气体列配置只保留实际消费的 `target/ratio/pressure_scale`。merged sidecar默认载荷把既有 `Temp/P_fit` 行为改为显式报告级键，并从每气体映射删除两个本来就被忽略的键，输出行为不变。修改前89项、修改后90项配置/报告/sidecar/正式纠正报告回归通过。审计同时确认当前V2新建summary中的 `Temp` 优先来自箱内数字铂电阻温度计，但 `P_fit` 为分析仪内部压力，而成熟V1 `reference_first` 优先选外部参考压力 `P`；这是真实物理来源差异，不是命名问题，本批不以改默认值、换列或缩放系数方式顺带处理，转为独立P1。V1.5正式门禁148项、入口/最终产品边界/模块清单67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集4998项通过；成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。处置清单保持168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批没有改变任何温压数值、拟合输入、拟合公式、压力优先/SENCO9、点计划、稳态、采样/QC、气路、V1报告或生产导出；未打开COM、控制气路、连接数据库、写系数/设备ID，也未修改成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`。全部工件均为simulation/replay/dry-run证据，不是real acceptance；
- 压力来源同帧科学审计批次：Gate 104复用共享 `gas_calibrator.validation.common`，未新增模块、未复制拟合器，在0613/0620/0621成熟CO₂历史批次的45个成功点、450行记录和6台分析仪上，以完全相同的行、气体、目标值、吸收比、箱内参考温度、模型和拟合设置，分别使用外部参考 `P` 与分析仪内部 `BAR/P_fit` 拟合。审计明确执行 `P(hPa) × 0.1 = kPa`，不再把不同单位直接相减；六台均为 `diagnostic_comparable`，只表示同样本、同单位、同模型可比较，不代表等价或可替换。外部/内部压力的平均绝对差为0.64—0.93 kPa，最大绝对差为1.47—1.86 kPa；内部压力在5/6台的训练集RMSE较低、GA04较高，但两套设计矩阵条件数均约为 `1.5×10^9—2.9×10^9`，说明常压附近开流批次的压力激励不足，压力项与比值、温度及交互项高度共线，不能用训练残差选择权威来源。新增工件同时给出残差、偏差、R²、条件数、完整系数差和 `R×T×P` 项差；缺列、无共同样本或拟合失败均fail-closed。成熟V1.5仍保持 `reference_first`，现有受保护runner中未换算单位的P/BAR诊断日志未修改且不得作为量化依据，本批离线审计工件取代其诊断用途。聚焦验证102项、V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5001项通过；成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run。处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。所有同帧结果均保持 `promotion_state=blocked`、`not_real_acceptance_evidence=true`；本批未打开COM、控制气路、连接正式数据库、写系数/设备ID，未修改拟合输入、公式、压力优先/SENCO9、点计划、采样/QC、成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`；
- 候选比值多项式配置validation所有权迁移批次：Gate 105确认 `CoefficientSummaryColumnConfig`、`H2OSummarySelectionConfig` 和 `CoefficientsConfig` 只服务离线ratio-poly候选报告与 `AppConfig` 快照，不控制成熟V1.5 runner、设备、气路或正式系数下载；三类也不依赖V2私有类型，可以作为一个不可拆分的候选报告配置闭包迁移。本批把三个类原样迁入既有 `gas_calibrator.validation.simulation.config`，V2旧路径只再导出同一对象，ratio-poly报告改为直连validation所有者；没有新增模块、配置字段、状态源或工件角色。迁移前后三类AST SHA-256分别保持 `164d78a6dbf06c167ee7a150a6b8546d421326bd68ed9fb8be286108d5756470`、`2112917bcaeda42759c2eac7d9399231dfa4d3c9afccf4aad3ef123e8b85624f`、`54b38f4ce3a5c2030ef67d7ed0a4328d22c6911e62c4b52aa4ad5f2a12f6198d`，字段、默认值、解析、报告温压键和H₂O露点支持的干气锚点门禁均未漂移。新增身份测试后聚焦配置/ratio-poly/sidecar/清单/正式纠正报告82项通过；42个非线程配置消费者339项通过、2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁仍为non-blocking。`config.models` 的V2内部静态引用由16降至15、测试引用由44降至43、总静态引用由60降至58；处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批没有改变拟合公式、拟合输入、报告输出、压力优先/SENCO9、点计划、稳态、采样/QC、气路、CO₂零气/H₂O干气锚点或生产导出；未打开COM、控制气路、连接正式数据库、写系数/设备ID，也未修改成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`。全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- 阀路快照配置validation所有权迁移批次：Gate 106在Gate 101九字段逐项审计基础上确认，`ValveConfig` 仅由 `AppConfig.valves` 用于配置/manifest快照，实际 `ValveRoutingService` 始终通过 `_cfg_get("valves...")` 直接读取原始字典；因此类所有权迁移不会介入阀号、气瓶映射、支路切换、总阀动作或驱动调用。本批把九字段类原样迁入既有 `gas_calibrator.validation.simulation.config`，V2旧路径只再导出同一对象，没有新增模块、字段、状态源或工件角色；迁移前后AST SHA-256均为 `7e87abc4744b8f26c24517ff92b6fb9c67e37a5de2b22f4d44e5ca5ab558d923`。既有九字段测试增加唯一身份断言后，配置/阀路/manifest/CO₂/H₂O route聚焦50项通过；42个非线程配置消费者339项通过、2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核及 `legacy_ratio_production` profile继续按CO₂ 45点/H₂O 13点完成no-write dry-run，点位、温度分组、证书non-blocking门禁均未漂移。`config.models` 静态引用保持V2内部15、测试43、总计58；处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批未修改原始阀路字典、`ValveRoutingService`、阀驱动、成熟runner、设备层、默认配置、V1.5 full-flow或 `run_app.py`，未打开COM、控制气路、连接正式数据库、写系数/设备ID；全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- Step 2配置风险盘点叶子闭包validation所有权迁移批次：Gate 107先审计剩余 `WorkflowConfig`、`SingleDeviceConfig`、`DeviceConfig`、`AppConfig` 和配置安全摘要链，确认前四者仍直接承载工作流、设备通信字段、V2异常及顶层装配语义，本批冻结不动；只把 `STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS`、`port_requires_real_device_review`、`iter_config_device_ports` 和 `enabled_engineering_only_flags` 迁入既有 `gas_calibrator.validation.simulation.config`。该叶子闭包只规范化端口字符串、枚举已启用设备并列出三个默认关闭的压力工程开关，不打开COM、不实例化驱动、不提供解锁、不改变执行门禁；V2旧路径继续导出同一常量/函数对象，没有新增模块、状态源或工件角色。常量与端口判定函数AST SHA-256迁移前后分别保持 `804cc995bac129c71115eede5d1174cdd27412e2c43c4c74b59f85a4228541af`、`8a960bd353c550d8dddbdbd4e37d918a76d39c96122aecadcdd7e3f52f7740d7`；两个设备对象读取函数仅把类型注解从V2 `AppConfig` 放宽为 `Any` 以消除validation反向依赖，函数体与输出未变，AST SHA-256分别由 `7adeaa1031d4a3ad220bb18a00b3ae45274a411a54fc5a228d38c3bcb2dd56e1` 变为 `8e1d77de18637e40306ff305a150568dc7e54a0aabb99bb551d4158092aa5729`、由 `8219b86289a2fb36d9db86dea0135619b5524ba3c4cb1431c524c667246f3463` 变为 `ebcb50fd7465dc7a9d9091d067528975cb6974612f986ab00909a8de4776257b`。聚焦配置安全/入口/离线工件71项、42个非线程配置消费者339项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking；`config.models` 静态引用保持V2内部15、测试43、总计58。处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批未修改双解锁条件、设备配置、压力工程开关默认值、路由、驱动、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`，未打开COM、控制气路、连接正式数据库、写系数/设备ID；全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- Step 2配置安全展示叶子闭包validation所有权迁移批次：Gate 108对Gate 107后的11函数安全摘要调用图逐项闭包，拒绝一次性迁移 `hydrate_step2_config_safety_summary`、`build_step2_config_safety_review`、`build_step2_config_governance_handoff` 和 `summarize_step2_config_safety` 四个公共摘要/门禁函数，仅把 `_step2_config_safety_classification`、`_step2_config_safety_classification_display`、`_step2_config_safety_badge_spec`、`_build_step2_config_safety_badges`、`_build_shared_pressure_flag_inventory`、`_build_step2_config_safety_inventory`、`_build_step2_blocked_reason_details` 七个无V2类型、无I/O的展示叶子原样迁入既有 `gas_calibrator.validation.simulation.config`。V2旧路径继续导出同一函数对象，公共摘要函数的入口、返回schema、中文分类、徽章、设备库存、共享压力开关状态和阻断原因均不变；没有新增模块、解锁入口、状态源或工件角色。七函数AST SHA-256迁移前后分别保持 `704531d9685956440770101c1251b3af6d8bb47d13d0bb5bc5759df479571391`、`3061a5d214b6c92f6d3598d23bcaa3570a4d3b68fb6e9e78e339e09438faef92`、`099915d0163647975b777ece22580f2a1826f3ec63d92634f582262a7b1cabcf`、`09eea8a3beed7488f48527133153cd11d0f6dcd7fed2baba080cbed2e25571e6`、`70c67d237287836a0508345d58a5c3e0c996fe9675f907e1d5849330493c4bb0`、`d56bb0f8575b1c2d09585411b7e74eb62d177cd173c8c869097729cfa2789d4d`、`bcbd1d38cf387832da7be34a6daf1e54ada07f8a05df1d9e1c7ffebc7fd0fa7b`。聚焦配置安全/入口/离线工件71项、42个非线程配置消费者339项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking；`config.models` 静态引用保持V2内部15、测试43、总计58。处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批未修改双解锁条件、执行门禁、设备配置、压力工程开关默认值、路由、驱动、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`，未打开COM、控制气路、连接正式数据库、写系数/设备ID；全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- Step 2配置安全摘要复核/交接叶子闭包validation所有权迁移批次：Gate 109继续按函数调用图收口，只把完全基于字典输入与Gate 108共享展示叶子的 `hydrate_step2_config_safety_summary`、`build_step2_config_safety_review` 和 `build_step2_config_governance_handoff` 三个纯函数原样迁入既有 `gas_calibrator.validation.simulation.config`；V2旧路径继续导出同一函数对象，摘要补全、审查状态、阻断原因、交接状态和返回schema均不变。`summarize_step2_config_safety` 明确保留在V2作为本轮停止边界，因为它仍读取 `AppConfig`、枚举设备端口与工程压力开关，并组合双重解锁输入；本批没有把V2配置装配或执行门禁迁入共享validation。三函数AST SHA-256迁移前后分别保持 `b0471c21837a108bf893b1234ced41b6ab2a4fd669c2e8db1b897925f2e297c8`、`d3546829ae5c39e57ef41fd56672dff35b4fd2ea25ad6601bfd6e5da85697414` 和 `39600b44e898d508b5ef10d93ef3aeb9dd50c4b482d3e2227bcd4e4d46b48192`。聚焦配置安全/入口/离线工件71项、42个非线程配置消费者339项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking；`config.models` 静态引用保持V2内部15、测试43、总计58。处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批未修改双解锁条件、执行门禁、设备配置、压力工程开关默认值、路由、驱动、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`，未打开COM、控制气路、连接正式数据库、写系数/设备ID；全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- 传感器预检输入规范化validation所有权迁移批次：Gate 110复核Gate 109后的剩余配置闭包，确认 `SingleDeviceConfig` 仍依赖V2专属配置异常、`DeviceConfig` 依赖前者、`WorkflowConfig` 仍组合压力点选择和分析仪初始化、`AppConfig` 仍组合存储/V2异常/顶层装配，四类均继续作为明确停止边界，不做扩范围迁移。本批只把无V2类型、无I/O、无解锁、无设备命令的 `_normalize_sensor_precheck_config` 原样迁入既有 `gas_calibrator.validation.simulation.config`；V2旧路径继续导出同一函数对象，`WorkflowConfig` 继续通过兼容身份调用，`analyzer_fleet_service` 和V1/V2控制流比较器改为直连共享所有者。profile、scope、validation mode、首台/全分析仪范围和未知自定义键透传语义均未改变，函数AST SHA-256迁移前后保持 `345979c5e036a6b50651d0ab3126b52b2f84911f964fa01d7325d64e29b30b05`，七组代表输入的规范化输出SHA-256逐项不变。直接配置/分析仪队列/控制流比较74项和聚焦配置安全/入口/离线工件71项通过；42个非线程配置消费者339项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking；`config.models` 静态引用由V2内部15降至14、测试保持43、总计由58降至57。处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批未修改传感器预检规则值、分析仪初始化/设备ID配置、双解锁条件、执行门禁、设备端口、压力工程开关、路由、驱动、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`，未打开COM、控制气路、连接正式数据库、写系数/设备ID；全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- 运行模式别名规范化validation所有权迁移批次：Gate 111对Gate 110后剩余三个规范化函数逐项按物理动作风险拆分，确认 `_normalize_analyzer_mode2_init_config` 会生成分析仪流模式重试/命令间隔参数，`_normalize_analyzer_setup_config` 会生成软件版本、设备ID分配及 `apply_device_id`，两者继续留在V2并作为设备命令/ID写入相关停止边界；本批只把不访问设备、文件、环境或解锁状态的 `_normalize_run_mode` 原样迁入既有 `gas_calibrator.validation.simulation.config`。V2旧路径继续导出同一函数对象，`WorkflowConfig` 仍通过兼容身份调用；空值、auto/calibration、CO₂、H₂O/water、experiment/lab和未知值回退到四种既有运行模式的语义均未改变。函数AST SHA-256迁移前后保持 `38babe3ad9c8e1885fd9a9c4bc2f76854b9a44dee095b5fae481b589c9bf369d`，13组代表输入的规范化输出SHA-256逐项不变。模式/计划/路由/编排聚焦59项、配置安全/入口/离线工件71项通过；42个非线程配置消费者339项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking；`config.models` 静态引用保持V2内部14、测试43、总计57。处置清单仍为168个V2模块、23个validation收口模块、0个受保护路径导入违规、0个未解释静态零引用，自动删除继续为false。本批未修改运行模式输出值、route mode、点计划、分析仪初始化/设备ID配置、双解锁条件、执行门禁、设备端口、压力工程开关、路由、驱动、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`，未打开COM、控制气路、连接正式数据库、写系数/设备ID；全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- 压力选择纯语义模块validation所有权迁移批次：Gate 112对23个validation候选按源码规模、调用数、依赖与副作用重新排序，排除会直接执行预检、初始化、采样和CO₂/H₂O路由的 `workflow_steps`，选择仅依赖标准库且不访问设备、气路、文件、数据库或解锁状态的145行 `domain.pressure_selection`。本批把ambient别名、ambient/sealed模式、目标标签、压力值/列表去重和压力键十个常量/函数整体原样迁入 `gas_calibrator.validation.simulation.pressure_selection`，将7个V2运行调用者与3个测试调用者一次性切换到共享所有者后删除旧V2模块，不保留平行实现或兼容包装。迁移前后完整模块AST SHA-256保持 `2aa4b5f0c89c9b5704f30f4270d6e4a6ab379e9caa3eba7a1850f91535bfc907`，ambient/sealed、标签、列表和键的代表输出SHA-256保持 `f9086a3f4fdba7b06559b19e55ef12fcc9abe400ae97b4668ddfa74ea02ceded`。V2计划/解析/路由/结果/离线工件直接消费者67项、成熟V1压力选择18项、配置安全/入口/离线工件71项通过；42个非线程配置消费者339项通过且2项按既有环境合同跳过，`CalibrationService` 8项逐项隔离通过；V1.5正式门禁148项、入口/最终产品边界67项、resilience/parity/suite合同13项、parity 1/1、nightly 24/24和全仓收集5002项通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由168降至167，validation待迁移闭包由23降至22，其中simulation runtime由13降至12；受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批未修改ambient/sealed判定、任何压力数值/顺序、SENCO9压力优先、压力控制、点计划、稳态、采样/QC、拟合、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`，未打开COM、控制气路、连接正式数据库、写系数/设备ID；全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- EC系统辨识纯离线计量模块validation所有权迁移批次：Gate 113对Gate 112剩余22个validation候选继续按物理执行风险排序，冻结带设备读数回调和等待循环的 `stability_checker`、直接执行预检/初始化/采样/气路的 `workflow_steps` 及所有工程探针，选择仅依赖NumPy和共享有限数转换器、不访问设备、气路、文件、数据库或解锁状态的534行 `domain.services.ec_system_identification`。本批把上游参考到DUT的Welch H1传递函数辨识和离线验收汇总两个公共函数整体迁入 `gas_calibrator.validation.metrology.ec_system_identification`，两个V2模拟消费者和两组测试直接切换到共享所有者后删除旧V2模块，不保留平行实现或兼容包装。迁移前后完整模块AST SHA-256保持 `e99fc2faa98a3ca026157bd1a31fa823757689c46df3ebe8e5bc0f7eb6cb5837`，缺失上游参考、样本不足及模拟验收失败三类代表输出SHA-256保持 `91c9907512c89e957ae5e1b258c6e75f587618d3a9ff5c200ad352f818e7d3a9`。所有权/输出/清单聚焦29项、六项离线计量能力70项、V1.5正式门禁148项、入口/最终产品边界56项、导出韧性与parity/suite合同38项、parity 1/1、nightly 24/24和全仓收集5002项通过；nightly中的 `ec_dynamic_system_identification_contract` 与 `gas_analyzer_dynamic_uncertainty_contract` 均保持预期一致。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由167降至166，validation待迁移闭包由22降至21，其中metrology由6降至5；受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批只迁移模拟计量分析所有权，没有改变EC辨识算法、频率点、阈值、工件schema、静态气体校准、拟合输入/公式、压力优先/SENCO9、点计划、稳态、采样/QC、气路、CO₂零气/H₂O干气锚点、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持dry-run/simulated、promotion blocked且不是real acceptance；
- 0620/0621资产资料包纯离线计量模块validation所有权迁移批次：Gate 114对Gate 113剩余5个metrology候选逐一比较依赖、I/O和物理动作边界，冻结包含传递函数、随机台架矩阵或更大运行包络计算的四个模块，选择仅依赖标准库和共享有限数转换器、不读取文件、不访问设备/气路/数据库且只分析字典快照的581行 `domain.services.gas_analyzer_asset_dossier`。本批把0620/0621历史资产资料分析及资料包验收汇总两个公共函数整体迁入 `gas_calibrator.validation.metrology.gas_analyzer_asset_dossier`，V2模拟报告消费者和测试直接切换到共享所有者后删除旧V2模块，不保留平行实现或兼容包装。迁移前后完整模块AST SHA-256保持 `9abf4a38246c5b78305f8ee54b9d71ca98158f304b689e866bc8ba010be6e86f`，观察资料缺口、完整资料但不真实放行及越界设备意图三类代表输出SHA-256保持 `082b22cb104c6553e6740a6aec7525fa074fb73c0729a93f529754b621bb773d`。所有权/输出/清单聚焦20项，0620/0621资产回放、证书与工件治理78项，V1.5正式门禁148项，入口/最终产品边界56项，导出韧性与parity/suite合同38项，parity 1/1、nightly 24/24和全仓收集5002项通过；nightly的 `ga_d5_0620_0621_asset_dossier_gaps` 继续得到“预期缺口已确认”，历史恢复值没有被误认成证书原件，资料完整也不自动授权真实执行。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由166降至165，validation待迁移闭包由21降至20，其中metrology由5降至4；受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批没有改变0620/0621历史事实、气瓶数值、证书字段要求、CO₂零气与H₂O干气/露点参考的独立性、拟合输入/公式、压力优先/SENCO9、点计划、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持replay/simulated/dry-run、promotion blocked且不是real acceptance；
- EC动态路径计量纯离线模块validation所有权迁移批次：Gate 115在Gate 114剩余4个metrology候选中选择规模最小且无I/O的645行 `domain.services.ec_dynamic_metrology`，先冻结20字段物理路径元数据、CO₂/H₂O四通道模拟阶跃响应、传输延迟、上升/下降时间常数、有效传递函数、串联位置延迟顺序和整体验收输出；该模块只依赖NumPy与共享有限数转换器，不读取文件、不访问设备/气路/数据库，也不包含运行入口。本批把 `DynamicPathMetadata`、`analyze_dynamic_channel` 和 `build_dynamic_acceptance` 三个公共符号整体迁入 `gas_calibrator.validation.metrology.ec_dynamic_metrology`，EC动态模拟、系统辨识模拟及两组测试直接切换到共享所有者后删除旧V2模块，不保留平行实现或兼容包装。迁移前后完整模块AST SHA-256保持 `147397ac15386930c50f8eab583ffa25ad20647bd5bf1a68f6c15a355504b253`，四通道物理元数据、分析与验收代表输出SHA-256保持 `7f0fcf404bf5119da6a86aa25a3c7c344573d04a839d1f0250aed158954e12d9`。所有权/输出/清单聚焦27项、六类离线计量70项、V1.5正式门禁148项、入口/最终产品边界56项、导出韧性与parity/suite合同38项、parity 1/1、nightly 24/24和全仓收集5002项通过；nightly中的EC动态、系统辨识和气体分析仪动态不确定度三层合同均保持预期一致，静态校准状态仍为 `not_evaluated`，真实验收仍为blocked。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由165降至164，validation待迁移闭包由20降至19，其中metrology由4降至3；受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批没有改变管路长度/内径/材质、流量、池压/池温、相对湿度、加热管状态、过滤器、传输延迟、时间常数、频率点、阈值、CO₂/H₂O动态记忆规则、静态气体校准、拟合输入/公式、压力优先/SENCO9、点计划、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 气体分析仪动态不确定度纯离线模块validation所有权迁移批次：Gate 116比较Gate 115剩余3个metrology候选，选择仅依赖NumPy和共享有限数转换器、无I/O/日期状态/随机台架生成的805行 `domain.services.gas_analyzer_dynamic_uncertainty`，先冻结系统辨识输入到CO₂/H₂O动态带宽、幅相响应、相干性、工程不确定度预算、贡献项及验收输出的完整链路。本批把动态性能分析和动态不确定度验收两个公共函数整体迁入 `gas_calibrator.validation.metrology.gas_analyzer_dynamic_uncertainty`，V2模拟报告消费者和测试直接切换到共享所有者后删除旧V2模块，不保留平行实现或兼容包装。迁移前后完整模块AST SHA-256保持 `e7a173bd9fe788aa692b88b55d3716e5cb9e9456414ffd1a443cc643af61b537`，清洁CO₂/H₂O、整体验收和高噪声失败代表输出SHA-256保持 `37d07c68e4ea7d6b2b3ff45fedea218c232749d1ab34e39de25f11710c1b3e80`；默认模拟可用带宽继续为CO₂ `0.31614477 Hz`、H₂O `0.137087574 Hz`，高噪声样本继续失败8项门禁。所有权/输出/清单聚焦19项、六类离线计量70项、V1.5正式门禁148项、入口/最终产品边界56项、导出韧性与parity/suite合同38项、parity 1/1、nightly 24/24和全仓收集5002项通过；动态衰减继续作为独立系统偏差而不并入不确定度，工程预算继续明确不是正式计量不确定度，不生成逆向修正系数，EC通量/协谱修正继续不在范围，真实验收仍为blocked。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由164降至163，validation待迁移闭包由19降至18，其中metrology由3降至2；受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批没有改变频率网格、带宽阈值、幅相公式、相干性规则、覆盖因子、RSS合成、时钟/参考/泄漏贡献项、CO₂/H₂O动态模型、静态气体校准、拟合输入/公式、压力优先/SENCO9、点计划、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 气体分析仪台架协议准备度纯离线模块validation所有权迁移批次：Gate 117比较Gate 116剩余2个metrology候选，冻结依赖NumPy且包含更大测量/干扰矩阵的1209行 `gas_analyzer_operating_envelope`，选择只依赖标准库和共享有限数转换器、无设备/气路/文件/数据库I/O的960行 `domain.services.gas_analyzer_bench_readiness`。本批先冻结协议日期、证书有效期、8类溯源资产、27格温度/压力/流量环境矩阵、CO₂/H₂O独立锚点与气路、固定随机种子、双向/双会话/双重复测量计划、conditioning、干扰计划、k=2 RSS工程不确定度预算和46项准备度门禁，再把分析与验收两个公共函数整体迁入 `gas_calibrator.validation.metrology.gas_analyzer_bench_readiness`；V2模拟报告消费者和测试直接切换到共享所有者后删除旧V2模块，不保留平行实现或兼容包装。迁移前后完整模块AST SHA-256保持 `6ba9000f581d4f941d5dda54d3e17d2ad24ae01034a80c8478394adcaa0682e0`，固定夹具SHA-256保持 `bdfa5474066838ce5fb3ba507dac0fa165479830e688d9a049d1d6e984f14cae`，清洁、错误随机种子、过期证书和越界写入意图四组代表输出SHA-256保持 `d5831f57aef6253723644fc9f330284b4824aa51b87c2df27199d90a67f28b9f`。清洁模拟继续由种子CO₂ `20260724`、H₂O `20260725`生成各1296行计划，CO₂/H₂O扩展不确定度span fraction分别保持 `0.001843908891` 和 `0.003498571137` 且均在合同限内；错误随机种子、过期证书和任一I/O/系数/数据库写入意图分别继续失败对应门禁。所有权/输出/清单聚焦24项、六类离线计量70项、V1.5正式门禁148项、入口/最终产品保护边界22项、离线治理合同93项及非nightly suite执行合同4项通过；实际parity 1/1、nightly 24/24和全仓收集5002项闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由163降至162，validation待迁移闭包由18降至17，其中metrology由2降至1；受保护路径导入违规、未解释静态零引用、旧所有者引用和兼容包装均为0，自动删除继续为false。台架协议准备度仍只表示离线设计完整，不是执行授权或真实计量验收；CO₂零气与H₂O干气/低水锚点继续保持不同物理含义，工程预算不替代正式不确定度评定，EC通量继续不在范围。本批没有改变协议日期、证书判定、环境矩阵、随机化算法/种子、conditioning、干扰/预算公式与阈值、静态气体校准、拟合输入/公式、压力优先/SENCO9、点计划、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 气体分析仪综合工作包络纯离线模块validation所有权迁移批次：Gate 118处理最后1个metrology候选，确认1209行 `domain.services.gas_analyzer_operating_envelope` 只依赖NumPy、标准库和共享有限数转换器，输入为内存测量行、干扰行、动态性能字典与合同，不读取文件、不访问设备/气路/数据库或解锁状态。本批先冻结CO₂/H₂O静态精度、重复性、滞后、漂移、线性、温度/压力/流量敏感度、交叉干扰、27格环境包络、动态带宽/时延依赖和64项整体验收门禁，再把分析与验收两个公共函数整体迁入 `gas_calibrator.validation.metrology.gas_analyzer_operating_envelope`；V2模拟报告消费者和测试直接切换到共享所有者后删除旧V2模块，不保留平行实现或兼容包装。迁移前后完整模块AST SHA-256保持 `102c4179395878b0e06c19a1a8294433208cca12de6790c73d9e24ab3065b1c6`，2592行静态测量加18行干扰夹具SHA-256保持 `ec97a97aa8cb7c09b025b7cfe07ba42445c77cbe97b92c934fa29f7136cd6b12`，清洁、矩阵缺失/重复/越界、坏环境角点、动态带宽/时延退化及参考质量/帧异常五组代表输出SHA-256保持 `eb62ca53929478dd30b36386c0fe15f90cc0e777291637c9465e499407f1676d`。清洁模拟继续由CO₂/H₂O各1296行完整测量、各9行干扰和各27格合格环境包络组成，静态、动态依赖和综合包络状态分别保持 `simulation_static_envelope_pass`、`simulation_dynamic_dependency_pass` 和 `simulation_operating_envelope_pass`；坏角点继续触发8项物理指标/环境格门禁，H₂O动态退化继续触发带宽与时延2项门禁，参考质量、不可用帧和非有限值继续失败关闭。聚焦所有权/输出/清单20项、六类离线计量70项、V1.5正式门禁148项、入口/最终产品保护边界22项、离线治理合同93项和未使用导入3项通过；实际parity 1/1、nightly 24/24、全仓收集5002项及Ruff闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由162降至161，validation待迁移闭包由17降至16，metrology由1降至0；剩余闭包严格为simulation config 1、simulation runtime 12和engineering probe 3，受保护路径导入违规、未解释静态零引用、旧所有者完整导入和兼容包装均为0，自动删除继续为false。工作包络结果继续只是simulated diagnostic，不是静态正式校准、真实执行授权或real acceptance；不执行系数拟合/写回，不施加自动动态修正，EC通量/协谱闭合继续不在范围，CO₂零气与H₂O干气锚点继续保持不同物理含义。本批没有改变矩阵轴、目标值、阈值、任何数值公式、动态依赖、拟合输入/公式、压力优先/SENCO9、点计划、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- V2纯仿真领域模型validation所有权迁移批次：Gate 119在metrology闭包归零后重新比较1个simulation config与12个simulation runtime候选，明确冻结3个engineering probe；`route_planner`仍反向依赖V2 `AppConfig`、`CalibrationPoint`与`PointParser`，`device_factory/device_manager`会装配驱动，`stability_checker`包含等待循环，`workflow_steps`直接执行预检、初始化、采样及CO₂/H₂O runner，均不作为本批迁移对象。最终选择400行 `v2.domain`：它只定义5组枚举、15个dataclass、2个模式函数和22个公共符号，不读写文件、不访问设备/气路/数据库、不包含执行入口。全部源码与测试消费者一次性切换到 `gas_calibrator.validation.simulation.domain` 后删除旧V2所有者，不保留平行实现或兼容包装。旧模块仅有的V2反向依赖位于不会在运行时执行的 `TYPE_CHECKING` `OutlierResult` 导入；本批不迁移或复制QC实现，而以共享模块内非导出 `Any` 类型占位维持 `CleanedData.outlier_result` 原有字段和前向注解文本，使共享validation不反向导入V2。22个公共定义AST束SHA-256迁移前后保持 `738dbcec58f726be1d2d359a5b58d7c3008659c4c7b83c709af7c3cc18fed00c`，枚举值、15组字段结构、模式别名/路由/正式报告规则、dataclass序列化及四类解释文本的代表行为SHA-256保持 `141c485dec8a12c43e3b96c97d2b5b72cf4258afcadf3c306c5efaef354ce0c7`。直接消费者/导入顺序/清单61项迁移前后通过，完整算法/QC/计划/智能消费面59项、V1.5正式门禁148项、入口/最终产品保护边界22项、离线治理合同93项和未使用导入3项通过；实际parity 1/1、nightly 24/24、全仓收集5002项及Ruff闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由161降至160，validation待迁移闭包由16降至15，其中simulation runtime由12降至11；剩余闭包严格为simulation config 1、simulation runtime 11和engineering probe 3，受保护路径导入违规、未解释静态零引用、旧所有者完整导入和兼容包装均为0，自动删除继续为false。本批没有改变枚举值、模式归一化、route mode、正式报告默认规则、模型字段/默认值、QC对象、算法建议文本、计划编译、拟合、压力优先/SENCO9、点计划、稳态、采样、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 稳定性判据与等待器validation所有权迁移批次：Gate 120在Gate 119冻结的等待循环上先做依赖切割，确认439行 `v2.core.stability_checker` 不直接打开设备、串口、气路、文件或数据库，只对上层提供的读数回调执行单调时钟轮询、窗口裁剪和稳定判定；由于共享validation不得反向依赖V2，本批同时把 `CalibrationError`、`StabilityError`、`StabilityTimeoutError` 和 `StabilityNotReachedError` 四类异常迁入唯一所有者 `gas_calibrator.validation.exceptions`，V2异常表面显式再导出同一对象，使其余设备/压力/配置/工作流/数据异常继续继承完全相同的 `CalibrationError`，不形成第二套异常层。全部运行与测试消费者切换到 `gas_calibrator.validation.simulation.stability_checker` 后删除旧V2实现，不保留旧模块包装；`v2.core` 包级历史出口只延迟再导出共享对象，并以身份测试和旧模块不存在测试锁定。迁移前后五个稳定性公共/私有定义AST束SHA-256保持 `6faf0a4015925883b4a2c5b30929c8258eafea2bd86b7a4bcec1edaa6a8b89ed`，四类异常AST束SHA-256保持 `495a2463b589a53e01671c312b8eb5ec178a6d84c81b80319f136acea36c8a85`；温度、湿度、压力、信号代表判据、假时钟超时轨迹、调试首末行、异常消息/context/to_dict/MRO的行为SHA-256保持 `9fe484081692e536a0381d4ee035cc87129f276f07508fd27ecf004d712b112b`。迁移前55项、迁移后带身份合同56项直接回归，服务消费面69项、所有权/边界/卫生32项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项通过；实际parity 1/1、nightly 24/24、全仓收集5003项及Ruff/diff检查闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non-blocking。V2模块由160降至159，validation待迁移闭包由15降至14，其中simulation runtime由11降至10；剩余闭包严格为simulation config 1、simulation runtime 10和engineering probe 3，受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批没有改变严格小于容差的判据、至少2个有效样本、窗口/标准差/最小等待/超时语义、轮询间隔、停止事件、四类稳定性阈值、温箱/露点/压力/信号物理口径、拟合、压力优先/SENCO9、点计划、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备读数、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 点位解析器与运行点模型validation所有权迁移批次：Gate 121重新审计剩余simulation runtime闭包，冻结会构造 `AppConfig`、`RoutePlanner` 和运行计划的 `plan_compiler`，只选择不装配设备、不执行气路且不产生运行副作用的 `point_parser` 依赖单元。本批把 `PointFilter`、`TemperatureGroup`、`LegacyExcelPointLoader`、`PointParser` 整体迁入 `gas_calibrator.validation.simulation.point_parser`，把解析器与既有运行消费者共同依赖的详细 `CalibrationPoint` 迁入 `gas_calibrator.validation.simulation.runtime_point`，并把 `DataError`、`DataParseError`、`DataValidationError` 迁入共享异常唯一所有者；V2包级出口、`core.models` 和 `v2.exceptions` 只再导出同一对象，旧 `v2.core.point_parser` 实现删除且不保留兼容模块。四个解析器定义AST束SHA-256迁移前后保持 `5bf9f5d76bc0c22c117f67ca0a188b293d98ec7c8a5c5f4aa021bd8e1b274897`，详细运行点AST SHA-256保持 `565930eaaa3bcb074fdf84c60f8bdb09876b6365d881179163d8b70a5cadf66a`，三类数据异常AST束SHA-256保持 `eccdd9cb19c2986b0573e4c06b20b82d6dc39ff2e9b8cf3668c206bb81c5b7f5`，JSON/CSV/旧Excel兼容、压力选择、过滤、分组、异常及对象身份代表行为SHA-256保持 `7f99f8e22c81fd1c485506a3649ffd934095b6ad7c40a219f47161fdac997ed7`。迁移前53项通过、2项因仓库真实 `points.xlsx` 夹具缺失按合同跳过，迁移后增加身份合同为54项通过、2项同因跳过；运行点消费面125项、比较/入口/导入顺序/卫生/清单/边界60项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24、全仓收集5004项及Ruff闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。V2模块由159降至158，validation待迁移闭包由14降至13，其中simulation runtime由10降至9；剩余闭包严格为simulation config 1、simulation runtime 9和engineering probe 3，受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批没有改变旧Excel点位兼容语义、压力选择/标签、点位过滤/温度分组、运行点字段或属性，也没有改变CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、点计划、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 路由规划器validation所有权迁移批次：Gate 122把 `route_planner` 与 `plan_compiler` 作为一个依赖单元重新审计，确认 `plan_compiler` 仍直接构造V2 `AppConfig`、调用V2产品报告清单和 `calibration_service.prepare_points_for_execution`，因此继续冻结而不强迁；`RoutePlanner` 自身则只读取上层传入的内存 `workflow` 配置，并对共享 `CalibrationPoint`、`PointParser` 与压力选择值执行温度分组、CO₂/H₂O顺序、压力展开、carry-forward、ambient标签和进度键规划，不装配设备、不等待、不读写文件/数据库，也不打开串口或控制气路。本批把该类整体迁入 `gas_calibrator.validation.simulation.route_planner`，以模块内非导出 `Any` 类型别名解除V2 `AppConfig` 的静态反向依赖；`calibration_service`、`orchestrator`、冻结的 `plan_compiler` 及全部路由runner测试直接切换到共享所有者，旧 `v2.core.route_planner` 删除且不保留兼容包装。迁移前后完整 `RoutePlanner` 类AST SHA-256保持 `d82c1c6c921bbc4cca296f0faef6a0025eb88498082586b00d198081c357225b`；覆盖水气顺序、低温禁水、阈值、skip CO₂、稀疏carry-forward、ambient/numeric压力展开、CO₂来源排序、H₂O分组、采样点构造、标签和去重进度键的代表行为SHA-256保持 `324f85984c14ea0b949b9ee235bab6f1c3f9138f860a86d0427a079238685140`。迁移前32项、迁移后增加所有权合同为33项通过；运行消费者61项通过、2项因真实 `points.xlsx` 缺失按合同跳过，清单/入口/边界/导入卫生32项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24、全仓收集5005项及Ruff/compileall闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。V2模块由158降至157，validation待迁移闭包由13降至12，其中simulation runtime由9降至8；剩余闭包严格为simulation config 1、simulation runtime 8和engineering probe 3，受保护路径导入违规、未解释静态零引用和兼容包装均为0，自动删除继续为false。本批没有改变route mode、低温H₂O规则、water-first阈值、压力顺序、carry-forward、skip CO₂、点位构造、标签、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 点位执行准备子闭包validation所有权迁移批次：Gate 123继续拆解Gate 122冻结的 `plan_compiler -> calibration_service` 依赖，确认 `normalize_negative_temperature_route`、`filter_selected_temperatures`、`reorder_points_for_execution`、`prepare_points_for_execution` 和 `parse_points_for_execution` 五个函数只依赖共享点模型/解析器/路由规划器及现有V1纯点位重排函数；前四个只处理内存对象，第五个只通过已共享解析器读取显式点位文件，不装配设备、不等待、不访问串口/气路/数据库，也不产生写入。本批把五个定义整体迁入唯一所有者 `gas_calibrator.validation.simulation.point_preparation`；`CalibrationService` 只通过私有身份别名调用共享实现，冻结的 `PlanCompiler` 直接调用共享准备函数，不再导入整个 `calibration_service`，旧模块不保留同名公共函数表面。迁移前后五函数AST束SHA-256保持 `f8f96b60a4af5b4e9afec6da226b4c4d8770f0f648889348c5fa613155cb88e7`；覆盖负温H₂O转CO₂并清空水汽上下文、有效/无效温度筛选及日志、升降温排序、route过滤和JSON解析准备的代表行为SHA-256保持 `4fe2612da78876759149211ae463d37591d007dafc3a9649b9d22da17d9659f0`。迁移前31项通过、2项因真实 `points.xlsx` 缺失按合同跳过，迁移后增加唯一所有者合同为32项通过、2项同因跳过；计划编译/执行服务/入口/逐帧消费者65项、清单/入口/边界/导入卫生32项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24、全仓收集5006项及Ruff/compileall闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。本批是依赖切割而非删除整个执行模块，因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3）；但 `calibration_service` 的V2内部消费者由3降至2，兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变负温水汽处理、温度筛选容差/日志、升降温和water-first顺序、点位过滤、解析格式、route mode、压力顺序、carry-forward、skip CO₂、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 计划输入模型validation所有权迁移批次：Gate 124继续拆解Gate 123后的 `plan_compiler`，逐一定义确认 `AnalyzerSetupSpec`、`TemperatureSpec`、`HumiditySpec`、`GasPointSpec`、`PressureSpec`、`PlanOrderingOptions` 和 `CalibrationPlanProfile` 及其九个规范化函数只处理内存数据、模式/压力共享值和字典序列化，不依赖V2 `AppConfig`、产品报告、执行服务、设备、气路、文件或数据库。本批把七类计划输入合同整体迁入唯一所有者 `gas_calibrator.validation.simulation.plan_models`；`PlanCompiler` 只通过私有类型别名消费共享对象，三个测试消费者改为直连共享所有者，旧编译器模块不再定义或公开同名模型。`CompiledPlan` 明确保留在V2，因为其 `to_runtime_payload()` 仍承载 `formal_calibration_report`、`report_family` 和 `report_templates` 等V2产品报告语义；`PlanCompiler` 本体也继续留在V2，因为仍构造 `AppConfig` 并调用 `build_product_report_manifest`，没有为了模块数强行把配置或报告反向迁入validation。迁移前后七类模型与九函数AST束SHA-256保持 `28fb386977e61cb3920a5c5b042aa7a44f159dd393e4697beb9a7017d17d9b58`，覆盖分析仪版本/设备ID、温湿度、CO₂组别与气瓶标称值、ambient/controlled压力、显式排序标记、模式及profile往返的代表序列化SHA-256保持 `b6d090087bc31ed486239439f62bf934b23241e7d12c004aee22e0ff216e059c`；`plan_compiler.py` 从820行收口到429行。迁移前计划模型/编译/状态机15项通过，迁移后含唯一所有者和无旧公共表面合同为19项通过；计划编译/点解析/路由/执行/入口/报告消费者59项、清单/入口/边界/卫生41项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24、全仓收集5007项及Ruff/compileall闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `no_write=true`、`opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。本批是模型所有权与编译适配分层，不删除仍有2个V2内部消费者的编译模块，因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3）；兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变字段名/默认值/别名、排序、压力标签、点计划、report metadata、route mode、carry-forward、skip CO₂、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 计划行构建纯闭包validation所有权迁移批次：Gate 125继续拆解Gate 124收口后的429行 `plan_compiler`，确认源行生成、H₂O carry-forward展开、行转运行点、执行预览排序、温度/通用规格/压力排序及压力行字段生成共八个方法只处理共享计划模型、运行点、解析器和路由规划器；原方法仅通过 `effective_config.workflow` 读取 `selected_temps_c`、`selected_pressure_points`、`skip_co2_ppm` 与 `h2o_carry_forward`。本批将这四个配置值改为显式函数参数，把八个定义整体迁入唯一所有者 `gas_calibrator.validation.simulation.plan_rows`，共享模块不导入V2配置、报告、设备或执行层；`PlanCompiler.compile()` 继续负责从V2 `AppConfig` 取值并私有调用共享函数，旧类不再保留八个方法。迁移前旧方法AST束SHA-256为 `995504fd98620b22b78a1eb8c74a9ff368d3265627ef6b6508fe1a60bb1e17d5`；因方法改为模块函数、去除 `self/effective_config` 并改用显式参数，迁移后共享函数AST束SHA-256为 `83fe90880adbb584bb57222aec07b91c321d3f7494a4c82d7c11c7886932b806`。覆盖disabled/order、温度筛选、ambient+900 hPa选择、skip0、H₂O carry-forward、负温禁水、CO₂组别/气瓶标称值、行转点、执行预览和报告封装的14行代表计划行为SHA-256迁移前后保持 `f1c01aded450f59eb2cfeef4cd525ed19608f38a41e3001f7cf1b00bdf8976d6`；`plan_compiler.py` 从429行进一步收口到244行。迁移前模型/编译/状态机/卫生19项通过，迁移后增加共享函数直测、无V2导入和旧方法不存在合同为23项通过；计划编译/点解析/路由/执行/入口/报告消费者63项、清单/入口/边界/卫生45项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24、全仓收集5011项及Ruff/compileall闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `no_write=true`、`opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。`CompiledPlan`、`PlanCompiler.compile()`、`_effective_config()`、V2报告manifest和metadata继续留在V2，未把产品报告语义或配置装配反向塞入validation；因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3），兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变点位字段/索引、温湿度或压力顺序、ambient标签、carry-forward、skip CO₂、route mode、report metadata、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 编译计划预览匹配纯闭包validation所有权迁移批次：Gate 126继续拆解Gate 125后的244行 `plan_compiler`，确认 `CompiledPlan.preview_rows()` 与 `_match_runtime_row()` 只按route、温度、压力选择键、CO₂浓度和CO₂组别匹配内存运行行，再生成预览序号、点位字段、压力标签与气瓶标称值；两者不读取配置、报告、文件、数据库或设备。本批把预览行构建和运行行匹配迁入唯一所有者 `gas_calibrator.validation.simulation.plan_preview`，`CompiledPlan.preview_rows()` 仅私有调用共享函数，旧 `_match_runtime_row()` 方法删除；共享模块只依赖压力选择键和共享运行点，不导入V2。迁移前两方法AST束SHA-256为 `059ddeb159c202c91d0a12e205dfcb98910315d70d0687862424c5cfb8ed226a`，因实例方法改为显式 `preview_points/runtime_rows` 参数的模块函数，迁移后共享函数AST束SHA-256为 `bf6dfa9568531c6d4f55ac650f617d2fcdc7e8ba9db2c4a4edc291f8cddbc403`；覆盖同温同浓度不同CO₂组别、ambient压力、H₂O受控压力、气瓶标称值、无匹配行和V2报告载荷的代表行为SHA-256迁移前后保持 `c88817f8a3b0d889eab2b338de1951e7e76f88dd06d481aaabb6e6edf5a69360`。新增反向边界测试明确 `CompiledPlan` 及 `to_runtime_payload()` 继续属于V2，`formal_calibration_report`、`report_family`、`report_templates` 和 `analyzer_setup` 没有迁入validation；`plan_compiler.py` 从244行进一步收口到203行。迁移前模型/行规划/编译/状态机/卫生23项通过，迁移后增加共享所有者、零V2导入、CO₂组别匹配、无输入变异和报告边界合同为26项通过；计划编译/点解析/路由/执行/入口/报告消费者66项、清单/入口/边界/卫生48项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24、全仓收集5014项及Ruff/compileall闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `no_write=true`、`opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。`CompiledPlan`、V2报告payload、`PlanCompiler.compile()`、`_effective_config()` 和产品报告manifest继续作为明确停止边界，因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3），兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变预览字段/schema、匹配优先级、点位索引、CO₂组别或气瓶标称值、压力键/标签、report metadata、点计划、路由、carry-forward、skip CO₂、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、采样/QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 采样快照纯函数子闭包validation所有权迁移批次：Gate 127重新盘点剩余8个simulation runtime模块，冻结直接装配驱动的 `device_factory/device_manager`、直接采样且含等待的 `sampling_service` 执行部分、运行编排器、workflow steps、入口和已到V2配置/报告停止边界的203行 `plan_compiler`，只从 `sampling_service` 中选择对内存字典执行展平、数值/文本选择、湿度范围过滤、非空判定和重试原因生成的7个纯函数。本批把 `normalize_snapshot`、`pick_numeric`、`pick_text`、`sanitize_humidity_value`、`pick_humidity_value`、`snapshot_has_data` 和 `snapshot_retry_reason` 迁入唯一所有者 `gas_calibrator.validation.simulation.sampling_snapshot`；共享模块只依赖共享 `utils`，不导入V2，不读取设备、不等待、不读写文件/数据库，也不控制气路。V2 `SamplingService` 保留同名静态/类方法作为内部兼容委托，现有运行消费者无需改变调用序列；设备读取、重试循环、线程池、采样间隔、质量判定、结果行、分析仪完整性和持久化均未迁移。迁移前7方法AST束SHA-256为 `9678a9414cb5fd5cedc1c8f1e523e283ee9ba15a68d1f78f715e6736f44f4ee7`；去除类/self调用并改为模块函数后的共享AST束SHA-256为 `f114866fac6f58c395a16f140d6224ccb664ad75bcb4cfb466eef674e8d04902`，覆盖嵌套data外层覆盖、非字典、候选数值/文本、0%与100%湿度边界、越界湿度、缺数值、空快照和成功快照的代表行为SHA-256迁移前后保持 `ebee794a6124e47557deee645d1d8b82f5f66a55c0d1c6f54728db9edf87dff7`；`sampling_service.py` 从992行收口到966行。新增3项共享所有权/零V2导入/行为/委托合同，直接聚焦14项、采样相关消费面94项（首次并行线程停止测试出现一次超时，单项及完整校准服务模块复跑均通过）、边界/卫生44项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24和全仓收集5017项闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `no_write=true`、`opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。本批是执行模块内部纯函数依赖切割，不删除仍有运行消费者的整个模块，因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3），兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变快照键优先级/覆盖语义、湿度有效范围、重试次数/间隔/日志、线程或采样时序、逐帧字段、采样质量阈值、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、QC、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 采样质量跨度纯函数子闭包validation所有权迁移批次：Gate 128继续审计Gate 127后的966行 `sampling_service`，确认 `span` 与 `evaluate_sample_quality` 只对调用者提供的内存采样行计算CO₂浓度、H₂O摩尔分数、压力和露点跨度，并与显式质量配置阈值比较；两者不读取设备、不等待、不访问文件/数据库，也不控制气路。采样次数/间隔仍读取运行配置且影响物理采样时序，分析仪完整性摘要仍包含用户可见中文语义，因此本批继续冻结 `sampling_params` 与 `summarize_analyzer_integrity`。本批把跨度与质量判定迁入唯一所有者 `gas_calibrator.validation.simulation.sampling_quality`，质量配置改为显式 `quality_config` 参数；V2 `SamplingService` 只读取原路径 `workflow.sampling.quality` 后调用共享函数，并保留 `span/evaluate_sample_quality` 内部兼容表面。迁移前两方法AST束SHA-256为 `46d1ad437777bad530fc05a4b668e3b9a3fe7affe1a1a2d3c190f5a808d9f1cb`，去除host/self依赖后的共享函数AST束SHA-256为 `c4e26e5dbbb020aba39d14eebaa56b775cb0a0665dc33f1673803c879f82a9bd`；覆盖质量禁用、单样本/空样本跨度0、四类物理量、缺失值、精确等于阈值放行和严格超限失败的代表行为SHA-256迁移前后保持 `a8167dab2ba6953d4a038363e64d9e1dad4d5a91bd51d8d7f953f350f05a169b`。`sampling_service.py` 从966行进一步收口到951行；新增3项共享所有权/零V2导入/边界/委托合同，直接聚焦17项、采样相关消费面97项、边界/卫生47项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24和全仓收集5020项闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `no_write=true`、`opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。本批仍是执行模块内部纯函数依赖切割，不删除整个运行模块，因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3）；兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变质量配置路径、四个字段键、阈值值、严格大于比较、缺失值处理、采样次数/间隔、设备读取/重试、线程池、逐帧字段、分析仪完整性、持久化、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 分析仪帧完整性摘要纯函数子闭包validation所有权迁移批次：Gate 129继续审计Gate 128后的951行 `sampling_service`，确认 `summarize_analyzer_integrity` 只根据调用者提供的采样行和分析仪标签计算期望数、有帧数、可用数、覆盖率、缺失/异常标签及中文完整性状态；它不读取设备、不等待、不访问文件/数据库，也不参与正式QC判定。迁移前先用源码字节、Git基线和运行时Unicode码点确认终端偶发乱码只是Windows输出编码，程序真实状态文本始终为“无分析仪、无帧、仅异常帧、部分缺失、含异常帧、部分缺失且含异常帧、完整”；本批没有顺带改文案。`sim.parity` 存在输入为 `SamplingResult` 集合并采用集合排序的相似摘要，但语义与顺序合同不同，继续独立冻结，避免为了去重误改parity口径。本批把采样行摘要迁入唯一所有者 `gas_calibrator.validation.simulation.sampling_integrity`，V2 `SamplingService` 保留同名内部委托。迁移前方法AST SHA-256为 `a6a12afc36646fbe8bb4543f67b9c0fb95c4107cf6e6f06c915790625f88e04a`，去除self后的共享函数AST SHA-256为 `2e4203ea0e2d4cccdab60078562072289faea14b4fded731c6eda3e571695ee7`；覆盖九种代表组合、全部可达中文状态、零可用但同时缺失/异常时的优先级、标签空格转下划线、显示大写和逗号顺序的Unicode行为SHA-256迁移前后保持 `fdfbe01e507a645bd04c4644b7f356ecaa4906378ff3bd28e9dc60bc98ab593f`。`sampling_service.py` 从951行进一步收口到915行；新增9项共享所有权/零V2导入/状态分支/委托合同，直接聚焦26项、采样相关消费面106项、边界/卫生56项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24和全仓收集5029项闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `no_write=true`、`opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。本批仍是执行模块内部纯函数依赖切割，不删除整个运行模块，因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3）；兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变帧存在/可用键、跨行any语义、状态优先级、标签顺序、CSV字段、采样次数/间隔、设备读取/重试、线程池、结果行、质量阈值、持久化、正式QC、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- 采样结果行schema纯转换子闭包validation所有权迁移批次：Gate 130继续审计Gate 129后的915行 `sampling_service`，确认 `STANDARD_ANALYZER_ROW_FIELDS`、`standard_analyzer_row_values` 与 `sampling_result_to_row` 只读取调用者提供对象的结构属性并生成内存字典；不需要V2运行类型本身，不读取设备、不等待、不写CSV/文件/数据库，也不执行正式QC或拟合。本批先冻结9个标准分析仪字段的名称/顺序与缺失属性回退、31个完整采样行字段的名称/顺序/值，特别保持目标 `co2_ppm` 与实测 `sample_co2_ppm` 分离、`dew_point_c` 历史拼写、气瓶标称值/CO₂组别、参考压力/温度状态、帧状态和 `timestamp.isoformat()`。随后以结构化 `Any` 解除V2 `SamplingResult` 静态依赖，将常量和两个转换函数迁入唯一所有者 `gas_calibrator.validation.simulation.sampling_rows`；V2 `SamplingService` 的类常量继续与共享常量保持对象身份相同，两个旧方法仅作内部委托，`orchestrator` 的既有包装调用顺序不变。迁移前常量与两方法AST束SHA-256为 `0efda470cfe7aec31cbdf8d97acce4499efc9f0ef49ecb1823078e5d82519218`，共享常量与两函数AST束SHA-256为 `22f10ca0b559542e8504aab03c28bd77409790e4e517ee528068b2eb9da3da8e`；包含字段顺序的完整代表行为SHA-256迁移前后保持 `54e57816556be3c39f74399cd5d34ac8157e94cb170c2332c7d0eedd459f3588`。`sampling_service.py` 从915行进一步收口到875行；新增3项共享所有权/零V2导入/字段顺序/值/缺失回退/委托合同，直接聚焦29项、采样相关消费面109项、边界/卫生59项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24和全仓收集5032项闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；dry-run明确 `no_write=true`、`opens_com_ports=false`、`controls_water_or_gas_routes=false`、`writes_coefficients=false`、`writes_device_id=false`。本批仍是执行模块内部纯转换依赖切割，不删除整个运行模块，因此V2模块保持157、最终validation闭包保持12项（simulation config 1、simulation runtime 8、engineering probe 3）；兼容包装、受保护路径导入违规和未解释静态零引用仍为0，自动删除继续为false。本批没有改变任何字段名/顺序/含义、目标与实测区分、时间格式、空值、逐帧解析、采样次数/间隔、设备读取/重试、线程池、质量阈值、完整性状态、CSV写入、持久化、正式QC、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部suite证据保持simulated/dry-run、promotion blocked且不是real acceptance；
- CO₂ 45点 + H₂O 13点成熟 runner dry-run 通过，COM/气路/系数/设备ID均未写；
- 采样纯合同合并与样本选择闭包批次：Gate 131先冻结 `samples_for_point` 的逐对象选择语义：点索引与气路精确匹配，调用方点标签仅去除两端空格后区分大小写精确匹配，phase两端去空格并转小写，历史结果phase为空时仍与指定phase兼容，结果保持输入顺序和对象身份；选择方法迁移前AST SHA-256为 `a2700f298de8dc256a324c62f3d6bc3ccb12206cac696ab474033dbdc34d947d`，代表行为SHA-256迁移前后保持 `11c08ef634d60c0d3f703257514c3933a7d7ff82fbac203559530364b2121baa`。为避免Gate 127—130形成五个过小共享模块，本批同时把采样快照、质量跨度、帧完整性、结果行schema与样本选择统一收口到单一所有者 `gas_calibrator.validation.simulation.sampling_contracts`，删除五个临时碎片模块，净减少四个共享源码模块；合并文件307行、文件SHA-256为 `897cd01b2d6711d21fdc7a550183d4e9cbdf295aec4d285304083f293c2e1bb6`，只依赖标准库、`typing.Any` 和共享 `gas_calibrator.utils`，不导入V2。五组已冻结代表行为SHA-256继续分别保持 `ebee794a6124e47557deee645d1d8b82f5f66a55c0d1c6f54728db9edf87dff7`、`a8167dab2ba6953d4a038363e64d9e1dad4d5a91bd51d8d7f953f350f05a169b`、`fdfbe01e507a645bd04c4644b7f356ecaa4906378ff3bd28e9dc60bc98ab593f`、`54e57816556be3c39f74399cd5d34ac8157e94cb170c2332c7d0eedd459f3588` 和 `11c08ef634d60c0d3f703257514c3933a7d7ff82fbac203559530364b2121baa`；`SamplingService` 改为一个共享导入块并保留原委托表面，从Gate 130的875行进一步收口到864行。最终合并形态直接聚焦32项、采样相关消费面112项、边界/卫生62项、成熟V1.5路由与正式预检150项、导出韧性/套件合同20项、parity 1/1、nightly 24/24和全仓收集5035项闭合。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；V2清单保持157个模块、最终validation闭包12项（simulation config 1、simulation runtime 8、engineering probe 3、metrology 0），兼容包装、受保护路径导入违规和未解释静态零引用均为0，自动删除为false。本批没有改变设备读取/重试、线程池、采样次数/间隔、逐帧解析、字段顺序、质量阈值、完整性状态、持久化、正式QC、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部证据仍是simulation/replay/dry-run、promotion blocked且不是real acceptance；
- 采样重复转发表面退役批次：Gate 132在Gate 131统一共享所有者后，对 `SamplingService` 类常量和方法逐项做仓库调用图审计，确认 `STANDARD_ANALYZER_ROW_FIELDS` 常量别名、12个纯合同转发方法以及1个零调用的 `sensor_read_retry_settings` 实例转发没有独立运行语义；12个纯转发方法AST束SHA-256为 `7e67b6b9c71658b72dd618efe4520aa16af6efe2768eea73b3f08bc519f78f2c`，仓库内唯一独立消费者均为专属兼容断言。实际共享所有者 `sampling_contracts.py` 保持307行和文件SHA-256 `897cd01b2d6711d21fdc7a550183d4e9cbdf295aec4d285304083f293c2e1bb6` 不变；本批不新增模块，改由 `SamplingService` 执行方法直接调用共享函数，`WorkflowOrchestrator` 保留被温控、压力、湿度、露点和分析仪服务使用的host兼容入口，但其实现直接调用共享合同。`samples_for_point` 因仍负责从 `result_store` 取样后执行共享过滤而保留；设备快照、重试等待、线程池、采样时序、结果落盘和点级编排也全部保留。`sampling_service.py` 从864行降至810行，`orchestrator.py` 为直连共享所有者增加7行导入，产品源码净减少47行；最终服务文件SHA-256为 `218c2934be4139b440ad94ee3541577945aca130000637ec1246bda93be9a696`，13个已退役方法和常量别名的运行引用归零。最终形态聚焦32项、直接采样/编排消费者133项、边界/卫生47项、成熟V1.5路由与正式预检183项、导出/套件韧性26项、parity 1/1、nightly 24/24和全仓收集5035项闭合；其中nightly合同因单项约148秒按可审计单项执行，不把工具组合超时冒充程序失败或通过。成熟 `v1_5_legacy_ratio_0613_0620_0621` 内核继续按CO₂ 45点/H₂O 13点完成no-write dry-run，证书启动门禁保持non_blocking；V2清单仍为157个模块、最终validation闭包12项（simulation config 1、simulation runtime 8、engineering probe 3、metrology 0），兼容包装、受保护路径导入违规和未解释静态零引用均为0，自动删除为false。本批没有改变五组冻结采样行为、逐帧解析、字段顺序、质量阈值、完整性状态、采样次数/间隔、设备读取/重试、持久化、正式QC、点计划、CO₂零气与H₂O干气锚点、拟合、压力优先/SENCO9、稳态、气路、成熟runner、默认配置、V1.5 full-flow或 `run_app.py`；未打开COM、调用真实设备、控制气路、连接正式数据库、写系数/设备ID，全部证据仍是simulation/replay/dry-run、promotion blocked且不是real acceptance；
- 全量离线发布冻结批次：Gate 133不增加或修改功能代码，把Gate 132当前树的5035个唯一pytest node ID按536个完整测试文件加权划分为8个互斥分片，每片629—630项；node ID清单SHA-256为 `2d72aa356caad653b53707538e672c8b8e3e8558b38fc0e14c98a8898bda1813`，分配SHA-256为 `0f1851c179503d67fd5d593668520315103734f4b788813a8d92d8051bd141b9`，各文件只属于一个分片。有效JUnit终态合计5035项、5027通过、8跳过、0失败、0错误，分片累计耗时2261.548秒；8项跳过全部是明确治理边界：2项缺正式900 ppm配置/点表、3项缺real-COM no-OUTP工程配置、2项缺真实仓库 `points.xlsx`、1项缺PostgreSQL staging测试DSN，均按既有合同跳过，未伪造真实夹具、COM或数据库证据。首次分片尝试因未创建 `--basetemp` 父目录产生统一WinError 3；第二次长临时根触发现有Windows长路径P1；第三次把临时根放在仓库 `_runtime` 下又按设计命中证书普查的软件生成物排除规则。三类无效尝试均单独保留、没有计入通过率；最终全部分片使用系统Temp下的中性短路径完成。有效运行仅有两类非失败警告：2项openpyxl工作表标题超过31字符，1项Tk变量析构发生在非主循环；它们列为P1，不改变本次离线冻结结论。每个分片启动前显式移除V2 real-COM、engineering probe、设备写入、正式DSN等环境变量；运行期间未打开COM、未调用真实设备、未控制气路、未连接正式数据库、未写系数/设备ID。结合当前树g136 parity 1/1、nightly 24/24和成熟0613/0620/0621内核CO₂ 45点/H₂O 13点no-write dry-run，本批结论为 `offline_release_freeze_pass`；它不是real acceptance，不解除证书/真实夹具缺口，不切换默认入口，不刷新real primary latest，promotion继续blocked；
- 全仓可收集5035项测试，V1.5及共享validation保护路径导入V2为0；
- 证据位于 `output/v1_5_version_convergence_20260728/gate2_application_service/`
  、`output/v1_5_version_convergence_20260728/gate2_health_validation_migration/`
  、`output/v1_5_version_convergence_20260728/gate3_algorithm_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/gate3_results_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_single_snapshot_feed/`
  、`output/v1_5_version_convergence_20260728/gate4_qc_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate3_results_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_single_snapshot_feed/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_qc_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_v2_simulation_launcher_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_v2_simulation_launcher_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_reports_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_reports_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_device_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_device_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_workbench_producer_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_workbench_producer_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_review_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_review_surface_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_app_facade_plan_editor_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_app_facade_plan_editor_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_run001_probe_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_run001_probe_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_probe_core_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_probe_core_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_r1_conditioning_core_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_r1_conditioning_core_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_dynamic_sim_reference_audit/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_dynamic_sim_reference_audit/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_analytics_service_dynamic_audit/`
  、`output/v1_5_version_convergence_20260728/gate4_profile_store_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_profile_store_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_collapsible_section_retirement/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_collapsible_section_retirement/`
  、`output/v1_5_version_convergence_20260728/gate4_historical_frame_parity_cli_migration/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_historical_frame_parity_cli_migration/`
  、`output/v15_g4_frame_cli/`
  、`output/v15_g4_cert_census/`
  、`output/v15_g4_cert_migrate/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate4_certificate_evidence_census_migration/`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate5_certificate_operational_admission_migration/`
  、`D:\gas_calibrator\_runtime\g5\r\`
  、`D:\gas_calibrator\_runtime\g5\p\`
  、`D:\gas_calibrator\_runtime\g5\admission\`
  、`D:\gas_calibrator\_runtime\g5\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate5_regression_scoreboard_retirement/`
  、`D:\gas_calibrator\_runtime\g6\p\`
  、`D:\gas_calibrator\_runtime\g6\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate5_rs485_alignment_retirement/`
  、`D:\gas_calibrator\_runtime\g7\p\`
  、`D:\gas_calibrator\_runtime\g7\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate6_step3a_r0_entrypoint_audit/`
  、`D:\gas_calibrator\_runtime\g8\p\g8_step3a_r0_entrypoint_audit_20260728\`
  、`D:\gas_calibrator\_runtime\g8\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate7_r0_reader_extraction/`
  、`D:\gas_calibrator\_runtime\g9\p\g9_r0_reader_extraction_20260728\`
  、`D:\gas_calibrator\_runtime\g9\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate8_r0_p3_state_semantics/`
  、`D:\gas_calibrator\_runtime\g10\p\g10_r0_p3_state_semantics_20260728\`
  、`D:\gas_calibrator\_runtime\g10\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate9_analyzer_identity_contract_retirement/`
  、`D:\gas_calibrator\_runtime\g11\p\g11_analyzer_identity_contract_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g11\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate10_serial_assistant_probe_retirement/`
  、`D:\gas_calibrator\_runtime\g12\p\g12_serial_assistant_probe_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g12\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate11_mode2_setup_retirement/`
  、`D:\gas_calibrator\_runtime\g13\p\g13_mode2_setup_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g13\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate12_analyzer_diagnostics_retirement/`
  、`D:\gas_calibrator\_runtime\g14\p\g14_analyzer_diagnostics_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g14\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate13_cutover_worksheet_retirement/`
  、`D:\gas_calibrator\_runtime\g15\p\g15_cutover_worksheet_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g15\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate14_orphan_info_ui_retirement/`
  、`D:\gas_calibrator\_runtime\g16\p\g16_orphan_info_ui_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g16\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate15_orphan_feedback_widgets_retirement/`
  、`D:\gas_calibrator\_runtime\g17\p\g17_orphan_feedback_widgets_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g17\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate16_orphan_preferences_retirement/`
  、`D:\gas_calibrator\_runtime\g18\p\g18_orphan_preferences_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g18\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate17_orphan_session_recovery_retirement/`
  、`D:\gas_calibrator\_runtime\g19\p\g19_orphan_session_recovery_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g19\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate18_orphan_presentation_widgets_retirement/`
  、`D:\gas_calibrator\_runtime\g20\p\g20_orphan_presentation_widgets_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g20\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate19_orphan_diagnostic_redaction_retirement/`
  、`D:\gas_calibrator\_runtime\g21\p\g21_orphan_diagnostic_redaction_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g21\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate20_orphan_review_scan_contract_retirement/`
  、`D:\gas_calibrator\_runtime\g22\p\g22_orphan_review_scan_contract_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g22\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate21_orphan_review_scope_export_index_retirement/`
  、`D:\gas_calibrator\_runtime\g23\p\g23_orphan_review_scope_export_index_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g23\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate22_orphan_review_artifact_scope_retirement/`
  、`D:\gas_calibrator\_runtime\g24\p\g24_orphan_review_artifact_scope_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g24\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate23_orphan_certificate_compatibility_retirement/`
  、`D:\gas_calibrator\_runtime\g25\p\g25_orphan_certificate_compatibility_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g25\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate24_ui_compatibility_exit_retirement/`
  、`D:\gas_calibrator\_runtime\g26\p\g26_ui_compatibility_exit_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g26\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate25_certificate_registry_wrapper_retirement/`
  、`D:\gas_calibrator\_runtime\g27\p\g27_certificate_registry_wrapper_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g27\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate26_historical_frame_adapter_retirement/`
  、`D:\gas_calibrator\_runtime\g28\p\g28_historical_frame_adapter_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g28\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate27_converter_submodule_retirement/`
  、`D:\gas_calibrator\_runtime\g29\p\g29_converter_submodule_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g29\dry\`
  、`output/v1_5_version_convergence_20260728/v2_inventory_gate28_initialization_wrapper_retirement/`
  、`D:\gas_calibrator\_runtime\g30\p\g30_initialization_wrapper_retirement_20260728\`
  、`D:\gas_calibrator\_runtime\g30\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate29_shared_storage_adapter_retirement/`
  、`D:\gas_calibrator\_runtime\g31\p\g31_shared_storage_adapter_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g31\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate30_utils_package_retirement/`
  、`D:\gas_calibrator\_runtime\g32\p\g32_utils_package_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g32\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate31_storage_compatibility_zero/`
  、`D:\gas_calibrator\_runtime\g33\p\g33_storage_compatibility_zero_20260729\`
  、`D:\gas_calibrator\_runtime\g33\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate32_storage_package_facade_retirement/`
  、`D:\gas_calibrator\_runtime\g34\p\g34_storage_package_facade_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g34\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate33_storage_exporter_retirement/`
  、`D:\gas_calibrator\_runtime\g35\p\g35_storage_exporter_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g35\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate34_storage_import_adapter_retirement/`
  、`D:\gas_calibrator\_runtime\g36\p\g36_storage_import_adapter_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g36\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate35_storage_migration_ownership/`
  、`D:\gas_calibrator\_runtime\g37\p\g37_storage_migration_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g37\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate36_legacy_postprocess_entry_retirement/`
  、`D:\gas_calibrator\_runtime\g38\p\g38_legacy_postprocess_entry_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g38\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate37_no500_bridge_retirement/`
  、`D:\gas_calibrator\_runtime\g39\p\g39_no500_bridge_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g39\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate38_v1_postprocess_runner_retirement/`
  、`D:\gas_calibrator\_runtime\g40\p\g40_v1_postprocess_runner_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g40\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate39_offline_refit_retirement/`
  、`D:\gas_calibrator\_runtime\g41\p\g41_offline_refit_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g41\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate40_analytics_retirement/`
  、`D:\gas_calibrator\_runtime\g42\p\g42_analytics_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g42\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate41_sidecar_copilot_retirement/`
  、`D:\gas_calibrator\_runtime\g43\p\g43_sidecar_copilot_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g43\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate42_results_gateway_retirement/`
  、`D:\gas_calibrator\_runtime\g44\p\g44_results_gateway_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g44\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate43_orphan_stage_entry_retirement/`
  、`D:\gas_calibrator\_runtime\g45\p\g45_orphan_stage_entry_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g45\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate44_orphan_governance_entry_retirement/`
  、`D:\gas_calibrator\_runtime\g46\p\g46_orphan_governance_entry_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g46\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate45_stage3_entry_retirement/`
  、`D:\gas_calibrator\_runtime\g47\p\g47_stage3_entry_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g47\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate46_phase_bridge_presenter_retirement/`
  、`D:\gas_calibrator\_runtime\g48\p\g48_phase_bridge_presenter_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g48\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate47_replacement_wrapper_retirement/`
  、`D:\gas_calibrator\_runtime\g49\p\g49_replacement_wrapper_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g49\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate48_workflow_step_consolidation/`
  、`D:\gas_calibrator\_runtime\g50\p\g50_workflow_step_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g50\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate49_domain_execution_model_consolidation/`
  、`D:\gas_calibrator\_runtime\g51\p\g51_domain_execution_model_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g51\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate50_context_builder_consolidation/`
  、`D:\gas_calibrator\_runtime\g52\p\g52_context_builder_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g52\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate51_orphan_review_formatter_retirement/`
  、`D:\gas_calibrator\_runtime\g53\p\g53_orphan_review_formatter_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g53\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate52_ai_advisor_consolidation/`
  、`D:\gas_calibrator\_runtime\g54\p\g54_ai_advisor_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g54\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate53_parallel_temperature_compensation_retirement/`
  、`D:\gas_calibrator\_runtime\g55\p\g55_parallel_temperature_compensation_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g55\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate54_ai_explainer_consolidation/`
  、`D:\gas_calibrator\_runtime\g56\p\g56_ai_explainer_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g56\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate55_prompt_asset_retirement/`
  、`D:\gas_calibrator\_runtime\g57\p\g57_prompt_asset_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g57\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate56_read_only_gateway_consolidation/`
  、`D:\gas_calibrator\_runtime\g58\p\g58_read_only_gateway_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g58\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate57_ai_runtime_consolidation/`
  、`D:\gas_calibrator\_runtime\g59\p\g59_ai_runtime_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g59\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate58_certificate_sim_adapter_consolidation/`
  、`D:\gas_calibrator\_runtime\g60\r\g60_certificate_sim_adapter_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g60\p\g60_certificate_sim_adapter_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g60\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate59_sim_device_models_consolidation/`
  、`D:\gas_calibrator\_runtime\g61\s\g61_sim_device_models_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g61\p\g61_sim_device_models_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g61\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate60_ai_explanation_service_consolidation/`
  、`D:\gas_calibrator\_runtime\g62\p\g62_ai_explanation_service_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g62\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate61_foundational_domain_models_consolidation/`
  、`D:\gas_calibrator\_runtime\g63\p\g63_foundational_domain_models_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g63\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate62_algorithm_types_registry_consolidation/`
  、`D:\gas_calibrator\_runtime\g64\p\g64_algorithm_types_registry_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g64\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate63_orchestration_context_consolidation/`
  、`D:\gas_calibrator\_runtime\g65\p\g65_orchestration_context_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g65\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate64_explanation_models_consolidation/`
  、`D:\gas_calibrator\_runtime\g66\p\g66_explanation_models_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g66\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate65_mode_models_consolidation/`
  、`D:\gas_calibrator\_runtime\g67\p\g67_mode_models_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g67\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate66_route_run_result_consolidation/`
  、`D:\gas_calibrator\_runtime\g68\p\g68_route_run_result_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g68\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate67_event_bus_consolidation/`
  、`D:\gas_calibrator\_runtime\g69\p\g69_event_bus_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g69\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate68_core_static_hygiene/`
  、`D:\gas_calibrator\_runtime\g70\p\g70_core_static_hygiene_20260729\`
  、`D:\gas_calibrator\_runtime\g70\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate69_route_context_consolidation/`
  、`D:\gas_calibrator\_runtime\g71\p\g71_route_context_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g71\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate70_run_session_consolidation/`
  、`D:\gas_calibrator\_runtime\g72\p\g72_run_session_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g72\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate71_conditioning_type_resolution/`
  、`D:\gas_calibrator\_runtime\g73\p\g73_conditioning_type_resolution_20260729\`
  、`D:\gas_calibrator\_runtime\g73\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate72_dictionary_key_hygiene/`
  、`D:\gas_calibrator\_runtime\g74\p\g74_dictionary_key_hygiene_20260729\`
  、`D:\gas_calibrator\_runtime\g74\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate73_duplicate_definition_hygiene/`
  、`D:\gas_calibrator\_runtime\g75\p\g75_duplicate_definition_hygiene_20260729\`
  、`D:\gas_calibrator\_runtime\g75\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate74_fstring_literal_hygiene/`
  、`D:\gas_calibrator\_runtime\g76\p\g76_fstring_literal_hygiene_20260729\`
  、`D:\gas_calibrator\_runtime\g76\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate75_unused_local_and_private_duplicate_hygiene/`
  、`D:\gas_calibrator\_runtime\g77\p\g77_unused_local_and_private_duplicate_hygiene_20260729\`
  、`D:\gas_calibrator\_runtime\g77\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate76_unused_import_hygiene/`
  、`D:\gas_calibrator\_runtime\g78\p\g78_unused_import_hygiene_20260729\`
  、`D:\gas_calibrator\_runtime\g78\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate77_import_order_hygiene/`
  、`D:\gas_calibrator\_runtime\g79\p\g79_import_order_hygiene_20260729\`
  、`D:\gas_calibrator\_runtime\g79\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate78_domain_services_facade_retirement/`
  、`D:\gas_calibrator\_runtime\g80\p\g80_domain_services_facade_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g80\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate79_coefficient_artifact_service_consolidation/`
  、`D:\gas_calibrator\_runtime\g81\p\g81_coefficient_artifact_service_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g81\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate80_algorithm_models_ownership_consolidation/`
  、`D:\gas_calibrator\_runtime\g82\p\g82_algorithm_models_ownership_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g82\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate81_plan_profile_ownership_consolidation/`
  、`D:\gas_calibrator\_runtime\g83\p\g83_plan_profile_ownership_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g83\n\g83_plan_profile_ownership_consolidation_20260729\`
  、`D:\gas_calibrator\_runtime\g83\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate82_v2_root_product_facade_retirement/`
  、`D:\gas_calibrator\_runtime\g84\p\g84_v2_root_product_facade_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g84\n\g84_v2_root_product_facade_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g84\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate83_config_facade_retirement/`
  、`D:\gas_calibrator\_runtime\g85\p\g85_config_facade_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g85\n\g85_config_facade_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g85\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate84_speculative_spectral_diagnostics_retirement/`
  、`D:\gas_calibrator\_runtime\g86\p\g86_speculative_spectral_diagnostics_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g86\n\g86_speculative_spectral_diagnostics_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g86\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate85_final_validation_ownership_stop_boundary/`
  、`D:\gas_calibrator\_runtime\g87\p\g87_final_validation_ownership_stop_boundary_20260729\`
  、`D:\gas_calibrator\_runtime\g87\n\g87_final_validation_ownership_stop_boundary_20260729\`
  、`D:\gas_calibrator\_runtime\g87\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate86_sidecar_config_contract_decoupling/`
  、`D:\gas_calibrator\_runtime\g88\p\g88_sidecar_config_contract_decoupling_20260729\`
  、`D:\gas_calibrator\_runtime\g88\n\g88_sidecar_config_contract_decoupling_20260729\`
  、`D:\gas_calibrator\_runtime\g88\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate87_stability_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g89\p\g89_stability_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g89\n\g89_stability_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g89\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate88_ai_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g90\p\g90_ai_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g90\n\g90_ai_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g90\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate89_paths_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g91\p\g91_paths_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g91\n\g91_paths_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g91\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate90_features_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g92\p\g92_features_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g92\n\g92_features_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g92\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate91_inert_algorithm_config_retirement/`
  、`D:\gas_calibrator\_runtime\g93\p\g93_inert_algorithm_config_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g93\n\g93_inert_algorithm_config_retirement_20260729\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate92_simulation_storage_fail_closed/`
  、`D:\gas_calibrator\_runtime\g94\p\g94_simulation_storage_fail_closed_20260729\`
  、`D:\gas_calibrator\_runtime\g94\n\g94_simulation_storage_fail_closed_20260729\`
  、`D:\gas_calibrator\_runtime\g94\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate93_explicit_storage_sidecar_contract/`
  、`D:\gas_calibrator\_runtime\g95\p\g95_explicit_storage_sidecar_contract_20260729\`
  、`D:\gas_calibrator\_runtime\g95\n\g95_explicit_storage_sidecar_contract_20260729\`
  、`D:\gas_calibrator\_runtime\g95\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate94_shared_storage_settings_ownership/`
  、`D:\gas_calibrator\_runtime\g96\p\g96_shared_storage_settings_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g96\n\g96_shared_storage_settings_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g96\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate95_qc_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g97\p\g97_qc_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g97\n\g97_qc_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g97\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate96_sampling_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g98\p\g98_sampling_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g98\n\g98_sampling_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g98\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate97_precheck_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g99\p\g99_precheck_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g99\n\g99_precheck_config_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g99\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate98_pressure_tolerance_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g100\p\g100_pressure_tolerance_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g100\n\g100_pressure_tolerance_validation_ownership_20260729\`
  、`D:\gas_calibrator\_runtime\g100\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate99_dead_v2_startup_connect_mirror_retirement/`
  、`D:\gas_calibrator\_runtime\g101\p\g101_dead_v2_startup_connect_mirror_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g101\n\g101_dead_v2_startup_connect_mirror_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g101\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate100_reference_thermometer_device_chain_closure/`
  、`D:\gas_calibrator\_runtime\g102\p\g102_reference_thermometer_device_chain_closure_20260729\`
  、`D:\gas_calibrator\_runtime\g102\n\g102_reference_thermometer_device_chain_closure_20260729\`
  、`D:\gas_calibrator\_runtime\g102\dry\`
  、`output/v1_5_version_convergence_20260729/v2_inventory_gate101_dead_valve_config_mirror_retirement/`
  、`D:\gas_calibrator\_runtime\g103\p\g103_dead_valve_config_mirror_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g103\n\g103_dead_valve_config_mirror_retirement_20260729\`
  、`D:\gas_calibrator\_runtime\g103\dry\`
  、`D:\gas_calibrator\_runtime\g93\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate104_pressure_source_same_frame_scientific_audit/`
  、`D:\gas_calibrator\_runtime\g106\historical_replay\g106_pressure_source_same_frame_scientific_audit_20260730\`
  、`D:\gas_calibrator\_runtime\g106\parity\g106_pressure_source_same_frame_scientific_audit_20260730\`
  、`D:\gas_calibrator\_runtime\g106\nightly\g106_pressure_source_same_frame_scientific_audit_20260730\`
  、`D:\gas_calibrator\_runtime\g106\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate105_coefficients_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g107\parity\g107_coefficients_config_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g107\nightly\g107_coefficients_config_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g107\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate106_valve_config_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g108\parity\g108_valve_config_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g108\nightly\g108_valve_config_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g108\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate107_config_risk_inventory_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g109\parity\g109_config_risk_inventory_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g109\nightly\g109_config_risk_inventory_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g109\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate108_config_safety_presentation_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g110\parity\g110_config_safety_presentation_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g110\nightly\g110_config_safety_presentation_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g110\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate109_config_safety_review_handoff_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g111\parity\g111_config_safety_review_handoff_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g111\nightly\g111_config_safety_review_handoff_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g111\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate110_sensor_precheck_normalization_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g112\parity\g112_sensor_precheck_normalization_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g112\nightly\g112_sensor_precheck_normalization_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g112\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate111_run_mode_alias_normalization_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g113\parity\g113_run_mode_alias_normalization_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g113\nightly\g113_run_mode_alias_normalization_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g113\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate112_pressure_selection_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g114\parity\g114_pressure_selection_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g114\nightly\g114_pressure_selection_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g114\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate113_ec_system_identification_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g115\parity\g115_ec_system_identification_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g115\nightly\g115_ec_system_identification_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g115\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate114_gas_analyzer_asset_dossier_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g116\parity\g116_gas_analyzer_asset_dossier_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g116\nightly\g116_gas_analyzer_asset_dossier_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g116\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate115_ec_dynamic_metrology_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g117\parity\g117_ec_dynamic_metrology_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g117\nightly\g117_ec_dynamic_metrology_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g117\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate116_gas_analyzer_dynamic_uncertainty_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g118\parity\g118_gas_analyzer_dynamic_uncertainty_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g118\nightly\g118_gas_analyzer_dynamic_uncertainty_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g118\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate117_gas_analyzer_bench_readiness_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g119\parity\g119_gas_analyzer_bench_readiness_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g119\nightly\g119_gas_analyzer_bench_readiness_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g119\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate118_gas_analyzer_operating_envelope_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g120\parity\g120_gas_analyzer_operating_envelope_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g120\nightly\g120_gas_analyzer_operating_envelope_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g120\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate119_domain_model_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g121\parity\g121_domain_model_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g121\nightly\g121_domain_model_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g121\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate120_stability_checker_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g122\parity\g122_stability_checker_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g122\nightly\g122_stability_checker_validation_ownership_verified_20260730\`
  、`D:\gas_calibrator\_runtime\g122\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate121_point_parser_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g123\parity\g123_point_parser_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g123\nightly\g123_point_parser_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g123\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate122_route_planner_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g124\parity\g124_route_planner_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g124\nightly\g124_route_planner_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g124\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate123_point_preparation_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g125\parity\g125_point_preparation_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g125\nightly\g125_point_preparation_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g125\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate124_plan_models_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g126\parity\g126_plan_models_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g126\nightly\g126_plan_models_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g126\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate125_plan_rows_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g127\parity\g127_plan_rows_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g127\nightly\g127_plan_rows_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g127\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate126_plan_preview_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g128\parity\g128_plan_preview_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g128\nightly\g128_plan_preview_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g128\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate127_sampling_snapshot_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g129\parity\g129_sampling_snapshot_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g129\nightly\g129_sampling_snapshot_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g129\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate128_sampling_quality_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g130\parity\g130_sampling_quality_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g130\nightly\g130_sampling_quality_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g130\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate129_sampling_integrity_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g131\parity\g131_sampling_integrity_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g131\nightly\g131_sampling_integrity_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g131\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate130_sampling_rows_validation_ownership/`
  、`D:\gas_calibrator\_runtime\g132\parity\g132_sampling_rows_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g132\nightly\g132_sampling_rows_validation_ownership_20260730\`
  、`D:\gas_calibrator\_runtime\g132\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate131_sampling_contracts_consolidation/`
  、`D:\gas_calibrator\_runtime\g134\parity\g134_sampling_contracts_consolidation_20260730\`
  、`D:\gas_calibrator\_runtime\g134\nightly\g134_sampling_contracts_consolidation_20260730\`
  、`D:\gas_calibrator\_runtime\g134\dry\`
  、`output/v1_5_version_convergence_20260730/v2_inventory_gate132_sampling_delegate_surface_retirement_final/`
  、`D:\gas_calibrator\_runtime\g136\parity\g136_sampling_delegate_surface_retirement_20260730\`
  、`D:\gas_calibrator\_runtime\g136\nightly\g136_sampling_delegate_surface_retirement_20260730\`
  、`D:\gas_calibrator\_runtime\g136\dry\`
  和 `D:\gas_calibrator\_runtime\g137\release_freeze_full_suite_20260730\`。

### Gate 3：平行实现删除

- 已删除首个完整旧UI单元：算法页面壳、算法表格和“胜出”徽标；V1.5算法页仍由统一快照提供生产锁定和候选阻断信息；
- 已删除第二个完整旧UI单元：结果页面壳和专属残差图；结果数据、纯工件范围合同与报告导出保持存在；
- 已删除第三个完整旧UI单元：QC页面壳、概览组件和拒绝原因图；V1.5明确使用成熟runner工件作为点级QC唯一权威，V2报告所需QC数据保持存在；
- 已删除 V2 仿真产品启动链、完整旧桌面 shell、五个产品页面壳、设备工作台生产器、旧审阅展示层、AppFacade、可编辑计划链、14个历史Run001/real-COM/cutover单用途CLI、4个失去入口的probe core及R1 conditioning core；V2 不再有可启动的 UI/headless/device-helper 产品入口；
- 继续按依赖删除 V2 runner、workflow、device factory 和已完成合同提取的 real-COM probe core；
- 删除 V2 UI 壳；
- 删除重复 QC、builder、repository 和未收集测试；
- V2 只剩兼容包装。

### Gate 4：兼容包装和 V2 包退出

- 全仓不再导入 `gas_calibrator.v2`；
- 删除包装和 V2 configs/scripts/docs 运行入口；
- 冻结提交完整验证；
- V1.5 正式发布后保留 V1 fallback。

## 12. 防止项目再次膨胀

后续每个新需求必须回答：

1. 是否能在现有 V1.5 service/page/model 中完成？
2. 是否已经存在等价 V2/V1/工具实现？
3. 新增代码是否同时减少旧代码？
4. 是否增加新的状态源、入口或证据格式？
5. 删除和退出条件是什么？

如果新增一个模块却不能减少重复、关闭门禁或提升可验证性，则默认不应新增。

## 13. 当前 P0/P1

### P0

1. 新 V1.5 GUI 仍只完成 dry-run，尚未接入受控真实 queue execution。
2. 当前共用 application service 仍是 dry-run-only，尚未覆盖未来受控真实执行。
3. V2 产品启动器已删除，但 V2 包内仍有大量平行运行内核、真机探针和历史治理代码待按依赖继续退出。
4. 本轮尚未在最终冻结状态执行真实 acceptance。

### P1

1. 证书编辑页已进入 V1.5，但当前仍是隔离草稿层，正式签发规则和校准输入准入需另行评审。
2. V1.5 工作站全部导航已有 V1.5 自有页面；设备仍只覆盖 simulation-only 状态，算法仍只覆盖生产锁定和离线候选边界。
3. V2 仍有157个模块，经Gate 132复核已无可直接删除候选；最终validation闭包剩12项（simulation config 1、simulation runtime 8、engineering probe 3、metrology 0），后续仍须按完整依赖闭包等价迁移，不能按文件大小删除，也不能一次性整目录迁移；`route_planner`、点位执行准备、七类计划输入模型、计划行构建、预览匹配以及采样快照、采样质量、分析仪帧完整性、采样结果行schema和样本选择五组纯合同已成为共享所有者，其中五组采样合同为避免碎片化已合并到单一 `sampling_contracts.py`，`SamplingService` 的重复转发表面已退出。`plan_compiler` 已从820行收口到203行，只保留 `CompiledPlan` 的V2报告payload、V2配置覆盖和编译适配；它仍依赖 `AppConfig` 与产品报告清单，不能直接整模块搬迁。`sampling_service` 已从992行收口到810行，剩余方法均承担设备读数、重试等待、线程池、采样时序、结果仓库访问、持久化或点级执行职责；停止继续微拆，后续只能按另一个完整模块闭包推进。
4. 五个旧产品页面壳、设备工作台、审阅展示层、AppFacade、可编辑计划链、profile store包装、零调用折叠组件、历史逐帧CLI、证书证据普查、证书运行资料准入核心/CLI、regression scoreboard孤立单元、RS-485旧对齐链、14个历史Run001/real-COM/cutover单用途CLI、4个无调用probe core、R1 conditioning core、旧证书页/主题单元、UI兼容出口、证书注册表包装、历史逐帧适配器、转换子模块包装、初始化/就绪事件兼容组、四个共享存储窄适配器、通用工具包入口、V2包根产品启动门面、V2配置包级再导出门面、概念性频域诊断及其配置/UI/工件枝杈、最后两个共享数据库/模型包装、V2存储包级共享门面、无产品调用数据库导出器、最后一个V2 Python存储导入适配器、V2 storage包根、旧后处理GUI及目录watcher、no-500可执行桥、悬空旧GUI根入口、无产品入口的V1平行后处理runner、旧离线筛选重拟合三模块、两层孤立analytics服务、领域服务聚合门面、独立系数报告代理服务、算法domain模型碎片、计划profile domain模型碎片、实时旁路索引/Copilot、聚合ResultsGateway及其三个专属仓库/条目包装、五个无消费者的阶段/治理工件展示包装、一个重复阶段桥presenter、四个replacement快捷包装器、七个workflow-step碎片模块、两个领域执行模型碎片、三个AI解释上下文碎片、孤立V2复核格式器、两个AI建议器碎片、五个历史证据网关文件、AI运行时组装碎片、证书准入仿真适配文件、仿真设备规格模型文件、AI解释服务碎片、四个基础领域模型碎片、算法结果类型/注册表碎片、编排上下文碎片、解释领域模型碎片、运行模式模型碎片、路由执行结果模型碎片、进程内事件总线碎片、路由上下文状态模型碎片和运行会话状态模型碎片均已删除、归并或迁入正式所有者，兼容包装现为0；001—004 PostgreSQL迁移历史已归共享storage所有。`sim.protocol`、`sim` 包内证书准入仿真函数和 `sim.devices` 包内十个规格类保持活跃；analytics包已全部退出，历史目录只通过 `adapters` 包内五个延迟加载的窄网关类读取persisted artifacts，不再构造第二套结果/报告/审阅表面。V2报告计划已明确H₂O正式导出器不存在，V1.5正式报告链为唯一所有者。清单显示1项原始静态零引用且已解释，未解释静态零引用为0。
5. Gate 133已把当前树5035项测试全部执行完成：5027通过、8项按真实夹具/real-COM配置/真实点表/staging DSN缺失合同跳过、0失败、0错误，离线发布冻结通过；该结论不等于real acceptance。剩余两类非失败警告为openpyxl工作表标题超过31字符和Tk变量非主循环析构，应作为独立P1清理，不在冻结批次中顺带改代码。
6. V1.5干跑证据路径达到272字符时，Windows Python无法创建不可变声明文件；154字符短路径正常通过。当前应保持短输出根目录，长路径兼容属于发布工具P1，不应通过修改成熟runner顺带处理。
7. R0压力表P3瞬时状态语义已在schema v2关闭：软件明确区分查询字节、持久/控制写入和连续输出取消，并要求operator显式确认query-only并非状态中性；`mode_switch_command_sent=false`只表示未发送显式模式选择命令，不再否认P3可能终止P4/P7。真实执行仍必须满足完整Step 3A双重解锁和更新后的operator confirmation。
8. V1.5 analyzer runtime setup 当前在一次受控执行完成后写出结果工件；异常由逐设备结果收口，但若进程被强制终止，尚没有旧V2模块那种命令前占位和逐命令增量落盘。该差异属于P1审计韧性，不影响当前MODE2/FTD/AVERAGE/身份/频率放行逻辑，不得通过恢复旧V2初始化器解决；未来如补强，应直接在V1.5唯一初始化服务中以原子工件实现并单独评审。
9. Gate 104已关闭压力来源的软件侧同帧比较与单位正确性缺口：0613/0620/0621的45个成功点、450行记录和6台分析仪均完成 `P(hPa) × 0.1` 对 `BAR/P_fit(kPa)` 的同样本双拟合，残差、偏差、R²、条件数、完整系数差和温压交互项差均已落账且fail-closed。但该批为常压附近开流数据，两套设计矩阵条件数均约 `1.5×10^9—2.9×10^9`，压力相关系数不可稳健识别；内部压力在5/6台训练RMSE更低也不能证明其计量权威。压力来源权威决策因此仍是P1：必须使用独立、可溯源、同步误差已知且具有充分压力跨度的多压力数据，预先定义判定限并实施留出验证。成熟V1.5继续保持 `reference_first`；受保护runner中旧P/BAR诊断日志未统一单位，不能用于定量判断且本批不为修日志触碰生产路径；V2结果继续只作离线候选，不得下载、刷新正式主证据或提升为正式系数。

## 14. 下一工程动作

继续按最小可删单元推进 Gate 2/3，并复用现有 `WorkstationSnapshot`：

1. V2 仿真启动器、旧 shell 和专属控制/打包诊断层已删除；后续不得恢复第二套产品入口、页面专属状态通道、第二套QC判定源或UI阈值编辑；
2. `reports_page` 及三个专属展示组件已经删除；后续不得恢复第二套报告UI或把 V2 审阅状态误作 V1.5 正式签发状态；
3. `devices_page` 及三个页面专属小组件已经删除；后续不得恢复第二套设备产品页或把仿真状态误作真实设备状态；
4. 设备工作台生产器和专属组件已经删除；不得恢复预设、故障注入、设备动作或第二套设备状态源；
5. AppFacade、PlanGateway、可编辑计划页和14个无调用方的历史Run001/real-COM/cutover CLI已删除；不得恢复第二套探针入口；
6. 四个静态零调用probe core已完成安全合同提取与删除；不得恢复端口硬编码、V2探针执行器或按旧Run001工件隐式放行；
7. `run001_r1_conditioning_only_probe`及旧R0.1 standalone均已删除；R0/query-only三模块链现由人工入口、唯一准入/工件核心和无standalone的只读reader组成，只保留至Step 3A关闭，并由正式tests覆盖双重解锁、operator confirmation、显式端口、no-write和blocked promotion；不得把它迁入V1.5、默认入口或真实acceptance，也不得在未授权时运行；
8. `sim.protocol`确认为延迟导出的活跃仿真验证链，不得按旧清单误删；若未来退出，必须与`run_simulated_compare`、suite和协议测试作为一个完整迁移/删除单元处理；
9. `v1_postprocess_runner`、旧离线refit链、两层孤立analytics、实时sidecar索引、自动Copilot和聚合`adapters.results_gateway`已退出；共享validation继续唯一拥有analyzer/instrument health，正式QC和候选系数链不变。V1.5结果/报告/复核页继续只消费`WorkstationSnapshot`，历史诊断JSON只由五个窄文件网关和`historical_artifacts`按reviewer-only读取；不得恢复总结果网关、第二套报告/审阅表面或UI专属artifact registry；
10. `storage.profile_store`、零调用 `ui_v2.widgets.collapsible_section`、历史逐帧CLI、证书证据普查、证书运行资料准入核心/CLI、regression scoreboard孤立单元、RS-485旧对齐链、R0.1 standalone、旧分析仪身份/映射合同对、串口助手等效探针、MODE2第二初始化器、分析仪第二诊断入口、V2切换候选表、旧信息窗口组、孤立运行反馈组件、偏好设置双链、旧会话/恢复工具、孤立展示组件、诊断脱敏助手、旧审阅扫描/索引/工件范围三层、旧证书页/主题单元、UI兼容出口、证书注册表包装、历史逐帧适配器、转换子模块包装、初始化/就绪事件兼容组、四个共享存储窄适配器、通用工具包入口、共享数据库/模型包装、V2存储包级共享门面、旧数据库导出器、最后一个V2 Python存储导入适配器、V2 storage包根、旧后处理GUI及目录watcher、no-500可执行桥、悬空旧GUI根入口、无产品入口的V1平行后处理runner和旧离线refit三模块均已退出，兼容包装归零；001—004迁移历史已归共享storage所有，已知ORM—PostgreSQL DDL差异维持独立P1，不得在无真实部署清单和回滚方案时修改。纯 `_no500_filter` 及其ambient/sealed容差合同继续由V1.5 corrected-autodelivery使用，正式压力点集和500 hPa判定未改；正式候选系数链继续唯一拥有逐台fit准入、CO₂认证零气、H₂O露点/干气、rank/condition和no-write合同。现行V1.5正式报告/逐台证书、初始化/就绪事件、共享存储实现、共享转换实现、共享逐帧审计/tools入口、V1.5证书注册表、参观展示、截图回退、滚动骨架、运行清单、数据库脱敏合同、共享阶段术语/摘要合同及 `results_payload`、`reports`、`historical_artifacts` 消费链必须保留。`analyzer_coefficient_downloader` 因merged sidecar仍有显式调用本批不动，任何真实下载必须保持单独授权和写前/读回门禁；
11. 持续执行 protected-import、中文UI、45/13 dry-run、逐帧和全收集门禁。
