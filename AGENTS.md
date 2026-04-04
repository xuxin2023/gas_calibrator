# 气体分析仪自动校准 V2 项目长期总控规则

## 一、项目总目标
本项目目标是：在**绝不破坏 V1 已经可用的生产校准流程**前提下，稳步推进 V2，最终建设为**全球行业领先的气体分析仪自动校准与数据分析系统**。

## 二、最高硬约束
1. 绝不能破坏 V1 已经可以正常跑的生产校准流程。
2. 不要修改 V1 生产代码，除非用户明确要求只改 V1。
3. 不要修改 `run_app.py` 默认入口。
4. 不要把任何新功能接回 V1 UI。
5. 在真实 acceptance 未明确允许前：
   - 默认不允许打开任何真实串口/COM
   - 仅当用户明确授权时，允许对 V1 执行最小范围的 real smoke / short run，用于工程验证流程与数据存储
   - V2 仍不允许打开真实串口/COM 或进行任何真机测试
   - 不允许运行 real compare / real verify
   - 不允许进行真实设备手动操作
   - 不允许刷新 real primary latest
6. 当前阶段所有验证优先通过：
   - simulation
   - replay
   - suite regression
   - parity
   - resilience
7. 任何 simulated / replay / parity / resilience / suite 结果，都**不能**被解释为 real acceptance 证据。

## 三、分阶段推进规则
1. 必须按阶段推进，不允许为了“看起来更完整”跨阶段插入大需求。
2. 即使用户提出更超前的目标，也先判断是否会打乱当前阶段节奏；若会增加风险，先提醒并收口到当前阶段内的可执行版本。
3. 第一层采用“双完成制”：
   - **仿真完成**：允许继续推进第二层平台化建设。
   - **真机完成**：才允许讨论替代 V1、真实 acceptance、默认入口切换。
4. 在 real acceptance 未闭环前，禁止：
   - 宣称 V2 已可替代 V1
   - 默认入口切换
   - 真实放行结论
   - 刷新 real primary latest 作为正式主证据

## 四、当前默认主线
当前默认主线不是大改 V2 主流程，而是：
1. 仿真矩阵常态化运行
2. 工件治理与导出韧性守稳
3. parity 持续门禁
4. 中文默认产品体验
5. 设备工作台 simulation-only 产品化
6. 为未来真实 acceptance 做治理框架准备

## 五、当前产品边界
1. UI 默认中文，英文只作 fallback。
2. 设备工作台当前仅允许 simulation-only，不得连接真实设备。
3. 所有用户可见状态尽量中文化，不暴露内部英文 key。
4. 工件角色必须保持清晰：
   - `execution_rows`
   - `execution_summary`
   - `diagnostic_analysis`
   - `formal_analysis`
5. 统一导出状态必须保持：
   - `ok`
   - `skipped`
   - `missing`
   - `error`
6. 所有 simulated 证据都必须明确标注：
   - `evidence_source = simulated`
   - `not_real_acceptance_evidence = true`

## 六、当前阶段重点
当前阶段重点不是再大改流程，而是持续补强这些方面：
1. simulation / replay / suite 体系
2. 中文默认 UI 与 1920×1080 可视布局
3. 设备工作台的 simulation-only 体验
4. acceptance / analytics / lineage / artifact registry 骨架
5. V1/V2 采样、存储、导出口径的 parity 守稳

## 七、任务执行规则
每次任务必须先给出一个简短计划，再开始改代码。计划至少包含：
1. 当前目标
2. 影响范围
3. 风险边界
4. 验证方式
5. 完成标准

如果任务超过当前阶段：
- 先明确指出为什么超阶段
- 再给出当前阶段可执行的收口方案
- 不要直接跨阶段开工

## 八、优先级
优先级从高到低：
1. 不影响 V1
2. 不打乱当前阶段节奏
3. 守住数据口径与证据治理
4. 提升仿真覆盖和产品体验
5. 最后才是扩新功能

## 九、允许做的事
1. simulation / replay / suite / parity / resilience 改进
2. UI 中文化与 1920×1080 布局优化
3. 设备工作台 simulation-only 增强
4. 工件治理、acceptance 治理、analytics、lineage、registry
5. V1/V2 小字段 diff 对齐（仅在确认有差异时）
6. 增加测试、报告、摘要、文档
7. 增强 operator / engineer / reviewer / approver 的信息视图，但当前不做真实权限系统
8. 在用户明确授权下，对 V1 执行最小范围 real smoke / short run，用于工程验证流程与数据存储

## 十、禁止做的事
1. 擅自改 V1 生产逻辑
2. 擅自接入真实设备，或在没有用户明确授权时对 V1 执行 real smoke / short run
3. 擅自刷新 real primary latest
4. 擅自宣称 V2 已可替代 V1
5. 擅自把功能接回 V1 UI
6. 擅自为了“更完整”引入跨阶段大功能
7. 在没有明确授权的情况下，运行任何 real compare / real verify / real manual operation
8. 对 V2 运行任何真机测试

## 十一、UI 规则
1. 默认语言必须是中文。
2. 英文只作为 fallback，不是默认展示语言。
3. 所有新增用户可见文案必须走统一 i18n key。
4. 页面在 1920×1080 下应尽量一屏看全；若内容较多，必须有滚动，不允许内容超出可见区域却无滚动入口。
5. 设备工作台要优先保证：
   - operator 视图清晰
   - engineer 视图可展开
   - simulated 状态清晰
   - 不误导为真实设备控制

## 十二、验证规则
每次任务完成后，至少要说明：
1. 修改文件列表
2. 新增/修改测试
3. 验证命令
4. 剩余风险（P0 / P1）

能用 suite 验证时，优先使用：
- `smoke`
- `regression`
- `nightly`
- `parity`

涉及口径变动时，必须跑 parity。
涉及导出/工件变动时，必须跑 resilience/导出韧性测试。
涉及 UI 改动时，必须跑 UI 相关测试和中文化测试。

## 十三、完成标准（Done when）
一个任务只有在以下条件满足时才算完成：
1. 目标清晰落地
2. 相关 tests / suite 通过
3. 没有破坏当前：
   - simulation-only 边界
   - 中文默认体验
   - artifact role 清晰度
   - parity / resilience / suite 门禁
4. 没有触碰真实设备；若任务经用户明确授权并执行 V1 real smoke / short run，需明确标注其仅为工程验证，不是 real acceptance 结论
5. 没有影响 V1
6. 输出了变更摘要、测试命令、剩余风险

## 十四、当前阶段一句话原则
先把当前阶段的仿真、口径、工件、体验做稳；在 real acceptance 未闭环前，不允许把 V2 往替代 V1 的结论上推进。
