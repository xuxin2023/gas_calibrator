# GA-D9 有限发布治理检查

检查日期：2026-07-26
远端基线：`origin/main = ff15e7f841c893da81bf0ec10c9744b4878b72b9`
候选代码：`42a8c99e101047fde952acfae6aa4f0a4c401199`
分支：`codex/ga-d7-v15-production-candidate-freeze-20260725`

## 结论

- PR 准入：`READY_FOR_DRAFT_PR_REVIEW`
- 合并状态：`BLOCKED_PENDING_CI_AND_REVIEW`
- 软件证据状态：`simulation_complete`
- 真实推广状态：`real_promotion_blocked`
- 真实 acceptance：`not_executed`

该结论只允许进入代码审查，不允许切换默认入口、刷新
`real_primary_latest`、连接真实 COM、写数据库或写设备系数。

## Git 拓扑

刷新 `origin/main` 后，候选代码与远端基线的 merge-base 均为
`ff15e7f841c893da81bf0ec10c9744b4878b72b9`，候选领先 10 个提交、
落后 0 个提交：

1. `bb23c1a57` GA-D6A certificate evidence census
2. `e81f120b5` GA-D6B owner-attested certificate admission
3. `b8140bbde` GA-D6C shared offline helper convergence
4. `49765ac20` GA-D7 isolated calibration-gate test alignment
5. `ad42d7212` GA-D7 V1 runner safety-test convergence
6. `ae52fb57a` GA-D7 offline evidence and test governance
7. `7115f5d44` GA-D8 stale controlled-OUTP test retirement
8. `cf0ade803` GA-D8 redundant workbench snapshot removal
9. `e8b484603` GA-D8 formatter test and lint debt closure
10. `42a8c99e1` GA-D9 convergence lint residue removal

本文件是候选代码之后的文档型治理提交，不计入上述 10 个候选代码提交
及下方 85 个候选代码文件统计。

## 累计差异

相对 `origin/main`：

| 分类 | 文件 | 新增 | 删除 | 净变化 |
|---|---:|---:|---:|---:|
| V2 源码 | 33 | 2,152 | 311 | +1,841 |
| 共享源码 | 6 | 113 | 22 | +91 |
| 配置 | 3 | 483 | 0 | +483 |
| 文档 | 4 | 152 | 0 | +152 |
| 测试 | 39 | 1,096 | 10,292 | -9,196 |
| 合计 | 85 | 3,996 | 10,625 | -6,629 |

GA-D6B 之后的冻结收敛段为 `+827/-10,659`，净减少 9,832 行；
其中 V2 运行时代码净减少 186 行。GA-D7/GA-D8 的共享层净增加
64 行，来源仅为温度补偿写入命令的 fail-closed 门禁：温度证据缺失、
被阻断或门禁未提供时，不导出 SENCO7/SENCO8 写入命令。

`run_app.py` 在远端基线与候选中的 blob 均为
`cfc0c2024ed21d17364b8a321c1ae1bd19580dc8`。V1 工作流、设备层、
默认配置均无差异。

## 验证证据

| 门禁 | 结果 |
|---|---:|
| 证书普查、证书准入、文件 I/O、温度写入门禁 | 43 passed |
| 压力、路由、PACE、多分析仪、no-write 契约 | 252 passed |
| 格式化、中文 UI、ResultsGateway、工作台与导出韧性 | 93 passed |
| smoke | 6/6 |
| regression | 22/22 |
| parity | 1/1 |
| nightly | 24/24 |
| 分支新增 Ruff 问题 | 0 |
| `origin/main` 已有 Ruff 问题（同文件严格回放） | 25 |
| compileall、`git diff --check` | passed |

nightly 工件目录：
`C:\Users\A\AppData\Local\Temp\gas_calibrator_ga_d9_release_audit_20260726\ga_d9_nightly`

关键 SHA-256：

- `suite_summary.json`:
  `325C1722100844FE8A199AF708B5FF428B4FBC285743378AC785916A61408702`
- `suite_acceptance_plan.json`:
  `0E64B73413D137261ACFD26B034BA9B91832AE6CC2129445542D65BEECC9BAED`
- `suite_evidence_registry.json`:
  `DD58AE23B119606E100D2B51988EA125F93397FB9BA48A9575B1DE2D3BB84FBE`

nightly 严格 UTF-8 JSON 回读结果：

- `all_passed = true`
- `acceptance_level = offline_regression`
- `promotion_state = dry_run_only`
- `ready_for_promotion = false`
- `simulated_readiness_only = true`

## 剩余风险与责任

### P0

无。

### P1

1. 远端 CI 尚未执行。责任人：软件维护者；截止：合并前。
2. PR 尚未完成人工审查与批准。责任人：指定 reviewer/approver；截止：合并前。

全量变更文件 Ruff 扫描会同时报告 `origin/main` 已存在的 25 个问题；
逐文件对远端版本进行 stdin 回放后，确认本分支引入的问题为 1 个，
并已由 `42a8c99e1` 删除。是否在本 PR 顺带清理远端基线问题由 reviewer
决定，默认不扩大本次发布范围。

### 非软件 PR 阻塞项

- 缺少真实 probe 与真实 acceptance 证据。
- 0620/0621 资产材料仍保留已登记缺口。
- 严格原始气瓶证书链未闭合。
- 未授权刷新 `real_primary_latest` 或执行任何真实写入。

这些事项阻止真实推广，但不阻止本离线软件候选进入 Draft PR 审查。

## 下一动作

停止新增功能。获得明确授权后，只允许：

1. 推送当前候选分支；
2. 创建 Draft PR；
3. 等待远端 CI 与人工审查；
4. 若出现 P0/P1，只做定点修复并重跑对应门禁。

未经再次明确授权，不创建 PR、不合并、不发布。
