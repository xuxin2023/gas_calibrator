# V1.5 测试门禁分层

## 目的

`scripts/test.ps1` 是统一的本地离线测试入口。它将 V1.5 当前改动验证、
正式离线发布验证和 V2 重型仿真分开，避免把全仓 5,090 项测试误当成每一批
V1.5 改动的即时门禁。

所有层级都检查真实 COM、设备写入和正式数据库相关环境变量；只要任意解锁
变量存在就会 fail-closed。所有结果均为离线、仿真或回放证据，不是真机验收。

## 层级

| 层级 | 当前范围 | 使用时机 | 结论边界 |
|---|---:|---|---|
| `quick` | 8 个 V1.5 关键测试文件，当前 117 项，并追加 parity | 每次 V1.5 小批改动 | 只证明当前关键路径与口径未回归 |
| `release` | 复用现有 28 文件、当前 566 项 final offline acceptance runner，并追加 parity | 候选发布、合并前冻结 | 只允许形成离线 program-level acceptance |
| `nightly` | 复用现有 V2 nightly simulation suite | 重型仿真、协议比较和导出韧性 | `simulated`，不得解释为 V1.5 真机验收 |

测试数量是当前树的收集结果，后续可随测试增加而变化；实际执行始终由精确入口
和现有 allowlist 决定。

## 命令

```powershell
# 默认层级是 quick
powershell -ExecutionPolicy Bypass -File scripts/test.ps1

# 显式快速门禁
powershell -ExecutionPolicy Bypass -File scripts/test.ps1 -Tier quick

# 正式离线发布门禁；必须显式绑定所审核的 origin/main 提交
powershell -ExecutionPolicy Bypass -File scripts/test.ps1 `
  -Tier release `
  -SourceOriginMainCommit <40-character-origin-main-sha> `
  -OutputDir D:\gas_calibrator\_runtime\test_gates\v1_5_release

# 重型夜间仿真
powershell -ExecutionPolicy Bypass -File scripts/test.ps1 `
  -Tier nightly `
  -OutputDir D:\gas_calibrator\_runtime\test_gates\v2_nightly

# 只查看层级，不执行测试
powershell -ExecutionPolicy Bypass -File scripts/test.ps1 -List
```

## 不再采用的默认做法

`python -m pytest -q` 仍可用于周期性的全仓离线冻结，但不再作为
`scripts/test.ps1` 的默认行为。当前全仓包含 5,090 项测试；V2 协议仿真中
单个完整路径用例约需 38 秒，因此全仓执行必须单独安排充足时限或采用经过审核
的完整文件分片。

## 安全锁

任何层级均不得：

- 打开真实 COM；
- 控制气路、水路或压力；
- 写入 SENCO、设备 ID 或分析仪系数；
- 连接正式 PostgreSQL；
- 刷新 real primary latest；
- 把 simulation、replay、parity 或离线 suite 结果解释为真实 acceptance。
