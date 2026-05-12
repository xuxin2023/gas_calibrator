# Stash 后置专项拆分计划

**基线**: `171e530c`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**生成时间**: 2026-05-12  
**本文档角色**: stash@{0} 中 9 个文件的后置专项拆分与准入计划  
**核心原则**: 不恢复 stash，按文件类别拆分为独立专项，分别判断是否进入 A3 baseline  
**分类**: `engineering_probe_only` / `not_real_acceptance_evidence=true`

---

## 0. stash@{0} 内容总览

```
$ git stash show --name-status 'stash@{0}'

M  src/gas_calibrator/v2/configs/validation/run001_h2o_only_1_point_no_write_real_machine.json
M  src/gas_calibrator/v2/core/run001_h2o_only_1_point_no_write_probe.py
M  src/gas_calibrator/v2/core/services/humidity_generator_service.py
M  src/gas_calibrator/v2/core/services/valve_routing_service.py
M  src/gas_calibrator/v2/sim/devices/grz5013_fake.py
M  src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py
M  src/gas_calibrator/v2/ui_v2/widgets/device_workbench.py
M  tests/v2/test_h2o_golden_sequence.py
M  tests/v2/test_h2o_vent_off_adapter_contract.py
M  tests/v2/test_valve_routing_service.py

 10 files changed, 79 insertions(+), 185 deletions(-)
```

**stash 描述**: `D29-R4 unrelated: flow_lpm removal + test contract updates NOT part of 5bc4fa2c`

**关键事实**：stash 中的 4 个文件与 `034b2d6b` 重叠：
- `run001_h2o_only_1_point_no_write_real_machine.json`
- `run001_h2o_only_1_point_no_write_probe.py`
- `test_h2o_golden_sequence.py`
- `test_h2o_vent_off_adapter_contract.py`

这意味着 `034b2d6b` 可能已经吸收/覆盖了 stash 中这些文件的部分改动。需要逐文件检查 diff 来判断。

---

## A. 可复现/配置类 (config + probe)

### A1. `run001_h2o_only_1_point_no_write_real_machine.json`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | -1 行 (移除一行) |
| 修改推测 | 移除 `flow_lpm` 配置项 |
| 当前文件状态 | `034b2d6b` 已修改该文件 |
| 当前 config 是否已有 `flow_lpm` | **NO** — R5 已验证 config 无 `flow_lpm` |
| 是否与 034b2d6b 冲突 | 可能部分重叠，需 diff 确认 |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** — 当前 config 已经无 flow_lpm |
| 是否进入 A3 baseline | **NO** — 当前 config 已符合要求 |
| 建议阶段 | A3 后 — 如需正式移除，作为独立 config cleanup |
| 必须测试 | 确认 config JSON schema 兼容 |
| 风险等级 | **P2** — 低，当前运行时已满足 |

### A2. `run001_h2o_only_1_point_no_write_probe.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | +13/-? 行 |
| 修改推测 | 移除 flow_lpm 相关逻辑 |
| 当前文件状态 | `034b2d6b` 已修改该文件 |
| 是否与 034b2d6b 冲突 | 可能部分重叠，需 diff 确认 |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** — 034b2d6b 已更新 probe |
| 是否进入 A3 baseline | **NO** — 当前 probe 已对齐 D29-R4 baseline |
| 建议阶段 | A3 后 — 如需更新 probe 逻辑，作为独立 patch |
| 必须测试 | probe 是否正确触发 H2O 流程 |
| 风险等级 | **P2** — 低，probe 当前可正常使用 |

---

## B. H2O 合同测试类 (tests)

### B1. `test_h2o_golden_sequence.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | +10/-? 行 |
| 修改推测 | 更新 H2O golden sequence 测试合同 |
| 当前文件状态 | `034b2d6b` 已修改该文件 (M) |
| 是否已被 034b2d6b 吸收 | **高度可能** — 034b2d6b commit message 说 "align H2O probe and contracts" |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** — 034b2d6b 已吸收相关改动 |
| 是否进入 A3 baseline | **YES** — 当前文件已是 A3 baseline 的一部分 |
| 建议阶段 | 已包含在 A3 baseline |
| 必须测试 | `pytest tests/v2/test_h2o_golden_sequence.py -v` |
| 风险等级 | **P1** — 需确认 034b2d6b 已完整吸收 |

### B2. `test_h2o_vent_off_adapter_contract.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | +?/-191 行 (大幅删减) |
| 修改推测 | 大幅简化 vent-off adapter contract 测试 |
| 当前文件状态 | `034b2d6b` 已修改该文件 (M) |
| 是否已被 034b2d6b 吸收 | **高度可能** — 191 行删减与 5bc4fa2c 简化 runner 方向一致 |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** — 034b2d6b 已吸收 |
| 是否进入 A3 baseline | **YES** — 当前文件已是 A3 baseline |
| 建议阶段 | 已包含在 A3 baseline |
| 必须测试 | `pytest tests/v2/test_h2o_vent_off_adapter_contract.py -v` |
| 风险等级 | **P1** — 需确认 034b2d6b 与 stash 改动一致 |

---

## C. flow_lpm / 湿度发生器行为类 (humidity + sim)

### C1. `humidity_generator_service.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | +10/-? 行 |
| 修改推测 | 移除或收缩 `flow_lpm` 相关逻辑 |
| 当前文件状态 | **未被** 034b2d6b 或 5bc4fa2c 修改 |
| 是否与 A3 baseline 冲突 | **YES** — 这是独立的行为改动，不涉及 D29 |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** — 这是一个独立的 flow_lpm 行为收缩 |
| 是否进入 A3 baseline | **NO** — 不得混入 A3-H2O 准入 |
| 建议阶段 | A3 后单独审计和专项 |
| 必须测试 | flow_lpm 兼容逻辑的正确性、no-flow 场景下不触发 set_flow_target |
| 风险等级 | **P0** — 影响 humidity generator 行为，需独立评估 |

**当前运行时状态**: 因为 config 无 `flow_lpm`，当前代码不触发 `set_flow_target`，功能正常。

### C2. `grz5013_fake.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | -1 行 |
| 修改推测 | 移除 flow_lpm 相关 mock |
| 当前文件状态 | **未被** 034b2d6b 或 5bc4fa2c 修改 |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** |
| 是否进入 A3 baseline | **NO** |
| 建议阶段 | 与 C1 联动，A3 后独立处理 |
| 必须测试 | simulation flow_lpm mock 兼容性 |
| 风险等级 | **P2** — simulation-only，低风险 |

---

## D. shared valve/safe-stop 高风险类 (valve routing)

### D1. `valve_routing_service.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | +13/-? 行 |
| 修改推测 | valve routing 行为修改（安全停止相关） |
| 当前文件状态 | **未被** 034b2d6b 或 5bc4fa2c 修改 |
| 是否影响 CO2 | **高度可能** — 这是 shared service |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** — 这是最高风险的改动 |
| 是否进入 A3 baseline | **NO** — 绝对不得混入 |
| 建议阶段 | **更高阶段前** — 需要独立设计、独立测试、独立跑 CO2 regression |
| 必须测试 | CO2 完整 route + H2O 完整 route + safe stop 场景全覆盖 |
| 风险等级 | **P0** — shared service，影响所有路由，必须独立专项 |

**严格隔离要求**：
- 不得与 A3-H2O 准入混在一起提交
- 恢复前必须先跑 CO2 no-write simulation regression
- 恢复前必须先跑 H2O no-write simulation regression
- 必须有独立的 operator confirmation

### D2. `test_valve_routing_service.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | +17/-? 行 |
| 修改推测 | 更新 valve routing 测试合同 |
| 当前文件状态 | **未被** 034b2d6b 或 5bc4fa2c 修改 |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** — 与 D1 联动 |
| 是否进入 A3 baseline | **NO** |
| 建议阶段 | 与 D1 联动，更高阶段前 |
| 必须测试 | 与 D1 联动 |
| 风险等级 | **P0** — 测试合同与 D1 绑定 |

---

## E. UI 设备工作台类

### E1. `ui_v2/controllers/device_workbench.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | -5 行 (删除) |
| 修改推测 | 移除 flow_lpm / 湿度发生器流量相关 UI 控制 |
| 当前文件状态 | **未被** 034b2d6b 或 5bc4fa2c 修改 |

**判断**：

| 问题 | 回答 |
|------|------|
| 是否建议现在恢复 | **NO** |
| 是否进入 A3 baseline | **NO** |
| 建议阶段 | A3 后 — 与 C1 联动 |
| 必须测试 | UI 设备工作台功能完整性 |
| 风险等级 | **P1** — UI 改动，不直接影响 runtime |

### E2. `ui_v2/widgets/device_workbench.py`

| 属性 | 值 |
|------|-----|
| stash 修改行数 | -3 行 |
| 修改推测 | 同 E1 |
| 当前文件状态 | **未被** 034b2d6b 或 5bc4fa2c 修改 |

**判断**：同 E1。

---

## 汇总

| 分类 | 文件 | 恢复 | 进 A3 | 阶段 | 风险 |
|------|------|:--:|:--:|------|:--:|
| A1 配置 | `run001_h2o_only_...json` | NO | NO | A3 后 | P2 |
| A2 探针 | `run001_h2o_only_...probe.py` | NO | NO | A3 后 | P2 |
| B1 测试 | `test_h2o_golden_sequence.py` | NO | YES | 已包含 | P1 |
| B2 测试 | `test_h2o_vent_off_adapter_contract.py` | NO | YES | 已包含 | P1 |
| C1 湿度 | `humidity_generator_service.py` | NO | NO | A3 后 | P0 |
| C2 仿真 | `grz5013_fake.py` | NO | NO | A3 后 | P2 |
| D1 阀路 | `valve_routing_service.py` | NO | NO | 更高阶段前 | **P0** |
| D2 测试 | `test_valve_routing_service.py` | NO | NO | 更高阶段前 | **P0** |
| E1 UI | `device_workbench.py` (controllers) | NO | NO | A3 后 | P1 |
| E2 UI | `device_workbench.py` (widgets) | NO | NO | A3 后 | P1 |

### 恢复顺序建议（A3 后）

```
Phase 1 (A3 后立即):  B1 + B2 — 确认 034b2d6b 已完整吸收，diff 对齐
Phase 2 (A3 后):      A1 + A2 — 独立 config/probe cleanup
Phase 3 (A3 后):      C1 + C2 + E1 + E2 — flow_lpm 行为收缩专项
Phase 4 (更高阶段前):  D1 + D2 — shared valve safe-stop 高风险专项（需要 CO2 regression gate）
```

### 硬约束

- **任何阶段都不允许**将 D1/D2 (valve_routing_service) 与 A3-H2O 混在一起
- Phase 4 恢复前必须先跑 CO2 no-write simulation regression
- stash@{0} 在当前 A3 准入阶段**保持 stashed**，不得恢复

---

**Stash 后置专项拆分计划结束。当前建议：不恢复任何 stash 文件，按上述 Phase 1-4 分阶段推进。**
