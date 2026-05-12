# CO2 Golden Path B1-R1 根因审计报告

**生成时间**: 2026-05-12  
**HEAD**: eb605517  
**分支**: codex/v2-golden-recovery-cdb82111  
**阶段**: A3-H2O PASS 后 → B1 CO2 golden path no-write 回归  
**报告类型**: ROOT_CAUSE_REPORT（根因审计，代码修改前）

---

## 1. 仓库状态

| 项目 | 值 |
|------|-----|
| 当前 branch | `codex/v2-golden-recovery-cdb82111` |
| 当前 HEAD | `eb605517` |
| Tracked worktree | **clean**（无 tracked 修改） |
| Staged | **空** |
| stash@{0} | **保留**：`D29-R4 unrelated: flow_lpm removal + test contract updates NOT part of 5bc4fa2c` |
| stash@{1} | `WIP on codex/run001-a1-no-write-dry-run` |
| Untracked 文件 | 大量 handoff/ 目录下的 untracked 工件（与本次任务无关） |

## 2. A3-H2O 当前结论

A3-H2O 准入 **PASS**：
- D29-R4 (5bc4fa2c): fix H2O ambient-to-sealed vent valve order ✅
- D29-R5 (034b2d6b): align H2O probe and contracts with D29-R4 baseline ✅
- eb605517: A3-H2O entry PASS decision and CO2 gate check ✅
- H2O D29 runtime 补丁已停止

## 3. 当前 B1 目标

证明 H2O D29 修复没有破坏 CO2 气路黄金路径。执行 CO2 golden path no-write 回归。

## 4. 历史 CO2 成功入口

- CO2 route_runner 单元测试：`tests/v2/test_co2_route_runner.py` ✅
- CO2 离线合同测试套件 47/47 ✅
- CO2 no-vent guard 合同测试 ✅
- CO2 artifact contract 测试 ✅
- CO2 golden master / shadow trace 测试 ✅
- pressure_control_service 单元测试 ✅

以上测试均**不经过** `WorkflowOrchestrator → conditioning_service → host._verify_co2_preseal_atmosphere_hold_pressure` 集成路径。

## 5. 本次失败入口

```bash
python -m gas_calibrator.v2.scripts.run_simulated_compare \
  --profile replacement_skip0_co2_only_simulated \
  --scenario co2_only_skip0_success_single_temp \
  --report-root "_handoff\a3_after_co2_golden_path_regression" \
  --run-name co2_golden_no_write_after_a3_h2o_eb605517 \
  --no-publish-latest
```

失败阶段：`v2:error`，V2 成功设置 CO2 route + VENT=ON 后，在 pre-seal atmosphere hold 阶段崩溃。

## 6. 两条路径差异

| 维度 | 历史成功路径（CO2 单元测试） | 本次失败路径（run_simulated_compare） |
|------|--------------------------|--------------------------------------|
| 入口 | pytest 直接调用 route_runner | run_simulated_compare → orchestrator 全栈 |
| 是否走 conditioning_service | 否 | 是 |
| 是否走 host._verify_co2_preseal_atmosphere_hold_pressure | 否（或 monkeypatch） | 是（真实调用） |
| 是否覆盖 orchestrator integration | 否 | 是 |

## 7. `_verify_co2_preseal_atmosphere_hold_pressure` 全部搜索结果

### 7.1 生产代码（3 处，均为调用点，无定义）

| 文件 | 行号 | 说明 |
|------|------|------|
| conditioning_service.py | 2082 | `high_pressure_first_point_mode` 路径调用 |
| conditioning_service.py | 2101 | `continuous_atmosphere_hold` 路径调用 |
| conditioning_service.py | 2144 | 非 positive_preseal 路径调用（忽略返回值） |

### 7.2 测试代码（9 处引用）

| 文件 | 行号 | 说明 |
|------|------|------|
| test_a2_no_write_pressure_sweep.py | 996 | 直接调用（通过 _preseal_arm_orchestrator） |
| test_a2_no_write_pressure_sweep.py | 1017 | 直接调用 |
| test_a2_no_write_pressure_sweep.py | 1664 | lambda mock 注入 |
| test_a2_no_write_pressure_sweep.py | 5489 | lambda mock 抛出 AssertionError |
| test_a2_no_write_pressure_sweep.py | 5759 | 直接调用 |
| test_a2_no_write_pressure_sweep.py | 5800 | 直接调用 |
| test_a2_no_write_pressure_sweep.py | 5802 | 直接调用 |
| test_a2_no_write_pressure_sweep.py | 5833 | 直接调用 |
| test_a2_no_write_pressure_sweep.py | 5908 | 直接调用 |

### 7.3 生产代码是否存在？

**不存在**。在当前分支 `codex/v2-golden-recovery-cdb82111` 的 orchestrator.py 和 conditioning_service.py 中均未定义。

### 7.4 Git 历史中是否存在？

**存在**。commit `8f411f34` 完整实现了该方法：
- orchestrator.py: 1 行 wrapper 委托到 conditioning_service
- conditioning_service.py: ~70 行实现，含压力采样 + ready/urgent/hard_abort 阈值判定

但 `8f411f34` **不在当前分支上**（`git merge-base --is-ancestor 8f411f34 HEAD` → exit=1, 否）。

`8f411f34` 所在分支：
- `codex/run001-a1-no-write-dry-run`
- `codex/v2-ui-cockpit-polish`

## 8. 调用链来源审计

| 事件 | 时间 |
|------|------|
| `2ec4682f` (batch-7: migrate 6 conditioning methods) 引入 conditioning_service.py 中的调用 | **是** HEAD 的祖先 |
| `8f411f34` (fix: 实现 _verify_co2_preseal_atmosphere_hold_pressure) | **否**，在其他分支 |
| `5bc4fa2c` (H2O D29 fix) | 是 HEAD 的祖先 |

## 9. 根因结论

| 问题 | 答案 |
|------|------|
| 是否是 H2O D29 引入？ | **否**。调用点在 `2ec4682f` 引入，早于 `5bc4fa2c` |
| 是否 CO2 主流程被破坏？ | **否**。CO2 route_runner / valve_routing / pressure_control 未受影响 |
| 是否测试 mock 掩盖接口缺口？ | **是**。`test_a2_no_write_pressure_sweep.py` 通过 monkeypatch 注入 mock，离线合同测试不经过集成路径 |
| 是否是错误入口？ | **否**。`run_simulated_compare` 是合理的 B1 CO2 integration regression 入口 |
| 是否是 orchestrator-host 接口合同缺口？ | **是**。batch-7 迁移将 conditioning 逻辑移到 conditioning_service.py，但未同步补齐 orchestrator→conditioning_service 的 host gate 接口 |

## 10. Root Cause 分类

**B — 历史 CO2 成功入口未覆盖 orchestrator integration，当前暴露 host interface 缺口。**

详细说明：
1. `2ec4682f` (batch-7) 将 CO2 conditioning 逻辑迁移到 conditioning_service.py，包括 3 处对 `self.host._verify_co2_preseal_atmosphere_hold_pressure(point)` 的调用
2. 该 commit 是 HEAD 的祖先（存在于当前分支）
3. 但 orchestrator 从未定义该方法的方法体
4. 修复已在 `8f411f34` 完成，但位于不同分支，未合并
5. 单元测试通过 monkeypatch 注入 mock，掩盖了集成路径的缺口
6. 首次通过 `run_simulated_compare` 全栈 simulation 触发该缺口

## 11. 离线测试结果

```
47 passed in 31.07s
```
全部通过，CO2 合同级别未受损。

## 12. Simulation 复现结果

- 入口：`run_simulated_compare --profile replacement_skip0_co2_only_simulated --scenario co2_only_skip0_success_single_temp`
- 状态：**MISMATCH**（V2 崩溃）
- 失败阶段：`v2:error` — pre-seal atmosphere hold
- V2 在崩溃前成功执行：analyzer setup → sensor precheck → wait temperature → route_baseline → set_vent(ON) → set_co2_valves([8,11,16,22]) → set_vent(ON, "CO2 route pre-seal atmosphere hold")
- 崩溃后自动执行：restore_baseline → final_safe_stop（fail-closed ✅）
- sealed 阶段未到达，无 vent=OFF
- 无 write 尝试
- 无 H2O 路径触发

## 13. 推荐处理方案

**允许进入最小修复**（分类 B），在 orchestrator.py 补齐 host gate wrapper，在 conditioning_service.py 补齐实现方法体。

修复范围严格遵守：
- 不改 V1
- 不改 H2O runtime
- 不改 CO2 route_runner
- 不改 valve_routing_service
- 不改 pressure_control_service shared 行为
- 不恢复 stash
- 不跑真实 COM
- 不写任何参数

## 14. Final Decision

**ALLOW_MINIMAL_FIX**

修复方案：从 `8f411f34` 中提取 orchestrator.py wrapper（1 行）+ conditioning_service.py 实现（约 70 行），适配到当前代码。

---

*此报告在任何代码修改前完成。*
