# CO2 Golden Path 保护与回归计划

**基线**: `171e530c`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**生成时间**: 2026-05-12  
**本文档角色**: A3 后 CO2 气路保护与回归计划  
**上下文**: A3-H2O 准入评审附带 — CO2 需要独立验证，不能因 H2O PASS 而跳过  
**分类**: `engineering_probe_only` / `not_real_acceptance_evidence=true`

---

## 1. CO2 气路物理逻辑

### 1.1 CO2 校准流程概览

CO2 校准分为三个物理阶段：

```
open conditioning → seal transition → sealed pressure control + sampling
```

### 1.2 Open Conditioning 阶段

- **VENT=ON**：压力控制器通大气
- **气路开放**：CO2 气路阀打开，目标气体从气瓶/稀释系统流经分析仪
- **目的**：用目标浓度气体充分置换/吹扫气路，排除残留气体
- **关键约束**：此阶段必须 VENT=ON，否则无法置换

### 1.3 Seal Transition 阶段

- **VENT=OFF**：断开大气通路
- **气路保持**：CO2 气路阀仍保持开放，但不再有气体流出
- **目的**：准备封闭回路进行压力控制
- **关键约束**：VENT=OFF 必须在任何密封操作之前完成

### 1.4 Sealed Pressure Control 阶段

- **VENT 保持 OFF**：气路完全封闭
- **压力控制**：PACE5000 通过进气/排气阀调节封闭回路内压力
- **逐点采样**：在每个压力目标点稳定后采集 CO2 浓度数据
- **关键约束**：
  - sealed 阶段 vent=ON count 必须为 0
  - 任何通大气动作都会破坏当前压力点的数据有效性

### 1.5 CO2 vs H2O 关键差异

| 维度 | CO2 | H2O |
|------|-----|-----|
| ambient 首点 | 可选（可跳过 ambient） | 必须（通水后 ambient 采样） |
| 气体源 | 气瓶/稀释系统（干气） | 湿度发生器（湿气） |
| 气路切换 | `set_valves_for_co2` | `set_h2o_path` |
| seal transition 触发 | `pressurize_and_hold(point, route=phase)` | `pressurize_and_hold(lead, route=phase, prefer_direct_vent_close=True)` |
| 独有约束 | CO2 零气特殊冲洗、高浓度首点模式 | dewpoint 对准、温度稳定 |

---

## 2. CO2 保护门当前结论

### 2.1 代码修改范围分析

基于 `git diff 5bc4fa2c..034b2d6b` 和 `git diff 54f4b2df..034b2d6b` 的静态分析：

| 检查项 | 结果 | 证据 |
|--------|:--:|------|
| 本轮是否修改 `co2_route_runner.py` | **NO** | `git show` 无该文件 |
| 本轮是否修改 CO2 route 主链 | **NO** | CO2 路由逻辑未被触碰 |
| 本轮是否修改 `valve_routing_service.py` | **NO** | 未在 commit diff 中 |
| 本轮是否恢复 flow_lpm/UI/shared-service stash | **NO** | stash@{0} 完整保留 |
| 本轮是否放松 CO2 sealed no-vent guard | **NO** | 未修改任何 CO2 guard |

### 2.2 pressure_control_service 的 5bc4fa2c 修改范围

`5bc4fa2c` 在 `pressurize_and_hold()` 方法中新增的逻辑：

```python
if route_text == "h2o" and prefer_direct_vent_close:
    # H2O-only: vent_closed_verified + 1.5s wait + set_h2o_path(False)
```

**隔离分析**：

| 隔离层 | 机制 | CO2 是否满足 |
|--------|------|:--:|
| route_text guard | `route_text == "h2o"` | **NO** — CO2 route_text 是 "co2" |
| prefer_direct_vent_close guard | 必须 truthy | **NO** — CO2 runner 不传此参数 |

**结论**：5bc4fa2c 修改仅在 `route_text == "h2o" and prefer_direct_vent_close` 时激活，CO2 路径**完全不受影响**。

### 2.3 静态分析局限性声明

| 局限 | 说明 |
|------|------|
| 静态 diff 只能证明修改范围 | ✅ 已证明修改限定在 H2O 路径 |
| 静态 diff 不能证明运行时交互 | ⚠️ CO2 runner 调用同一 `pressurize_and_hold()` 方法 |
| route_text guard 是可靠的隔离 | ✅ 字符串比较 "h2o" vs "co2"，不会混淆 |
| prefer_direct_vent_close 默认值 | ✅ CO2 不传 = 默认 `False`，不会进入 |
| 间接影响（如压力控制器状态残留） | ⚠️ 理论上 H2O 运行后可能影响 CO2 运行，但 CO2 runner 有自己的 preseal gate |

**保守建议**：虽然静态分析证明 CO2 路径未受影响，但 CO2 runner 调用的是同一个 `pressure_control_service` 实例，建议 A3 后跑一次 CO2 no-write dry run 作为最终锁门。

---

## 3. A3 后 CO2 回归计划

> **重要**：以下所有计划都是 **offline / simulation / contract test** 性质。
> **不跑真机**，仅在当前阶段做计划文档。

### 3.1 Offline CO2 Route Contract Tests（优先级 P0）

| 测试项 | 说明 | 工具 |
|--------|------|------|
| CO2 route trace structure | 检查 CO2 route_trace 是否包含完整事件链路 | pytest + fixture |
| CO2 sealed no-vent trace | 验证 sealed 阶段 vent=ON count = 0 | route_trace.jsonl 解析 |
| CO2 pressure control gate | 验证 pressure_control_ready_gate 正确性 | trace 分析 |
| CO2 与 H2O 隔离 | 验证 CO2 runner 不传 prefer_direct_vent_close | 代码级断言 |

**命令参考**：
```bash
pytest tests/v2/ -k "co2" -v --tb=short
pytest tests/v2/test_co2_route_runner.py -v  # 如存在
```

### 3.2 CO2 Route Trace / Sealed No-Vent Trace 检查（优先级 P0）

- 从已有的 CO2 simulation run 中提取 route_trace.jsonl
- 检查 sealed phase 所有 action 中 `vent_on: true` 的出现次数
- 预期结果：**sealed phase vent=ON count = 0**

### 3.3 CO2 No-Write Single-Route Dry Run 计划（优先级 P0，A3 后执行）

| 参数 | 值 |
|------|-----|
| 模式 | simulation-only |
| 路由 | CO2 single route |
| 浓度 | 单一浓度点（如 1000 ppm） |
| 压力点 | 1100/1000/900/800/700/600/500 |
| no-write | true |
| 目标 | 验证 CO2 golden path 完整链路 |

**验证清单**：
- [ ] CO2 ambient/conditioning phase 正常
- [ ] CO2 seal transition 正常
- [ ] CO2 七压力点全部完成
- [ ] sealed 阶段 vent=ON count = 0
- [ ] no-write guard 生效
- [ ] 无异常 safe stop

### 3.4 CO2 Golden Path Artifact 比对（优先级 P1）

- 从 CO2 simulation run 导出标准 artifacts
- 与已知 good baseline artifacts 做 diff
- 重点关注：
  - CO2 ppm 读数在各压力点的一致性
  - 压力控制精度 (actual vs target)
  - sample 采样时序

### 3.5 Shared Service 改动前的 CO2 Regression Gate（优先级 P0）

**硬约束**：任何对以下 shared service 的修改，在 merge 前必须跑 CO2 regression：

| Shared Service | 风险等级 | 必须回归项 |
|----------------|:--:|------|
| `pressure_control_service.py` | **P0** | CO2 seal transition + 七压力点 |
| `valve_routing_service.py` | **P0** | CO2 气路切换正确性 |
| `sampling_service.py` | **P1** | CO2 采样时序正确性 |
| `humidity_generator_service.py` | **P2** | CO2 不受影响，但需确认 |

### 3.6 进入更高阶段时的 CO2 真机回归准入条件（未来，当前不做）

| 准入条件 | 说明 |
|----------|------|
| CO2 no-write simulation 全部 PASS | 仿真门禁通过 |
| CO2 no-write dry run 全部 PASS | 至少 3 次 dry run 通过 |
| operator confirmation 完成 | 操作员确认 + 工程授权 |
| sealed vent=ON = 0 | 无可争议 |
| no-write guard 生效 | 无可争议 |

---

## 4. CO2 不允许事项

| # | 事项 | 状态 |
|---|------|:--:|
| 1 | 因为 H2O R4/R5 PASS 就认为 CO2 已通过 | **不允许** ❌ |
| 2 | 跳过 CO2 protection gate | **不允许** ❌ |
| 3 | CO2 回归前修改 shared pressure/valve/sampling runtime | **不允许** ❌ |
| 4 | 在 CO2 验证完成前 default cutover V2 | **不允许** ❌ |
| 5 | 恢复 stash@{0}（含 valve_routing_service 改动）进入 baseline | **不允许** ❌ |

---

## 5. 总结

- CO2 气路保护门当前状态：**通过** — 静态代码分析确认 CO2 路径未受 D29 修改影响
- CO2 golden path 回归是 A3 后的**最高优先级**任务
- CO2 回归**不跑真机**，优先通过 simulation / contract test 完成
- 任何 shared service 改动前必须跑 CO2 regression gate
- CO2 不允许因 H2O PASS 而享受"连带通过"待遇

---

**CO2 Golden Path 保护与回归计划结束。建议 A3 准入评审后立即启动 CO2 no-write simulation 回归。**
