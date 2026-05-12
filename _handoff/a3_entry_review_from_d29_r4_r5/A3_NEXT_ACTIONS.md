# A3 后续执行路线

**基线**: `e9b0bb95`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**生成时间**: 2026-05-12  
**本文档角色**: A3-H2O 准入评审通过后的执行路线  
**前置条件**: A3_H2O_ENTRY_REVIEW_DECISION = PASS ; CO2_PROTECTION_GATE = PASS  
**分类**: `engineering_probe_only` / `not_real_acceptance_evidence=true`

---

## A. 立即进入的下一步 (A3 准入即执行)

### A1. A3-H2O 准入结论固化

| 动作 | 说明 |
|------|------|
| ✅ 评审完成 | `A3_H2O_ENTRY_REVIEW_DECISION.md` → PASS |
| ✅ CO2 保护门通过 | `CO2_PROTECTION_GATE_OFFLINE_CHECK.md` → PASS |
| ⬜ 提交评审决策 | 等用户确认后 git add + commit + push |
| ⬜ 通知相关方 | A3-H2O 准入通过，可进入 A3 后阶段 |

### A2. CO2 保护门离线结论固化

- ✅ Git diff 静态分析完成：CO2 runner / valve_routing_service / CO2 主链均未触碰
- ✅ 5bc4fa2c 修改确认仅限 H2O + `prefer_direct_vent_close` 路径
- ✅ 43 离线测试全部通过
- ✅ CO2 保护门 PASS

### A3. 不再 D29 runtime 补丁

| 决策 | 理由 |
|------|------|
| 停止 D29 补丁 | R4+R5 已充分验证 vent/valve 顺序 |
| 不再修改 `h2o_route_runner.py` | 当前实现已稳定 |
| 不再修改 `pressure_control_service.py` (vent 相关) | H2O 路径已验证 |

### A4. 不恢复 high-risk stash

| stash | 状态 |
|-------|------|
| stash@{0} (flow_lpm + valve + UI) | **保持 stashed**，按 Phase 3/4 分阶段处理 |
| stash@{1} (WIP: no-write dry run) | **保持 stashed** |

---

## B. A3 后第一批工作 (CO2 Golden Path 回归)

### B1. CO2 No-Write Single-Route Simulation 回归

| 参数 | 值 |
|------|-----|
| 模式 | **simulation-only** — 不跑真机 |
| 路由 | CO2 single route |
| 浓度 | 单一浓度点 (如 1000 ppm) |
| 压力点 | 1100/1000/900/800/700/600/500 |
| no-write | true |

**执行步骤**：
1. 准备 CO2 no-write config 文件
2. 运行 `python -m gas_calibrator.v2.scripts.run001_h2o_only_1_point_no_write_probe` (CO2 版本)
3. 收集 route_trace.jsonl / summary.json / no_write_guard.json
4. 检查 sealed 阶段 vent=ON count = 0
5. 确认 7 压力点全部完成
6. 确认 no-write guard 生效

### B2. H2O/CO2 双路线 No-Write 证据对照

| 对照维度 | H2O (R4/R5) | CO2 (待执行) |
|----------|:--:|:--:|
| ambient 采样 | ✅ 完成 | 待验证 (CO2 可跳过 ambient) |
| seal transition | ✅ vent=OFF → closed → wait → set_h2o_path(False) | 待验证 (CO2 不传 prefer_direct_vent_close) |
| sealed vent=ON | ✅ 0 | 待验证 (预期 0) |
| 七压力点 | ✅ 全部完成 | 待验证 |
| no-write | ✅ 0 writes | 待验证 |
| no-flow (H2O) / N/A (CO2) | ✅ flow=NoneL/min | N/A (CO2 不控制湿度发生器) |

### B3. Route Trace / Vent Trace / Pressure Trace 对照

| Trace 类型 | H2O (已有) | CO2 (待生成) |
|------------|:--:|:--:|
| route_trace.jsonl | ✅ R4 + R5 | 待 CO2 simulation 生成 |
| vent=OFF command sent | ✅ 有 | 待检查 |
| vent_closed_verified | ✅ 有 (H2O only) | CO2 无此步骤 (正确) |
| seal_transition gate | ✅ 有 | 待检查 |
| pressure control gate | ✅ 有 | 待检查 |
| sample events | ✅ 完整 | 待检查 |

### B4. Artifact Registry 更新

| 动作 | 说明 |
|------|------|
| D29-R4 登记 | 已在 `_handoff/d29_h2o_vent_order_5bc4fa2c_r4_evidence_pack/` |
| D29-R5 登记 | 已在 `_handoff/d29_h2o_vent_order_034b2d6b_r5_evidence_pack/` |
| A3 决策登记 | 本次生成的 3 个文档 |
| CO2 回归登记 | B1 完成后登记 |

---

## C. 后置专项 (A3 后 / 更高阶段前)

以下专项 **一律不得混入 A3 baseline**，需按指定阶段分步推进。

### C1. flow_lpm / Humidity Generator 行为专项 (Phase 3)

| 文件 | 说明 | 风险 |
|------|------|:--:|
| `humidity_generator_service.py` | 移除/收缩 flow_lpm 兼容逻辑 | P0 |
| `grz5013_fake.py` | 同步 fake 行为 | P2 |

**准入条件**：
- A3 准入完成
- CO2 golden path 回归完成
- 独立 operator confirmation
- 确认 flow_lpm 不再被任何 config 使用

### C2. UI Flow 控件专项 (Phase 3)

| 文件 | 说明 | 风险 |
|------|------|:--:|
| `ui_v2/controllers/device_workbench.py` | 移除 flow_lpm UI 控制 | P1 |
| `ui_v2/widgets/device_workbench.py` | 移除 flow_lpm 控件 | P1 |

**准入条件**：
- 与 C1 联动
- UI 功能完整性回归测试

### C3. Valve Routing Service Safe-Stop 专项 (Phase 4 — 更高阶段前)

| 文件 | 说明 | 风险 |
|------|------|:--:|
| `valve_routing_service.py` | valve routing safe-stop 行为修改 | **P0** |
| `test_valve_routing_service.py` | 测试合同更新 | **P0** |

**准入条件**：
- **不得在 A3 准入阶段恢复**
- CO2 no-write simulation regression **必须先跑**
- H2O no-write simulation regression **必须先跑**
- 独立 operator confirmation
- 独立 code review

### C4. 未来单实例 PID/Lock Guard

- 目标：防止重复启动导致 COM 口占用（091713 同类问题）
- 建议阶段：Phase 4 之后，独立工程增强
- 当前不修改 runtime

### C5. VentManager / RoutePressureStateMachine Shadow 设计

- 目标：为 vent/valve 管理提供更结构化的框架
- 方式：shadow 设计 → 独立测试 → 逐步接入
- 建议阶段：Phase 4 之后
- 当前不改 runtime

---

## 执行路线总结

```
现在:
  └── A3-H2O 准入 PASS ✅ → 提交决策文档 → 进入 A3 后

A3 后 (立即):
  ├── B1: CO2 no-write simulation 回归
  ├── B2: H2O/CO2 双路线证据对照
  ├── B3: route/vent/pressure trace 对照
  └── B4: artifact registry 更新

A3 后 (Phase 1-2):
  ├── Phase 1: 确认 test contracts (B1/B2) 与 stash 一致
  └── Phase 2: config/probe cleanup (A1/A2)

A3 后 (Phase 3):
  ├── C1: flow_lpm 行为专项
  └── C2: UI flow 控件专项

更高阶段前 (Phase 4):
  ├── C3: valve_routing_service safe-stop 专项
  ├── C4: 单实例 PID/lock guard
  └── C5: VentManager/RoutePressureStateMachine shadow
```

### 关键约束

| 约束 | 适用范围 |
|------|----------|
| Phase 3/4 禁止混入 A3 baseline | 所有 C 类专项 |
| Phase 4 恢复前必须跑 CO2 regression | C3 专项 |
| stash@{0} 在 Phase 3/4 之前不得恢复 | 全局 |
| 不跑真机 (simulation-only) | B1-B3 |
| 不做 production acceptance | 全局 |
| 不做 formal switch | 全局 |

---

**A3 后续执行路线结束。建议按 Phase 顺序推进，当前优先执行 B1 (CO2 golden path no-write 回归)。**
