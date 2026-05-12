# A3-H2O 准入评审执行单

**基线**: `171e530c`  
**分支**: `codex/v2-golden-recovery-cdb82111`  
**生成时间**: 2026-05-12  
**本文档角色**: A3-H2O 准入评审执行单  
**证据来源**: D29-R4 (5bc4fa2c) + D29-R5 (034b2d6b) no-write 工程探针  
**分类**: `engineering_probe_only` / `promotion_state=blocked` / `not_real_acceptance_evidence=true`

---

## 1. A3 定位

1. 这是 **A3-H2O 准入评审**，不是 V2 整体 cutover。
2. 评审对象：H2O 水路的 ambient→sealed vent/valve 物理时序，及其 no-write / no-flow 合规性。
3. 评审不覆盖：CO2 气路完整验证（CO2 需要独立回归）。
4. 这不是 real acceptance，不是 production acceptance，不是 V2 replacement，不是 formal switch。
5. V1 fallback 完整保留，不做任何默认入口切换。
6. 当前阶段不允许 controlled write。

---

## 2. H2O 准入证据摘要

### 2.1 D29-R4 PASS 摘要

| 项目 | 结果 |
|------|------|
| Commit | `5bc4fa2c` fix(v2): enforce H2O ambient-to-sealed vent valve order |
| Run ID | `run_20260512_073103` |
| 决断 | **PASS** |
| vent/valve 顺序 | vent=OFF → vent_closed_verified → 1.5s wait → set_h2o_path(False) → seal gate |
| sealed 阶段 vent=ON | **0** |
| sealed 阶段 keepalive | **0** |
| no-write | PASS |
| 七压力点 | 全部完成 |

### 2.2 D29-R5 PASS 摘要（修正版）

| 项目 | 结果 |
|------|------|
| Commit | `034b2d6b` test(v2): align H2O probe and contracts with D29-R4 baseline |
| 主运行 ID | **`run_20260512_085322`**（启动 08:53 CST） |
| 决断 | **PASS** |
| vent/valve 顺序 | vent=OFF → vent_closed_verified → 1.5s wait → set_h2o_path(False) → seal gate → 1100 hPa |
| sealed 阶段 vent=ON | **0** |
| sealed 阶段 keepalive | **0** |
| no-write | PASS (`attempted_write_count=0`, `identity_write_command_sent=false`) |
| no-flow | PASS (`flow=NoneL/min`, 无 `set_flow_target`) |
| 七压力点 | ambient + 1100/1000/900/800/700/600/500 全部完成 |
| 重复启动附注 | `run_20260512_091713` — duplicate-start COM 占用 fail-closed，不是 R5 主失败 |

### 2.3 R4/R5 是否重复验证了完整序列

| 步骤 | R4 | R5 | 一致 |
|------|:--:|:--:|:--:|
| ambient sample complete | ✅ | ✅ | ✅ |
| stop keepalive (h2o_vent_keepalive_stopped) | ✅ | ✅ | ✅ |
| vent=OFF (vent=OFF command sent) | ✅ | ✅ | ✅ |
| vent_closed_verified | ✅ | ✅ | ✅ |
| wait 1.5s (post_vent_closed_wait) | ✅ | ✅ | ✅ |
| set_h2o_path(False) | ✅ | ✅ | ✅ |
| seal_transition gate | ✅ | ✅ | ✅ |
| seal_route (密闭完成) | ✅ | ✅ | ✅ |
| 1100 hPa pressure control (首点控压) | ✅ | ✅ | ✅ |
| 1000 hPa | ✅ | ✅ | ✅ |
| 900 hPa | ✅ | ✅ | ✅ |
| 800 hPa | ✅ | ✅ | ✅ |
| 700 hPa | ✅ | ✅ | ✅ |
| 600 hPa | ✅ | ✅ | ✅ |
| 500 hPa | ✅ | ✅ | ✅ |

**结论：R4 和 R5 在所有关键步骤上完美重复。** ✅

### 2.4 Sealed 阶段禁令检查

| 检查项 | R4 | R5 |
|--------|:--:|:--:|
| sealed 阶段 vent=ON count | **0** | **0** |
| sealed 阶段 h2o-vent-keepalive count | **0** | **0** |
| 控压期间通大气动作 | **0** | **0** |

### 2.5 No-Write 检查

| 检查项 | R5 (085322) |
|--------|-------------|
| attempted_write_count | **0** |
| identity_write_command_sent | **false** |
| persistent_write_command_sent | **false** |
| blocked_write_events | **[]** |
| ID write | none |
| SENCO write | none |
| Zero write | none |
| Span write | none |
| Coefficient write | none |

### 2.6 No-Flow 检查

| 检查项 | R5 (085322) |
|--------|-------------|
| config 含 flow_lpm | **NO** |
| run.log 含 set_flow_target | **NO** |
| run.log 含 flow=1.5 | **NO** |
| run.log flow 值 | `flow=NoneL/min` |
| route_trace 含 set_flow_target | **NO** |
| 本轮是否控制湿度发生器流量 | **NO** |

---

## 3. H2O 物理意义说明

### 3.1 VENT=ON 的物理意义

VENT 是 PACE5000 压力控制器的大气连通阀。

**VENT=ON (开放通大气)**：
- 物理上：压力控制器的测量腔与大气连通
- 气路效果：系统内气体可以自由与大气交换
- 何时使用：
  - **通水阶段**：通入湿度发生器的湿气，需要排放废气
  - **置换阶段**：用目标气体吹扫/置换气路中的残留气体
  - **常压采样阶段**：H2O ambient 首点在大气压下采样

### 3.2 VENT=OFF 的物理意义

**VENT=OFF (断开大气)**：
- 物理上：压力控制器测量腔与大气隔离
- 气路效果：系统成为一个封闭回路，可以进行压力调控
- 何时使用：
  - **准备封路**：ambient 采样完成后，准备进入控压扫点
  - **控压阶段全程**：密闭后通过 PACE 实现 1100→500 hPa 的逐点控压

### 3.3 为什么 vent_closed_verified 必须早于 set_h2o_path(False)

这是修复 `5bc4fa2c` 之前存在的物理顺序漏洞：

**修复前的风险**（已通过 5bc4fa2c 消除）：
- 如果先关 H2O 水路阀 (set_h2o_path(False))，再关 VENT
- 关阀 → 气路封闭 → 泵压迅速累积 → 可能在 vent 仍开放时造成不可控的压力冲击
- 或者 vent 关晚了，导致密闭后仍有通路向大气泄漏

**修复后的正确顺序**：
1. 先 VENT=OFF → 断开大气通路
2. 验证 vent 已物理关闭 (vent_closed_verified)
3. 等待 1.5s → 让机械阀完全就位
4. 再 set_h2o_path(False) → 关闭水路阀，完成封路

这样保证了**关闭所有对外通路后才封闭气路**，避免了压力冲击和泄漏风险。

### 3.4 为什么 1.5s wait 必须在 vent closed 后、关阀前完成

1.5 秒等待的物理意义是**给 vent 机械阀留出物理就位时间**。

- vent 阀是机械部件，打开/关闭不是瞬时的
- 虽然 vent 状态读回已经显示 closed，但机械动作可能需要额外几十到几百毫秒才能完全到位
- 1.5s 是一个保守的安全余量，确保 vent 阀在物理上已经完全关闭
- 如果在 vent 尚未完全就位时就关闭水路阀，可能造成瞬态压力波动

### 3.5 为什么 sealed pressure control 阶段绝不能 vent=ON

sealed 阶段意味着气路已经是封闭回路：

1. **压力控制依赖封闭回路**：PACE5000 通过进气阀/排气阀调节封闭回路内的压力，如果 vent=ON（通大气），系统就失去了控压能力
2. **校准精度受损**：各压力的 H₂O/CO₂ 浓度读数对应不同压力点，如果密闭被打破，当前压力点的读数就不可信
3. **安全考虑**：密闭后系统内压力可能远高于/低于大气压（如 1348 hPa 密闭峰值），突然 vent=ON 会造成剧烈的压力变化，可能损害传感器

**R4/R5 证据确认**：整个 sealed pressure sweep 阶段 vent=ON 次数 = **0**。

### 3.6 为什么 1100 hPa 首点是 ambient→sealed 成功的关键证据

1100 hPa 是 ambient 采样后的第一个控压目标点，它的成功证明了：

1. **Vent 关闭成功**：如果 vent 没有关闭或泄漏，压力控制器无法将压力从 ambient (~1010 hPa) 提升到 1100 hPa（需要正压差）
2. **密闭完整**：密闭后压力能上升到 1348 hPa 然后被控到 1100 hPa，说明气路完全封闭，没有泄漏
3. **控压能力正常**：PACE5000 能在密闭后正常追赶到目标压力
4. **整个 pressure sweep 的起点**：1100 hPa 成功后，1000→900→...→500 的逐点降压才有意义

**R5 证据**：
- 密闭后压力峰值：1348.398 hPa（证明完全密封）
- 1100 hPa 在限内：1099.92 hPa（22.5 秒内完成控压）
- 后续 1000/900/800/700/600/500 全部在限内

---

## 4. A3-H2O 准入门结论

| # | 准入门 | 结果 | 证据 |
|---|--------|:--:|------|
| 1 | H2O ambient→sealed 物理时序是否通过 | **YES** ✅ | R4+R5 route_trace.jsonl 双证据 |
| 2 | H2O 七压力点是否通过 | **YES** ✅ | R4+R5 point summaries |
| 3 | sealed 阶段 vent=ON 是否为 0 | **YES** ✅ | R5: 0 (2次 blocked cleanup 不计入) |
| 4 | sealed 阶段 keepalive 是否为 0 | **YES** ✅ | R5: 0 |
| 5 | no-write 是否通过 | **YES** ✅ | attempted_write_count=0 |
| 6 | no-flow 是否通过 | **YES** ✅ | flow=NoneL/min, 无 set_flow_target |
| 7 | 是否建议停止 D29/D3 runtime 补丁 | **YES** ✅ | R4/R5 已充分验证 vent/valve 顺序 |
| 8 | 是否建议进入 A3-H2O 准入评审 | **YES** ✅ | 证据完整，repeatability 成立 |

### 4.1 明确禁止

| 事项 | 状态 |
|------|:--:|
| 进入 controlled write | **NO** — 当前仍是 no-write only |
| V2 replacement | **NO** — V1 fallback + V2 engineering probe |
| default cutover | **NO** — 默认入口仍为 V1 |
| 跳过 CO2 保护门 | **NO** — CO2 需要独立回归 |
| 恢复 stash@{0} | **NO** — 高风险改动不得混入 A3 baseline |

---

## 5. 重复启动失败附注

Run `run_20260512_091713` 在 085322 运行期间重复启动，所有 COM 口被占用导致 11 个关键设备全部初始化失败（~3 秒内 fail-closed）。

- 这是环境/运维问题，不是 vent/valve 回归
- 当前不为 091713 修改 runtime
- A3 后可考虑单实例 PID/lock guard 作为后置工程增强

---

**A3-H2O 准入评审执行单结束。建议进入 A3-H2O 准入评审。**
