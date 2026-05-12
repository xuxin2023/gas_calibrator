# A4 Operator Confirmation Template

**生成时间**: 2026-05-12
**仓库**: `D:/gas_calibrator`
**分支**: `codex/v2-golden-recovery-cdb82111`
**角色**: `engineering_probe_only` / `not_real_acceptance_evidence=true` / `promotion_state=blocked`

---

## 说明

本模板用于 A4 单温 H2O+CO2 no-write simulation probe 未来升级为真实 no-write probe 时的用户确认。当前 A4 profile 是 simulation-only，**任何真实 COM 运行前必须逐项填写并签核**。未填写 = 禁止真机运行。

---

## 1. Exact Config

| 确认项 | 待用户填写 |
|---|---|
| profile 路径 | `____________________` |
| profile commit hash | `____________________` |
| points 文件路径 | `____________________` |
| output 目录 | `____________________` |

---

## 2. Route

| 确认项 | 选项 | 用户勾选 |
|---|---|---|
| 路由模式 | H2O+CO2 group / H2O-only / CO2-only | ⬜ ________ |
| 单温 | 是 / 否 | ⬜ 是 |

---

## 3. Temperature

| 确认项 | 待用户填写 |
|---|---|
| 目标温度 (°C) | `________` |
| 是否单温 | ⬜ 是 ⬜ 否 |

---

## 4. No-Write

| 确认项 | 预期值 | 实测值 |
|---|---|---|
| no_write | true | ________ |
| collect_only | true | ________ |
| no_write_guard_active | true | ________ |
| attempted_write_count | 0 | ________ |
| identity_write_command_sent | false | ________ |
| persistent_write_command_sent | false | ________ |

---

## 5. No Parameter Write

| 确认项 | 必须为 false/阻断 |
|---|---|
| no ID write | ⬜ 确认 |
| no SENCO write | ⬜ 确认 |
| no zero write | ⬜ 确认 |
| no span write | ⬜ 确认 |
| no coefficient write | ⬜ 确认 |
| no calibration parameter write | ⬜ 确认 |

---

## 6. V1 Fallback

| 确认项 | 预期 | 确认 |
|---|---|---|
| run_app.py unchanged | 是 | ⬜ |
| disable_v1 = false | 是 | ⬜ |
| V1 仍可回退 | 是 | ⬜ |

---

## 7. Abort / Safe-Stop

| 确认项 | 预期 | 确认 |
|---|---|---|
| valves restored to baseline | 是 | ⬜ |
| vent = ON after stop | 是 | ⬜ |
| PACE stopped | 是 | ⬜ |
| chamber safe / stopped | 是 | ⬜ |
| humidity generator safe | 是 | ⬜ |

---

## 8. Known Risks

| 风险项 | 已告知 | 用户确认 |
|---|---|---|
| 气路常压点缺口 | 已记录 | ⬜ 已知 |
| P5 fixture debt (114 failures) | 已后置 | ⬜ 已知 |
| A4 profile 当前 simulation-only | 是 | ⬜ 已知 |
| CO2 baseline [1100..500] intact | 是 | ⬜ 已知 |

---

## 9. Explicit Approval

**以下由用户手写/输入，不得由他人代签：**

```
批准码: APPROVE_A4_NO_WRITE_PROBE
签名:   ____________________
日期:   ____________________
备注:   ____________________
```

**未填写批准码 = 禁止真实 COM 运行。**

---

*结束*
