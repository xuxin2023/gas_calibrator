# A3 准入评审报告：从 D29-R4 + D29-R5 出发

## 评审日期
2026-05-12

## 评审来源
- D29-R4: `5bc4fa2c1602123426258a27756283bdd50973d0` (PASS)
- D29-R5: `034b2d6bf3321ebc40b078b6c8bd9f97eb6ab2ef` (PASS, primary run 085322)

---

## 1. 是否建议进入 A3 准入评审？

**YES** ✅

## 2. 是否建议停止 D3/D29 runtime 补丁？

**YES** ✅

Vent/valve 顺序已在 R4 修复、R5 复现。继续在现有架构上叠补丁只会增加技术债务。

## 3. R4 + R5 是否构成 H2O ambient→sealed repeatability engineering evidence？

**YES** ✅

| Run | Result | Key evidence |
|---|---|---|
| D29-R4 (5bc4fa2c) | PASS | vent/valve order correct, 7 pts, no-write |
| D29-R5 (034b2d6b, 085322) | PASS | vent/valve order reproduced, 7 pts, no-write, no-flow |

## 4. A3 准入依据

| Evidence | R4 | R5 |
|---|---|---|
| vent=OFF before set_h2o_path(False) | YES | YES |
| vent_closed_verified before set_h2o_path(False) | YES | YES |
| 1.5s wait completed before h2o_path close | YES (1.501s) | YES (1.501s) |
| sealed phase vent=ON = 0 | YES | YES |
| sealed phase keepalive = 0 | YES | YES |
| attempted_write_count = 0 | YES | YES |
| all 7 pressure points completed | YES | YES |
| no-flow confirmed | N/A | YES |

## 5. A3 前必须保留的边界

| 边界 | 说明 |
|---|---|
| 不写参数 | No-write mode must remain |
| 不替代 V1 | V1 fallback must remain |
| 不默认切 V2 | `run_app.py` must not change |
| 不改 CO2 主链 | CO2 golden path protected |
| 不做 controlled write | Even A3 must stay no-write |

## 6. A3 前剩余 P0

| # | Item | Status |
|---|---|---|
| 1 | CO2 golden path 保护 | Must continue |
| 2 | high-risk stash 不恢复 | stash@{0} still isolated, verified |
| 3 | 不做 controlled write | Must stay no-write |
| 4 | 不做 formal switch | Must not change default entry |
| 5 | push 需用户确认 | Not yet pushed |

## 7. 明确不允许

- ❌ real acceptance
- ❌ production acceptance
- ❌ V2 replacement
- ❌ formal switch
- ❌ controlled write
- ❌ `real_primary_latest` refresh
- ❌ 关闭 V1 fallback

## 8. Appendix: 091713 duplicate-start note

`run_20260512_091713` was a duplicate-start fail-closed caused by COM port contention with the primary run (085322). It is not physical-process evidence and does not affect the R4+R5 repeatability conclusion.

## 9. 结论

| Question | Answer |
|---|---|
| Enter A3 review? | **YES** |
| Stop D3/D29 patches? | **YES** |
| R4+R5 = valid H2O repeatability evidence? | **YES** |
| Push current commits? | **NO** (wait for user confirmation) |
| Continue runtime patches? | **NO** |
