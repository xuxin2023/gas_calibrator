# 质控复核摘要

运行 run_20260430_233102 质控评分 0.60 / 等级 D；通过 0，预警 2，拒绝 0，跳过 0；门禁 warn。

## 运行门禁
- 状态: warn
- 说明: review_required

## 点级门禁
- 状态: warn
- 关注路由: h2o
- 非通过点数: 2

## 气路分布
- h2o: pass=0, warn=2, reject=0, skipped=0

## 拒绝原因分类
- time_not_continuous (timing): 2

## 失败检查分类
- time_continuity (timing): 2

## 建议
- Review invalid points before fitting.
- Improve sampling stability and reduce outliers.

## 审阅结论
- 运行 run_20260430_233102 质控评分 0.60 / 等级 D；通过 0，预警 2，拒绝 0，跳过 0；门禁 warn。
- 点级门禁: warn | 关注路由: h2o

## 门禁状态
- 运行门禁: warn | 原因: review_required
- 点级门禁: warn | 关注路由: h2o | 非通过点数: 2
- 结果分级: 通过 0 / 预警 2 / 拒绝 0 / 跳过 0

## 风险归类
- 主要拒绝原因: time_not_continuous | 失败检查: time_continuity
- 路由分布 h2o: 通过 0 / 预警 2 / 拒绝 0 / 跳过 0
- 拒绝原因分类: time_not_continuous(2) | 失败检查分类: time_continuity(2)

## 证据边界
- evidence_source: simulated_protocol
- 仅限 simulation/offline 复核，不代表 real acceptance evidence
