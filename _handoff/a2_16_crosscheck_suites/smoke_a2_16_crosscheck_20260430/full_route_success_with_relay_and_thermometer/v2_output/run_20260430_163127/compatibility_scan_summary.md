# Compatibility Scan Summary

- run_id: run_20260430_163127
- summary: 当前 run 已具备 canonical contract 直读，compatibility sidecar 已就绪。
- reader_mode: canonical_direct
- compatibility_status: compatibility_read
- regenerate_recommended: False
- schema_contract_summary: compatibility bundle step2-artifact-compatibility-v1 | observed 1.0 26 | 1.1 2 | 2.0 2 | 2.2 1 | inferred-unversioned 66 | step2-artifact-compatibility-v1 8 | step2-closeout-bundle-v1 2 | step2-closeout-digest-v1 1 | step2-method-confirmation-wp4-v1 6 | step2-software-validation-wp5-v1 16 | step2-wp6-builder-v1 6
- primary_evidence_rewritten: False

- 读取方式: canonical contract 直读
- 兼容状态: 兼容读取
- 状态计数: 当前 canonical 工件 93 | 兼容读取 43
- 版本识别: explicit 70 | inferred 66
- canonical reader 可用: 136/136
- 建议动作: 当前 compatibility sidecar 已齐备
- 边界提醒: regenerate 目标仅限 reviewer/index sidecar，不改写原始主证据

- boundary: Step 2 收尾 / Step 3 桥接边界 | 仅用于 reviewer readiness / compatibility | 仅限 simulation / offline / headless | 兼容读取与再生成只服务 reviewer/index sidecar | 不改写原始主证据
- non_claim: 仅为 simulation / synthetic reviewer evidence | 不是 real acceptance | 不是 live gate / release gate | 不是 compliance / accreditation claim
