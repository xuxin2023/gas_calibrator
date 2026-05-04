# Reindex Manifest

- run_id: run_20260430_150846
- schema_contract_summary: compatibility bundle step2-artifact-compatibility-v1 | observed 1.0 26 | 1.1 2 | 2.0 2 | 2.2 1 | inferred-unversioned 66 | step2-artifact-compatibility-v1 8 | step2-closeout-bundle-v1 2 | step2-closeout-digest-v1 1 | step2-method-confirmation-wp4-v1 6 | step2-software-validation-wp5-v1 16 | step2-wp6-builder-v1 6
- regenerate_scope: reviewer_index_sidecar_only
- primary_evidence_preserved: True
- primary_evidence_rewritten: False

- 读取方式: canonical contract 直读
- 兼容状态: 兼容读取
- 作用范围: 仅重建 reviewer/index sidecar
- 保护边界: 不改写 summary / manifest / results 等原始主证据
- 建议: 当前 sidecar 已齐备，仅在索引变化时再生成

- boundary: Step 2 收尾 / Step 3 桥接边界 | 仅用于 reviewer readiness / compatibility | 仅限 simulation / offline / headless | 兼容读取与再生成只服务 reviewer/index sidecar | 不改写原始主证据
- non_claim: 仅为 simulation / synthetic reviewer evidence | 不是 real acceptance | 不是 live gate / release gate | 不是 compliance / accreditation claim
