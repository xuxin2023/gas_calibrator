# V2 文档索引

本目录只描述 V2 在 `Step 2: production-grade platformization` 阶段已经落地或正在守稳的能力，不把未来 cutover、真实 acceptance、真实设备联调写成当前默认能力。

建议阅读顺序：

1. [软件架构说明](software_architecture.md)
2. [运行与验证指南](runtime_and_validation.md)
3. [工件与证据治理](artifact_governance.md)
4. [Step 2 V1/V2 同步矩阵](step2_v1_sync_matrix.md)

按角色阅读：

- 开发者：先读 [软件架构说明](software_architecture.md)，再读 [运行与验证指南](runtime_and_validation.md)。
- 测试 / 评审：优先读 [运行与验证指南](runtime_and_validation.md) 与 [工件与证据治理](artifact_governance.md)。
- 需求整理 / 范围治理：结合 [软件架构说明](software_architecture.md) 与 [Step 2 V1/V2 同步矩阵](step2_v1_sync_matrix.md)。

本目录覆盖的主题：

- V2 标准入口、分层架构与模块职责
- simulation / replay / suite / parity / resilience 的推荐用法
- 中文默认 UI、simulation-only workbench 与 review/report 流程
- 输出工件、证据边界、artifact role / export status 合同

本目录不覆盖的主题：

- V1 生产逻辑说明
- `run_app.py` 入口切换
- 真实串口 / COM 操作手册
- real compare / real verify / real acceptance 放行流程
