# V1.5 协议身份权威证据签名门禁

## 目标与边界

V1.5 的协议 ID 受控写入只能接受由外部资产系统签发、且能被部署信任根验证的全局唯一性证据。该门禁只保护协议身份写入，不运行校准、不控制气路、不写 SENCO，也不构成真实验收证据。

证据规范化和离线审查只检查字段、范围、资产记录与候选值语义，不产生写入授权。真正进入受控写入预检时，还必须通过本页定义的签名验证。

## 信任边界

- 生产签名私钥只属于外部资产系统，禁止进入本仓库、校准软件或测试工件；自动化测试只使用无法被部署信任库接受的合成测试密钥。
- 校准软件只读取部署管理员维护的 Ed25519 公钥信任库。
- 部署程序必须把信任库目录 ACL 限制为管理员可写、普通操作员只读。
- 生产命令行不提供信任库覆盖参数，避免操作员选择任意公钥绕过门禁。
- Windows 固定路径：`C:\ProgramData\GasCalibrator\trust\v1_5_identity_authorities.json`
- Linux 固定路径：`/etc/gas_calibrator/trust/v1_5_identity_authorities.json`
- `tests/fixtures` 下的信任库永远不能作为部署信任根。

## 外部资产系统签名步骤

1. 生成完整的只读权威导出，但暂不加入 `authority_signature`。
2. 以 UTF-8、键排序、无多余空白的 JSON 形式规范化顶层证据，并计算 SHA-256。
3. 构造签名元数据：schema、algorithm、key_id、signed_at、payload_sha256。
4. 用资产系统中的 Ed25519 私钥对规范化后的签名元数据签名。
5. 将 Base64 签名和元数据写入顶层 `authority_signature`。

签名时间必须使用带时区的 ISO 8601，且不得早于资产导出时间。

## 信任库最小字段

信任库顶层必须包含：

- `schema_version = v1_5_identity_authority_trust_store_v1`
- `deployment_managed = true`
- `test_fixture_only = false`
- `keys`：一个或多个具有唯一 `key_id` 的密钥记录

每个密钥记录必须包含 Ed25519 原始公钥、active 状态、允许的来源类型和来源系统、有效期以及不超过 86400 秒的签名最大年龄。

## 失败关闭条件

以下任一情况都会在创建分析仪/打开 COM 之前阻断：信任库缺失或无效、测试夹具路径、未知或停用密钥、来源未授权、证据摘要不一致、签名无效、签名过期或来自未来、密钥有效期不匹配、签名早于导出。

离线语义审查结果即使为 ready，也不能直接交给执行函数；执行函数会再次要求 `trusted_authority_signature.status = verified`。
