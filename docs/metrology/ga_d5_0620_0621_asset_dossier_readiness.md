# GA-D5：0620/0621 计量资产资料包准备度

## 目标

GA-D5 把 0620、0621 已存在的历史数据转换成可重复的只读治理检查。它回答三个不同问题：

1. 历史事实是否被完整、无漂移地重放；
2. 形成这些数据所依赖的参考资产证书资料是否完整；
3. 当前环境是否已经具备未来受控工程探针或真实验收的前置条件。

三者不能相互替代。历史数据完整不代表计量溯源完整；计量资料完整也不代表已经获得真实执行授权或通过真实验收。

## 0620/0621 锁定事实

离线 fixture 固定保存以下已核验事实：

- 0620 CO2 成熟路线共有 45 个预期点，45 个被接受，2 个带警告，缺失或拒绝为 0。
- 0621 CO2 完成的入口点只有 40 °C 下的 0 ppm 和 400 ppm；另有 3 次未形成完整结论的零点尝试。
- 历史 H2O 资料覆盖 6 台分析仪，其中 3 台在当时的闭环评审中被阻断。
- 从旧证书值使用记录恢复了 10 个非零 CO2 实际值，历史拟合使用核对为 228/228 一致。
- 上述恢复记录关联的原始标气证书文档数量仍为 0，不能把恢复值当作证书文件。
- 当前正式授权的需求资产数量为 0；环境机器证据为 Q0，目标为 Q4；实际 R0 结果导入数量为 0。

每个来源工件均以固定角色和 SHA-256 摘要进入 contract。fixture 或 contract 只要修改成熟方法、历史数值、来源摘要或 no-write 边界，加载即失败。

## 科学与物理口径

### CO2 标准系列与零气

CO2 标准系列要求 100–1000 ppm 的 10 个独立资料条目，并另有一个 CO2 零气条目。每个条目必须包含钢瓶身份、证书身份、实际值、基体/平衡气、制备方法、有效期、溯源链和不确定度。

恢复出来的 99.91、199.8 等实际值证明历史拟合曾使用这些数值，但没有同时恢复钢瓶序列号、证书编号、证书文件、有效期和扩展不确定度，因此只能作为历史数值来源，不能升级为证书证据。

### H2O 干气与露点

H2O 低端参考不是 CO2 零气。H2O 必须使用实际露点读回和实际压力形成水汽参考；湿度发生器的设定值或状态只能作为辅助状态，不能替代露点参考。资料包因此单独要求 `h2o_dewpoint_reference` 和 `digital_pressure_reference`。

### 温度、流量与时间基准

温度、流量和时间基准会分别影响温度补偿、传输/停留时间和采样频率。GA-D5 将它们作为独立计量资产，而不是“设备有读数即合格”。

### 相关性与协方差

同一物理参考资产若用于多个量或多个门禁，误差并不自动独立。资料包必须声明独立物理资产，或给出共同 `correlation_group_id` 并明确已在不确定度中包含协方差，避免重复低估总不确定度。

## 固定方法边界

- 生产默认仍为成熟 V1.5 legacy ratio 路线。
- 吸收比拟合仅为 shadow review。
- 保持 pressure-first `SENCO9` 流程。
- 正式采样口径固定为 1 Hz、`AVERAGE1=49`、`AVERAGE2=49`。
- 不切换默认入口，不修改 V1，不连接真实 COM。
- 不执行拟合、系数写回、数据库写入或 `real_primary_latest` 刷新。

## 工件与状态

GA-D5 生成：

- `gas_analyzer_asset_dossier_replay_inputs.json`：`execution_summary`，记录只读回放输入；
- `gas_analyzer_asset_dossier_report.json`：`diagnostic_analysis`，记录门禁和阻断原因；
- `gas_analyzer_asset_dossier_report.md`：中文评审摘要。

当前 0620/0621 fixture 的正确状态是 `EXPECTED_GAPS`：

- `historical_baseline_consistent=true`
- `asset_documentary_ready=false`
- `current_prerequisites_ready=false`
- `ready_for_real_execution=false`
- `execution_authorization_status=not_requested`
- `real_acceptance_status=blocked`

`EXPECTED_GAPS` 是治理用的预期通过状态，表示系统正确识别并保留了资料缺口，不表示资产合格，更不是真实验收证据。

## 验证

```powershell
$env:PYTHONPATH = "src"
python -m pytest tests/v2/test_gas_analyzer_asset_dossier.py -q
python -m pytest tests/v2/sim/test_suites.py -q
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite regression
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite nightly
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite parity
```
