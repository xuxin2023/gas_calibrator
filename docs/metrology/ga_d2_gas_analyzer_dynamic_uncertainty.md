# GA-D2 气体分析仪动态性能与工程不确定度

## 1. 为什么纯气体分析仪仍可能需要动态评价

若分析仪只用于缓慢、稳态的零点和跨度校准，首要指标仍是零点、跨度、线性、重复性、漂移、压力/温度影响以及水汽交叉敏感。此时不需要涡动相关的协谱修正或通量闭合。

当分析仪用于快速过程监测、泄漏响应、呼吸箱、通量箱或其他秒级变化场景时，仅有静态浓度准确度还不够。分析仪和采样管路会引入：

- 高频幅值衰减；
- 频率相关相位滞后；
- H2O 吸附/解吸形成的慢记忆；
- 参考仪器噪声、时钟和谱估计带来的动态判定不确定度。

GA-D2 只评价这些**气体分析仪动态性能**。它不计算 EC 协谱，不做通量闭合。

## 2. 与 EC-D1 的关系

EC-D1 提供上游参考到 DUT 的经验传递函数、相干性和真值回归。GA-D2 将这些结果转换为分析仪工程指标：

- 5% 衰减带宽；
- 10% 衰减带宽，作为默认可用带宽；
- −3 dB 带宽；
- 低频等效相位延迟；
- 每个频点的动态幅值偏差；
- 幅值、相位和带宽的离线工程不确定度。

命令信号仍只用于气源链诊断。正式输入必须是与 DUT 共时钟的上游参考。

## 3. 为什么动态衰减不能混进不确定度

幅值从 1.0 衰减到 0.9 是可重复的系统响应偏差，不是随机不确定度。如果把这 10% 衰减并入一个较大的“总不确定度”，会掩盖分析仪已经不能保真跟随输入的事实。

因此 GA-D2 分开报告：

1. `dynamic_amplitude_bias_relative`：系统性动态偏差；
2. `expanded_amplitude_relative_uncertainty`：对该动态判定的工程不确定度。

当前阶段不输出反卷积或频响补偿系数，也不自动修正任何生产数据。

## 4. 带宽判定

评估网格采用 20 Hz 采样率、512 点 Welch 分段，对 0.0390625–1.9921875 Hz 的 51 个正频率格点计算经验传递函数。

带宽阈值为：

- 5% 衰减：`|H(f)| = 0.95`
- 10% 衰减：`|H(f)| = 0.90`
- −3 dB：`|H(f)| = 1/sqrt(2)`

阈值交点在相邻 DFT 格点间线性插值。带宽不确定度同时包含局部幅值不确定度映射和频率格点分辨率。

clean fixture 表明，5% 带宽处曲线较平，其扩展相对不确定度可能接近或超过带宽本身。因此 5% 带宽保留为诊断量；只有扩展相对不确定度不超过合同限值的 10% 和 −3 dB 带宽可标记为 `qualified`。

## 5. 工程不确定度预算

每个频点的标准不确定度包含：

- Welch 相干性对应的随机分量；
- 上游参考幅值分量；
- 上游参考相位分量；
- 共享时钟定时分量；
- 谱泄漏分量。

分量按平方和开根号合成，默认覆盖因子 `k = 2`。50% 重叠 Welch 分段使用保守的有效独立平均数。

这些数值用于 simulation fixture 回归和工程诊断，不具备真实参考仪器证书、时钟溯源、重复台架试验和自由度评定，因此不是正式计量不确定度声明。

## 6. 受控门禁

clean CO2/H2O fixture 必须同时满足：

- 上游参考存在，且与 DUT 共用明确时钟域；
- 物理路径元数据完整；
- 预热后至少覆盖一个完整 511-chip PRBS 周期；
- 51 个频点全部可计算且相干性通过；
- 10% 与 −3 dB 带宽达到合成 fixture 限值；
- 带宽真值误差和扩展相对不确定度通过；
- 幅值、相位工程不确定度通过；
- 不输出动态修正系数；
- EC 通量范围明确为 `not_in_scope`。

高噪声、缺时钟、缺物理元数据、PRBS 周期不足或严重 H2O 记忆必须失败。

## 7. 证据边界

所有 GA-D2 工件固定标记：

- `evidence_source = simulated`
- `not_real_acceptance_evidence = true`
- `promotion_state = blocked`
- `ec_flux_status = not_in_scope`
- `dynamic_correction_status = not_applied`
- `real_acceptance_status = blocked`

GA-D2 不修改 V1、不修改 `run_app.py`、不连接真实 COM、不写系数、不刷新 `real_primary_latest`。

## 8. 验证命令

```powershell
$env:PYTHONPATH = "src"
python -m pytest -q tests/v2/test_gas_analyzer_dynamic_uncertainty.py
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite regression
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite nightly
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite parity
```

真实分析仪若未来要使用这些指标，需要独立的台架协议、可溯源上游参考、共享时钟验证和多轮重复试验；在这些条件具备前，不能把 GA-D2 的仿真限值当作产品规格。
