# EC-D1 离线系统辨识合同

## 1. 当前目标

EC-D1 在 EC-D0 阶跃动态计量基础上，增加一个受控、可回归的经验系统辨识切片：

- 用 9 阶最大长度 PRBS 激励模拟气源；
- 同时保留命令量、上游参考和被测分析仪（DUT）响应；
- 以**上游参考**而不是命令量作为 DUT 传递函数输入；
- 用 Welch H1 估计经验传递函数；
- 同时输出幅值、相位、幅平方相干性及 Welch 分段离散区间；
- 用已知数字模型真值验证估计器回归误差。

这仍然只是 simulation-only 的离线回归合同，不是真实分析仪频响验收，也不是 EC 通量闭合。

## 2. 为什么必须有上游参考

气源阀、混合容积、管路以及参考传感器本身都会形成动态链。若直接用命令方波或 PRBS 命令作为 DUT 输入，气源的衰减和相移会被错误归入分析仪。

EC-D1 因而并行计算两条传递关系：

1. `upstream_reference -> DUT`：作为门禁和真值比较的主结果；
2. `command -> DUT`：只作诊断，用来证明气源动态已与 DUT 动态分离。

合同要求气源动态在目标频带内可观测；如果上游参考缺失，分析直接无效，不能退化为命令量输入后继续通过。

## 3. 激励和频点

默认采样率为 20 Hz，PRBS chip rate 为 2 Hz，持续 280 s。9 阶 m 序列的完整周期为 511 chips；扣除 10 s 预热后仍有约 540 chips，因此合同硬性要求至少覆盖一个完整周期。Welch 分段长度为 512 点、50% 重叠。门禁频点与 512 点 DFT 的频率格点严格对齐：

- 0.234375 Hz
- 0.4296875 Hz
- 0.703125 Hz
- 1.328125 Hz

选择这些频点的目的，是覆盖低频到约 1.3 Hz 的动态衰减与相移，同时避免把 PRBS 单段激励能量很弱的谱线用于回归门禁。它们是仿真回归点，不是产品频响规格。

## 4. 估计量

对去均值并加 Hann 窗的分段信号，计算：

`H1(f) = S_yx(f) / S_xx(f)`

其中 `x` 为上游参考，`y` 为 DUT。幅平方相干性为：

`gamma2(f) = |S_yx(f)|^2 / (S_xx(f) S_yy(f))`

合成模型同时给出参考链和 DUT 链的离散传递函数真值。门禁比较估计幅值、相位与该真值的误差。

报告中的 `amplitude_ci95_db` 与 `phase_ci95_deg` 是各 Welch 分段复传递比的 2.5%–97.5% 分位范围。它用于暴露激励不足、噪声或不稳定性，**不是**带计量溯源、自由度修正或覆盖因子的正式置信区间。

## 5. 回归门禁

清洁 CO2/H2O fixture 必须同时满足：

- 上游参考存在且确实作为输入；
- 上游参考与 DUT 明确处于同一个模拟采样时钟域；
- 管路、流量、压力、温湿度、过滤器及时间戳来源等物理路径元数据完整；
- 命令链与上游参考链已分离；
- 4 个目标频点全部可计算；
- 扣除预热后至少覆盖一个完整的 511-chip PRBS 周期；
- 时间戳抖动不超过合同限值；
- 每个频点相干性、幅值真值误差、相位真值误差和分段离散宽度均通过；
- 高噪声、低相干、缺少上游参考或证据边界被解锁时必须失败。

EC-D1 接入 `regression` 与 `nightly`，暂不加入最小 `smoke`，避免把较重的频谱回归放入快速烟测。

## 6. 证据边界

所有 EC-D1 工件固定标记：

- `evidence_source = simulated`
- `not_real_acceptance_evidence = true`
- `acceptance_level = offline_regression`
- `promotion_state = blocked`
- `real_acceptance_status = blocked`

合同禁止设备 I/O、系数写入和 `real_primary_latest` 刷新。EC-D1 不修改 V1、不修改 `run_app.py`、不连接 COM。

## 7. 验证命令

```powershell
$env:PYTHONPATH = "src"
pytest -q tests/v2/test_ec_system_identification.py
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite regression
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite nightly
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite parity
```

后续 EC-D2 才讨论频响修正不确定度、协谱损失与通量闭合；在真实 bench protocol、同步时钟和独立参考链具备前，不能把本合同外推为真实 EC 验收。
