# V1 800 ppm 混气主验证操作测试单

## 本轮目标
- 只验证低压段混气是否已明显减轻或消失。
- 这轮主验证只跑 V1 新 sealed sampling 流程，不跑旧流程对照。
- 这轮主验证不混入 `1100 hPa`，`1100 hPa` 另做后续单独 smoke。

## 本轮固定条件
- 只用一瓶 `800 ppm` 标气。
- 全程 same-gas，不换气源，不换主路由。
- 不控温箱，按室温执行。
- 压力点顺序固定为 `1000 -> 800 -> 600 -> 500 hPa`。
- 连续跑两轮。
- 专用覆盖配置：
  - [v1_800ppm_ingress_smoke.json](/D:/gas_calibrator/configs/overrides/v1_800ppm_ingress_smoke.json)
- 专用点位文件：
  - [points_v1_800ppm_ingress_smoke_20c.xlsx](/D:/gas_calibrator/configs/points_v1_800ppm_ingress_smoke_20c.xlsx)

## 运行前确认
- 温箱不参与本轮验证。
  - 覆盖配置中已显式设为 `temperature_chamber.enabled = false`。
- 湿度发生器不参与本轮验证。
  - 覆盖配置中已显式设为 `humidity_generator.enabled = false`。
- 本轮只跑 `20°C / 800 ppm / 1000,800,600,500 hPa`。
- `adaptive pressure sampling` 保持开启。
- `dewpoint gate / rebound guard / presample long guard / sampling window QC` 保持开启。
- 本轮最短观测窗固定为 `10 s`。
  - 专用覆盖把 `co2_sampling_gate_window_s` 和 `co2_sampling_gate_pressure_fill_s` 都设为 `10.0`。
- `continuous atmosphere hold` 必须关闭。

## 执行方式
- 用同一份覆盖配置连续运行两次。
- 这两次运行都不要手动切回大气，不要插入 `1100 hPa`，不要临时改气源。
- 若使用支持 `base_config` 的工程入口，可直接加载该 override。
- 若使用只支持 `load_config` 的旧入口，先将该 override 合并到 `configs/default_config.json` 后再执行。

## 每点必须记录
- `CO2`
- `dewpoint / H2O`
- `handoff_mode`
- `pressure in limits`
- `OUTP state`
- `ISOL state`
- `capture_hold state`
- `pressure_gate result`
- `dewpoint gate result`
- `reject reason`

## 重点检查项
1. `sampling_begin` 前是否出现以下任一动作：
   - `VENT 1`
   - `atmosphere refresh`
   - `OUTP ON`
   - `route reopen`
2. `800 ppm` 是否仍随降压明显向空气背景方向下拉。
3. `dewpoint / H2O` 是否在 `800 -> 600 -> 500 hPa` 段持续抬升。

## handoff 预期
- 本轮主验证不应触发 `same_gas_superambient_precharge_handoff`。
- 本轮低压段不应重新通大气，不应 reopen 主路。
- 第一轮首个 `1000 hPa` 是建路后的首个 sealed point，可作为建路入点单独看待。
- 第一轮后续低压点以及第二轮压力切点，应重点检查是否维持 `same_gas_pressure_step_handoff`。

## 何时暂停并查看日志
- 出现 `ambient_ingress_suspect`
- 出现 `presample_lock_violation:*`
- 连续出现 `controller_hunting_suspect`
- 连续出现 `dewpoint_conversion_dynamic`
- 连续出现 `adsorption_tail_suspect`
- 任一点 `capture_hold state != pass`
- 任一点 `OUTP state != 0` 或 `ISOL state != 0` 发生在采样前窗口

## 跑完后的离线分析
- 分析脚本：
  - [analyze_v1_800ppm_ingress_smoke.py](/D:/gas_calibrator/scripts/analyze_v1_800ppm_ingress_smoke.py)
- 典型用法：

```powershell
python scripts/analyze_v1_800ppm_ingress_smoke.py `
  --run-dir D:\gas_calibrator\logs\run_round1 `
  --run-dir D:\gas_calibrator\logs\run_round2
```

- 脚本会输出：
  - `pressure_vs_co2_rounds.png`
  - `pressure_vs_dewpoint_h2o_rounds.png`
  - `same_gas_two_round_point_summary.csv`
  - `presample_lock_violations.csv`
  - `reject_reason_summary.csv`
  - `same_gas_two_round_summary.json`

## 结论口径
- 只接受以下三种结论：
  - `混气已基本解决`
  - `混气明显减轻但未完全解决`
  - `混气仍明显存在`
- 本轮 `1000/800/600/500 hPa` 两轮主验证通过后，再单独安排 `1100 hPa` smoke。
