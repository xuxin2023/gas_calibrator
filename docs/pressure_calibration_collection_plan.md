# 压力校准采集方案

## 目标
- 为气体分析仪建立可用于压力校准系数拟合的正式采集方案。
- 优先复用现有 V1 诊断/验证工具，不改 V1 生产主流程。
- 所有真机动作仅定位为工程验证，不构成 real acceptance 证据。

## 为什么上一轮数据不能直接当正式压力标定
- `run_20260406_180137` 的正式样本里，`目标压力hPa` 为空，缺少独立压力扫描点。
- 参考压力只覆盖常压附近约 `33 hPa` 漂移，跨度太窄，不足以支撑正式线性压力标定。
- 当前离线结果里，压力误差与温度相关性明显，说明这轮更像“压力 + 温漂混合效应”，不是干净的纯压力系数。
- 因此，这轮只能用于工程参考，不能直接收成设备正式压力系数。

## 总体策略
分两步做，先筛查，再正式采集：

1. 低风险筛查
   - 用 `validate_pressure_only.py` 做人工准备压力平台的只读采样。
   - 不切气路，不依赖湿度发生器，不写设备。
   - 目标是先确认：外部压力参考、分析仪 `BAR/P`、采样落盘链路三者关系是稳定的。

2. 正式压力扫描
   - 用 `run_room_temp_co2_pressure_diagnostic.py` 做独立房温压力诊断。
   - 目标是拿到带 `pressure_target_hpa` 的 `raw_timeseries.csv`、阶段门禁结果、点均值与压力曲线。
   - 这一步也先只做诊断与分析，不直接写压力系数。

如果正式扫描后仍发现压力残差和温度强相关，再补第三步：

3. 温度解耦复核
   - 在固定温度以外，再补 `0C` 和 `30C` 两档压力扫描。
   - 只有在跨温度残差仍能被统一模型解释时，才考虑收成统一压力系数。

## 推荐采集矩阵

### 阶段 1：低风险筛查
- 温度：固定 `20C`
- 气体：环境空气即可
- 压力点：`ambient, 1100, 800, 500 hPa`
- 重复：`1` 轮
- 目的：
  - 确认每个平台都能同时采到 `gauge_pressure`、`controller_pressure`、分析仪压力字段
  - 确认平台内压力波动、采样跨度、丢帧率在可接受范围内

### 阶段 2：正式压力扫描
- 温度：固定 `20C`
- 气体：
  - 首选 `0 ppm` 和 `1000 ppm`
  - 不建议一开始就上完整 `0/200/400/600/800/1000 ppm`，因为压力标定主目标不是做浓度拟合
  - 如果 `0 ppm` 与 `1000 ppm` 下的压力偏差表现不一致，再升级到完整气点矩阵
- 压力点：
  - 正式点：`1100, 1000, 900, 800, 700, 600, 500 hPa`
  - 每轮都保留 vent/ambient 段，作为平台前后参考
- 重复：`3` 轮
- 推荐变体：
  - 首轮建议先跑 `Variant B`
  - 若 `Variant B` 稳定，再扩到 `A,B,C`
  - 若目标只是压力系数，不必默认展开全部 layer/variant 组合

### 阶段 3：温度解耦复核
- 触发条件：
  - 正式扫描后，压力残差与温度相关仍明显
  - 或同一设备在不同时间段的压力拟合斜率明显漂移
- 温度：`0C / 20C / 30C`
- 气体：仍保持 `0 ppm, 1000 ppm`
- 压力点：继续用 `1100..500 hPa`
- 重复：每个温度至少 `2` 轮

## 稳定与门禁建议

### 直接复用的现有默认口径
- 压力点默认集：
  - screening: `1100, 800, 500 hPa`
  - full: `1100, 1000, 900, 800, 700, 600, 500 hPa`
- 刷气最小时间：`120 s`
- screening 目标刷气时间：`180 s`
- 最大刷气时间：`300 s`
- 压力稳定等待超时：`180 s`
- 稳定窗口：`20 s`
- 采样轮询：`1 s`
- sealed hold：`180 s`

### 推荐采样门禁
- 每个压力平台至少保留 `10` 个稳定样本。
- 平台内压力跨度建议 `<= 0.6 hPa`。
- 如果走 adaptive pressure sampling，建议优先参考现有配置：
  - `co2_sampling_gate_pressure_span_hpa = 0.2`
  - `h2o_sampling_gate_pressure_span_hpa = 0.3`
  - `co2_sampling_gate_min_samples = 6`
  - `h2o_sampling_gate_min_samples = 8`
- 分析仪压力偏差门限建议沿用现有诊断阈值：
  - `|bias| <= 5 hPa` 视为 pass
  - `5 < |bias| <= 10 hPa` 视为 warn
  - `|bias| > 10 hPa` 视为 fail
- 压力斜率偏差门限建议沿用：
  - `|slope_bias| <= 0.02` 视为 pass
  - `0.02 < |slope_bias| <= 0.05` 视为 warn
  - `|slope_bias| > 0.05` 视为 fail

### 额外建议的工程门禁
- 单个平台内温度波动尽量控制在 `0.2C` 内。
- 正式拟合前，先看压力残差与温度的相关性：
  - 若 `|corr(residual, temp)| > 0.3`，不要直接收成纯压力系数
  - 先补温度解耦复核
- 每台分析仪必须同时满足：
  - 参考压力字段存在
  - 分析仪压力字段存在
  - 平台样本数够
  - 无系统性 stale/fallback 主导

## 推荐执行顺序

### 1. 先做低风险筛查
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.tools.validate_pressure_only `
  --config "D:\gas_calibrator\configs\default_config.json" `
  --pressure-points "ambient,1100,800,500" `
  --count 15 `
  --interval-s 1.0
```

说明：
- 这个工具只采样，不控制压力源。
- 需要人工把压力平台准备到位后再继续采样。
- 适合先确认 `Pace/压力计/分析仪 BAR` 三者能否稳定同采。

### 2. 再做正式房温压力诊断
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.tools.run_room_temp_co2_pressure_diagnostic `
  --config "D:\gas_calibrator\configs\default_config.json" `
  --allow-live-hardware `
  --configure-analyzer-stream `
  --variants "B" `
  --layers "1,2,3" `
  --repeats 3 `
  --gas-points "0,1000" `
  --pressure-points "1100,1000,900,800,700,600,500"
```

说明：
- 这一步是正式压力诊断入口，但仍然是“诊断/验证”，不是直接下发压力系数。
- 若 `Variant B` 稳定，再考虑扩到 `A,B,C`。

### 3. 跑完后导出单气体压力曲线
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.tools.export_single_gas_pressure_curve `
  --run-dir "D:\gas_calibrator\logs\<pressure_run_dir>" `
  --variant "B" `
  --gas-ppm 0 `
  --repeat-index 1
```

产物重点看：
- `pressure_curve_point_means.csv`
- `pressure_curve_summary.json`
- 曲线图

## 拟合与收口建议
- 正式压力系数拟合优先用“平台点均值”，不要直接把全量时序点混进拟合。
- 第一版模型建议从最简单的线性关系开始：
  - `P_ref_hPa = offset + scale * P_analyzer_hPa`
- 只有在以下条件同时满足时，才建议把结果推进到设备写入候选：
  - 压力跨度覆盖 `500..1100 hPa`
  - 每台设备每个压力点都有足够稳定样本
  - `bias` 和 `slope_bias` 落在 pass/warn 可接受区间
  - 残差与温度无明显耦合
  - 多次重复的 `offset/scale` 漂移足够小

## 当前阶段的推荐收口
- 先不要复用普通校准 run 的正式样本来硬算压力系数。
- 先按上面的两阶段方案补出真正的压力扫描数据。
- 等正式压力扫描通过后，再单独出一版：
  - `pressure_fit_summary.csv`
  - `pressure_download_plan.csv`
  - `pressure_calibration_report.xlsx`

## 对本项目边界的说明
- 本方案只使用现有 V1 工具做最小范围工程验证，不改 V1 生产逻辑。
- 不涉及 V2 真机测试。
- 所有真机结果都只能视为工程验证，不可表述为 real acceptance。
