# V1 稳态判定算法文档

## 文档信息

- **文档名称**: V1 稳态判定算法文档
- **适用范围**: V1 生产基线
- **版本**: v1.0
- **创建日期**: 2026-04-15
- **最后更新**: 2026-04-15
- **对应代码版本**: HEAD 截止 2026-04-15
- **维护人**: Codex

---

## 目录

1. [算法总览](#1-算法总览)
2. [通用稳态（StabilityWindow）](#2-通用稳态stabilitywindow)
3. [封压后露点门禁](#3-封压后露点门禁)
4. [自适应压力采样门禁](#4-自适应压力采样门禁)
5. [气路放行露点门禁](#5-气路放行露点门禁)
6. [CO2冷态质量门禁](#6-co2冷态质量门禁)
7. [参数配置映射汇总](#7-参数配置映射汇总)

---

## 1. 算法总览

| 算法 | 适用场景 | 核心判定条件 | 代码定位 |
|------|---------|-------------|---------|
| StabilityWindow | 通用稳态检测 | 滑动窗口峰峰值 ≤ 容差 | runner.py:42-60 |
| 封压后露点门禁 | CO2/H2O封压后采样前 | 窗口span + slope双条件 | runner.py:17124起 |
| 自适应压力采样门禁 | 采样前压力稳定性 | 压力span + 填充时间 | runner.py:12281起 |
| 气路放行露点门禁 | CO2/H2O气路放行 | 尾窗span + slope + 反弹 | dewpoint_flush_gate.py:342-411 |
| CO2冷态质量门禁 | 低温CO2分析仪温度 | 温度偏差+温差+硬坏值 | runner.py:8830-8890 |

---

## 2. 通用稳态（StabilityWindow）

**代码定位**: runner.py:42-60

### 2.1 算法描述

滑动时间窗口 + 峰峰值容差检测器。

### 2.2 判定条件

```
is_stable = (样本数 ≥ 2) AND (max(window) - min(window) ≤ tol)
```

- 窗口内保留最近 `window_s` 秒的 (timestamp, value) 对
- 每次添加新值时淘汰窗口外的旧值

### 2.3 参数

| 参数 | 说明 | 典型用途 |
|------|------|---------|
| `tol` | 峰峰值容差阈值 | 各场景不同 |
| `window_s` | 滑动窗口时长（秒） | 各场景不同 |

---

## 3. 封压后露点门禁

**代码定位**: runner.py:17124起（判定逻辑）、runner.py:3549-3680（参数配置）

### 3.1 算法描述

压力控制器封压后、采样前的露点稳定性门禁。使用滑动窗口内的 span（峰峰值）和 slope（端点斜率）双条件判定，并可选反弹检测和物理一致性 QC。

### 3.2 判定条件

```
passed = (live_dewpoint_c ≠ None)
       AND (count ≥ min_samples)
       AND (span_c ≤ span_threshold)
       AND (|slope_c_per_s| ≤ slope_threshold)
```

其中：
- `span_c` = 窗口内露点 max - min
- `slope_c_per_s` = (last_value - first_value) / duration_s

### 3.3 CO2 封压后露点门禁参数

| 参数 | 配置路径 | 默认值 | config.py行号 |
|------|---------|--------|-------------|
| window_s | `workflow.pressure.co2_postseal_dewpoint_window_s` | 4.0 | 171 |
| timeout_s | `workflow.pressure.co2_postseal_dewpoint_timeout_s` | 6.0 | 172 |
| span_c | `workflow.pressure.co2_postseal_dewpoint_span_c` | 0.12 | 173 |
| slope_c_per_s | `workflow.pressure.co2_postseal_dewpoint_slope_c_per_s` | 0.04 | 174 |
| min_samples | `workflow.pressure.co2_postseal_dewpoint_min_samples` | 6 | 175 |

### 3.4 H2O 封压后露点门禁参数

| 参数 | 配置路径 | 默认值 | config.py行号 |
|------|---------|--------|-------------|
| window_s | `workflow.pressure.h2o_postseal_dewpoint_window_s` | 2.5 | 206 |
| timeout_s | `workflow.pressure.h2o_postseal_dewpoint_timeout_s` | 5.5 | 207 |
| span_c | `workflow.pressure.h2o_postseal_dewpoint_span_c` | 0.18 | 208 |
| slope_c_per_s | `workflow.pressure.h2o_postseal_dewpoint_slope_c_per_s` | 0.06 | 209 |
| min_samples | `workflow.pressure.h2o_postseal_dewpoint_min_samples` | 4 | 210 |

### 3.5 CO2/H2O 参数对比

| 参数 | CO2 | H2O | 说明 |
|------|-----|-----|------|
| window_s | 4.0s | 2.5s | H2O窗口更短 |
| timeout_s | 6.0s | 5.5s | H2O超时更短 |
| span_c | 0.12°C | 0.18°C | H2O容差更宽松 |
| slope_c_per_s | 0.04 | 0.06 | H2O容差更宽松 |
| min_samples | 6 | 4 | H2O要求更少样本 |

### 3.6 附加守护（仅CO2低压点位）

| 守护 | 配置路径 | 默认值 | config.py行号 |
|------|---------|--------|-------------|
| 反弹守护开关 | `workflow.pressure.co2_postseal_rebound_guard_enabled` | True | 176 |
| 反弹窗口 | `workflow.pressure.co2_postseal_rebound_window_s` | 8.0 | 177 |
| 反弹最小回升 | `workflow.pressure.co2_postseal_rebound_min_rise_c` | 0.12 | 178 |
| 物理QC开关 | `workflow.pressure.co2_postseal_physical_qc_enabled` | True | 186 |
| 物理QC最大偏差 | `workflow.pressure.co2_postseal_physical_qc_max_abs_delta_c` | 1.0 | 187 |

---

## 4. 自适应压力采样门禁

**代码定位**: runner.py:12281起（参数配置）、runner.py:13630起（判定逻辑）

### 4.1 算法描述

采样前的压力稳定性门禁。等待压力填充时间后，检查窗口内压力 span 是否满足阈值。

### 4.2 判定条件

```
passed = (elapsed_s ≥ fill_s)
       AND (pressure_count ≥ min_samples)
       AND (pressure_span_hpa ≤ pressure_span_limit_hpa)
```

### 4.3 CO2 参数

| 参数 | 配置路径 | 默认值 | config.py行号 |
|------|---------|--------|-------------|
| window_s | `workflow.pressure.co2_sampling_gate_window_s` | 8.0 | 163 |
| pressure_span_hpa | `workflow.pressure.co2_sampling_gate_pressure_span_hpa` | 0.20 | 165 |
| pressure_fill_s | `workflow.pressure.co2_sampling_gate_pressure_fill_s` | 5.0 | 167 |
| min_samples | `workflow.pressure.co2_sampling_gate_min_samples` | 6 | 169 |

### 4.4 H2O 参数

| 参数 | 配置路径 | 默认值 | config.py行号 |
|------|---------|--------|-------------|
| window_s | `workflow.pressure.h2o_sampling_gate_window_s` | 12.0 | 164 |
| pressure_span_hpa | `workflow.pressure.h2o_sampling_gate_pressure_span_hpa` | 0.30 | 166 |
| pressure_fill_s | `workflow.pressure.h2o_sampling_gate_pressure_fill_s` | 8.0 | 168 |
| min_samples | `workflow.pressure.h2o_sampling_gate_min_samples` | 8 | 170 |

---

## 5. 气路放行露点门禁

**代码定位**: dewpoint_flush_gate.py:342-411（核心判定）、runner.py:15632-15666（参数配置）

### 5.1 算法描述

CO2/H2O 气路放行前的露点稳定性门禁。检查冲洗时长、尾窗露点斜率和跨度，并检测反弹。

### 5.2 判定条件

```
gate_pass = NOT (
    flush_duration_below_min
    OR dewpoint_tail_slope_too_large
    OR dewpoint_tail_span_too_large
    OR dewpoint_rebound_detected
)
```

子条件：
- `flush_duration_below_min`: 冲洗时长 < `min_flush_s`
- `dewpoint_tail_slope_too_large`: 尾窗 |slope| > `tail_slope_abs_max_c_per_s`
- `dewpoint_tail_span_too_large`: 尾窗 span > `tail_span_max_c`
- `dewpoint_rebound_detected`: 反弹检测（窗口内露点回升 ≥ `rebound_min_rise_c`）

### 5.3 参数

| 参数 | 配置路径 | 默认值 | config.py行号 |
|------|---------|--------|-------------|
| enabled | `workflow.stability.gas_route_dewpoint_gate_enabled` | True | 239 |
| policy | `workflow.stability.gas_route_dewpoint_gate_policy` | "warn" | 241 |
| window_s | `workflow.stability.gas_route_dewpoint_gate_window_s` | 60.0 | 243 |
| max_total_wait_s | `workflow.stability.gas_route_dewpoint_gate_max_total_wait_s` | 1080.0 | 245 |
| poll_s | `workflow.stability.gas_route_dewpoint_gate_poll_s` | 2.0 | 247 |
| tail_span_max_c | `workflow.stability.gas_route_dewpoint_gate_tail_span_max_c` | 0.45 | 249 |
| tail_slope_abs_max_c_per_s | `workflow.stability.gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s` | 0.005 | 251 |
| rebound_window_s | `workflow.stability.gas_route_dewpoint_gate_rebound_window_s` | 180.0 | 253 |
| rebound_min_rise_c | `workflow.stability.gas_route_dewpoint_gate_rebound_min_rise_c` | 1.3 | 255 |
| log_interval_s | `workflow.stability.gas_route_dewpoint_gate_log_interval_s` | 15.0 | 257 |

---

## 6. CO2冷态质量门禁

**代码定位**: runner.py:8830-8890

### 6.1 算法描述

低温 CO2 点位（温箱温度 ≤ 0°C）的分析仪温度质量检查。检测分析仪温度是否在合理范围内。

### 6.2 检查项

| 检查项 | 条件 | 说明 |
|--------|------|------|
| 温度范围 | raw_temp ∈ [temp_min_c, temp_max_c] | 分析仪温度在物理合理范围内 |
| 参考偏差 | |analyzer_temp - ref_temp| ≤ max_abs_delta_from_ref_c | 与参考温度偏差不超过阈值 |
| Cell-Shell温差 | |cell_temp - shell_temp| ≤ max_cell_shell_gap_c | 腔体与外壳温差不超过阈值 |
| 硬坏值 | |analyzer_temp - bad_value| ≤ hard_bad_value_tolerance_c | 不匹配已知硬坏值 |

### 6.3 参数

| 参数 | 配置路径 | 默认值 | config.py行号 |
|------|---------|--------|-------------|
| enabled | `workflow.stability.co2_cold_quality_gate.enabled` | True | 260 |
| policy | `workflow.stability.co2_cold_quality_gate.policy` | "warn" | 261 |
| apply_temp_max_c | `workflow.stability.co2_cold_quality_gate.apply_temp_max_c` | 0.0 | 262 |
| raw_temp_min_c | `workflow.stability.co2_cold_quality_gate.raw_temp_min_c` | -30.0 | 263 |
| raw_temp_max_c | `workflow.stability.co2_cold_quality_gate.raw_temp_max_c` | 85.0 | 264 |
| max_abs_delta_from_ref_c | `workflow.stability.co2_cold_quality_gate.max_abs_delta_from_ref_c` | 20.0 | 265 |
| max_cell_shell_gap_c | `workflow.stability.co2_cold_quality_gate.max_cell_shell_gap_c` | 15.0 | 266 |
| hard_bad_values_c | `workflow.stability.co2_cold_quality_gate.hard_bad_values_c` | [-40.0, 60.0] | 267 |
| hard_bad_value_tolerance_c | `workflow.stability.co2_cold_quality_gate.hard_bad_value_tolerance_c` | 0.05 | 268 |

---

## 7. 参数配置映射汇总

### 7.1 稳态判定参数

| 算法 | 参数 | 配置路径 | 默认值 |
|------|------|---------|--------|
| CO2封压后露点 | window_s | `workflow.pressure.co2_postseal_dewpoint_window_s` | 4.0 |
| CO2封压后露点 | timeout_s | `workflow.pressure.co2_postseal_dewpoint_timeout_s` | 6.0 |
| CO2封压后露点 | span_c | `workflow.pressure.co2_postseal_dewpoint_span_c` | 0.12 |
| CO2封压后露点 | slope_c_per_s | `workflow.pressure.co2_postseal_dewpoint_slope_c_per_s` | 0.04 |
| CO2封压后露点 | min_samples | `workflow.pressure.co2_postseal_dewpoint_min_samples` | 6 |
| H2O封压后露点 | window_s | `workflow.pressure.h2o_postseal_dewpoint_window_s` | 2.5 |
| H2O封压后露点 | timeout_s | `workflow.pressure.h2o_postseal_dewpoint_timeout_s` | 5.5 |
| H2O封压后露点 | span_c | `workflow.pressure.h2o_postseal_dewpoint_span_c` | 0.18 |
| H2O封压后露点 | slope_c_per_s | `workflow.pressure.h2o_postseal_dewpoint_slope_c_per_s` | 0.06 |
| H2O封压后露点 | min_samples | `workflow.pressure.h2o_postseal_dewpoint_min_samples` | 4 |
| 气路放行露点 | window_s | `workflow.stability.gas_route_dewpoint_gate_window_s` | 60.0 |
| 气路放行露点 | max_total_wait_s | `workflow.stability.gas_route_dewpoint_gate_max_total_wait_s` | 1080.0 |
| 气路放行露点 | tail_span_max_c | `workflow.stability.gas_route_dewpoint_gate_tail_span_max_c` | 0.45 |
| 气路放行露点 | tail_slope | `workflow.stability.gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s` | 0.005 |
| 气路放行露点 | rebound_min_rise_c | `workflow.stability.gas_route_dewpoint_gate_rebound_min_rise_c` | 1.3 |

### 7.2 策略参数

| 策略 | 配置路径 | 默认值 |
|------|---------|--------|
| Postseal超时 | `workflow.pressure.co2_postseal_timeout_policy` | pass |
| Postseal物理QC | `workflow.pressure.co2_postseal_physical_qc_policy` | warn |
| Postsample反弹 | `workflow.pressure.co2_postsample_late_rebound_policy` | warn |
| Presample长稳 | `workflow.pressure.co2_presample_long_guard_policy` | warn |
| 采样窗QC | `workflow.pressure.co2_sampling_window_qc_policy` | warn |
| 低温CO2质量 | `workflow.stability.co2_cold_quality_gate.policy` | warn |
| 气路放行露点 | `workflow.stability.gas_route_dewpoint_gate_policy` | warn |

---

## 文档更新记录

| 日期 | 版本 | 更新内容 |
|------|------|---------|
| 2026-04-15 | v1.0 | 首次创建，覆盖5种判定场景的算法、参数与配置映射 |
