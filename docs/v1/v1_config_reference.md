# V1 配置模型完整映射文档

## 文档信息

- **文档名称**: V1 配置模型完整映射文档
- **适用范围**: V1 生产基线
- **版本**: v1.0
- **创建日期**: 2026-04-15
- **最后更新**: 2026-04-15
- **对应代码版本**: HEAD 截止 2026-04-15
- **维护人**: Codex

---

## 目录

1. [配置加载机制](#1-配置加载机制)
2. [覆盖机制](#2-覆盖机制)
3. [devices分区](#3-devices分区)
4. [workflow分区](#4-workflow分区)
5. [paths分区](#5-paths分区)
6. [coefficients分区](#6-coefficients分区)
7. [validation分区](#7-validation分区)
8. [modeling分区](#8-modeling分区)
9. [能力边界常量](#9-能力边界常量)

---

## 1. 配置加载机制

**实现文件**: `src/gas_calibrator/config.py`

### 1.1 load_config() 流程

1. 读取 JSON 配置文件
2. 路径展开：将 `paths` 中的相对路径转为仓库绝对路径
3. 默认值合并：`_merge_missing_defaults()` 递归合并 `_RUNTIME_DEFAULTS` 中缺失的键

### 1.2 点分路径访问

`get(cfg, "workflow.stability.tail_span_max_c")` — 支持点分路径访问嵌套字典

---

## 2. 覆盖机制

4 级叠加顺序（优先级从低到高）：

| 级别 | 来源 | 说明 |
|------|------|------|
| 1 | `configs/default_config.json` | 基础默认配置 |
| 2 | `configs/user_tuning.json` | UI可叠加的用户调参覆盖 |
| 3 | `configs/overrides/*.json` | 特定工程override |
| 4 | 运行时覆盖 | UI设备端口编辑等运行时修改 |

---

## 3. devices分区

### 3.1 设备段列表

| 设备段 | 说明 | 关键字段 |
|--------|------|---------|
| pressure_controller | PACE5000压力控制器 | enabled, port, baudrate |
| pressure_gauge | 数字压力计 | enabled, port, baudrate |
| dewpoint_meter | 露点仪 | enabled, port, baudrate, station |
| humidity_generator | 湿度发生器 | enabled, port, baudrate |
| gas_analyzer | 单分析仪 | enabled, port, baudrate, device_id |
| gas_analyzers | 多分析仪 | ga01~ga08, 各含port/baudrate/device_id |
| temperature_chamber | 温度箱 | enabled, port, baudrate, addr |
| thermometer | 测温仪 | enabled, port, baudrate |
| relay | 继电器 | enabled, port, baudrate, addr |
| relay_8 | 8通道继电器 | enabled, port, baudrate, addr |

### 3.2 多分析仪默认配置

- 分析仪ID：`ga01` ~ `ga08`
- 默认端口：`COM35` ~ `COM42`
- 默认波特率：`115200`

---

## 4. workflow分区

### 4.1 路由与范围

| 配置路径 | 类型 | 默认值 | 说明 |
|---------|------|--------|------|
| workflow.route_mode | string | "h2o_then_co2" | 路由模式 |
| workflow.selected_temps_c | list | [] | 温度范围筛选 |
| workflow.selected_pressure_points | list | [] | 压力点筛选 |
| workflow.skip_co2_ppm | list | [] | 跳过指定CO2点 |
| workflow.temperature_descending | bool | False | 温度降序 |
| workflow.collect_only | bool | False | 只采集不拟合不写回 |
| workflow.missing_pressure_policy | string | "require" | 压力缺失策略 |
| workflow.h2o_carry_forward | bool | False | H2O上下文继承 |

### 4.2 采样 (sampling)

| 配置路径 | 类型 | 默认值 | 行号 |
|---------|------|--------|------|
| workflow.sampling.interval_s | float | 1.0 | 58 |
| workflow.sampling.fixed_rate_enabled | bool | True | 61 |
| workflow.sampling.fast_signal_worker_enabled | bool | True | 63 |
| workflow.sampling.fast_signal_worker_interval_s | float | 0.1 | 64 |
| workflow.sampling.fast_signal_ring_buffer_size | int | 128 | 65 |
| workflow.sampling.pressure_gauge_continuous_enabled | bool | True | 66 |
| workflow.sampling.pressure_gauge_continuous_mode | string | "P4" | 67 |
| workflow.sampling.pre_sample_freshness_timeout_s | float | 1.0 | 72 |
| workflow.sampling.slow_aux_cache_enabled | bool | True | 76 |
| workflow.sampling.slow_aux_cache_interval_s | float | 5.0 | 77 |

### 4.3 稳态 (stability)

详见 [V1 稳态判定算法文档](v1_stability_algorithms.md) 第7章。

### 4.4 压力 (pressure)

详见 [V1 稳态判定算法文档](v1_stability_algorithms.md) 和 [V1 异常处理文档](v1_exception_handling.md)。

关键配置项：

| 配置路径 | 默认值 | 行号 |
|---------|--------|------|
| workflow.pressure.co2_post_stable_sample_delay_s | 10.0 | 124 |
| workflow.pressure.co2_preseal_pressure_gauge_trigger_hpa | 1110.0 | 114 |
| workflow.pressure.adaptive_pressure_sampling_enabled | True | 160 |

### 4.5 安全停机 (safe_stop)

| 配置路径 | 默认值 | 说明 |
|---------|--------|------|
| workflow.safe_stop.perform_attempts | 3 | 重试次数 |
| workflow.safe_stop.retry_delay_s | 1.5 | 重试间隔 |

---

## 5. paths分区

| 配置路径 | 类型 | 默认值 | 说明 |
|---------|------|--------|------|
| paths.points_excel | string | "points.xlsx" | 点表路径 |
| paths.output_dir | string | "logs" | 输出目录 |

---

## 6. coefficients分区

| 配置路径 | 类型 | 默认值 | 行号 | 说明 |
|---------|------|--------|------|------|
| coefficients.enabled | bool | True | — | 系数写回开关 |
| coefficients.h2o_zero_span.status | string | "not_supported" | 320 | H2O能力状态 |
| coefficients.h2o_zero_span.require_supported_capability | bool | False | 321 | 是否要求H2O能力 |
| coefficients.ratio_poly_fit.pressure_source_preference | string | "reference_first" | 324 | 压力源偏好 |

---

## 7. validation分区

| 配置路径 | 类型 | 默认值 | 行号 | 说明 |
|---------|------|--------|------|------|
| validation.offline.mode | string | "both" | 329 | 离线验证模式 |
| validation.offline.gas | string | "both" | 330 | 离线验证气体 |
| validation.dry_collect.write_coefficients | bool | False | 333 | 干采集写系数 |
| validation.dry_collect.include_pressure | bool | False | 334 | 干采集含压力 |
| validation.dry_collect.include_temperature | bool | False | 335 | 干采集含温度 |

---

## 8. modeling分区

离线建模相关配置，用于 `modeling/config_loader.py` 和 `modeling/offline_model_runner.py`。

---

## 9. 能力边界常量

### 9.1 H2O能力

| 配置路径 | 值 | 行号 | 说明 |
|---------|----|------|------|
| coefficients.h2o_zero_span.status | "not_supported" | 320 | V1 H2O zero/span 不支持 |

**生效逻辑**：`require_v1_h2o_zero_span_supported()`（config.py）在 H2O 不支持时抛出 `RuntimeError`。

### 9.2 分析仪帧质量

| 常量 | 值 | 说明 |
|------|----|------|
| MODE2_MIN_FIELD_COUNT | 16 | MODE2帧最少字段数 |
| bad_status_tokens | FAIL/INVALID/ERROR | 异常状态标记 |
| suspicious_co2_ppm_min | 2999.0 | 可疑CO2值阈值 |
| suspicious_h2o_mmol_min | 70.0 | 可疑H2O值阈值 |
| pressure_kpa_min | 30.0 | 压力下限 |
| pressure_kpa_max | 150.0 | 压力上限 |

---

## 文档更新记录

| 日期 | 版本 | 更新内容 |
|------|------|---------|
| 2026-04-15 | v1.0 | 首次创建，覆盖6个配置分区的字段级说明与4级覆盖机制 |
