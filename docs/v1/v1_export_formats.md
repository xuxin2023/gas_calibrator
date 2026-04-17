# V1 数据导出格式规范文档

## 文档信息

- **文档名称**: V1 数据导出格式规范文档
- **适用范围**: V1 生产基线
- **版本**: v1.0
- **创建日期**: 2026-04-15
- **最后更新**: 2026-04-15
- **对应代码版本**: HEAD 截止 2026-04-15
- **维护人**: Codex

---

## 目录

1. [导出机制](#1-导出机制)
2. [样本级CSV](#2-样本级csv)
3. [点位级CSV/XLSX](#3-点位级csvxlsx)
4. [系数写回CSV](#4-系数写回csv)
5. [分析仪汇总CSV/XLSX](#5-分析仪汇总csvxlsx)
6. [单点采样CSV](#6-单点采样csv)
7. [告警规则](#7-告警规则)
8. [字段翻译表](#8-字段翻译表)

---

## 1. 导出机制

**实现文件**: `src/gas_calibrator/logging_utils.py`

### 1.1 RunLogger 类

RunLogger 是运行时日志管理器，负责所有导出文件的创建和写入。

### 1.2 原子写入

所有 CSV/XLSX 文件通过原子写入机制避免 0 字节损坏：

- `_save_workbook_atomic()`: tempfile + `os.replace()`
- `_save_csv_atomic()`: tempfile + `os.replace()`

### 1.3 动态Header

样本级 CSV 使用动态 Header 机制：

- 新字段出现时触发全量重写（`_rewrite_dynamic_csv`）
- 已有行保持不变，Header 扩展后补空列

### 1.4 Run目录结构

```
logs/<run_id>/
├── samples_*.csv                    # 样本级导出
├── point_XXXX*_samples.csv          # 单点样本明细
├── points_*.csv                     # 点位级执行摘要
├── points_readable_*.csv            # 点位级可读摘要
├── points_readable_*.xlsx           # 点位级可读工作簿
├── coefficient_writeback_*.csv      # 系数写回审计
├── 分析仪汇总_*.csv                 # 汇总级CSV
└── 分析仪汇总_*.xlsx                # 汇总级XLSX
```

---

## 2. 样本级CSV

### 2.1 文件名模式

```
samples_{analyzer_id}.csv
```

### 2.2 特点

- 动态Header：新字段出现时全量重写
- 每行一个采样时刻的数据
- 包含所有分析仪的采样数据

### 2.3 关键字段

| 字段 | 类型 | 说明 |
|------|------|------|
| sample_ts | float | 采样时间戳 |
| save_ts | float | 保存时间戳 |
| point_row | int | 点位行号 |
| phase | string | 阶段（zero/span） |
| ppm_CO2 | float | CO2浓度 |
| ppm_H2O | float | H2O浓度 |
| pressure_kpa | float | 压力 |
| dewpoint_c | float | 露点温度 |
| chamber_temp_c | float | 温箱温度 |

---

## 3. 点位级CSV/XLSX

### 3.1 文件名模式

| 文件 | 模式 |
|------|------|
| 执行摘要 | `points_{timestamp}.csv` |
| 可读摘要CSV | `points_readable_{timestamp}.csv` |
| 可读摘要XLSX | `points_readable_{timestamp}.xlsx` |

### 3.2 关键字段

| 字段 | 类型 | 说明 |
|------|------|------|
| point_row | int | 点位行号 |
| phase | string | 阶段 |
| tag | string | 标记 |
| temp_chamber_c | float | 温箱温度 |
| co2_ppm_target | float | CO2目标值 |
| pressure_target_hpa | float | 压力目标值 |
| co2_ppm_mean | float | CO2均值 |
| pressure_mean_hpa | float | 压力均值 |
| dewpoint_mean_c | float | 露点均值 |
| sample_count | int | 采样数 |
| point_quality_status | string | 点位质量结果 |
| point_quality_flags | string | 点位质量标记 |
| point_quality_blocked | bool | 点位质量是否阻断 |

### 3.3 可读化版本

- 字段名翻译为中文标签（见第8章）
- 告警标记：偏差超阈值的单元格标红（`fgColor="F4CCCC"`）

---

## 4. 系数写回CSV

详见 [V1 系数写回闭环协议文档](v1_coefficient_writeback.md) 第10章。

---

## 5. 分析仪汇总CSV/XLSX

### 5.1 文件名模式

```
分析仪汇总_{timestamp}.csv
分析仪汇总_{timestamp}.xlsx
```

### 5.2 结构

- 每台分析仪一个独立 sheet（XLSX）
- CO2/H2O 分相文件

### 5.3 关键字段

| 字段 | 类型 | 说明 |
|------|------|------|
| NUM | int | 序号 |
| PointRow | int | 点位行号 |
| PointPhase | string | 阶段 |
| TempSet | float | 温度设定 |
| P | float | 压力 |
| ppm_CO2 | float | CO2浓度 |
| ppm_H2O | float | H2O浓度 |
| R_CO2 | float | CO2比值（6位小数） |
| R_CO2_dev | float | CO2比值偏差 |
| R_H2O | float | H2O比值（6位小数） |
| R_H2O_dev | float | H2O比值偏差 |
| ValidFrames | int | 有效帧数 |
| TotalFrames | int | 总帧数 |
| FrameStatus | string | 帧状态统计 |

---

## 6. 单点采样CSV

### 6.1 文件名模式

```
point_{point_row}{phase}{tag}_samples.csv
```

### 6.2 内容

单点位的所有采样时刻明细数据，字段与样本级CSV一致。

---

## 7. 告警规则

**代码定位**: logging_utils.py:1688-1753

### 7.1 告警阈值

| 比较对象 | 阈值 | 行号 |
|---------|------|------|
| CO2均值 vs 目标CO2 ppm | `max(20.0, abs(target) × 0.05)` | 1735-1740 |
| 压力控制器均值 vs 目标压力 | 5.0 hPa | 1741 |
| 数字压力计均值 vs 目标压力 | 5.0 hPa | 1742 |
| 温箱温度均值 vs 目标温度 | 0.5 °C | 1743 |
| 湿度发生器温度均值 vs 目标温度 | 0.5 °C | 1744-1748 |
| 湿度发生器湿度均值 vs 目标湿度 | 3.0 %RH | 1749-1753 |

### 7.2 标记方式

- 超阈值单元格前景色设为 `"F4CCCC"`（浅红色）
- 仅在可读化版本（`points_readable_*.csv/xlsx`）中标记

---

## 8. 字段翻译表

**代码定位**: logging_utils.py:109-306

### 8.1 关键翻译条目

| 英文字段名 | 中文标签 |
|-----------|---------|
| point_row | 点位行号 |
| phase | 阶段 |
| tag | 标记 |
| temp_chamber_c | 温箱温度C |
| co2_ppm_target | CO2目标ppm |
| pressure_target_hpa | 压力目标hPa |
| co2_ppm_mean | CO2均值ppm |
| pressure_mean_hpa | 压力均值hPa |
| dewpoint_mean_c | 露点均值C |
| sample_count | 采样数 |
| stable_flag | 稳态标记 |
| dewpoint_gate_result | 封压后露点门禁结果 |
| dewpoint_gate_elapsed_s | 封压后露点门禁耗时s |
| dewpoint_gate_span_c | 封压后露点门禁跨度C |
| dewpoint_gate_slope_c_per_s | 封压后露点门禁斜率C每s |
| dewpoint_time_to_gate | 气路放行露点判稳耗时s |
| flush_gate_status | 气路放行门禁结果 |
| point_quality_status | 点位质量结果 |
| point_quality_flags | 点位质量标记 |
| point_quality_blocked | 点位质量是否阻断 |
| postseal_physical_qc_status | 封压后物理一致性结果 |
| presample_long_guard_status | 采样前长稳守护结果 |
| postsample_late_rebound_status | 采样早期晚回潮结果 |
| sampling_window_qc_status | 采样窗露点质控结果 |

完整翻译表约 200 个字段，详见 `logging_utils.py:109-306`。

---

## 文档更新记录

| 日期 | 版本 | 更新内容 |
|------|------|---------|
| 2026-04-15 | v1.0 | 首次创建，覆盖4类导出文件schema、告警规则、字段翻译表 |
