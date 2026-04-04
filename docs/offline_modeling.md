# 离线建模分析说明

本功能默认不参与在线自动校准流程，仅用于离线建模分析与系数生成。

## 配置位置

- 主配置可见层：`configs/default_config.json` 中的 `modeling` 分组
- 独立离线配置：`configs/modeling_offline.json`

## 默认值

- `modeling.enabled = false`
- `modeling.fit_method = ordinary_least_squares`
- `modeling.ridge_lambda = 1e-6`
- `modeling.outlier_filter.enabled = false`
- `modeling.simplification.enabled = true`
- `modeling.simplification.method = column_norm`
- `modeling.simplification.auto_digits = true`
- `modeling.simplification.target_digits = 6`
- `modeling.export.enabled = true`
- `modeling.export.formats = ["json", "csv"]`

## 如何运行

```powershell
python run_modeling_analysis.py --base-config configs/default_config.json --modeling-config configs/modeling_offline.json
```

也可以在界面的“点位历史”页中使用“离线建模分析”区：

- 打开离线建模配置
- 选择离线建模输入文件
- 配置文件类型与 Excel Sheet
- 保存离线建模配置
- 运行离线建模
- 打开离线建模结果

## 输入数据

离线建模入口只读取已经处理好的数据文件，例如：

- `分析仪汇总_*.csv`
- `分析仪汇总_*.xlsx`

请在 `modeling.data_source.path` 中指定输入文件。

从阶段 6 开始，你也可以直接在 UI 中完成这些操作：

1. 在“点位历史”页找到“离线建模分析”区域
2. 点击“浏览...”选择 `csv / xlsx / xls` 输入文件
3. 如果是 Excel，可填写 `Excel Sheet`
4. 点击“保存离线建模配置”
5. 再点击“运行离线建模”

界面会回显：

- 当前已选文件路径
- 文件类型
- Excel Sheet
- 最近一次保存状态

如果文件路径为空、文件不存在，或扩展名不受支持，界面会给出中文提示。

## 水汽交叉干扰建模

离线建模支持通过可选特征项估计 `CO2 <- H2O` 的交叉干扰：

- `H`：H2O
- `H2`：H2O²
- `RH`：R * H2O

这些特征默认不会强制启用，只有在候选模型里显式写入时才参与拟合。例如：

- `Model_D = ["intercept", "R", "R2", "T", "P", "H", "H2"]`
- `Model_E = ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P", "H", "H2", "RH"]`

如果要使用这组能力，请保证输入文件中包含水汽列，并在 `modeling.data_source.humidity_keys` 中配置候选列名。

导出的推荐模型结果会额外包含：

- `H2O_cross_coefficients`
- `cross_interference`

其中会给出 `a_H / a_H2 / a_RH`，便于后续设备补偿使用。

## 输出结果

默认输出目录：

- `logs/modeling_offline/modeling_<timestamp>/`

输出内容至少包括：

- 模型比较 JSON / CSV
- 推荐模型结果文件
- `summary.txt`
- `summary.json`

## 如何确保不影响当前系统

满足以下条件时，当前在线自动校准系统行为保持不变：

1. `modeling.enabled = false`
2. 不主动点击“运行离线建模”
3. 不手动执行 `python run_modeling_analysis.py`

本阶段没有修改在线执行链路、设备控制、切气逻辑、等待逻辑和取均值逻辑。
