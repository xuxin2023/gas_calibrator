# 无气瓶 / 无湿度发生器预验证操作说明

## 适用场景
- 标准气瓶暂不可用
- 湿度发生器暂不可用
- 需要先验证程序链路、采样链路、落盘链路、summary 链路与系数读回链路是否基本可靠

## 不能验证什么
- 不能验证 CO2 / H2O 正式点准确性
- 不能替代最终实气 acceptance
- 不能替代最终写系数后的真实量程验收

## 推荐执行顺序
1. `offline`
   - 对历史 `run_dir` 做离线重算
   - 重点看 `frame_quality_summary.csv`、`pressure_source_check.csv`
2. `dry_collect`
   - 在环境空气下只验证采样、summary、落盘
   - 不跑正式校准
3. `roundtrip` 只读
   - 默认只读，不写设备
   - 先确认读回链路正常
4. `pressure_only`
   - 仅当压力链还可用时执行
   - 重点看外部 `P` 与分析仪 `BAR` 的关系

## 推荐命令
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.tools.run_prevalidation_no_sources `
  --config "D:\gas_calibrator\configs\default_config.json" `
  --offline-run-dir "D:\gas_calibrator\logs\run_20260320_093112" `
  --include-roundtrip
```

若压力链也要验证：

```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.tools.run_prevalidation_no_sources `
  --config "D:\gas_calibrator\configs\default_config.json" `
  --offline-run-dir "D:\gas_calibrator\logs\run_20260320_093112" `
  --include-roundtrip `
  --include-pressure `
  --pressure-points "ambient,900,1100" `
  --no-prompt
```

## 输出目录
总控脚本会统一生成：

```text
logs/prevalidation_YYYYMMDD_HHMMSS/
├── offline/
├── dry_collect/
├── roundtrip/
├── pressure_only/
├── summary.json
└── summary.md
```

## 运行中注意事项
- 不要在程序正在写文件时打开对应的 `xlsx`
- 优先检查 `csv` 和 `io/log`，再看 `xlsx`
- 若某一步失败，总控脚本默认会继续后续步骤；最后统一看 `summary.json` / `summary.md`

## 如何判定 PASS / WARN / FAIL
- `PASS`
  - 各已执行步骤返回成功
  - summary 文件存在
  - 关键 csv 可打开且内容完整
- `WARN`
  - 某一步被跳过
  - 或某一步因前置条件缺失未执行
  - 但其余步骤仍完成
- `FAIL`
  - 某一步实际执行失败
  - 或总控 summary 标记存在失败步骤

## 建议人工检查项
- `frame_quality_summary.csv`
- `pressure_source_check.csv`
- `analyzer_summary.csv` / `分析仪汇总_*.csv`
- 日志关键字：
  - `sample export failed`
  - `point export failed`
  - `analyzer-summary-csv`

## 结论原则
- 这套预验证只用于确认“程序链路与落盘链路是否基本可靠”
- 最终是否可上线，仍要等气瓶与湿度发生器恢复后做正式实气 acceptance
