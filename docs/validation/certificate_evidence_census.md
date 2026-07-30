# GA-D6A 计量证书证据普查

GA-D6A 用只读文件系统普查回答一个受限问题：本机工程盘和归档盘上，是否存在值得人工核对的参考资产证书文件。它不会自动确认文件真实性、有效期、量程覆盖、溯源链或标准器身份，也不会把历史测量数据和气体分析仪输出证书升级成参考资产证书。

## 固定边界

- 不连接 COM 或任何真实设备。
- 不写设备 ID、SENCO、校准系数或生产数据库。
- 不执行系数拟合，不刷新 `real_primary_latest`，不修改 `run_app.py`。
- 只读取源文件元数据和受限内容；只在显式 `--output-dir` 写派生清单和报告。
- `reference_asset_certificate_candidate` 只表示待人工复核，不表示 `confirmed`。
- CO2 零气与 H2O 干气点/露点参考保持独立。H2O 干气点本身不是露点参考证书。

## 文件分类

- `reference_asset_certificate_candidate`：同时命中证书语义和参考资产角色，仍需人工复核。
- `device_output_certificate`：气体分析仪自身的输出证书或报告，不属于参考标准器证书。
- `measurement_evidence`：0620/0621 等历史测量或运行数据，不属于证书。
- `manual_or_protocol`：说明书、协议、规格资料。
- `unknown_certificate_candidate` / `unknown_role_candidate`：证书或角色不完整，需要人工归类。

图片和扫描 PDF 会明确标记 OCR/视觉复核需求；旧版 DOC/XLS 需要兼容解析器；ZIP 只读取成员名，RAR/7Z 仅登记容器，不解压覆盖任何原文件。

超过合同单文件上限的文件仍会登记路径和大小，但不会读取内容；根目录状态标为 `complete_with_bounded_exclusions`，总体覆盖状态标为 `enumeration_complete_with_bounded_exclusions`。这表示目录枚举完成，不表示这些超大容器内部已审查。

为避免工程运行产物淹没审阅清单，非参考证书分类在 `execution_rows` 中按类别保留样本；完整分类计数仍写入每个 `root_summaries.classification_counts`。参考资产证书候选不采用该抽样规则。

## 运行

从仓库根目录执行：

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.build_certificate_evidence_census `
  --root "D:\工程资料" `
  --root "E:\归档" `
  --output-dir "D:\gas_calibrator\_runtime\ga_d6a_certificate_census"
```

输出包括：

- `certificate_evidence_execution_rows.csv`：候选及明确排除项，角色为 `execution_rows`。
- `certificate_evidence_execution_summary.json`：扫描覆盖与角色计数，角色为 `execution_summary`。
- `certificate_evidence_diagnostic_analysis.json/.md`：中文诊断结论，角色为 `diagnostic_analysis`。
- `certificate_evidence_sha256_manifest.json`：已列入清单源文件的 SHA-256。

真实盘扫描报告可能含本地绝对路径，因此只保存在 `_runtime`，不得提交到公共仓库。
