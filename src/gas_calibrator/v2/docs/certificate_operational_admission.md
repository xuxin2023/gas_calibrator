# GA-D6B 业主确认的证书运行资料门禁

GA-D6B 解决的是一个窄问题：在不伪造正式证书完整性、不授权真机执行的前提下，把 0620/0621 已使用的方法和业主确认的现场资料固化为可复核门禁，使离线程序、测试和治理工作可以继续。

## 两层门禁

1. **运行资料门禁**：照片的文件名和 SHA-256 全部核对一致，业主确认上一轮十个 CO2 实际值，压力、露点和温度参考证书身份/有效期满足当前离线推进用途，物理方法未漂移时，状态为 `PASSED_WITH_OWNER_ATTESTATION`。
2. **正式原始证书门禁**：仍为未通过。业主确认值没有逐瓶绑定到原始证书页；干燥空气证书没有给出 CO2 残余量及其不确定度；露点仪流量输出不是溯源流量标准；动态响应仍需要独立时间基准。

第一层通过只允许离线程序、测试、报告和治理继续，不允许：

- 真实 COM 或设备操作；
- 数据库、设备 ID、SENCO 或校准系数写入；
- 刷新 `real_primary_latest`；
- 宣称正式证书资料包完整；
- 宣称 V2 替代 V1 或 real acceptance。

## 固化的现场方法

- CO2 十点值使用项目业主确认的上一轮校准值：99.91、199.8、300.36、399.67、500.13、599.54、699.59、800.59、901.78、1000.22 µmol/mol。
- 桌面气瓶证书/标签照片作为运行资料的身份与连续性附件；照片不自动成为十点值的权威来源，也不证明物理气瓶数量。
- 干燥空气证书给出 O2 20.95%、N2 平衡。它只作为业主认可的低 CO2 过程锚点，不宣称是 CO2=0 ppm 的正式零气证书。
- H2O 低端点必须由实际露点和当次实际压力确定，不得与 CO2 低端气混用。
- 流量取露点仪输出 `flow_lpm`，只用于过程流量存在性与稳定性监测，不进入浓度拟合或修正。
- 温度真值取放置在温箱内的 RCY-1G 铂电阻数字测温仪；温箱设定值和温箱自身显示只作环境控制。
- 当前静态校准用软件单调时钟和分析仪 1 Hz 数据流定义采样及稳定窗口；该时间基准不能外推为动态响应或涡动相关时间同步验收。

## 本机只读核验

```powershell
$env:PYTHONPATH="src"
python -m gas_calibrator.v2.scripts.build_certificate_operational_admission `
  --evidence-root C:\Users\A\Desktop `
  --evidence-root D:\手册 `
  --output-dir D:\gas_calibrator\_runtime\ga_d6b_owner_attested_admission_20260725
```

命令只读取证据文件并写派生报告。输出角色固定为：

- `certificate_operational_admission_execution_rows.csv`：`execution_rows`
- `certificate_operational_admission_execution_summary.json`：`execution_summary`
- `certificate_operational_admission_diagnostic_analysis.json/.md`：`diagnostic_analysis`
- `certificate_operational_admission_formal_analysis.json`：`formal_analysis`
- `certificate_operational_admission_sha256_manifest.json`：输出文件哈希

## 当前科学限制

- FCDjw25074175 露点仪证书在 2026-08-17 到期，需按 P1 提前复校或更换证书。
- 真正的正式 CO2 零点需要给出 CO2 残余量及不确定度，不能只凭干燥空气的 O2/N2 组成推断。
- 如果未来把流量用于浓度修正、压降修正或正式不确定度预算，必须补独立可溯源流量标准。
- 如果未来进入动态响应、时延、频响或涡动相关验收，必须恢复独立可溯源时间基准与共同时间轴验证。
