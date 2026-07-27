# 气体分析仪自动校准 V2

## 当前定位

V2 是当前主开发线，但当前阶段仍然是 `Step 2: production-grade platformization`。这意味着 V2 的默认工作重心是仿真、回放、suite、parity、resilience、离线评审与工件治理，而不是替代 V1、切换默认入口或开展真实设备 acceptance。

Step 2 的硬边界：

- 不修改 `run_app.py` 默认入口。
- 不把任何新功能接回 V1 UI。
- 不打开真实串口 / COM，不做 V2 真机测试。
- 不运行 `real compare` / `real verify`。
- 不刷新 `real_primary_latest`。
- 不把 simulated / replay / suite / parity / resilience 结果解释为 real acceptance evidence。

## 文档导航

- [文档索引](docs/README.md)
- [软件架构说明](docs/software_architecture.md)
- [运行与验证指南](docs/runtime_and_validation.md)
- [工件与证据治理](docs/artifact_governance.md)
- [Step 2 V1/V2 同步矩阵](docs/step2_v1_sync_matrix.md)
- [EC-D0 离线动态计量骨架](../../../docs/metrology/ec_d0_offline_dynamic_metrology.md)
- [GA-D3 气体分析仪综合工作包络](../../../docs/metrology/ga_d3_gas_analyzer_operating_envelope.md)
- [GA-D4 气体分析仪真实台架协议准备度](../../../docs/metrology/ga_d4_gas_analyzer_bench_readiness.md)
- [GA-D5 0620/0621 计量资产资料包准备度](../../../docs/metrology/ga_d5_0620_0621_asset_dossier_readiness.md)
- [GA-D6B 业主确认的证书运行资料门禁](docs/certificate_operational_admission.md)

## 推荐入口

以下命令均默认从仓库根目录 `D:\gas_calibrator` 执行，并使用 PowerShell 写法设置 `PYTHONPATH`：

```powershell
$env:PYTHONPATH = "src"
```

安全仿真自检：

```powershell
python -m gas_calibrator.v2.scripts.test_v2_safe
```

Headless 仿真运行：

```powershell
python -m gas_calibrator.v2.scripts.run_v2 `
  --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json `
  --simulation `
  --headless
```

V2 UI 启动：

```powershell
python -m gas_calibrator.v2.ui_v2.app `
  --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json `
  --simulation
```

simulation-only 设备工作台辅助脚本：

```powershell
python -m gas_calibrator.v2.scripts.test_v2_device connection
python -m gas_calibrator.v2.scripts.test_v2_device single
python -m gas_calibrator.v2.scripts.test_v2_device full
```

仿真 suite：

```powershell
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite smoke
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite regression
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite nightly
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite parity
```

`smoke`、`regression` 和 `nightly` 均包含 `ec_dynamic_offline_contract`。该用例只验证 CO2/H2O 合成阶跃、串联延迟、采样时钟和异常拒绝能力；其结论固定为 simulation contract，不是 EC real acceptance。

`regression` 和 `nightly` 另包含 `ec_dynamic_system_identification_contract`。该用例以 PRBS、上游参考、Welch H1 和相干性验证经验传递函数估计器；命令量只作气源链诊断。分段区间不是正式计量置信区间，结果仍固定为 simulated / promotion blocked。详见 [`docs/metrology/ec_d1_offline_system_identification.md`](../../../docs/metrology/ec_d1_offline_system_identification.md)。

`regression` 和 `nightly` 还包含纯气体分析仪范围的 `gas_analyzer_dynamic_uncertainty_contract`：输出 5%、10% 和 −3 dB 带宽、等效相位延迟及分量化工程不确定度。动态衰减作为偏差单独报告，不输出反卷积系数；EC 协谱与通量闭合明确不在范围内。详见 [`docs/metrology/ga_d2_gas_analyzer_dynamic_uncertainty.md`](../../../docs/metrology/ga_d2_gas_analyzer_dynamic_uncertainty.md)。

同两套 suite 还包含 `gas_analyzer_operating_envelope_contract`：以 27 个温度×压力×流量格点整合静态误差、重复性、回差、漂移、线性、温压流敏感性、交叉干扰和 GA-D2 动态依赖。CO2 零气与 H2O 干气点保持独立，缺角/重复/越界数据会硬失败；限值只用于 simulated 回归，不是产品规格或真实验收。详见 [`docs/metrology/ga_d3_gas_analyzer_operating_envelope.md`](../../../docs/metrology/ga_d3_gas_analyzer_operating_envelope.md)。

`regression` 和 `nightly` 进一步包含 `gas_analyzer_bench_readiness_contract`：离线检查八类可溯源资产、证书有效期、完整且可重现的随机化环境顺序、CO2/H2O 独立稳定与恢复判据、不确定度预算及证据角色。通过只代表协议设计就绪；真实执行、设备 I/O、写入和 real acceptance 继续阻断。详见 [`docs/metrology/ga_d4_gas_analyzer_bench_readiness.md`](../../../docs/metrology/ga_d4_gas_analyzer_bench_readiness.md)。

同两套 suite 还包含 `ga_d5_0620_0621_asset_dossier_gaps`：按已核验的 0620/0621 历史注册表事实，分别检查历史基线一致性、参考资产证书资料完整性和当前机器前置条件。当前预期状态是 `EXPECTED_GAPS`，表示缺口被诚实识别而不是门禁通过；恢复的标气实际值不等于原始证书，CO2 零气与 H2O 干气/实际露点继续分开。详见 [`docs/metrology/ga_d5_0620_0621_asset_dossier_readiness.md`](../../../docs/metrology/ga_d5_0620_0621_asset_dossier_readiness.md)。

回放与离线治理：

```powershell
python -m gas_calibrator.v2.scripts.run_validation_replay --list-scenarios
python -m gas_calibrator.v2.scripts.build_offline_governance_artifacts --run-dir <run_dir>
python -m gas_calibrator.v2.scripts.build_offline_governance_artifacts --suite-dir <suite_dir>
```

说明：

- `test_v2_device` 默认是 simulation-only。
- 脚本里预留了 future real bench 双重解锁接口，但当前项目规则下不允许使用。
- `run_validation_replay` 与 `run_simulated_compare` 默认不会把离线回放写成真实验收证据。

## Step 3A: V2 受控真实 COM 工程探针例外

Step 3A 不是 V2 默认真机开放。它只允许在极窄范围内、双重解锁、no-write、operator confirmation record 齐备时，执行工程探针级别的真实 COM 接触。

基础原则：
- V1 仍是生产 fallback。
- `run_app.py` 不得修改。
- 默认仍禁止 V2 打开真实 COM。
- Step 3A 不是 real acceptance，不得刷新 `real_primary_latest`，不得宣称 V2 替代 V1，不得切默认入口。

允许的逐级范围：
- R0: query-only real-COM device inventory probe
- R1: conditioning-only real-COM probe
- R2: A1R CO2-only + skip0 + single route + single temperature + one non-zero point + no-write minimal sampling closure
- R3: A2 CO2-only + skip0 + single route + single temperature + 7 pressure points + no-write
- R4: V1/V2 real comparison audit

继续禁止：
- H2O full route
- 0 ppm formal acceptance
- full group
- multi-temperature
- ID write
- SENCO write
- calibration coefficient write
- `real_primary_latest` refresh
- default entry switch
- disabling V1 fallback

Step 3A evidence 必须固定标记：
- `evidence_source=real_probe_conditioning_only`（R1 conditioning-only）
- `not_real_acceptance_evidence=true`
- `acceptance_level=engineering_probe_only`
- `promotion_state=blocked`
- `real_primary_latest_refresh=false`

## 当前代码地图

| 目录 | 角色 |
| --- | --- |
| `entry.py` | V2 标准入口，负责配置装载、Step 2 配置门禁与 `CalibrationService` 构造。 |
| `config/` 与 `configs/` | 配置模型、默认配置、测试配置与离线建模配置。 |
| `core/` | 运行编排、设备工厂、状态机、结果存储、离线工件与治理骨架。 |
| `core/services/` | 采样、压力、温度、阀路、QC 等运行期服务。 |
| `domain/` | 领域模型、枚举、计划与结果对象。 |
| `qc/` | QC 管线、规则、评分与报告。 |
| `algorithms/` | 标定算法注册、拟合、比较与自动选择。 |
| `analytics/` | 运行分析、特征构造与分析导出。 |
| `storage/` | 样本/配置导入导出、数据库与 profile 存储。 |
| `adapters/` | V1/V2 对比、离线适配、结果网关与治理入口。 |
| `sim/` | 仿真设备、协议仿真、suite、replay、parity、resilience。 |
| `ui_v2/` | 中文默认 UI、控制器、页面、review/report/workbench 体验。 |
| `scripts/` | 安全入口、suite、回放、历史工件治理与辅助脚本。 |

## 当前工件合同

工件角色必须保持：

- `execution_rows`
- `execution_summary`
- `diagnostic_analysis`
- `formal_analysis`

统一导出状态必须保持：

- `ok`
- `skipped`
- `missing`
- `error`

用户可见的证据边界字段必须持续暴露：

- `evidence_source`
- `evidence_state`
- `acceptance_level`
- `promotion_state`
- `not_real_acceptance_evidence`

## 延伸阅读

如果要补功能、补测试或补治理骨架，优先阅读：

1. [软件架构说明](docs/software_architecture.md)
2. [运行与验证指南](docs/runtime_and_validation.md)
3. [工件与证据治理](docs/artifact_governance.md)
4. [Step 2 V1/V2 同步矩阵](docs/step2_v1_sync_matrix.md)
