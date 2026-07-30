# `gas_calibrator.v2` 迁移区

## 状态

**本目录不再是产品主线，也不是未来版本。V1.5 是唯一最终产品。**

2026-07-28 起，V2 的 `run_v2`、`test_v2_device`、`test_v2_safe` 和
`ui_v2.app` 启动链均已退役并删除。不得恢复这些入口；需要验证的通用
simulation/replay/parity 能力按调用关系迁入 `gas_calibrator.validation`，
产品候选入口只使用仓库根目录的 `run_v1_5_workstation.py`。

`gas_calibrator.v2` 只是在收口期间保留的历史资产池，用于：

1. 把可复用的证书、分析、报告、仿真、回放和证据能力迁入正式命名空间；
2. 为已经发布或仍有测试依赖的旧导入提供短期兼容包装；
3. 保留必要的历史审计线索，直到对应迁移批次完成。

## 禁止

- 不得新增 V2 页面、V2 runner、V2 产品入口或 V2 cutover 功能。
- 不得从 V2 打开真实 COM、控制设备、写入系数或刷新正式证据。
- 不得让 `gas_calibrator.v1_5`、共享存储或共享设备层依赖 V2。
- 不得把 V2 simulation/replay/parity 结果解释为真实 acceptance。

## 迁移目标

| 当前内容 | 正式去向 |
| --- | --- |
| 产品证书、审核、页面 | `gas_calibrator.v1_5` |
| 通用存储、转换、证据基础设施 | `gas_calibrator.storage` / `gas_calibrator.utils` |
| simulation、replay、parity、resilience、历史只读审计 | `gas_calibrator.validation` |
| 候选算法和离线拟合 | `gas_calibrator.modeling` |
| 平行运行内核、V2 真机探针、V2 UI 壳、cutover 逻辑 | 提取唯一安全合同后删除 |

最终完成条件是产品运行时不再导入 `gas_calibrator.v2`，随后删除本包。

## 正式文档

- [`V1_5_FINAL_PRODUCT_ARCHITECTURE_20260728.md`](../../../docs/architecture/V1_5_FINAL_PRODUCT_ARCHITECTURE_20260728.md)
- [`V1_5_OPERATOR_WORKSTATION_DECISION_20260727.md`](../../../docs/architecture/V1_5_OPERATOR_WORKSTATION_DECISION_20260727.md)
- [`V1_5_FINAL_STRUCTURE_AND_FLOW.md`](../../../docs/v1_5_flow_contract/V1_5_FINAL_STRUCTURE_AND_FLOW.md)
