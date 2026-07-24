# EC-D0 离线动态计量骨架

## 1. 阶段定位

EC-D0 是 V2 Step 2 范围内的 simulation-only 动态计量合同。它验证软件能否从已知的合成阶跃中恢复分析仪和流路的有效动态参数，并能否拒绝时间戳抖动、丢帧及 H2O 吸附/解吸滞后异常。

EC-D0 不执行以下操作：

- 不打开 COM；
- 不控制真实阀、流量、压力或湿度设备；
- 不写 ID、SENCO 或校准系数；
- 不修改 V1 或 `run_app.py`；
- 不刷新 `real_primary_latest`；
- 不产生 real acceptance evidence。

所有 EC-D0 工件必须包含：

- `evidence_source = simulated`
- `not_real_acceptance_evidence = true`
- `promotion_state = blocked`

## 2. 物理模型边界

### 2.1 CO2

CO2 合成响应采用纯输运延迟加一阶分析仪响应：

`H(s) = exp(-s * delay) / (1 + s * tau)`

该模型用于验证延迟、时间常数、增益、幅频和相频计算，不宣称足以描述任何真实分析仪。

### 2.2 H2O

H2O 使用快响应与慢吸附记忆的加权组合：

`y = (1 - memory_fraction) * y_fast + memory_fraction * y_slow`

上升和下降分别使用独立时间常数，用来表达干到湿、湿到干方向不对称。该模型的目的，是确保软件不会把明显的水汽长尾或滞后错误判为正常动态响应。

### 2.3 时间轴

每个分析仪通道独立模拟：

- 名义采样率；
- 采样间隔抖动；
- 丢帧；
- 串联位置对应的输运延迟。

分析同时输出采样率误差、间隔抖动率和丢帧率。采样时间与数据接收时间仍需在未来真实 H3 台架中分别取证。

## 3. 必需流路元数据

每个分析仪路径至少记录：

- 分析仪 ID 和串联位置；
- 气体种类；
- 管长、内径、材质；
- 流量；
- 分析仪腔体压力和温度；
- RH；
- 管路是否加热；
- 过滤器标识；
- 输运延迟；
- 时间戳来源。

合同位于：

`src/gas_calibrator/v2/configs/metrology/ec_dynamic_acceptance_contract_v1.json`

合同内阈值仅用于离线 fixture 回归，不是产品指标或真实放行限值。

## 4. 输出指标

每个通道输出：

- 动态增益；
- t10、t50、t90；
- 10% 到 90% 响应时间；
- 有效纯延迟；
- 有效时间常数；
- 上升/下降时间常数比；
- 采样率、抖动率、丢帧率；
- 稳态噪声；
- Allan deviation；
- 由阶跃等效模型推导的幅频比和相位。

当前幅相结果是 `first_order_effective_from_bidirectional_step`，不是由真实扫频或湍流协谱直接测得。

## 5. 验收语义

EC-D0 强制分离三类结论：

- `static_calibration_status`：本合同不评价，固定为 `not_evaluated`；
- `ec_dynamic_status`：只允许 `simulation_contract_pass/fail`；
- `real_acceptance_status`：固定为 `blocked`。

即使所有离线门禁通过，也只能证明动态分析代码正确处理了当前合成模型，不能证明真实分析仪、真实管路或涡动通量系统合格。

## 6. 验证

定向测试：

```powershell
python -m pytest tests/v2/test_ec_dynamic_metrology.py -q
```

simulation suite：

```powershell
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite smoke
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite regression
python -m gas_calibrator.v2.scripts.run_simulation_suite --suite parity
```

## 7. 后续阶段

EC-D1 应扩展多频或 PRBS 输入、输入发生器自身传递函数及不确定度传播。真实 H3 台架仍需单独授权，并必须使用上游快速参考，才能区分气源/阀/混合腔、管路和被测分析仪的动态贡献。
