# V2 压力-路由状态机架构合同

## 1. 合同目的与阶段边界

本文档定义 V2 后续压力、路由、VENT、采样、证据与工件治理的架构合同。合同目标不是为了让局部测试通过，而是确保后续任何 V2 修改都符合气体分析仪自动校准的物理意义、证据意义和生产安全边界。

V2不是推倒 V1。V2 的方向是把 V1 已验证的物理流程模块化、状态化、证据化，使压力控制、路由阀动作、VENT 动作、采样门控、工件输出和 no-write证据都能被明确追踪、复核和回退。V1继续作为生产 fallback，默认入口不得切换，V1 fallback 不得被破坏。

当前阶段只允许 no-write 分阶段推进。任何 simulation、replay、characterization、golden-master、suite、parity、resilience证据都不能被解释为 real acceptance。当前 CO₂ sealed-only golden 行为必须保护；H₂O v2.1.0 中 ambient_open 与 sealed sweep 的物理流程有价值，但只能以函数级、route-aware、state-aware 的方式移植，不能把 H₂O 的裸 vent 行为污染 CO₂ 默认路径。

当前阶段状态：

- 阶段0：CO₂ 成功证据已冻结。
- 阶段1：CO₂ golden-master / characterization tests 已完成，commit `adede9333dc9d4aaeef55fe385e4c02a609110e4`。
- 阶段2：架构合同文档化，即本文档。
- 阶段3 shadow state trace 尚未开始，本文档不引入 shadow trace runtime。

## 2. 不可违反的物理原则

1. VENT=ON不是普通软件命令，而是系统与大气连通的物理状态。
2. 通气、通水、open conditioning 阶段，如果 route valve open，VENT 必须 ON。
3. ambient_open采样期间 VENT 必须 ON。
4. seal transition 必须按物理顺序执行：stop keepalive → VENT=OFF → settle → read pressure → close route valve。
5. sealed pressure control 阶段 VENT 必须 OFF。
6. sealed 阶段任何真实 VENT=ON 都必须 fail-closed，或被 no-VENT guard 阻断。
7. cleanup / emergency 的 VENT 状态由 cleanup/emergency policy 决定，必须记录 reason。
8. 常压点不是0 hPa 控压点；常压采样必须表达为 ambient/atmosphere/open route语义，而不是 pressure controller target=0。
9. sample只能发生在合法状态内，并且必须由 route、VENT、pressure、dewpoint、humidity、temperature 等 gate 明确放行。
10. watchlist / legacy-compatible诊断不能替代真实物理安全。诊断位可以解释历史兼容行为，但不能证明设备处于安全物理状态。
11. no-write不能放松，attempted_write_count 必须保持可审计。
12. V1 fallback不能破坏。

## 3. 状态机合同

### 3.1 BASELINE

| 字段 | 合同 |
| --- | --- |
|物理意义 | 建立路线、压力、设备、传感器、工件与 run state 的初始可追溯状态。BASELINE 不等价于必须通大气，也不等价于必须关闭所有阀。 |
| VENT 是否允许 ON |允许，但必须由 route policy 或设备安全策略显式决定，并记录 reason。 |
| VENT 是否允许 OFF |允许，但必须记录当前 route 与下一状态所需条件。 |
| route valve 是否允许 open |允许，前提是下一状态需要 open conditioning，且 VENT/route policy 已满足。 |
| route valve 是否允许 close |允许，前提是不会隐藏未知压力或残留通路风险。 |
| 是否允许 set_pressure | 默认不允许作为采样目标控压；只允许初始化、归零、预检查或安全恢复性质动作。 |
| 是否允许 sample | 不允许正式 point sample。允许设备健康检查类 diagnostic sample，但必须标记 diagnostic。 |
| CO₂ default sealed-only policy | 不进入 ambient block；仅为 route conditioning 与后续 seal transition 建立前置证据。 |
| CO₂ explicit ambient-block policy |仅当配置显式启用时，BASELINE 可转入 CO₂ ambient block 的 open conditioning。 |
| H₂O policy | 建立 humidity generator、temperature chamber、dewpoint meter 与 H₂O route 的初始 readiness。 |
| fail-closed 条件 | route 状态未知、VENT 状态未知、pressure reference 不可信、operator/no-write证据缺失、run_dir/probe_dir 映射缺失。 |
| 必须 trace/artifact 字段 | `state=BASELINE`、`route`、`vent_state`、`route_valve_state`、`pressure_reference_status`、`operator_confirmation`、`no_write_assertion`、`evidence_source`、`promotion_state`、`run_id`、`probe_id`。 |

### 3.2 OPEN_CONDITIONING

| 字段 | 合同 |
| --- | --- |
|物理意义 | route open 条件下进行通气、通水、路线冲洗、稳定或 conditioning，使路线达到采样或封路前的物理准备状态。 |
| VENT 是否允许 ON | 必须允许且通常必须 ON；如果 route open 且进行通气/通水，VENT 必须 ON。 |
| VENT 是否允许 OFF | 原则上不允许在 open conditioning 主体阶段 OFF；只有退出 conditioning进入 seal transition 前才允许。 |
| route valve 是否允许 open |允许，且必须与 VENT=ON 的物理连通状态一致。 |
| route valve 是否允许 close |允许作为退出或 fail-closed 动作；必须记录 reason。 |
| 是否允许 set_pressure | 不允许 sealed target pressure 控压；允许 atmosphere/ambient/readiness 类检查。 |
| 是否允许 sample |允许 conditioning diagnostic sample；正式 ambient sample 必须进入 AMBIENT_OPEN_SAMPLING。 |
| CO₂ default sealed-only policy |允许 CO₂ route conditioning，但默认不做 ambient block sample。 |
| CO₂ explicit ambient-block policy | CO₂ ambient block 启用时，OPEN_CONDITIONING 是 ambient sample 的前置状态。 |
| H₂O policy | H₂O route open conditioning、通水、keepalive、dewpoint meter ready 与 humidity stable 应在此状态建立。 |
| fail-closed 条件 | route open但 VENT 未 ON、keepalive 与 VENT 状态矛盾、dewpoint/humidity/temperature readiness 不满足、route open pressure 异常。 |
| 必须 trace/artifact 字段 | `state=OPEN_CONDITIONING`、`route`、`vent_state=ON`、`route_open=true`、`conditioning_reason`、`conditioning_started_at`、`conditioning_completed_at`、`keepalive_state`、`readiness_gates`。 |

### 3.3 AMBIENT_OPEN_SAMPLING

| 字段 | 合同 |
| --- | --- |
|物理意义 | route open 且与大气连通时采集 ambient/open-route 样本；用于 H₂O ambient_open 或显式启用的 CO₂ ambient block。 |
| VENT 是否允许 ON | 必须 ON。 |
| VENT 是否允许 OFF | 不允许。采样期间 VENT=OFF 必须 fail-closed。 |
| route valve 是否允许 open | 必须 open。 |
| route valve 是否允许 close |采样期间不允许；退出状态时可 close 或进入 seal transition。 |
| 是否允许 set_pressure | 不允许把 ambient sample 表达为 sealed pressure target；常压点不是0 hPa 控压点。 |
| 是否允许 sample |允许，但必须满足 route open、VENT=ON、sample gate、schema、readiness 与 no-write约束。 |
| CO₂ default sealed-only policy | 默认不进入此状态。进入即说明默认 golden 路径被污染，应 fail-closed 或标记为 explicit ambient block。 |
| CO₂ explicit ambient-block policy |只有配置显式启用才允许；ambient block 与 sealed block 必须分离。 |
| H₂O policy | H₂O ambient_open sample 必须在 dewpoint meter ready、dewpoint alignment、humidity generator stable、temperature chamber ready 后进行。 |
| fail-closed 条件 | VENT 不为 ON、route 不为 open、sample gate 未通过、schema退化、no-write guard 缺失、ambient block 未显式启用。 |
| 必须 trace/artifact 字段 | `state=AMBIENT_OPEN_SAMPLING`、`route`、`vent_state=ON`、`route_open=true`、`sample_gate`、`ambient_block_enabled`、`sample_schema_version`、`not_real_acceptance_evidence`。 |

### 3.4 SEAL_TRANSITION

| 字段 | 合同 |
| --- | --- |
|物理意义 | 从 open/ambient/conditioning物理状态切换到 sealed pressure control物理状态。该状态是 VENT 和 route valve 最关键的安全边界。 |
| VENT 是否允许 ON |进入初始可短暂为 ON，但必须执行 stop keepalive 后关闭；完成状态必须 VENT=OFF。 |
| VENT 是否允许 OFF | 必须 OFF，且必须 settle 后确认。 |
| route valve 是否允许 open | 初始可 open，用于完成 pressure settle/read pressure；完成前必须按 route policy close/seal。 |
| route valve 是否允许 close | 必须允许，且完成 sealed route 前必须 close/seal。 |
| 是否允许 set_pressure | 不允许直接执行 pressure sweep；只能读取 pressure、做 preseal gate 或 sealed readiness。 |
| 是否允许 sample | 不允许正式 sample。 |
| CO₂ default sealed-only policy | route conditioning 后进入 seal transition，再进入 sealed pressure sweep。 |
| CO₂ explicit ambient-block policy | ambient sample 完成后必须经 seal transition，不能直接进入 sealed pressure control。 |
| H₂O policy | stop keepalive → VENT=OFF →1.5s settle → read pressure gauge → close H₂O path。 |
| fail-closed 条件 | stop keepalive失败、VENT 无法 OFF、settle 后压力异常、route close/seal失败、pressure gauge 不可信、sealed no-VENT guard 未 armed。 |
| 必须 trace/artifact 字段 | `state=SEAL_TRANSITION`、`stop_keepalive_result`、`vent_off_command`、`settle_s`、`pressure_after_settle_hpa`、`route_close_result`、`seal_transition_completed`、`preseal_watchlist_status`。 |

### 3.5 SEALED_PRESSURE_CONTROL

| 字段 | 合同 |
| --- | --- |
|物理意义 | route 已 sealed，系统不与大气连通，pressure controller 按目标压力点控制并采样。 |
| VENT 是否允许 ON | 不允许。sealed 阶段真实 VENT=ON 必须 fail-closed 或被 no-VENT guard 阻断。 |
| VENT 是否允许 OFF | 必须 OFF。 |
| route valve 是否允许 open | 不允许打开到大气或 open route；仅允许 sealed 内部安全阀态。 |
| route valve 是否允许 close | 必须保持 sealed/closed。 |
| 是否允许 set_pressure |允许，且只能对合法 pressure point 执行。 |
| 是否允许 sample |允许，但必须在 pressure stable、route sealed、VENT=OFF、sample gate通过后执行。 |
| CO₂ default sealed-only policy | 执行 CO₂ golden pressure sweep：1100/1000/900/800/700/600/500 hPa。 |
| CO₂ explicit ambient-block policy |只有完成 explicit ambient block 与 seal transition 后才允许进入。 |
| H₂O policy | 执行 sealed pressure sweep，并保持 dry-air correction 与 dewpoint/pressure evidence 可追踪。 |
| fail-closed 条件 | sealed 阶段 VENT=ON、route open、pressure controller 不稳定、pressure reference 不可信、sample schema退化、attempted write、no-write guard 未 armed。 |
| 必须 trace/artifact 字段 | `state=SEALED_PRESSURE_CONTROL`、`route`、`target_pressure_hpa`、`actual_pressure_hpa`、`pressure_stable`、`vent_state=OFF`、`route_sealed=true`、`sample_count`、`sample_schema_version`、`no_write_guard`。 |

### 3.6 CLEANUP

| 字段 | 合同 |
| --- | --- |
|物理意义 | 完成 run 后把设备、阀、pressure controller、humidity generator、temperature chamber、route state、artifact state 收束到可复核的安全状态。 |
| VENT 是否允许 ON |允许，但由 cleanup policy 决定，必须记录 reason。 |
| VENT 是否允许 OFF |允许，但必须记录最终安全状态。 |
| route valve 是否允许 open |仅允许 cleanup policy需要的受控动作。 |
| route valve 是否允许 close |允许，通常应收束到 close/safe。 |
| 是否允许 set_pressure | 不允许执行校准目标控压；允许安全释放、停控或归零。 |
| 是否允许 sample | 不允许正式 point sample；允许 cleanup diagnostic snapshot。 |
| CO₂ default sealed-only policy |结束 sealed sweep 后不得 re-open形成新的 ambient evidence，除非 cleanup policy 明确且不污染 golden evidence。 |
| CO₂ explicit ambient-block policy | cleanup 不得追加 ambient block。 |
| H₂O policy | 停止 keepalive、释放或关闭 H₂O route、记录 dry-air correction 与最终状态。 |
| fail-closed 条件 | cleanup 动作失败、最终 VENT/route/pressure 状态未知、artifact finalize失败、no-write summary 缺失。 |
| 必须 trace/artifact 字段 | `state=CLEANUP`、`cleanup_reason`、`final_vent_state`、`final_route_state`、`final_pressure_hpa`、`artifact_finalize_result`、`no_write_summary`。 |

### 3.7 EMERGENCY_SAFE_STOP

| 字段 | 合同 |
| --- | --- |
|物理意义 | 任意状态发生安全风险时进入紧急安全停止，优先保护设备、人员、样品路线和数据证据完整性。 |
| VENT 是否允许 ON |允许，但必须由 emergency policy 决定，且记录 reason 与物理证据。 |
| VENT 是否允许 OFF |允许，但必须由 emergency policy 决定，且记录 reason 与物理证据。 |
| route valve 是否允许 open |只允许 emergency policy需要的安全动作。 |
| route valve 是否允许 close |允许，通常应优先收束到安全隔离状态。 |
| 是否允许 set_pressure | 不允许校准控压；允许停控、泄压、安全恢复。 |
| 是否允许 sample | 不允许正式 sample；允许 emergency diagnostic snapshot。 |
| CO₂ default sealed-only policy | 必须停止 golden path，标记 fail-closed，不得继续采样补齐点。 |
| CO₂ explicit ambient-block policy | 必须停止 ambient/sealed block，标记 fail-closed。 |
| H₂O policy | 停止 keepalive、humidity route、pressure sweep 与采样；记录 dewpoint/humidity/temperature 当前状态。 |
| fail-closed 条件 | 任意安全 gate失败即进入；如果 safe stop 自身失败，必须升级为 hard failure evidence。 |
| 必须 trace/artifact 字段 | `state=EMERGENCY_SAFE_STOP`、`trigger_reason`、`source_state`、`emergency_policy`、`safe_stop_actions`、`final_device_state`、`fail_closed_reason`。 |

## 4. CO₂ 行为合同

### 4.1 CO₂ sealed-only golden 默认路径

当前 CO₂ primary golden baseline 为：

- commit：`cdb821110a8d49e56bc29e18d029d74303b479c2`
- tag：`v2.0.1`
- output_dir：`run_20260508_101607`
- Stage1 commit：`adede9333dc9d4aaeef55fe385e4c02a609110e4`

默认路径合同：

1. 不进入 ambient block。
2.先进行 route conditioning。
3. 再进行 seal transition。
4. 再进行 sealed pressure sweep。
5. 压力点顺序必须保持：1100 /1000 /900 /800 /700 /600 /500 hPa。
6. sealed 阶段真实 VENT=ON计数必须为0。
7. `attempted_write_count=0`。
8. no-write guard 不得放松。
9. sampling schema 不得退化。
10. sample只能发生在合法 sealed pressure state。
11. 所有 characterization / replay / simulation evidence 必须标记为非 real acceptance。

必须由测试保护的行为包括：默认不进入 ambient block、pressure point 顺序、sample count、sealed no-VENT、artifact contract、no-write guard、route trace、sampling schema 与 final decision。

### 4.2 CO₂ explicit ambient block

CO₂ ambient block 是架构允许能力，但不是当前 sealed-only golden 默认路径。合同如下：

1.只有配置显式启用才允许进入 CO₂ ambient block。
2. ambient block 与 sealed block 必须物理和证据上分离。
3. ambient采样期间 VENT=ON。
4. ambient采样期间 route open。
5. sealed 前必须 stop keepalive、VENT=OFF、settle、read pressure、close/seal route。
6. sealed 后 no-VENT guard 生效。
7. explicit ambient block 不得污染默认 sealed-only golden 路径。
8. explicit ambient block 的 artifacts 必须标记 `ambient_block_enabled=true`，并与 sealed pressure artifacts 明确分段。

## 5. H₂O 行为合同

H₂O v2.1.0 的 ambient_open + sealed sweep 流程有物理价值，但不得以整文件覆盖或裸补丁方式接入当前 CO₂ 路径。H₂O 合同流程如下：

1. 设置 humidity generator target。
2. 等待 humidity generator stable。
3. 设置或确认 temperature chamber。
4. open H₂O route。
5. 执行 H₂O route open conditioning。
6. VENT=ON 并启动 keepalive。
7. dewpoint meter ready。
8. dewpoint alignment。
9. ambient_open sample。
10. stop keepalive。
11. VENT=OFF。
12.1.5s settle。
13. read pressure gauge。
14. close H₂O path。
15. sealed pressure sweep。
16. dry-air correction。
17. cleanup。

H₂O 裸 `controller.vent(True/False)` 是架构债。它表达了真实物理流程中 VENT 的必要动作，但不应继续以裸 controller 调用散落在 runner 内。也不能把 H₂O direct vent close直接接入当前 CO₂ 补丁化 `PressureControlService`，更不能让 H₂O direct vent close 成为 CO₂ 默认行为。

目标方式是引入 route-aware / state-aware `VentManager` 薄层：

- 输入当前 route、state、policy、reason。
- 输出受控 VENT 动作、trace、diagnostic 与 fail-closed结果。
- 底层硬件动作保持一致。
-先通过 shadow trace 验证，再替换 H₂O 裸 vent。

## 6. 模块职责合同矩阵

| 文件/模块 | 当前职责 | 当前控制的物理动作 | 是否 shared | CO₂ 风险 | H₂O 风险 |目标归属层 |近期是否允许修改 | 修改前必须 tests | 禁止整文件覆盖原因 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `co2_route_runner.py` | 编排 CO₂ route conditioning、seal transition、sealed pressure sweep、采样与 QC。 | CO₂ route、pressure hold、sample gate、sealed no-VENT相关编排。 | CO₂ 专属但依赖 shared services。 | P0 | P2 | RoutePressureStateMachine / RoutePolicy / SampleGate | 阶段2 不允许；阶段3只允许 shadow trace 外挂；阶段4 后按计划小步改。 | CO₂ golden、no-VENT guard、artifact contract、route runner tests、pressure service tests。 | CO₂ golden 已验证，整文件覆盖会破坏 sealed-only 行为与 evidence口径。 |
| `h2o_route_runner.py` | 编排 H₂O humidity、dewpoint、ambient_open、seal transition、sealed sweep。 | H₂O route open/close、keepalive、VENT、dewpoint alignment、sample。 | H₂O 专属但依赖 shared services。 | P2 | P0 | RoutePressureStateMachine / RoutePolicy / VentManager / SampleGate | 阶段2 不允许；阶段5 才允许替换裸 vent。 | H₂O original no-write、ambient_open、dewpoint alignment、sealed sweep、cleanup tests。 | H₂O物理流程复杂，整文件覆盖会丢失 v2.1.0 有价值流程。 |
| `pressure_control_service.py` | pressure setpoint、hold、VENT 包装、startup precheck、safe stop、watchlist diagnostics。 | pressure controller output、setpoint、vent/atmosphere mode、安全停止。 | shared。 | P0 | P0 | PressureTargetController / VentManager / RoutePressureStateMachine | 阶段2 不允许；阶段4只允许薄层旁路，不改默认行为。 | pressure control service tests、CO₂ golden、H₂O no-write、safe stop、watchlist、no-VENT tests。 | shared high-risk，局部补丁容易破坏 CO₂/H₂O 双路线。 |
| `valve_routing_service.py` | route valve / relay 状态编排与 route trace。 | CO₂/H₂O 路线阀、relay、route baseline、seal/open。 | shared。 | P0 | P0 | ValveService / RoutePolicy | 阶段2 不允许；后续只允许小粒度 route-aware 修改。 | route valve tests、CO₂ no-VENT、H₂O route open/close、relay mismatch tests。 | 阀状态直接决定是否通大气或 sealed，整文件覆盖风险极高。 |
| `dewpoint_alignment_service.py` | dewpoint readiness 与 alignment。 | 不直接控阀，但决定 H₂O sample gate。 | H₂O 为主，可能 shared diagnostic。 | P2 | P0 | SampleGate / DeviceService | 阶段2 不允许。 | dewpoint ready、alignment、timeout、sample gate tests。 | H₂O采样有效性依赖此服务，覆盖会破坏 ambient_open证据。 |
| `humidity_generator_service.py` | humidity generator target/stable 控制。 | 湿度发生器 target、stable wait、状态读取。 | H₂O 为主。 | P3 | P0 | DeviceService / RoutePolicy | 阶段2 不允许。 | humidity target/stable、timeout、cleanup tests。 | 湿度稳定是 H₂O物理前提，不能用简单 mock语义覆盖。 |
| `sampling_service.py` | sample params、sample collection、schema、runtime rows。 | 不应直接控阀或 VENT；负责采样执行与数据结构。 | shared。 | P0 | P0 | SampleCollector / SampleGate | 阶段2 不允许。 | sampling schema、sample count、route tag、artifact rows、CO₂/H₂O regression tests。 | shared schema 是 artifact/parity 基础，整文件覆盖会造成数据口径漂移。 |
| `route_pressure_block_service.py` | route pressure block / state片段治理。 | 间接决定 pressure/route block 是否可进入。 | shared。 | P1 | P1 | RoutePressureStateMachine | 阶段2 不允许。 | route block transition、pressure block artifact、CO₂/H₂O block tests。 | 它应成为状态机归属点，不能继续被局部补丁覆盖。 |
| `orchestrator.py` | 顶层 run 编排、services组合、startup/precheck 调用。 | 间接触发所有设备动作。 | shared。 | P0 | P0 | RoutePressureStateMachine / ArtifactMapper / StatusService | 阶段2 不允许。 | full route dry-run、startup precheck、artifact finalize、suite regression。 | 顶层入口影响所有 route，整文件覆盖会破坏 fallback 与证据链。 |
| `status_service` | 状态、日志、route_trace、timing、point timing。 | 不应直接控制硬件动作。 | shared。 | P1 | P1 | StatusService | 阶段2仅可文档化，不改 runtime。 | route_trace、timing、state trace、diagnostic field tests。 | 状态服务是证据层，不应混入物理动作。 |
| `artifact_service` | run_dir、summary、manifest、samples_runtime、point_results/point_summaries、no_write_guard。 | 不应直接控制硬件动作。 | shared。 | P1 | P1 | ArtifactService | 阶段2 不改 runtime。 | artifact contract、manifest、no-write、resilience tests。 | 工件服务是证据落盘边界，覆盖会破坏可追溯性。 |

目标归属层定义：

- `DeviceService`：单设备驱动与状态读取。
- `VentManager`：route-aware / state-aware VENT 动作与证据。
- `ValveService`：route valve / relay 的物理阀态控制。
- `RoutePressureStateMachine`：BASELINE 到 CLEANUP/EMERGENCY 的状态转移。
- `RoutePolicy`：CO₂/H₂O route-specific允许动作与禁止动作。
- `PressureTargetController`：sealed pressure target 与 pressure stability。
- `SampleGate`：采样前 gate 判定。
- `SampleCollector`：采样执行与 schema。
- `StatusService`：状态、日志、trace、timing。
- `ArtifactService`：run_dir 内证据写入。
- `ArtifactMapper`：probe_dir 与 downstream run_dir 的显式映射。

## 7. probe_dir / run_dir 合同

`probe_dir` 不等于 `run_dir`。

- `probe_dir` 是工程探针、admission、监督证据、wrapper evidence 的位置。
- `run_dir` 是真实流程执行证据、runtime artifacts、samples、summary、manifest、route trace 的位置。

wrapper 不得因为 artifact 不在 probe_dir 就误判 downstream 未生成。未来 `ArtifactMapper` 必须显式建立 probe evidence 与 downstream run evidence 的映射，避免 `calibration_output_dir=UNKNOWN` 或证据错配。

manifest 必须关联以下字段：

- `HEAD`
- `branch`
- `operator_confirmation`
- `no_write_assertion`
- `probe_id`
- `run_id`
- `output_dir`
- `promotion_state`
- `evidence_source`

对于 Step3A 工程探针例外，必须额外标记：

- `engineering_probe_only=true`
- `promotion_state=blocked`
- `not_real_acceptance_evidence=true`
- no-write证据
- operator confirmation record
- 双重解锁证据

## 8. 后续阶段实施路线

### 阶段0：冻结 CO₂ 成功证据

-目标：冻结 `cdb82111 / v2.0.1 / run_20260508_101607` 为 CO₂ sealed-only golden evidence。
-允许改文件：测试、只读证据整理文档。
- 禁止改文件：runtime、V1、run_app.py、真实配置。
- 是否允许跑真机：不允许。
- 必须新增测试：无，阶段目标是证据冻结。
-通过标准：baseline口径明确，不误用 `c135e8e7`。
- 回退方式：回到只读证据，不推进后续测试。

### 阶段1：CO₂ golden-master / characterization tests

-目标：用 tests 锁住 CO₂ sealed-only golden 行为。
-允许改文件：`tests/v2/**` 中指定测试文件。
- 禁止改文件：runtime、V1、run_app.py、workflow/runner.py、真实配置。
- 是否允许跑真机：不允许。
- 必须新增测试：CO₂ golden route、no-VENT guard、artifact contract、existing runner/service baseline。
-通过标准：新增20 passed；现有 CO₂ runner / pressure service19 passed；commit `adede9333dc9d4aaeef55fe385e4c02a609110e4`。
- 回退方式：撤销测试变更，不修改 runtime。

### 阶段2：架构合同文档化

-目标：定义 V2 压力-路由状态机架构合同，即本文档。
-允许改文件：`docs/architecture/v2_pressure_route_state_machine_contract.md`。
- 禁止改文件：runtime、tests、V1、run_app.py、workflow/runner.py、真实配置。
- 是否允许跑真机：不允许。
- 必须新增测试：无，文档阶段只做 diff 范围检查。
-通过标准：diff只包含本文档；提交 `docs(v2): define pressure route state machine contract`。
- 回退方式：撤销本文档，不影响 runtime。

### 阶段3：shadow state trace，不改变硬件动作

-目标：在不改变硬件动作的前提下记录 state trace，对照本文档发现状态漂移。
-允许改文件：最小 shadow trace记录层、tests、artifact/status 边界内的非侵入字段。
- 禁止改文件：底层硬件动作顺序、CO₂/H₂O route物理行为、V1、run_app.py。
- 是否允许跑真机：默认不允许。
- 必须新增测试：state trace snapshot、CO₂ sealed-only trace、H₂O ambient/sealed trace replay、no behavior change tests。
-通过标准：shadow trace 与现有 tests 同时通过，硬件动作 diff 为零。
- 回退方式：关闭 shadow trace，不影响 runtime 行为。

### 阶段4：VentManager 薄层，默认行为不变

-目标：引入 route-aware / state-aware VentManager 薄层，先包装不改变底层动作。
-允许改文件：VentManager 新层、最小调用点、tests。
- 禁止改文件：默认硬件动作语义、CO₂ golden path、V1。
- 是否允许跑真机：默认不允许。
- 必须新增测试：VENT state contract、CO₂ sealed no-VENT、H₂O ambient VENT=ON、emergency policy。
-通过标准：所有既有 tests + 新 VentManager tests通过，trace证明默认动作不变。
- 回退方式：切回原 direct call path。

### 阶段5：替换 H₂O 裸 controller.vent

-目标：用 VentManager 替换 H₂O 裸 `controller.vent(True/False)`。
-允许改文件：H₂O runner 中最小 vent 调用点、VentManager tests。
- 禁止改文件：CO₂ 默认 sealed-only 行为、PressureControlService 大改、V1。
- 是否允许跑真机：默认不允许。
- 必须新增测试：H₂O ambient_open VENT=ON、stop keepalive、VENT=OFF、1.5s settle、close path、sealed sweep。
-通过标准：H₂O no-write replay/simulation通过，CO₂ golden tests 不变。
- 回退方式：恢复 H₂O 原 direct vent path。

### 阶段6：v2.1.0 H₂O 原版 no-write 验证

-目标：验证 H₂O v2.1.0 原版 no-write物理流程与证据链。
-允许改文件：tests、replay fixtures、只读验证脚本。
- 禁止改文件：runtime 大改、CO₂ golden path、V1。
- 是否允许跑真机：默认不允许；若未来进入受控真实工程探针，必须另行授权并满足 Step3A规则。
- 必须新增测试：H₂O original route replay、dewpoint、humidity、ambient/sealed artifact、no-write。
-通过标准：H₂O 原版 no-write evidence 可复核，不作为 real acceptance。
- 回退方式：回到 replay/simulation-only。

### 阶段7：以 CO₂ 成功路径为主，函数级移植 H₂O

-目标：把 H₂O 有价值流程按函数级、route-aware方式迁入统一状态机。
-允许改文件：小粒度 route policy、VentManager、H₂O runner 调用点、tests。
- 禁止改文件：整文件覆盖、CO₂ default sealed-only 污染、V1。
- 是否允许跑真机：默认不允许。
- 必须新增测试：CO₂/H₂O route policy matrix、ambient/sealed separation、sample gate、artifact parity。
-通过标准：CO₂ golden 不变，H₂O no-write 流程更可追踪。
- 回退方式：逐函数 revert，不影响 CO₂。

### 阶段8：CO₂/H₂O 双路线 no-write 回归

-目标：CO₂ 与 H₂O 双路线 no-write 回归，验证状态机合同稳定。
-允许改文件：tests、suite、replay、artifact resilience。
- 禁止改文件：V1、run_app.py、真实配置、controlled-write。
- 是否允许跑真机：默认不允许。
- 必须新增测试：dual-route regression、parity、resilience、artifact registry、no-write summary。
-通过标准：CO₂/H₂O no-write suite 全通过，证据均标记非 real acceptance。
- 回退方式：回退到单路线 no-write。

### 阶段9：再讨论 controlled-write /生产替代

-目标：在真实 acceptance 闭环后，才讨论 controlled-write 与生产替代。
-允许改文件：待未来授权定义。
- 禁止改文件：未经授权的真实写入、默认入口切换、V1 fallback 禁用。
- 是否允许跑真机：只有明确授权并满足真实 acceptance 治理后才允许。
- 必须新增测试：real acceptance、controlled-write audit、rollback、operator approval、V1/V2 comparison。
-通过标准：真实 acceptance 闭环、no-write 到 controlled-write 的治理证据完整。
- 回退方式：V1 fallback继续有效，V2 不替代生产。

## 9. 修改准入规则

后续任何 V2 修改必须先回答：

1. 当前动作属于哪个状态？
2. route 是 CO₂还是 H₂O？
3. 当前状态下 VENT=ON/OFF 是否物理合法？
4. 当前 route valve open/close 是否物理合法？
5. 是否允许 set_pressure？
6. 是否允许 sample？
7. 是否会污染 CO₂ default sealed-only golden 路径？
8. 是否会把 H₂O direct vent close变成 CO₂ 默认行为？
9. no-write guard 是否仍 armed？
10. trace/artifact 是否足以证明动作顺序？
11. V1 fallback 是否完全不受影响？

如果答案不明确，必须先补文档、测试或 shadow trace，不能通过 runtime 局部补丁绕过物理合同。
