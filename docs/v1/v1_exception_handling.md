# V1 异常处理与容错路径文档

## 文档信息

- **文档名称**: V1 异常处理与容错路径文档
- **适用范围**: V1 生产基线
- **版本**: v1.0
- **创建日期**: 2026-04-15
- **最后更新**: 2026-04-15
- **对应代码版本**: HEAD 截止 2026-04-15
- **维护人**: Codex

---

## 目录

1. [异常分类总览](#1-异常分类总览)
2. [设备断连与IO错误](#2-设备断连与io错误)
3. [通信超时](#3-通信超时)
4. [采样失败](#4-采样失败)
5. [系数写回失败](#5-系数写回失败)
6. [稳态超时与质量门禁](#6-稳态超时与质量门禁)
7. [安全停机](#7-安全停机)
8. [点位跳过条件](#8-点位跳过条件)
9. [异常传播路径](#9-异常传播路径)
10. [策略配置汇总](#10-策略配置汇总)

---

## 1. 异常分类总览

| 异常类型 | 触发条件 | 影响范围 | 检测机制 | 处理策略 |
|---------|---------|---------|---------|---------|
| 设备断连/IO错误 | 串口断开、权限丢失、设备移除 | 当前设备不可用 | `_is_recoverable_serial_error()` | 自动重连重试 |
| 通信超时 | 命令响应超时 | 当前操作失败 | 各设备超时参数 | 重试→降级→失败上报 |
| 采样失败 | 帧解析无效、压力/露点缺失 | 当前采样点数据缺失 | 帧质量评估 | 标记无效→跳过→继续 |
| 系数写回失败 | SENCO/GETCO异常 | 设备系数状态不确定 | 回读验证比较 | 回滚→审计→终止 |
| 稳态超时 | 门禁条件未满足 | 当前点位质量受影响 | policy配置 | warn/reject标记 |
| 安全停机 | 流程结束或异常中止 | 所有设备 | 验证检查 | 重试→重连→上报 |

---

## 2. 设备断连与IO错误

### 2.1 串口IO恢复机制

**代码定位**: `serial_base.py:196-214`（判定）、`serial_base.py:138-139`（重试参数）

**可恢复错误列表**（`_is_recoverable_serial_error()`）：

| 错误类型 | 判定条件 |
|---------|---------|
| PermissionError | 端口权限丢失 |
| OSError | 系统级IO错误 |
| serial.SerialException | pyserial 异常 |
| 文本匹配 | `"could not open port"` / `"Access is denied"` / `"设备不存在"` / `"设备不可用"` / `"端口被占用"` |

**恢复流程**：

```
检测可恢复错误 → 关闭串口 → 等待 0.2s → 重新打开 → 重试IO操作（1次）
```

| 参数 | 值 | 行号 |
|------|----|------|
| IO_RECOVERY_RETRY_COUNT | 1 | serial_base.py:138 |
| IO_RECOVERY_RETRY_DELAY_S | 0.2s | serial_base.py:139 |

### 2.2 PACE 状态查询失败

| 项目 | 说明 |
|------|------|
| 触发条件 | PACE 设备 `get_output_state`/`get_isolation_state`/`get_vent_status` 等查询抛出异常 |
| 处理策略 | 保留缓存旧值，不中断流程（静默降级） |
| 恢复路径 | 下次轮询时重新尝试查询 |
| 代码定位 | runner.py:763-961 |

### 2.3 压力计/露点仪读取失败

| 项目 | 说明 |
|------|------|
| 触发条件 | `read_pressure()`/`read_pressure_fast()`/`get_current_fast()` 抛出异常 |
| 处理策略 | 记录到 `context["fast_signal_errors"]`，采样窗口继续运行 |
| 恢复路径 | 下次轮询重试 |
| 代码定位 | runner.py:4986-5024 |

### 2.4 压力计连续模式启动失败

| 项目 | 说明 |
|------|------|
| 触发条件 | `start_pressure_continuous()` 失败 |
| 处理策略 | 回退到查询模式，日志 `"Sampling pressure-gauge continuous mode start failed; fallback to query mode"` |
| 代码定位 | runner.py:5058-5070 |

### 2.5 慢辅助设备读取失败

| 项目 | 说明 |
|------|------|
| 触发条件 | 温箱/测温仪/湿度发生器读取异常 |
| 处理策略 | 记录 error_text 到缓存条目，下次轮询重试 |
| 代码定位 | runner.py:5133-5195 |

### 2.6 压力计不可用

| 项目 | 说明 |
|------|------|
| 触发条件 | `devices.get("pressure_gauge")` 为 None |
| 处理策略 | 抛出 `RuntimeError("pressure_gauge unavailable")`，导致 Run aborted |
| 代码定位 | runner.py:3353-3414 |

---

## 3. 通信超时

### 3.1 各设备超时重试参数汇总

| 设备 | 操作 | 重试次数 | 重试间隔 | 超时 | 最终失败处理 |
|------|------|---------|---------|------|-------------|
| 分析仪 | 被动读数据 | 1 | 0.05s | 1.0s | 返回 None |
| 分析仪 | 主动流读取 | 4 | 0.01s | 1.0s | 返回 None |
| 分析仪 | 配置ACK | 1 | 0.1s | 1.0s | 返回 False |
| 分析仪 | SETCOMWAY ACK | 3 | 0.2s | 1.0s | 返回 False |
| 分析仪 | GETCO回读 | 2 | 0.1s | 0.3s | RuntimeError |
| PACE5000 | 压力读取 | 3 | 0.1s | 1.0s | RuntimeError("NO_RESPONSE") |
| PACE5000 | 输出使能验证 | — | 0.1s | 5.0s | RuntimeError |
| PACE5000 | Vent等待 | — | 0.25s | 30.0s | RuntimeError("VENT_TIMEOUT") |
| 湿度发生器 | ensure_run | 2 | 0.25s | 2.5s | 返回 {"ok": False} |
| 湿度发生器 | 激活验证 | — | 1.0s | 30.0s | 返回当前状态 |
| 露点仪 | get_current | 2 | — | 2.0s | 返回 {"ok": False} |
| 露点仪 | get_current_fast | — | — | 0.35s | 返回 {"ok": False} |
| 压力计 | read_pressure | 3 | 0.1s | 1.2s | RuntimeError |
| 压力计 | read_pressure_fast | 1 | 0.0s | 1.2s | RuntimeError |

---

## 4. 采样失败

### 4.1 帧解析返回 None

| 项目 | 说明 |
|------|------|
| 触发条件 | `_read_sensor_parsed()` 重试耗尽后仍无法获得可用帧 |
| 检测机制 | `_assess_sensor_frame_for_read()` 返回 `(False, reason)` |
| 处理策略 | 调用 `_log_sensor_read_reject()` 记录拒绝原因（15秒窗口去重） |
| 代码定位 | runner.py:8617-8688 |

**帧拒绝原因分类**（runner.py:8350-8427）：

| 拒绝原因 | 格式 | 说明 |
|---------|------|------|
| 无帧 | `"无帧"` | parsed 为 None 或空字典 |
| 字段缺失 | `"{key}缺失"` | 必需字段不存在 |
| 字段为空 | `"{key}为空"` | 字段值为空字符串 |
| 哨兵值 | `"{key}=sentinel({value})"` | 匹配到无效哨兵值 |
| 非数值 | `"{key}非数值({value})"` | 无法转为 float |
| 非有限值 | `"{key}非有限值({value})"` | float 为 inf/nan |
| 状态异常 | `"状态异常({status})"` | 状态含 FAIL/INVALID/ERROR |
| 状态告警 | `"状态告警({status})"` | 状态含 NO_RESPONSE/NO_ACK |

### 4.2 采样窗口工作线程错误

| 项目 | 说明 |
|------|------|
| 触发条件 | fast_signal 工作线程抛出异常 |
| 处理策略 | 首次出现时记录日志，后续相同签名静默（签名去重） |
| 用户感知 | 日志 `"Sampling window worker warning [fast_signal] err=..."` |
| 代码定位 | runner.py:5034-5042 |

### 4.3 Presample 采样锁违反

| 项目 | 说明 |
|------|------|
| 触发条件 | presample 锁定期内发生不允许的操作 |
| 处理策略 | 设置 `capture_hold_status="fail"`，抛出 RuntimeError |
| 恢复路径 | 当前点位失败，流程继续下一点位 |
| 代码定位 | runner.py:2144-2184 |

---

## 5. 系数写回失败

### 5.1 写回闭环失败路径

**代码定位**: runner.py:21586-21721

| 失败场景 | 触发条件 | 处理策略 | 恢复路径 | 用户感知 |
|---------|---------|---------|---------|---------|
| SENCO 写入 ACK 失败 | 分析仪未返回成功ACK | 进入回滚路径 | 回滚旧系数 | 日志 `"SENCO{n} writeback failed"` |
| GETCO 回读不匹配 | 回读值与目标值超出容差 | 进入回滚路径 | 回滚旧系数 | 日志 `"SENCO{n} writeback failed"` |
| 回滚成功 | 重新写入旧系数并验证通过 | 记录回滚结果，抛出 RuntimeError | 设备恢复原系数 | 日志 `"SENCO{n} writeback failed and rolled back safely"` |
| 回滚失败 | 回滚写入或验证失败 | 抛出 RuntimeError | 设备系数状态不确定 | 日志 `"SENCO{n} writeback failed"` |
| unsafe 失败 | `result["unsafe"]==True` | 抛出 RuntimeError("Coefficient writeback unsafe") | 需人工介入 | 日志 `"Coefficient writeback unsafe: ..."` |

### 5.2 SENCO 系数格式校验

**代码定位**: runner.py:21769-21793

| 校验项 | 条件 | 异常 |
|--------|------|------|
| 类型校验 | 非 dict/list/tuple | `ValueError("SENCO coefficients must be a dict, list, or tuple")` |
| 空载荷 | 载荷为空 | `ValueError("SENCO coefficient payload is empty")` |
| 数量超限 | 超过 6 个值 | `ValueError("SENCO coefficient payload exceeds 6 values")` |

### 5.3 审计记录

无论写回成功或失败，均调用 `_persist_coefficient_write_result()`（runner.py:21723-21766）写入 `coefficient_writeback_{stamp}.csv`，包含写入前/目标/回读/回滚的完整系数向量。

---

## 6. 稳态超时与质量门禁

### 6.1 策略配置体系

V1 使用统一的策略配置体系控制各类质量门禁的行为：

| 策略值 | 效果 |
|--------|------|
| `off` / `pass` | 不记录问题，继续执行 |
| `warn` | 记录警告标记，继续执行（点位质量状态为 warn） |
| `reject` | 记录失败标记，阻止该点位（点位质量状态为 fail，`blocked=True`） |

### 6.2 各门禁策略详情

| 门禁名称 | 配置路径 | 允许值 | 默认值 | 作用域 | 代码定位 |
|---------|---------|--------|--------|--------|---------|
| Postseal 超时 | `workflow.pressure.co2_postseal_timeout_policy` | pass/warn/reject | pass | CO2低压postseal露点超时 | runner.py:3544-3586 |
| Postseal 物理 QC | `workflow.pressure.co2_postseal_physical_qc_policy` | off/warn/reject | off | CO2低压postseal露点物理偏差 | runner.py:3552-3556 |
| Postsample 反弹 | `workflow.pressure.co2_postsample_late_rebound_policy` | off/warn/reject | off | CO2低压采样后露点反弹 | runner.py:3562-3566 |
| Presample 长期守护 | `workflow.pressure.co2_presample_long_guard_policy` | off/warn/reject | off | CO2低压采样前露点长期不稳 | runner.py:3567-3571 |
| 采样窗口 QC | `workflow.pressure.co2_sampling_window_qc_policy` | off/warn/reject | off | CO2低压采样窗口露点漂移 | runner.py:3572-3576 |
| 低温CO2质量门控 | `workflow.stability.co2_cold_quality_gate.policy` | off/warn/reject | warn | 低温CO2分析仪温度异常 | runner.py:8830-8890 |

### 6.3 点位质量综合评估

**代码定位**: runner.py:4193-4339

每个点位采样完成后，`_update_point_quality_summary()` 综合评估所有 QC 标记：

- 按优先级：`fail` > `warn` > `pass`
- `blocked=True` 当任何标记为 `fail`
- 检查项包括：postseal_rebound_veto、postseal_timeout、postseal_physical_qc、postsample_late_rebound、presample_long_guard、sampling_window_qc、cold_co2_quality_gate、pressure_gauge_stale_ratio、preseal_trigger_overshoot

---

## 7. 安全停机

### 7.1 停机顺序

**代码定位**: safe_stop.py:217-340

| 顺序 | 设备 | 操作 | 失败处理 |
|------|------|------|----------|
| 1 | PACE 压力控制器 | `enter_atmosphere_mode()`；不可用时回退 `set_output(False)` + `set_isolation_open(True)` + `vent(True)` | 日志 `"pace atmosphere sequence failed"` |
| 2 | PACE 读取验证 | `read_pressure()` + 查询 vent/output/isolation 状态 | 日志 `"pace read failed"` |
| 3 | Relay 继电器 | 按基线配置设置所有通道状态 | 逐通道日志 `"relay ch{idx} -> {state} failed"` |
| 4 | Relay 状态验证 | `read_coils(0, 16)` | 日志 `"relay verify failed"` |
| 5 | Relay_8 继电器 | 同 Relay | 同 Relay |
| 6 | 温箱 | `chamber.stop()` + 验证温度/RH/运行状态 | 日志 `"chamber stop failed"` |
| 7 | 湿度发生器 | `safe_stop()` + 可选 `wait_stopped()` + `fetch_all()` 验证 | 日志 `"hgen safe_stop failed"` |
| 8 | 压力计 | `read_pressure()` | 日志 `"gauge read failed"` |

### 7.2 重试验证机制

**代码定位**: safe_stop.py:343-368

```
perform_safe_stop_with_retries(devices, *, attempts=3, retry_delay_s=1.5)
```

**流程**：

1. 循环 `attempt = 1..max_attempts`
2. 调用 `perform_safe_stop()` 获取 result
3. 调用 `validate_safe_stop_result()` 获取 issues
4. 若 issues 为空 → 返回成功
5. 若非最后一次 → 等待 `retry_delay_s` 后重试
6. 返回最后一次结果（含验证失败信息）

| 参数 | 默认值 | 配置路径 |
|------|--------|---------|
| attempts | 3 | `workflow.safe_stop.perform_attempts` |
| retry_delay_s | 1.5s | `workflow.safe_stop.retry_delay_s` |

### 7.3 验证项

**代码定位**: safe_stop.py:153-214

| 验证项 | 条件 | issue 文本 |
|--------|------|-----------|
| Relay 状态不匹配 | 实际 != 基线 | `"relay state mismatch"` |
| 温箱未停止 | run_state 不为 0/停止 | `"chamber run_state not stopped"` |
| 湿度发生器步骤失败 | safe_stop 某步失败 | `"humidity generator {key} failed"` |
| 湿度发生器快照无效 | raw 以 "ERROR" 开头 | `"humidity generator current snapshot invalid"` |
| 湿度发生器流量仍高 | flow > 0.05 L/min | `"humidity generator flow still high"` |
| PACE 输出未关 | output 不为 OFF | `"pace output not off"` |
| PACE 隔离未开 | isolation 不为 ON | `"pace isolation not open"` |

### 7.4 UI 安全停机调用

**代码定位**: app.py:9974-10069

| 参数 | 默认值 | 说明 |
|------|--------|------|
| wait_timeout_s | 90.0s | 等待流程释放设备 |
| perform_attempts | 3 | 安全停机重试次数 |
| reopen_attempts | 2 | 设备重连重试次数 |
| retry_delay_s | 1.5s | 重试间隔 |
| reopen_retry_delay_s | max(2.0, retry_delay_s) | 重连重试间隔 |

**流程**：防重入检查 → 等待流程释放 → 重建设备连接 → 执行安全停机 → 验证 → 失败重试（含重连）

---

## 8. 点位跳过条件

| 跳过条件 | 检测机制 | 处理 | 代码定位 |
|---------|---------|------|---------|
| stop_event 已设置 | `self.stop_event.is_set()` | 立即返回，进入清理 | runner.py:6519等多处 |
| CO2 PPM 在跳过集合中 | `ppm in skip_ppm` | 静默跳过 | runner.py:6618-6660 |
| 路由模式不匹配 | `route_mode=="h2o_only"` 跳过CO2，`"co2_only"` 跳过H2O | 日志并跳过 | runner.py:7100-7115 |
| 分析仪禁用 | 持续无响应 | 冷却期后重探测 | runner.py:8044-8105 |
| 传感器预检查失败 | 有效帧数 < 最低要求 | strict=True时Run aborted | runner.py:7496-7520 |

---

## 9. 异常传播路径

```
设备层（devices/）
  │ IO异常/超时/断连
  ▼
runner层（workflow/runner.py）
  │ try/except 捕获
  │ ├─ 可恢复：静默降级/重试/回退
  │ ├─ 点位级：跳过点位/标记质量
  │ └─ 不可恢复：Run aborted → _cleanup() → safe_stop
  ▼
UI层（ui/app.py）
  │ worker线程结束检测
  │ ├─ 控件解锁
  │ ├─ 状态栏更新
  │ └─ 安全停机（用户触发或流程结束时）
```

**关键传播规则**：

1. 设备层异常**不直接传播到UI**，而是在 runner 内部捕获处理
2. runner 通过 `stop_event` 和 `worker.is_alive()` 与 UI 通信
3. 唯一导致 Run aborted 的异常会触发 `_cleanup()` → 安全停机 → 设备关闭
4. UI 的安全停机有独立的重连重试机制，不依赖 runner 的安全停机

---

## 10. 策略配置汇总

| 策略名称 | 配置路径 | 允许值 | 默认值 |
|---------|---------|--------|--------|
| Postseal 超时策略 | `workflow.pressure.co2_postseal_timeout_policy` | pass/warn/reject | pass |
| Postseal 物理 QC | `workflow.pressure.co2_postseal_physical_qc_policy` | off/warn/reject | off |
| Postsample 反弹策略 | `workflow.pressure.co2_postsample_late_rebound_policy` | off/warn/reject | off |
| Presample 长期守护 | `workflow.pressure.co2_presample_long_guard_policy` | off/warn/reject | off |
| 采样窗口 QC | `workflow.pressure.co2_sampling_window_qc_policy` | off/warn/reject | off |
| 低温CO2质量门控 | `workflow.stability.co2_cold_quality_gate.policy` | off/warn/reject | warn |

---

## 文档更新记录

| 日期 | 版本 | 更新内容 |
|------|------|---------|
| 2026-04-15 | v1.0 | 首次创建，覆盖6类异常的完整处理路径、安全停机、策略配置体系 |
