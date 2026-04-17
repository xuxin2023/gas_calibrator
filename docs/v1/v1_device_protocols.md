# V1 设备通信协议文档

## 文档信息

- **文档名称**: V1 设备通信协议文档
- **适用范围**: V1 生产基线
- **版本**: v1.0
- **创建日期**: 2026-04-15
- **最后更新**: 2026-04-15
- **对应代码版本**: HEAD 截止 2026-04-15
- **维护人**: Codex

---

## 目录

1. [串口基础协议](#1-串口基础协议)
2. [分析仪协议](#2-分析仪协议)
3. [PACE5000 压力控制器协议](#3-pace5000-压力控制器协议)
4. [湿度发生器协议](#4-湿度发生器协议)
5. [露点仪协议](#5-露点仪协议)
6. [数字压力计协议](#6-数字压力计协议)
7. [温度箱协议](#7-温度箱协议)
8. [测温仪协议](#8-测温仪协议)
9. [继电器协议](#9-继电器协议)
10. [模拟层协议](#10-模拟层协议)
11. [设备通信参数汇总](#11-设备通信参数汇总)

---

## 1. 串口基础协议

**实现文件**: `src/gas_calibrator/devices/serial_base.py`

### 1.1 SerialDevice 类

| 项目 | 说明 |
|------|------|
| 类定义 | `serial_base.py:133-462` |
| 线程安全 | `threading.RLock()` 保护所有 IO 操作（第164行） |
| 编码方式 | ASCII（`errors="ignore"`），写入 `encode("ascii")`，读取 `decode("ascii")` |
| 默认超时 | `timeout=1.0s` |
| 默认串口参数 | `parity="N"`, `stopbits=1`, `bytesize=8` |

### 1.2 连接管理

| 参数 | 值 | 行号 |
|------|----|------|
| `OPEN_RETRY_COUNT` | 2 | 136 |
| `OPEN_RETRY_DELAY_S` | 0.2s | 137 |

打开串口时最多重试 2 次，每次间隔 0.2s。

### 1.3 IO 恢复机制

| 参数 | 值 | 行号 |
|------|----|------|
| `IO_RECOVERY_RETRY_COUNT` | 1 | 138 |
| `IO_RECOVERY_RETRY_DELAY_S` | 0.2s | 139 |

**可恢复错误判定**（`_is_recoverable_serial_error()`, 第196-214行）：

- `PermissionError`
- `OSError`
- `serial.SerialException`
- 错误文本包含以下任一关键词：
  - `"could not open port"`
  - `"Access is denied"`
  - `"设备不存在"` / `"设备不可用"` / `"端口被占用"`

**恢复流程**：检测到可恢复错误 → 关闭串口 → 等待 0.2s → 重新打开 → 重试 IO 操作（1次）

### 1.4 核心 IO 方法

| 方法 | 行号 | 说明 |
|------|------|------|
| `write(data)` | 324-333 | ASCII 编码写入，调用 `_run_io` |
| `readline()` | 335-345 | 读取一行，ASCII 解码并 strip |
| `query(data, delay_s=0.05)` | 365-369 | write + sleep(0.05) + readline |
| `exchange_readlines(data, *, response_timeout_s, read_timeout_s=0.1)` | 371-416 | 写入后按超时轮询读取多行 |
| `drain_input_nonblock(drain_s=0.35, read_timeout_s=0.05)` | 439-461 | 非阻塞排空输入缓冲区 |
| `flush_input()` | 417-425 | 清除输入缓冲区 |

### 1.5 ReplaySerial 模拟串口

| 项目 | 说明 |
|------|------|
| 类定义 | `serial_base.py:22-131` |
| 用途 | 测试/回放场景，不打开真实串口 |
| 机制 | 基于脚本（`script`）+ 队列（`queue`）的模拟 IO |

---

## 2. 分析仪协议

**实现文件**: `src/gas_calibrator/devices/gas_analyzer.py`  
**类定义**: `gas_analyzer.py:13-708`

### 2.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 115200 | 55 |
| 默认超时 | 1.0s | 56 |
| 设备 ID | `"000"` | 57 |
| 命令目标 ID | `"FFF"`（广播） | 16 |
| 行结束符 | `\r\n` | 174 |

### 2.2 命令格式

**通用命令格式**（`_cmd()`, 第173-174行）：

```
{CMD},YGAS,FFF\r\n
```

**带参数命令格式**（`_cmd_with_args()`, 第176-178行）：

```
{CMD},YGAS,FFF,{ARG1},{ARG2},...\r\n
```

### 2.3 命令清单

| 命令 | 方法 | 格式 | 说明 |
|------|------|------|------|
| MODE | `set_mode()` / `set_mode_with_ack()` | `MODE,YGAS,FFF,{mode}\r\n` | 模式切换（0=正常, 2=校准） |
| SETCOMWAY | `set_comm_way()` / `set_comm_way_with_ack()` | `SETCOMWAY,YGAS,FFF,{way}\r\n` | 通信方式切换（0=被动, 1=主动） |
| SENCO | `set_senco()` | `SENCO{index},YGAS,FFF,{C0},{C1},...,{C5}\r\n` | 系数写入（最多6个系数） |
| GETCO | `read_coefficient_group()` | `GETCO{index},YGAS,FFF\r\n` | 系数回读 |
| FTD | `set_active_freq()` | `FTD,YGAS,FFF,{freq}\r\n` | 主动发送频率设置 |
| AVERAGE | `set_average()` / `set_average_filter()` | `AVERAGE{ch},YGAS,FFF,{value}\r\n` | 通道平均次数设置 |
| READDATA | `read_data_passive()` | `READDATA,YGAS,FFF\r\n` | 被动请求数据 |

### 2.4 ACK 机制

**ACK 格式**（`_is_success_ack()`, 第88-91行）：

```
YGAS,{DEVICE_ID},T    ← 成功
YGAS,{DEVICE_ID},F    ← 失败
```

匹配正则：`r"YGAS,[0-9A-F]{3},T"`

### 2.5 超时重试参数

| 参数 | 值 | 行号 | 适用命令 |
|------|----|------|---------|
| `PASSIVE_READ_RETRY_COUNT` | 1 | 19 | READDATA |
| `PASSIVE_READ_DELAY_S` | 0.05s | 20 | READDATA |
| `ACTIVE_READ_RETRY_COUNT` | 4 | 21 | 主动流读取 |
| `ACTIVE_READ_RETRY_DELAY_S` | 0.01s | 22 | 主动流读取 |
| `CONFIG_ACK_RETRY_COUNT` | 1 | 23 | MODE/SENCO/FTD/AVERAGE |
| `CONFIG_ACK_RETRY_DELAY_S` | 0.1s | 24 | MODE/SENCO/FTD/AVERAGE |
| `COMM_WAY_ACK_RETRY_COUNT` | 3 | 25 | SETCOMWAY |
| `COMM_WAY_ACK_RETRY_DELAY_S` | 0.2s | 26 | SETCOMWAY |
| `COEFFICIENT_COMM_QUIET_DELAY_S` | 0.15s | 27 | SENCO/GETCO 前静默 |
| `COEFFICIENT_READ_RETRY_COUNT` | 2 | 28 | GETCO |
| `COEFFICIENT_READ_DELAY_S` | 0.1s | 29 | GETCO |
| `COEFFICIENT_READ_TIMEOUT_S` | 0.3s | 30 | GETCO |

### 2.6 MODE2 帧格式

**字段定义**（`_MODE2_KEYS`, 第34-49行；解析 `_parse_mode2()`, 第637-663行）：

```
YGAS,{id},{co2_ppm},{h2o_mmol},{co2_density},{h2o_density},{co2_ratio_f},{co2_ratio_raw},{h2o_ratio_f},{h2o_ratio_raw},{ref_signal},{co2_signal},{h2o_signal},{chamber_temp_c},{case_temp_c},{pressure_kpa},{status},[extra...]
```

| 字段序号 | 字段名 | 类型 | 说明 |
|---------|--------|------|------|
| 0 | id | string | 设备ID（3位十六进制） |
| 1 | co2_ppm | float | CO2 浓度（ppm） |
| 2 | h2o_mmol | float | H2O 浓度（mmol/mol） |
| 3 | co2_density | float | CO2 密度 |
| 4 | h2o_density | float | H2O 密度 |
| 5 | co2_ratio_f | float | CO2 比值（滤波后） |
| 6 | co2_ratio_raw | float | CO2 比值（原始） |
| 7 | h2o_ratio_f | float | H2O 比值（滤波后） |
| 8 | h2o_ratio_raw | float | H2O 比值（原始） |
| 9 | ref_signal | float | 参考信号 |
| 10 | co2_signal | float | CO2 信号 |
| 11 | h2o_signal | float | H2O 信号 |
| 12 | chamber_temp_c | float | 腔体温度（°C） |
| 13 | case_temp_c | float | 机壳温度（°C） |
| 14 | pressure_kpa | float | 压力（kPa） |
| 15 | status | string | 状态标记 |

最少字段数：16（`MODE2_MIN_FIELD_COUNT=16`, 第50行）

### 2.7 Legacy 帧格式

**解析**（`_parse_legacy()`, 第665-683行）：

```
YGAS,{id},{co2_ppm},{h2o_mmol},{co2_sig},{h2o_sig},{temp_c},{pressure_kpa},{status}
```

| 字段序号 | 字段名 | 类型 | 说明 |
|---------|--------|------|------|
| 0 | id | string | 设备ID |
| 1 | co2_ppm | float | CO2 浓度（ppm） |
| 2 | h2o_mmol | float | H2O 浓度（mmol/mol） |
| 3 | co2_sig | float | CO2 信号 |
| 4 | h2o_sig | float | H2O 信号 |
| 5 | temp_c | float | 温度（°C） |
| 6 | pressure_kpa | float | 压力（kPa） |
| 7 | status | string | 状态标记 |

最少字段数：6

### 2.8 帧解析策略

`parse_line()`（第696-708行）按以下优先级解析：

1. 按 `YGAS,` 标记定位帧候选（`_iter_frame_candidates()`, 第619-635行）
2. 先尝试 MODE2 格式解析
3. MODE2 失败则回退 Legacy 格式解析
4. 全部失败返回 `None`

---

## 3. PACE5000 压力控制器协议

**实现文件**: `src/gas_calibrator/devices/pace5000.py`  
**类定义**: `pace5000.py:13-1007`

### 3.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 9600 | 26 |
| 默认超时 | 1.0s | 27 |
| 行结束符 | `\n`（可配置为 `\r`/`\r\n`） | 53, 63 |
| 协议 | SCPI over Serial | — |

### 3.2 SCPI 命令集

| 操作 | 命令格式 | 行号 | 说明 |
|------|---------|------|------|
| 设置单位 | `:UNIT:PRES HPA` | 193-194 | 设置压力单位为 hPa |
| 输出开关 | `:OUTP {1\|0}` | 196-197 | 1=开启, 0=关闭 |
| 输出模式 Active | `:OUTP:MODE ACT` | 202-203 | 主动控制模式 |
| 输出模式 Passive | `:OUTP:MODE PASS` | 205-206 | 被动模式 |
| 查询输出模式 | `:OUTP:MODE?` | 208-209 | — |
| Slew 模式 Linear | `:SOUR:PRES:SLEW:MODE LIN` | 222-223 | 线性斜率 |
| Slew 模式 Max | `:SOUR:PRES:SLEW:MODE MAX` | 225-226 | 最大斜率 |
| Slew 速率 | `:SOUR:PRES:SLEW {hpa_per_s}` | 228-229 | 斜率值（hPa/s） |
| 过冲允许 | `:SOUR:PRES:SLEW:OVER {1\|0}` | 231-232 | — |
| 查询输出状态 | `:OUTP:STAT?` | 234-235 | — |
| 隔离阀控制 | `:OUTP:ISOL:STAT {1\|0}` | 273-274 | 1=隔离, 0=不隔离 |
| 查询隔离状态 | `:OUTP:ISOL:STAT?` | 266-267 | — |
| 设定压力值 | `:SOUR:PRES:LEV:IMM:AMPL {hpa}` | 305-307 | 目标压力（hPa） |
| Vent 命令 | `:SOUR:PRES:LEV:IMM:AMPL:VENT {1\|0}` | 328-330 | 1=开始vent, 0=停止 |
| 查询 Vent 状态 | `:SOUR:PRES:LEV:IMM:AMPL:VENT?` | 332-333 | — |
| 设备标识 | `*IDN?` | 358-359 | — |
| Vent 后阀门 | `:SOUR:PRES:LEV:IMM:AMPL:VENT:AFT:VVAL:STAT {OPEN\|CLOSED}` | 434-438 | — |
| Vent 弹窗 ACK | `:SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT {ENABled\|DISabled}` | 462-466 | — |
| InLimits 设置 | `:SOUR:PRES:INL {pct}` + `:SOUR:PRES:INL:TIME {time_s}` | 980-982 | — |
| 查询 InLimits | `:SENS:PRES:INL?` | 984-985 | — |

### 3.3 压力查询优先级

`read_pressure()` 按以下 4 级优先级查询（第87-92行）：

| 优先级 | 命令 | 说明 |
|--------|------|------|
| 1 | `:SENS:PRES:INL?` | InLimits 压力 |
| 2 | `:SENS:PRES:CONT?` | 控制压力 |
| 3 | `:SENS:PRES?` | 传感压力 |
| 4 | `:MEAS:PRES?` | 测量压力 |

重试：3 次，间隔 0.1s（第311行, 第323行）

### 3.4 Vent 状态码

| 状态码 | 常量名 | 含义 | 行号 |
|--------|--------|------|------|
| 0 | `VENT_STATUS_IDLE` | 空闲 | 16 |
| 1 | `VENT_STATUS_IN_PROGRESS` | Vent 进行中 | 17 |
| 2 | `VENT_STATUS_COMPLETED` / `VENT_STATUS_TIMED_OUT` | 完成/超时 | 18-19 |
| 3 | `VENT_STATUS_TRAPPED_PRESSURE` | 残留压力 | 20 |
| 4 | `VENT_STATUS_ABORTED` | 已中止 | 21 |

### 3.5 Legacy 设备检测

通过 `*IDN?` 响应判定（第358-359行）：

- 响应包含 `"GE DRUCK"` → Legacy 设备
- 响应包含 `"PACE5000 USER INTERFACE"` → Legacy 设备

### 3.6 关键超时参数

| 操作 | 超时 | 轮询间隔 | 行号 |
|------|------|---------|------|
| `verify_output_enabled` | 5.0s | 0.1s | 245-246 |
| `wait_for_vent_idle` | 30.0s | 0.25s | 720-721 |
| `clear_completed_vent_latch` | 5.0s | 0.25s | 609-610 |
| `enter_atmosphere_mode` | 30.0s | 0.25s | 867-868 |
| 大气保持线程间隔 | 2.0s | — | 793 |

---

## 4. 湿度发生器协议

**实现文件**: `src/gas_calibrator/devices/humidity_generator.py`  
**类定义**: `humidity_generator.py:13-390`

### 4.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 9600 | 19 |
| 默认超时 | 1.0s | 20 |
| 行结束符 | `\r\n` | — |

### 4.2 Target 命令集

| 操作 | 命令格式 | 行号 | 说明 |
|------|---------|------|------|
| 设置目标温度 | `Target:TA={value_c}` | 108-109 | 温度值（°C） |
| 设置目标湿度 | `Target:UwA={value_pct}` | 112-113 | 相对湿度（%） |
| 设置流量目标 | `Target:FA={flow_lpm}` | 128-129 | 流量（L/min） |
| 启停控制 | `Target:CTRL={ON\|OFF}` | 132-133 | 控制开关 |
| 加热开 | `Target:HEAT=ON` | 136-137 | — |
| 加热关 | `Target:HEAT=OFF` | 140-141 | — |
| 制冷开 | `Target:COOL=ON` | 144-145 | — |
| 制冷关 | `Target:COOL=OFF` | 148-149 | — |

### 4.3 FETC 查询

| 操作 | 命令格式 | 行号 | 说明 |
|------|---------|------|------|
| 查询全部 | `FETC? (@All)` | 155-156 | 返回所有 Key=Value 对 |
| 查询单字段 | `FETC? (@{field})` | 152-153 | 返回单个字段值 |
| 查询标签值 | `FETC? (@{tag})` | 163-164 | 按标签名查询 |

**响应格式**：逗号分隔的 `Key=Value` 对，如 `Td=-20.0,Tc=25.0,Fl=10.0,...`

**解析方法**：`_parse_kv_line()`（第88-102行）

### 4.4 露点设定点转换

`set_target_dewpoint()` 调用 `humidity_math.derive_humidity_generator_setpoint()` 将露点温度转换为湿度发生器的温度+湿度设定点，然后分别下发 `Target:TA` 和 `Target:UwA`。

### 4.5 安全停机序列

`safe_stop()`（第290-360行）按以下顺序执行，每步独立 try/except：

1. `Target:FA=0`（流量归零）
2. `Target:CTRL=OFF`（控制关闭）
3. `Target:COOL=OFF`（制冷关闭）
4. `Target:HEAT=OFF`（加热关闭）

### 4.6 超时参数

| 操作 | 超时/参数 | 行号 |
|------|----------|------|
| `_drain` 默认 | drain_s=0.35s, read_timeout_s=0.05s | 52 |
| `_query_collect_lines` | drain_s=0.55s | 64 |
| `fetch_tag_value` | drain_s=0.40s | 164 |
| `ensure_run` | wait_s=2.5s, poll_s=0.25s, tries=2 | 237-239 |
| `verify_runtime_activation` | timeout_s=30.0s, poll_s=1.0s | 261-262 |
| `wait_stopped` | timeout_s=5.0s, poll_s=0.25s | 369-370 |

---

## 5. 露点仪协议

**实现文件**: `src/gas_calibrator/devices/dewpoint_meter.py`  
**类定义**: `dewpoint_meter.py:11-213`

### 5.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 115200 | 17 |
| 默认超时 | 1.0s | 18 |
| 站号 | `"001"` | 19 |

### 5.2 命令格式

**GetCurData 命令**（第45-47行）：

```
{station}_GetCurData_END
```

默认：`001_GetCurData_END`

### 5.3 行结束符变体

`get_current()` 依次尝试 4 种行结束符（第47行）：

| 变体序号 | 行结束符 | 完整命令 |
|---------|---------|---------|
| 1 | `\r\n` | `001_GetCurData_END\r\n` |
| 2 | `\n` | `001_GetCurData_END\n` |
| 3 | `\r` | `001_GetCurData_END\r` |
| 4 | 无 | `001_GetCurData_END` |

### 5.4 响应格式

**帧格式**（解析 `parse_response()`, 第65-97行）：

```
{station}_GetCurData_{dewpoint_c}_{temp_c}_{v3}_{v4}_{v5}_{v6}_{v7}_{rh_pct}_{flag1}_{flag2}_{flag3}_{flag4}_END
```

**字段映射**：

| payload 索引 | 字段名 | 类型 | 说明 |
|-------------|--------|------|------|
| 0 | dewpoint_c | float | 露点温度（°C） |
| 1 | temp_c | float | 环境温度（°C） |
| 7 | rh_pct | float | 相对湿度（%） |
| 8-11 | flags | bool[4] | 4 个状态标志 |

### 5.5 超时参数

| 操作 | 超时 | 重试 | 行号 |
|------|------|------|------|
| `get_current` | 2.0s | 2 次 | 99 |
| `get_current_fast` | 0.35s | — | 151 |

---

## 6. 数字压力计协议

**实现文件**: `src/gas_calibrator/devices/paroscientific.py`  
**类定义**: `paroscientific.py:13-292`

### 6.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 9600 | 19 |
| 默认超时 | 1.0s | 20 |
| 目标 ID | `"01"` | 21 |
| 响应超时 | `max(1.2, timeout)` = 1.2s | 35 |
| 行结束符 | `\r\n` | 52 |

### 6.2 命令格式

**通用命令格式**（`_cmd()`, 第51-52行）：

```
*{dest_id}00{CMD}\r\n
```

默认：`*0100{CMD}\r\n`

| 命令 | 格式 | 说明 |
|------|------|------|
| P3 | `*0100P3\r\n` | 单次压力读取 / 停止连续模式 |
| P4 | `*0100P4\r\n` | 连续压力输出（高速） |
| P7 | `*0100P7\r\n` | 连续压力输出（标准速） |

### 6.3 响应格式

```
*{dest_id}00{value}
```

解析（`_parse_pressure_value()`, 第54-70行）：前 5 字符为头 `*0100`，第 6 字符起为数值。

### 6.4 超时重试参数

| 操作 | 重试 | 间隔 | 行号 |
|------|------|------|------|
| `read_pressure` | 3 次 | 0.1s | 130-131 |
| `read_pressure_fast` | 1 次 | 0.0s | 160-161 |
| `read_pressure_continuous_latest` drain | — | 0.12s | 221 |

---

## 7. 温度箱协议

**实现文件**: `src/gas_calibrator/devices/temperature_chamber.py`  
**类定义**: `temperature_chamber.py:14-337`

### 7.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 9600 | 32 |
| 默认超时 | 1.0s | 49 |
| 协议 | Modbus RTU | — |
| 默认地址 | 1 | 33 |

### 7.2 Modbus 寄存器映射

| 功能 | 寄存器地址 | 类型 | 行号 | 说明 |
|------|-----------|------|------|------|
| 读取当前温度 | 7991 (Input) | 有符号/10 | 163-174 | `_decode_signed_tenth` |
| 读取当前 RH | 7992 (Input) | 无符号/10 | 176-187 | `registers[0] / 10.0` |
| 读取运行状态 | 7990 (Input) | 整数 | 189-204 | 0=停止, 1=运行 |
| 读取设定温度 | 8100 (Holding) | 有符号/10 | 206-217 | — |
| 读取设定 RH | 8101 (Holding) | 无符号/10 | 219-230 | — |
| 写入设定温度 | 8100 (Holding) | 有符号×10 | 232-242 | `_encode_signed_tenth` |
| 写入设定 RH | 8101 (Holding) | 无符号×10 | 244-254 | `int(round(value_pct * 10))` |
| 启动（线圈） | 8000 (Coil) | True | 256-277 | 主路径 |
| 启动（寄存器回退） | 8010 (Holding) | 1 | 268-272 | 回退路径 |
| 停止（线圈） | 8001 (Coil) | True | 279-300 | 主路径 |
| 停止（寄存器回退） | 8010 (Holding) | 2 | 291-295 | 回退路径 |

### 7.3 超时参数

| 参数 | 值 | 行号 |
|------|----|------|
| `_RUN_STATE_POLL_INTERVAL_S` | 0.2s | 17 |
| `_RUN_STATE_POLL_ATTEMPTS` | 10 | 18 |

---

## 8. 测温仪协议

**实现文件**: `src/gas_calibrator/devices/thermometer.py`  
**类定义**: `thermometer.py:11-118`

### 8.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 2400 | 20 |
| 默认超时 | 1.0s | 21 |
| 通信模式 | 被动流（设备持续发送） | — |

### 8.2 数据读取

测温仪为被动流模式，无需主机命令，设备持续发送温度数值。

| 方法 | 行号 | 说明 |
|------|------|------|
| `read_current()` | 80-105 | 先 drain 0.8s，再 readline，再 read_available 回退 |
| `parse_line(line)` | 55-71 | 正则 `r"([+-]?\d+(?:\.\d+)?)"` 提取温度数值 |

### 8.3 超时参数

| 参数 | 值 | 行号 |
|------|----|------|
| `LATEST_FRAME_DRAIN_S` | 0.8s | 14 |
| `LATEST_FRAME_READ_TIMEOUT_S` | 0.01s | 15 |

---

## 9. 继电器协议

**实现文件**: `src/gas_calibrator/devices/relay.py`  
**类定义**: `relay.py:13-257`

### 9.1 通信参数

| 参数 | 值 | 行号 |
|------|----|------|
| 默认波特率 | 38400 | 28 |
| 默认超时 | 1.0s | 45 |
| 协议 | Modbus RTU | — |
| 默认地址 | 1 | 29 |

### 9.2 Modbus 操作

| 操作 | 地址 | 类型 | 行号 | 说明 |
|------|------|------|------|------|
| 读取线圈 | `start`（默认0） | Read Coils, count=1 | 144-155 | 单线圈状态 |
| 设置阀门 | `channel-1` | Write Single Coil | 173-183 | 线圈地址=通道号-1 |
| 批量写线圈 | 连续地址块 | Write Multiple Coils | 204-241 | 自动合并连续地址 |
| 状态读取 | 0, count=8 | Read Coils | 252-253 | 读取前 8 个线圈 |

---

## 10. 模拟层协议

**实现文件**: `src/gas_calibrator/devices/simulation.py`

### 10.1 ReplaySerial

见第1章串口基础协议中的 ReplaySerial 说明。

### 10.2 ReplayModbusClient

| 项目 | 说明 |
|------|------|
| 类定义 | `simulation.py:25-122` |
| 用途 | 模拟 Modbus RTU 通信，用于温度箱和继电器的测试 |
| 机制 | 通过 `script` 列表预编程响应序列，按 method/args/kwargs 匹配 |

**ReplayModbusResponse 字段**（第13-16行）：

| 字段 | 类型 | 默认值 |
|------|------|--------|
| registers | list[int] | [] |
| bits | list[bool] | [] |
| error | bool | False |
| text | str | "" |

---

## 11. 设备通信参数汇总

| 设备 | 协议 | 波特率 | 超时 | 行结束 | 地址/ID |
|------|------|--------|------|--------|---------|
| GasAnalyzer | Serial ASCII | 115200 | 1.0s | `\r\n` | device_id="000", target="FFF" |
| Pace5000 | SCPI over Serial | 9600 | 1.0s | `\n` | — |
| HumidityGenerator | Serial ASCII | 9600 | 1.0s | `\r\n` | — |
| DewpointMeter | Serial ASCII | 115200 | 1.0s | `\r\n`（多变体） | station="001" |
| ParoscientificGauge | Serial ASCII | 9600 | 1.2s | `\r\n` | dest_id="01" |
| TemperatureChamber | Modbus RTU | 9600 | 1.0s | — | addr=1 |
| Thermometer | Serial ASCII（被动流） | 2400 | 1.0s | 流式 | — |
| RelayController | Modbus RTU | 38400 | 1.0s | — | addr=1 |

---

## 文档更新记录

| 日期 | 版本 | 更新内容 |
|------|------|---------|
| 2026-04-15 | v1.0 | 首次创建，覆盖 9 台设备 + 串口基础 + 模拟层的完整协议说明 |
