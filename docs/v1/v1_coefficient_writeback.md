# V1 系数写回闭环协议文档

## 文档信息

- **文档名称**: V1 系数写回闭环协议文档
- **适用范围**: V1 生产基线
- **版本**: v1.0
- **创建日期**: 2026-04-15
- **最后更新**: 2026-04-15
- **对应代码版本**: HEAD 截止 2026-04-15
- **维护人**: Codex

---

## 目录

1. [闭环总览](#1-闭环总览)
2. [Step 1: 准备](#2-step-1-准备)
3. [Step 2: 写入](#3-step-2-写入)
4. [Step 3: 回读](#4-step-3-回读)
5. [Step 4: 比较](#5-step-4-比较)
6. [Step 5: 回滚](#6-step-5-回滚)
7. [Step 6: 审计](#7-step-6-审计)
8. [SENCO值转换与格式化](#8-senco值转换与格式化)
9. [GETCO解析](#9-getco解析)
10. [审计CSV Schema](#10-审计csv-schema)

---

## 1. 闭环总览

**代码定位**: runner.py:21586-21721（`_maybe_write_coefficients`）

系数写回闭环在 `collect_only=false` 且 `coefficients.enabled=true` 时进入，包含 6 步协议序列：

```
Step 1 准备 → Step 2 写入 → Step 3 回读 → Step 4 比较 → Step 5 回滚(失败时) → Step 6 审计
```

**触发条件**：
- `workflow.collect_only` = `false`
- `coefficients.enabled` = `true`
- 拟合成功产生系数结果

---

## 2. Step 1: 准备

**代码定位**: gas_analyzer.py（`_prepare_coefficient_io`）

### 2.1 操作序列

1. 调用 `set_comm_way(0)` — 切换到被动通信方式（`SETCOMWAY,YGAS,FFF,0\r\n`）
2. 等待 `COEFFICIENT_COMM_QUIET_DELAY_S` = 0.15s（静默延迟）
3. 调用 `flush_input()` — 清除输入缓冲区

### 2.2 参数

| 参数 | 值 | 行号 |
|------|----|------|
| COEFFICIENT_COMM_QUIET_DELAY_S | 0.15s | gas_analyzer.py:27 |

### 2.3 失败处理

SETCOMWAY 失败时 `except Exception: pass`（静默继续），不阻断后续写入。

---

## 3. Step 2: 写入

**代码定位**: gas_analyzer.py（`set_senco`）、runner.py:21769-21793（`_coerce_senco_values`）

### 3.1 值转换

`_coerce_senco_values()` 将拟合结果转换为 SENCO 可接受的格式：

- 输入：dict/list/tuple 类型的系数载荷
- 校验：类型必须为 dict/list/tuple，载荷不能为空，最多 6 个值
- 输出：最多 6 个 float 值的 tuple

### 3.2 格式化

`senco_format.format_senco_value()` 将每个系数值格式化为 SENCO 字符串：

- 格式：1位整数 + 5位小数 + 2位指数，如 `1.23456e-03`
- 零值归一化为 `"0.00000e00"`
- 非有限值抛出 `ValueError`

### 3.3 命令发送

```
SENCO{index},YGAS,FFF,{C0},{C1},...,{C5}\r\n
```

- `index`：系数组号
- `C0`~`C5`：经 SENCO 格式化后的系数值字符串

### 3.4 ACK 等待

| 参数 | 值 | 行号 |
|------|----|------|
| CONFIG_ACK_RETRY_COUNT | 1 | gas_analyzer.py:23 |
| CONFIG_ACK_RETRY_DELAY_S | 0.1s | gas_analyzer.py:24 |

ACK 成功：`YGAS,{ID},T`  
ACK 失败：`YGAS,{ID},F` 或超时，返回 `False`

---

## 4. Step 3: 回读

**代码定位**: gas_analyzer.py（`read_coefficient_group`）

### 4.1 操作序列

1. 调用 `_prepare_coefficient_io()` — 再次准备（切被动+静默+flush）
2. 发送 GETCO 命令：`GETCO{index},YGAS,FFF\r\n`
3. 多行扫描：在超时窗口内持续 readline，搜索含 `C0:` 的行
4. 正则解析匹配行

### 4.2 GETCO 命令格式

```
GETCO{index},YGAS,FFF\r\n
```

### 4.3 响应格式

```
C0:{value},C1:{value},C2:{value},...
```

### 4.4 解析正则

```python
_COEFFICIENT_TOKEN_RE = re.compile(r"C(?P<index>\d+)\s*:\s*(?P<value>[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)")
```

**代码定位**: gas_analyzer.py:31-33

### 4.5 超时重试参数

| 参数 | 值 | 行号 |
|------|----|------|
| COEFFICIENT_READ_RETRY_COUNT | 2 | gas_analyzer.py:28 |
| COEFFICIENT_READ_DELAY_S | 0.1s | gas_analyzer.py:29 |
| COEFFICIENT_READ_TIMEOUT_S | 0.3s | gas_analyzer.py:30 |

### 4.6 失败处理

重试耗尽后仍无法解析有效系数行 → 抛出 `RuntimeError("GETCO{index} read failed: {last_line}")`

---

## 5. Step 4: 比较

**代码定位**: senco_format.py:40-53（`senco_readback_matches`）

### 5.1 比较逻辑

1. 将 `expected` 值经 SENCO 格式化后取 round-trip 值（`rounded_senco_values`）
2. 将 `actual` 回读值转为 `float`
3. 逐元素比较：`abs(got - exp) <= atol`

### 5.2 容差参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| atol | 1e-9 | 绝对容差（极小，要求几乎精确匹配） |

### 5.3 比较结果

- 全部匹配 → `verify_status = "success"`
- 任一不匹配 → 进入回滚路径

---

## 6. Step 5: 回滚

**代码定位**: runner.py:21586-21721

### 6.1 回滚触发条件

GETCO 回读值与目标值不匹配（`verify_status != "success"`）

### 6.2 回滚操作

1. 重新写入旧系数（写前快照值）：`set_senco(index, old_C0, ..., old_C5)`
2. 再次回读验证：`read_coefficient_group(index)`
3. 比较回读值与旧系数

### 6.3 回滚结果

| 回滚结果 | verify_status | 处理 |
|---------|--------------|------|
| 回滚成功 | `rollback_status = "success"` | 日志 `"SENCO{n} writeback failed and rolled back safely"`，抛出 RuntimeError |
| 回滚失败 | `rollback_status != "success"` | 日志 `"SENCO{n} writeback failed"`，抛出 RuntimeError |
| unsafe | `result["unsafe"] = True` | 抛出 `RuntimeError("Coefficient writeback unsafe: ...")` |

---

## 7. Step 6: 审计

**代码定位**: runner.py:21723-21766（`_persist_coefficient_write_result`）

### 7.1 审计记录写入

无论写回成功或失败，均调用 `_persist_coefficient_write_result()` 写入审计 CSV。

### 7.2 文件名模式

```
coefficient_writeback_{timestamp}.csv
```

### 7.3 审计内容

审计记录包含完整的系数向量链路：

- 写入前系数（快照值）
- 目标系数（拟合结果）
- 回读系数（GETCO结果）
- 回滚目标系数（旧系数）
- 回读回滚系数（回滚后GETCO结果）
- 各阶段状态标记

---

## 8. SENCO值转换与格式化

**代码定位**: senco_format.py:9-27

### 8.1 格式化规则

| 规则 | 说明 |
|------|------|
| 尾数 | 1位整数 + 5位小数（共6位有效数字） |
| 指数 | 始终2位数字，正指数省略 `+` 号 |
| 零值 | 归一化为 `"0.00000e00"` |
| 非有限值 | 抛出 `ValueError` |

### 8.2 格式化示例

| 输入值 | SENCO字符串 |
|--------|------------|
| 0.00123456 | `1.23456e-03` |
| 987.654 | `9.87654e02` |
| 0.0 | `0.00000e00` |
| -0.0054321 | `-5.43210e-03` |

### 8.3 Round-trip

`rounded_senco_values()`（senco_format.py:34-37）：先格式化为 SENCO 字符串，再解析回 float，用于回读比较时消除格式化精度差异。

---

## 9. GETCO解析

**代码定位**: gas_analyzer.py:375-388

### 9.1 解析流程

1. 在超时窗口内持续 readline
2. 搜索含 `C0:` 的行
3. 对匹配行应用正则 `_COEFFICIENT_TOKEN_RE` 提取所有 `C{index}:{value}` 对
4. 返回 dict `{index: value}`

### 9.2 正则定义

```python
_COEFFICIENT_TOKEN_RE = re.compile(
    r"C(?P<index>\d+)\s*:\s*(?P<value>[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
)
```

**代码定位**: gas_analyzer.py:31-33

---

## 10. 审计CSV Schema

### 10.1 文件名

```
coefficient_writeback_{timestamp}.csv
```

### 10.2 核心字段

| 字段 | 类型 | 说明 |
|------|------|------|
| analyzer | string | 分析仪标识 |
| coefficient_group | int | 系数组号 |
| write_timestamp | string | 写入时间戳 |
| before_C0 ~ before_C5 | float | 写入前系数（快照） |
| target_C0 ~ target_C5 | float | 目标系数（拟合结果） |
| readback_C0 ~ readback_C5 | float | 回读系数（GETCO结果） |
| verify_status | string | 验证状态：success/failed |
| rollback_target_C0 ~ rollback_target_C5 | float | 回滚目标系数（旧系数） |
| rollback_readback_C0 ~ rollback_readback_C5 | float | 回滚后回读系数 |
| rollback_status | string | 回滚状态：success/failed/none |
| unsafe | bool | 是否unsafe |
| failure_reason | string | 失败原因 |

---

## 文档更新记录

| 日期 | 版本 | 更新内容 |
|------|------|---------|
| 2026-04-15 | v1.0 | 首次创建，覆盖6步协议序列、SENCO格式化、GETCO解析、审计Schema |
