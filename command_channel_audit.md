# 079 Command Channel Audit

## 范围

- 只审计 V1。
- 只关注 `COM39 / 079` 命令通道问题，并用 `COM35 / 012` 做只读对照。
- 不包含任何 `SENCO` 写入、不包含重拟、不包含微扰。

## 结论先行

这次代码审计加真实只读矩阵之后，主因已经不是“PC TX 到设备不通”。更像是：

1. 旧 probe / safe readback / state matrix 会话里，`GETCO` 虽然已经发出，但大多是在 active legacy `YGAS` 流里只抓到了流数据，没有命中明确的 `C0:` 行。
2. 真正最关键的变量不是单纯 terminator，也不是 target，而是 **命令形式**：
   - `GETCO,YGAS,{target},{group}` 这类 parameterized 形式可以在 `079` 和 `012` 上稳定打到 explicit `C0`
   - `GETCO1,YGAS,{target}` / `GETCO7,YGAS,{target}` 这类 compact 形式明显更差，常常只拿到 legacy `YGAS` 流，或者夹杂非 `YGAS` 垃圾片段
3. `MODE / SETCOMWAY / FTD / AVERAGE` 这类配置命令在历史 transcript 里仍然没有看到可靠 ACK，所以“配置写命令 ACK 通道”依旧不可信；但 **GETCO 只读查询通道本身不是全断的**。

## 代码层审计

### 1. 实际发到串口的 bytes 是什么

源头在 [gas_analyzer.py](D:/gas_calibrator/src/gas_calibrator/devices/gas_analyzer.py) 和 [serial_base.py](D:/gas_calibrator/src/gas_calibrator/devices/serial_base.py)。

- `SerialDevice.write(...)` 会把字符串按 ASCII 编码后直接发出。
- 历史 V1 driver 默认发送的都是文本命令加 `\\r\\n`。

关键示例：

- `SETCOMWAY,YGAS,FFF,0\r\n`
  - hex: `534554434f4d5741592c594741532c4646462c300d0a`
- `MODE,YGAS,FFF,1\r\n`
  - hex: `4d4f44452c594741532c4646462c310d0a`
- `FTD,YGAS,FFF,10\r\n`
  - hex: `4654442c594741532c4646462c31300d0a`
- compact `GETCO1,YGAS,079\r\n`
  - hex: `474554434f312c594741532c3037390d0a`
- parameterized `GETCO,YGAS,079,1\r\n`
  - hex: `474554434f2c594741532c3037392c310d0a`
- compact `GETCO7,YGAS,079\r\n`
  - hex: `474554434f372c594741532c3037390d0a`
- parameterized `GETCO,YGAS,079,7\r\n`
  - hex: `474554434f2c594741532c3037392c370d0a`

这轮新增的只读矩阵还额外验证了同一命令在 `LF / CRLF / CR / none` 四种结尾下的 raw bytes。

### 2. line ending 是 LF / CRLF / CR / none？

- 旧 V1 驱动默认是 `CRLF`
  - 例如 `capture_getco_command(...)` 内部固定 `self.ser.write(payload + "\r\n")`
  - `_send_config(...)` 也是 `payload + "\r\n"`
- 新增的 `run_v1_command_channel_matrix.py` 同时测试了：
  - `LF`
  - `CRLF`
  - `CR`
  - `none`

真实矩阵结果表明，`079` 和 `012` 的 parameterized `GETCO` 在四种 line ending 下都能打到 explicit `C0`，因此 **terminator 不是主因**。

### 3. 是否所有命令都用了同一套 terminator？

- 历史 V1 工具链：基本是，同一套 `CRLF`
- 这轮命令矩阵：不是，故意分开测了 `LF / CRLF / CR / none`

所以旧结论里“完全无响应”不能简单归咎于 `CRLF` 错了，因为 `CRLF` 本身并没有排除 explicit `C0` 的可能性。

### 4. device_id / target 是 079 / FFF / 000 中哪一个？

历史和这轮测试都覆盖过：

- `079`：实际设备 ID
- `FFF`：broadcast target
- `000`：占位/探测 target

真实矩阵结果：

- `079` 上，`GETCO,YGAS,079,1` 和 `GETCO,YGAS,079,7` 都打到 explicit `C0`
- `079` 上，`GETCO,YGAS,FFF,1` 和 `GETCO,YGAS,FFF,7` 也能打到 explicit `C0`
- `000` 基本没给出有效系数 readback

所以 **target 不是主因**；`079` 和 `FFF` 都可用。

### 5. compact GETCO 和 parameterized GETCO 的实际 bytes 分别是什么？

group 1:

- compact
  - string: `GETCO1,YGAS,079\r\n`
  - hex: `474554434f312c594741532c3037390d0a`
- parameterized
  - string: `GETCO,YGAS,079,1\r\n`
  - hex: `474554434f2c594741532c3037392c310d0a`

group 7:

- compact
  - string: `GETCO7,YGAS,079\r\n`
  - hex: `474554434f372c594741532c3037390d0a`
- parameterized
  - string: `GETCO,YGAS,079,7\r\n`
  - hex: `474554434f2c594741532c3037392c370d0a`

真实只读矩阵显示：

- parameterized 明显更可靠
- compact 更容易只拿到 legacy stream，或者拿到被污染/截断的非 `YGAS` 片段

### 6. 命令发送前后是否 flush / drain 过？

是，历史工具和当前驱动里都有。

主要链路：

- `_prepare_coefficient_io()`
  - `SETCOMWAY 0`
  - quiet delay
  - `flush_input()`
- `capture_getco_command(...)`
  - 进入循环前先 `flush_input()`
  - 每次发送后：
    - `readline()`
    - `drain_input_nonblock(...)`

所以旧工具并不是“完全不清流就发”；它有 quiet + flush。但在真实设备上，这套节奏仍然经常只抓到 legacy stream。

### 7. 有没有可能命令被 active stream 淹没，但设备其实有短 ACK？

有这个可能，而且现在更像是两类情况并存：

- 对 `GETCO`：
  - 不是完全被淹没，因为这轮真实矩阵已经在 active legacy stream 背景下抓到了 explicit `C0`
  - 说明设备会回 query response，只是旧会话抓取窗口没有稳定命中
- 对 `MODE / SETCOMWAY / FTD / AVERAGE`：
  - 历史 transcript 里依然只看到持续 `YGAS` 流，没有可靠 ACK
  - 所以这些命令的 ACK 的确可能被流淹没，或者 ACK 形式并不符合当前 `_is_success_ack(...)` 规则

也就是说：

- `GETCO` 通道不是死的
- 配置命令 ACK 通道仍然是可疑区

### 8. 是否存在“只能收到设备主动流，电脑发命令设备不响应”的证据？

旧证据曾经支持这个怀疑，但这轮新矩阵已经把它推翻了一半。

现在更准确的表述应该是：

- 设备主动流 `YGAS` 一直可见，证明设备 TX 通
- 电脑发出的 query 命令至少一部分确实被设备处理了，因为已经抓到真实 explicit `C0`
- 但电脑发出的配置命令当前仍缺乏可靠 ACK 证据

因此不能再笼统说“PC TX 到设备不通”；更像是：

- **query 通道部分可用**
- **配置 ACK/会话管理通道仍不稳定或未被当前 parser 正确识别**

## 历史 raw log 复盘

### `getco_probe_raw.log`

见 [getco_probe_raw.log](D:/gas_calibrator/logs/run_20260411_204123/runtime_micro_perturb_079_20260420_103526/getco_probe_20260420_110531/getco_probe_raw.log)

能确认：

- 旧 probe 已经发了 compact 和 parameterized 两种 GETCO
- 当时抓到的 `response_bytes_hex` 基本只有 legacy `YGAS` 流
- `GETCO7,YGAS,079` 还出现了明显的脏字节/混杂片段

这说明旧 probe 不是没发命令，而是 **没命中有效 readback**。

### `safe_readback_raw.log`

见 [safe_readback_raw.log](D:/gas_calibrator/logs/run_20260411_204123/runtime_micro_perturb_079_20260420_103526/safe_readback_session_20260420_114015/safe_readback_raw.log)

能确认：

- 会话前执行了 `SETCOMWAY,YGAS,FFF,0\r\n`
- 然后反复发 `GETCO,YGAS,079,1\r\n` 和 `GETCO,YGAS,FFF,1\r\n`
- 每次只抓到 `YGAS` 流，没有 explicit `C0`

这进一步说明：旧 safe readback 的失败不是“命令没发”，而是 **query response 抓取失败**。

### `readback_state_matrix_raw.log`

见 [readback_state_matrix_raw.log](D:/gas_calibrator/logs/run_20260411_204123/runtime_micro_perturb_079_20260420_103526/readback_state_matrix_20260420_133118/readback_state_matrix_raw.log)

能确认：

- 它发过 `GETCO,YGAS,079,1/3/7/8\r\n`
- 也发过 `GETCO,YGAS,FFF,1/3/7/8\r\n`
- 还发过 `MODE / FTD / AVERAGE / SETCOMWAY`
- 但 raw log 里仍然主要是 legacy `YGAS` 流，没有可靠 ACK，也没有 explicit `C0`

这说明旧 state matrix 失败同样不是 target 错或根本没发。

### `writeback_raw_transcript.log`

见 [writeback_raw_transcript.log](D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_after_runtime_gate_20260420_101715/noop_writeback_truth_probe_20260420_execute/writeback_raw_transcript.log)

能确认：

- writeback truth probe 在 group 1 prewrite 阶段反复发 `GETCO,YGAS,079,1\r\n`
- 这一轮仍只抓到 `YGAS` 流，没抓到 explicit `C0`
- 后续 `MODE / FTD / AVERAGE / SETCOMWAY` 也没有可靠 ACK

所以 truth probe 失败，不代表 query 通道完全不可用；更像是 **当时那次 driver-managed 会话窗口没有命中 explicit `C0`**。

## 新的只读命令矩阵结果

产物目录：
[command_channel_matrix_compare_20260420_1600](D:/gas_calibrator/logs/run_20260411_204123/command_channel_matrix_compare_20260420_1600)

核心工件：

- [command_channel_matrix.csv](D:/gas_calibrator/logs/run_20260411_204123/command_channel_matrix_compare_20260420_1600/command_channel_matrix.csv)
- [command_channel_matrix_summary.json](D:/gas_calibrator/logs/run_20260411_204123/command_channel_matrix_compare_20260420_1600/command_channel_matrix_summary.json)
- [command_channel_matrix_raw.log](D:/gas_calibrator/logs/run_20260411_204123/command_channel_matrix_compare_20260420_1600/command_channel_matrix_raw.log)

总结果：

- `only_legacy_ygas_stream = 78`
- `explicit_c0 = 29`
- `non_ygas_response = 12`
- `ambiguous = 1`
- `ack = 0`

### COM39 / 079

- 总计 60 行
- `explicit_c0 = 13`
- `only_legacy_ygas_stream = 44`
- `non_ygas_response = 2`
- `ambiguous = 1`
- `ack = 0`

命中 explicit `C0` 的代表行：

- `GETCO,YGAS,079,1` with `LF / CRLF / CR / none`
- `GETCO,YGAS,079,7` with `CRLF / CR / none`
- `GETCO,YGAS,FFF,1` with `LF / CRLF / CR / none`
- `GETCO,YGAS,FFF,7` with `CRLF / none`

真实样例：

- `C0:19846.2,C1:-38766.1,C2:22273.1,C3:-3565.31,C4:0,C5:0`
- `C0:-1.50402,C1:0.975407,C2:0.00190803,C3:-4.78878e-05`

### COM35 / 012

- 总计 60 行
- `explicit_c0 = 16`
- `only_legacy_ygas_stream = 34`
- `non_ygas_response = 10`
- `ack = 0`

命中 explicit `C0` 的代表行：

- `GETCO,YGAS,012,1` with `LF / CRLF / CR / none`
- `GETCO,YGAS,012,7` with `LF / CRLF / CR / none`

真实样例：

- `C0:-17305.6,C1:54412.4,C2:-46287.3,C3:11438.4,C4:0,C5:0`
- `C0:77.5353,C1:-6.87283,C2:0.251262,C3:-0.00258579`

## 主因判断

当前最合理的主因排序：

1. **命令形式问题是第一主因**
   - parameterized `GETCO,YGAS,{target},{group}` 明显优于 compact `GETCO1/7,...`
2. **会话抓取窗口问题是第二主因**
   - 同样是 parameterized `GETCO`，旧 probe/safe/state matrix/controlled truth probe 里常常没抓到 explicit `C0`
   - 这说明 query response 可能短、容易和 legacy 流交错，被原会话节奏错过
3. **target 不是第一主因**
   - `079` 和 `FFF` 都能回 explicit `C0`
4. **terminator 不是第一主因**
   - `LF / CRLF / CR / none` 都能命中 explicit `C0`
5. **配置命令 ACK 通道仍未打通**
   - `MODE / SETCOMWAY / FTD / AVERAGE` 还没有可靠 ACK 证据

## 对 079 当前结论的影响

这轮审计能确认：

- `079` 不是完全不能响应查询命令
- `079` 也不是明显的个体坏设备，因为 `012` 与它表现出同类模式
- 真正的问题是：旧 readback 会话没有拿到稳定的 strict explicit-`C0` 证据

所以：

- 不能据此反推“candidate 一定已经写入”
- 也不能再把旧 `6/6` 当成可靠闭环
- 后续如果要回到 strict explicit-`C0` readback，应优先沿着 **parameterized GETCO + 更稳的抓取窗口** 去修，而不是继续怀疑 PC TX 完全不通
