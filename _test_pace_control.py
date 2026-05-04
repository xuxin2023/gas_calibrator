"""Independent PACE pressure controller test script.

K0472 SCPI commands (verified against manual):
  *IDN?                              — identity query
  :SOUR:PRES:LEV:IMM:AMPL <value>   — set target pressure (p.154)
  :SOUR:PRES:LEV:IMM:AMPL:VENT 0    — close vent (p.156)
  :SOUR:PRES:LEV:IMM:AMPL:VENT 1    — open vent
  :OUTP:STAT ON                      — enable output (p.97)
  :OUTP:STAT OFF                     — disable output
  :OUTP:MODE ACT                     — set output mode active
  :SENS:PRES?                        — read pressure (p.112)
  :SYST:ERR?                         — query error (p.179)
  :OUTP:STAT?                        — output state
  :SOUR:PRES:LEV:IMM:AMPL:VENT?     — vent status
"""

import time
import sys
import traceback

try:
    import serial
except ImportError:
    print("pyserial not installed. Install with: pip install pyserial")
    sys.exit(1)

PORT = "COM23"
BAUD = 9600
TIMEOUT = 2.0

# Test below atmospheric since ambient < 1100 hPa
TARGET_PRESSURES = [1000.0, 900.0, 800.0]
STABLE_WINDOW_HPA = 20.0
STABLE_DURATION_S = 10.0
MAX_WAIT_S = 120.0


def send_cmd(ser, cmd: str) -> None:
    """Send SCPI command with proper line ending."""
    full = f"{cmd}\r\n"
    print(f"  TX: {cmd}")
    ser.write(full.encode("ascii"))
    ser.flush()


def query(ser, cmd: str) -> str:
    """Send query and read response."""
    full = f"{cmd}\r\n"
    print(f"  TX: {cmd}")
    ser.write(full.encode("ascii"))
    ser.flush()
    time.sleep(0.2)
    resp = ser.read_until(b"\n").decode("ascii", errors="replace").strip()
    # Read any remaining bytes
    extra = ser.read(ser.in_waiting).decode("ascii", errors="replace").strip()
    if extra:
        resp = resp + "|" + extra if resp else extra
    print(f"  RX: {resp}")
    return resp


def query_error(ser):
    """Query system error."""
    return query(ser, ":SYST:ERR?")


def main():
    print("=" * 60)
    print("PACE 压力控制器 独立控压测试")
    print(f"端口: {PORT}, 波特率: {BAUD}")
    print(f"目标压力序列: {TARGET_PRESSURES} hPa")
    print(f"稳定窗口: ±{STABLE_WINDOW_HPA} hPa, 持续 {STABLE_DURATION_S}s")
    print("=" * 60)

    # ── 1. Connect ──
    print("\n[1] 连接串口...")
    try:
        ser = serial.Serial(PORT, BAUD, timeout=TIMEOUT, bytesize=8, parity="N", stopbits=1)
        print(f"  已连接: {ser.name}")
    except Exception as e:
        print(f"  连接失败: {e}")
        return False

    try:
        # ── 2. Identity ──
        print("\n[2] 查询控制器身份...")
        idn = query(ser, "*IDN?")
        if not idn or "PACE" not in idn.upper():
            print(f"  WARNING: 未识别到 PACE 控制器: {idn}")
        print(f"  身份: {idn}")

        # ── 3. Clear errors ──
        print("\n[3] 清除错误状态...")
        send_cmd(ser, "*CLS")
        time.sleep(0.1)
        err = query_error(ser)
        print(f"  系统错误: {err}")

        # ── 4. Ensure vent is CLOSED for control ──
        print("\n[4] 关闭排气 (VENT 0)...")
        send_cmd(ser, ":SOUR:PRES:LEV:IMM:AMPL:VENT 0")
        time.sleep(0.5)
        vent_status = query(ser, ":SOUR:PRES:LEV:IMM:AMPL:VENT?")
        print(f"  VENT 状态: {vent_status}")
        err = query_error(ser)
        print(f"  系统错误: {err}")

        # ── 5. Set output mode and enable ──
        print("\n[5] 设置输出模式并启用...")
        send_cmd(ser, ":OUTP:MODE ACT")
        time.sleep(0.2)
        send_cmd(ser, ":OUTP:STAT ON")
        time.sleep(0.5)
        out_state = query(ser, ":OUTP:STAT?")
        print(f"  OUTP 状态: {out_state}")
        err = query_error(ser)
        print(f"  系统错误: {err}")

        if "0" in out_state and "1" not in out_state:
            print("  WARNING: 输出未能启用，检查 VENT 状态")
            # Try to clear vent popup
            query(ser, ":SOUR:PRES:LEV:IMM:AMPL:VENT:APOP:STAT?")
            time.sleep(0.3)
            send_cmd(ser, ":OUTP:STAT ON")
            time.sleep(0.5)
            out_state = query(ser, ":OUTP:STAT?")
            print(f"  OUTP 重试状态: {out_state}")

        # ── 6. Test each target pressure ──
        for idx, target in enumerate(TARGET_PRESSURES):
            print(f"\n[6.{idx+1}] 测试目标压力 {target} hPa...")
            print(f"  设置 SOUR:PRES:LEV:IMM:AMPL {target}")
            send_cmd(ser, f":SOUR:PRES:LEV:IMM:AMPL {target}")
            time.sleep(0.3)

            stable_start = None
            stable_count = 0
            start_time = time.time()
            last_pressure = None

            while True:
                elapsed = time.time() - start_time
                pressure_str = query(ser, ":SENS:PRES?")
                try:
                    pressure = float(pressure_str.split("|")[0].strip())
                except (ValueError, AttributeError):
                    pressure = None

                status_str = f"  [{elapsed:6.1f}s] 压力: {pressure}"
                if pressure is not None:
                    status_str += f" hPa, 偏差: {abs(pressure - target):.2f} hPa"
                print(status_str)

                if pressure is not None:
                    last_pressure = pressure
                    if abs(pressure - target) <= STABLE_WINDOW_HPA:
                        if stable_start is None:
                            stable_start = time.time()
                        stable_dur = time.time() - stable_start
                        if stable_dur >= STABLE_DURATION_S:
                            print(f"\n  ✅ 控压成功! {target} hPa 稳定 {stable_dur:.1f}s")
                            break
                    else:
                        stable_start = None

                if elapsed > MAX_WAIT_S:
                    print(f"\n  ❌ 控压失败: {target} hPa 超时 {MAX_WAIT_S}s")
                    err = query_error(ser)
                    print(f"  系统错误: {err}")
                    break

                time.sleep(1.0)

        # ── 7. Cleanup ──
        print("\n[7] 清理: 关闭输出, 排气...")
        send_cmd(ser, ":OUTP:STAT OFF")
        time.sleep(0.5)
        send_cmd(ser, ":SOUR:PRES:LEV:IMM:AMPL:VENT 1")
        time.sleep(0.5)
        err = query_error(ser)
        print(f"  系统错误: {err}")
        print("  清理完成")

    except KeyboardInterrupt:
        print("\n用户中断，正在清理...")
        try:
            send_cmd(ser, ":OUTP:STAT OFF")
            send_cmd(ser, ":SOUR:PRES:LEV:IMM:AMPL:VENT 1")
        except Exception:
            pass
    except Exception as e:
        print(f"\n错误: {e}")
        traceback.print_exc()
        return False
    finally:
        ser.close()
        print("串口已关闭")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
