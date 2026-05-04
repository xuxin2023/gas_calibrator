"""P4 快速读真机诊断脚本 — 只读，不修改业务代码"""
import serial
import time, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

COM_PORT = "COM22"
TIMEOUT_S = 0.3

def hex_dump(data: bytes) -> str:
    return " ".join(f"{b:02X}" for b in data)

def try_p4_raw(ser: serial.Serial, label: str) -> bytes:
    ser.reset_input_buffer()
    ser.write(b"*0100P4\r\n")
    ser.flush()
    deadline = time.time() + TIMEOUT_S
    raw = b""
    while time.time() < deadline:
        waiting = ser.in_waiting
        if waiting > 0:
            raw += ser.read(waiting)
        else:
            time.sleep(0.01)
    print(f"\n--- {label} ---")
    print(f"  RAW (hex): {hex_dump(raw) if raw else '(empty)'}")
    print(f"  RAW (text): {raw!r}")
    if raw:
        try:
            stripped = raw.decode("ascii", errors="replace").strip()
            print(f"  DECODED: {stripped}")
            for line in stripped.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith("*"):
                    try:
                        val = float(line[1:])
                        pressure_hpa = val / 1000.0
                        print(f"  → pressure_hpa = {pressure_hpa:.4f}")
                    except ValueError:
                        print(f"  → cannot parse: {line}")
        except Exception as e:
            print(f"  DECODE ERROR: {e}")
    return raw

def try_p3_raw(ser: serial.Serial, label: str) -> bytes:
    ser.reset_input_buffer()
    ser.write(b"*0100P3\r\n")
    ser.flush()
    deadline = time.time() + 1.0
    raw = b""
    while time.time() < deadline:
        waiting = ser.in_waiting
        if waiting > 0:
            raw += ser.read(waiting)
        else:
            time.sleep(0.02)
    print(f"\n--- {label} ---")
    print(f"  RAW (hex): {hex_dump(raw) if raw else '(empty)'}")
    print(f"  RAW (text): {raw!r}")
    if raw:
        try:
            stripped = raw.decode("ascii", errors="replace").strip()
            print(f"  DECODED: {stripped}")
            for line in stripped.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith("*"):
                    try:
                        val = float(line[1:])
                        pressure_hpa = val / 1000.0
                        print(f"  → pressure_hpa = {pressure_hpa:.4f}")
                    except ValueError:
                        print(f"  → cannot parse: {line}")
        except Exception as e:
            print(f"  DECODE ERROR: {e}")
    return raw

def main():
    print(f"=== P4 真机诊断: COM={COM_PORT}, timeout={TIMEOUT_S}s ===")
    
    # 方案1: 通过 V2 设备栈 (有完整初始化)
    print("\n[方案1] 通过 V2 Paroscientific 驱动...")
    try:
        from gas_calibrator.devices.paroscientific import Paroscientific
        from gas_calibrator.devices.serial_base import SerialDevice
        dev = SerialDevice(COM_PORT, 9600)
        dev.open()
        gauge = Paroscientific(dev)
        
        print("  驱动已初始化，尝试 P3...")
        try:
            p3 = gauge.read_pressure_fast()
            print(f"  ✓ P3 = {p3} hPa")
        except Exception as e:
            print(f"  ✗ P3 异常: {e}")
        
        print("  尝试 P4...")
        p4 = gauge.read_pressure_p4(response_timeout_s=0.3)
        if p4 is not None:
            print(f"  ✓ P4 = {p4} hPa")
        else:
            print("  ✗ P4 返回 None")
        
        print("  尝试 read_pressure (默认路径)...")
        try:
            rp = gauge.read_pressure()
            print(f"  ✓ read_pressure = {rp} hPa")
        except Exception as e:
            print(f"  ✗ read_pressure 异常: {e}")
        
        dev.close()
    except ImportError as e:
        print(f"  V2 设备栈导入失败: {e}")
    except Exception as e:
        print(f"  方案1 失败: {e}")
    
    # 方案2: 裸 serial + P0 初始化
    print("\n[方案2] 裸 serial + P0 初始化...")
    with serial.Serial(COM_PORT, 9600, timeout=0.1) as ser:
        print(f"  已打开 {COM_PORT}")
        
        # 进入连续模式 P0
        ser.reset_input_buffer()
        ser.write(b"*0100P0\r\n")
        ser.flush()
        time.sleep(0.5)
        # 读取连续输出以确认 P0 工作
        p0_data = ser.read(ser.in_waiting)
        if p0_data:
            print(f"  P0 连续输出 ({len(p0_data)} bytes): {hex_dump(p0_data[:80])}")
        else:
            print("  P0 连续输出: (empty)")
        
        # P3 与 P0 的数据可能混在一起，先清缓冲再发 P3
        ser.reset_input_buffer()
        time.sleep(0.05)
        _ = ser.read(ser.in_waiting)
        
        try_p3_raw(ser, "方案2: P0→清缓冲→P3")
        
        ser.reset_input_buffer()
        time.sleep(0.05)
        _ = ser.read(ser.in_waiting)
        
        try_p4_raw(ser, "方案2: P0→清缓冲→P4")
    
    # 方案3: 冷启动直接 P3/P4 (无 P0)
    print("\n[方案3] 冷启动直接 P3 / P4...")
    with serial.Serial(COM_PORT, 9600, timeout=0.1) as ser:
        print(f"  已打开 {COM_PORT} (冷启动)")
        try_p3_raw(ser, "方案3: 直接 P3")
        try_p4_raw(ser, "方案3: 直接 P4")

if __name__ == "__main__":
    main()
