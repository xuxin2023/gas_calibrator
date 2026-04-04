from gas_calibrator.devices import temperature_chamber


class _Resp:
    def __init__(self, registers=None, error=False):
        self.registers = registers or []
        self._error = error

    def isError(self):
        return self._error


class _FakeClient:
    def __init__(self, **_kwargs):
        self.calls = []
        self.run_state = 0
        self.connected = False
        self.set_temp = 271
        self.set_rh = 500

    def connect(self):
        self.connected = True
        return True

    def close(self):
        self.connected = False

    def read_input_registers(self, address, count, **_kwargs):
        self.calls.append(("read_input_registers", address, count))
        if address == 7990:
            return _Resp([self.run_state])
        if address == 7991:
            return _Resp([271])
        if address == 7992:
            return _Resp([500])
        return _Resp([0])

    def read_holding_registers(self, address, count, **_kwargs):
        self.calls.append(("read_holding_registers", address, count))
        if address == 8100:
            return _Resp([self.set_temp])
        if address == 8101:
            return _Resp([self.set_rh])
        return _Resp([0])

    def write_register(self, address, value, **_kwargs):
        self.calls.append(("write_register", address, value))
        if address == 8010 and int(value) == 1:
            self.run_state = 1
        if address == 8010 and int(value) == 2:
            self.run_state = 0
        if address == 8100:
            self.set_temp = int(value)
        if address == 8101:
            self.set_rh = int(value)
        return _Resp([address, value])

    def write_coil(self, address, value, **_kwargs):
        self.calls.append(("write_coil", address, bool(value)))
        return _Resp([address, int(bool(value))])


def test_start_stop_with_fallback_register(monkeypatch) -> None:
    holder = {}

    def _factory(**kwargs):
        cli = _FakeClient(**kwargs)
        holder["client"] = cli
        return cli

    monkeypatch.setattr(temperature_chamber, "ModbusSerialClient", _factory)

    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1)
    dev.open()
    dev.start()
    dev.stop()
    dev.close()

    calls = holder["client"].calls
    assert ("write_coil", 8000, True) in calls
    assert ("write_register", 8010, 1) in calls
    assert ("write_coil", 8001, True) in calls
    assert ("write_register", 8010, 2) in calls


def test_temperature_chamber_supports_read_write_and_status_with_injected_client() -> None:
    client = _FakeClient()
    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1, client=client)

    data = dev.read()
    dev.write({"temp_c": 30.1, "rh_pct": 55.2, "start": True, "stop": True})
    status = dev.status()

    assert data == {"temp_c": 27.1, "rh_pct": 50.0, "run_state": 0}
    assert status["ok"] is True
    assert ("write_register", 8100, 301) in client.calls
    assert ("write_register", 8101, 552) in client.calls
    assert ("write_coil", 8000, True) in client.calls
    assert ("write_coil", 8001, True) in client.calls


def test_temperature_chamber_can_read_back_setpoints() -> None:
    client = _FakeClient()
    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1, client=client)

    dev.set_temp_c(30.1)
    dev.set_rh_pct(55.2)

    assert dev.read_set_temp_c() == 30.1
    assert dev.read_set_rh_pct() == 55.2
    assert ("read_holding_registers", 8100, 1) in client.calls
    assert ("read_holding_registers", 8101, 1) in client.calls


def test_temperature_chamber_encodes_negative_set_temp_as_twos_complement() -> None:
    client = _FakeClient()
    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1, client=client)

    dev.set_temp_c(-20.0)

    assert ("write_register", 8100, 65336) in client.calls
    assert client.set_temp == 65336


def test_temperature_chamber_decodes_negative_temperature_registers() -> None:
    client = _FakeClient()
    client.set_temp = 65306  # -23.0C in 16-bit two's complement with one decimal

    class _NegativeTempClient(_FakeClient):
        def read_input_registers(self, address, count, **_kwargs):
            self.calls.append(("read_input_registers", address, count))
            if address == 7990:
                return _Resp([self.run_state])
            if address == 7991:
                return _Resp([65336])  # -20.0C
            if address == 7992:
                return _Resp([500])
            return _Resp([0])

    neg_client = _NegativeTempClient()
    neg_client.set_temp = 65306
    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1, client=neg_client)

    assert dev.read_temp_c() == -20.0
    assert dev.read_set_temp_c() == -23.0


def test_temperature_chamber_open_raises_when_connect_fails() -> None:
    client = _FakeClient()
    client.connect = lambda: False  # type: ignore[method-assign]
    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1, client=client)

    try:
        dev.open()
    except RuntimeError as exc:
        assert "CONNECT_FAILED" in str(exc)
    else:
        raise AssertionError("expected CONNECT_FAILED")


def test_temperature_chamber_read_temp_raises_on_modbus_error() -> None:
    class _ErrorClient(_FakeClient):
        def read_input_registers(self, address, count, **_kwargs):
            return _Resp(error=True)

    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1, client=_ErrorClient())

    try:
        dev.read_temp_c()
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected modbus read error")


def test_temperature_chamber_stop_raises_when_state_never_changes() -> None:
    class _StuckStopClient(_FakeClient):
        def write_register(self, address, value, **_kwargs):
            self.calls.append(("write_register", address, value))
            if address == 8010 and int(value) == 1:
                self.run_state = 1
            return _Resp([address, value])

    dev = temperature_chamber.TemperatureChamber("COM1", 9600, addr=1, client=_StuckStopClient())
    dev.start()

    try:
        dev.stop()
    except RuntimeError as exc:
        assert "STOP_STATE_MISMATCH" in str(exc)
    else:
        raise AssertionError("expected STOP_STATE_MISMATCH")
