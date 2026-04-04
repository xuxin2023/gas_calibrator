from gas_calibrator.devices import relay


class _Resp:
    def __init__(self, bits=None, error=False):
        self.bits = bits or []
        self._error = error

    def isError(self):
        return self._error


class _FakeRelayClient:
    def __init__(self, connected=True, read_error=False):
        self.connected = connected
        self.read_error = read_error
        self.calls = []

    def connect(self):
        self.calls.append(("connect",))
        return self.connected

    def close(self):
        self.calls.append(("close",))

    def read_coils(self, address, count, **kwargs):
        self.calls.append(("read_coils", address, count, kwargs))
        if self.read_error:
            return _Resp(error=True)
        return _Resp(bits=[True, False, True, False, False, False, False, False])

    def write_coil(self, address, value, **kwargs):
        self.calls.append(("write_coil", address, bool(value), kwargs))
        return _Resp()

    def write_coils(self, address, values=None, **kwargs):
        self.calls.append(("write_coils", address, list(values or []), kwargs))
        return _Resp()


def test_relay_controller_supports_injected_client_and_interface() -> None:
    client = _FakeRelayClient()
    dev = relay.RelayController("COM1", addr=2, client=client)

    dev.connect()
    bits = dev.read()
    dev.write((2, True))
    status = dev.status()
    dev.close()

    assert bits[:3] == [True, False, True]
    assert status["coils"][:3] == [True, False, True]
    assert ("write_coil", 1, True, {"slave": 2}) in client.calls or ("write_coil", 1, True, {"unit": 2}) in client.calls or ("write_coil", 1, True, {"device_id": 2}) in client.calls


def test_relay_controller_open_raises_when_connect_fails() -> None:
    dev = relay.RelayController("COM1", client=_FakeRelayClient(connected=False))

    try:
        dev.open()
    except RuntimeError as exc:
        assert "CONNECT_FAILED" in str(exc)
    else:
        raise AssertionError("expected CONNECT_FAILED")


def test_relay_controller_read_coils_raises_on_modbus_error() -> None:
    dev = relay.RelayController("COM1", client=_FakeRelayClient(read_error=True))

    try:
        dev.read_coils()
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected modbus read failure")


def test_relay_controller_bulk_write_uses_write_coils_for_contiguous_channels() -> None:
    client = _FakeRelayClient()
    dev = relay.RelayController("COM1", addr=2, client=client)

    dev.set_valves_bulk([(2, True), (3, False), (4, True)])

    assert ("write_coils", 1, [True, False, True], {"slave": 2}) in client.calls or (
        "write_coils",
        1,
        [True, False, True],
        {"unit": 2},
    ) in client.calls or (
        "write_coils",
        1,
        [True, False, True],
        {"device_id": 2},
    ) in client.calls


def test_relay_controller_bulk_write_falls_back_when_client_has_no_write_coils() -> None:
    class _NoBulkClient(_FakeRelayClient):
        write_coils = None

    client = _NoBulkClient()
    dev = relay.RelayController("COM1", client=client)

    dev.set_valves_bulk([(2, True), (4, False)])

    assert ("write_coil", 1, True, {"slave": 1}) in client.calls or ("write_coil", 1, True, {"unit": 1}) in client.calls or ("write_coil", 1, True, {"device_id": 1}) in client.calls
    assert ("write_coil", 3, False, {"slave": 1}) in client.calls or ("write_coil", 3, False, {"unit": 1}) in client.calls or ("write_coil", 3, False, {"device_id": 1}) in client.calls
