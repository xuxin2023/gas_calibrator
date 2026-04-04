from gas_calibrator.devices import ReplayModbusClient, ReplayModbusResponse
from gas_calibrator.devices.serial_base import ReplaySerial


def test_replay_serial_supports_expect_and_response_aliases() -> None:
    transport = ReplaySerial(
        script=[
            {
                "expect": "PING\r\n",
                "response": "PONG",
                "buffer": "TAIL",
            }
        ]
    )

    written = transport.write(b"PING\r\n")

    assert written == 6
    assert transport.readline() == b"PONG\r\n"
    assert transport.read(4) == b"TAIL"
    assert transport.events[-1]["method"] == "write"


def test_replay_modbus_client_replays_scripted_calls() -> None:
    client = ReplayModbusClient(
        script=[
            {"method": "connect", "response": True},
            {
                "method": "read_input_registers",
                "args": [7991, 1],
                "kwargs": {"slave": 1},
                "response": {"registers": [271]},
            },
            {
                "method": "write_register",
                "args": [8100, 301],
                "kwargs": {"slave": 1},
                "response": {},
            },
            {"method": "close", "response": None},
        ]
    )

    assert client.connect() is True
    rr = client.read_input_registers(7991, 1, slave=1)
    wr = client.write_register(8100, 301, slave=1)
    client.close()

    assert rr.registers == [271]
    assert wr.isError() is False
    assert client.calls[1]["method"] == "read_input_registers"


def test_replay_modbus_client_detects_call_mismatch() -> None:
    client = ReplayModbusClient(script=[{"method": "read_coils", "args": [0, 8], "response": {"bits": [True]}}])

    try:
        client.write_register(1, 2, slave=1)
    except RuntimeError as exc:
        assert "REPLAY_CALL_MISMATCH" in str(exc)
    else:
        raise AssertionError("expected mismatch error")


def test_replay_modbus_response_string_uses_text() -> None:
    resp = ReplayModbusResponse(error=True, text="ILLEGAL_FUNCTION")
    assert resp.isError() is True
    assert str(resp) == "ILLEGAL_FUNCTION"
