from gas_calibrator.v2.ui_v2.diagnostics.redact_helpers import redact_mapping, redact_text


def test_redact_helpers_mask_paths_ports_and_sensitive_keys() -> None:
    payload = redact_mapping(
        {
            "token": "abc123",
            "path": r"C:\Users\alice\secret\config.json",
            "port": "COM9",
            "nested": {"password": "pw", "note": r"D:\logs\capture.txt"},
        }
    )
    text = redact_text(r"Using COM4 with C:\Users\alice\Desktop\demo.json")

    assert payload["token"] == "<REDACTED>"
    assert payload["port"] == "<PORT>"
    assert payload["nested"]["password"] == "<REDACTED>"
    assert "<PATH>" in payload["path"]
    assert "<PATH>" in text
    assert "<PORT>" in text
