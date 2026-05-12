from __future__ import annotations

from pathlib import Path

HANDOFF_DIR = (
    Path(__file__).resolve().parents[2]
    / "_handoff"
    / "a4_single_temperature_no_write"
)

CONFIRMATION_PATH = HANDOFF_DIR / "A4_P12_OPERATOR_CONFIRMATION_PACKAGE.md"
PREFLIGHT_PATH = HANDOFF_DIR / "A4_P12_REAL_NO_WRITE_PREFLIGHT_MANIFEST.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _lines(path: Path) -> list[str]:
    return _read(path).splitlines()


# ═══════════════════════════════════════════════════════════
# A. File existence
# ═══════════════════════════════════════════════════════════


def test_confirmation_package_exists():
    assert CONFIRMATION_PATH.exists(), f"missing: {CONFIRMATION_PATH}"


def test_preflight_manifest_exists():
    assert PREFLIGHT_PATH.exists(), f"missing: {PREFLIGHT_PATH}"


def test_confirmation_package_is_readable_text():
    text = _read(CONFIRMATION_PATH)
    assert len(text) > 200, f"confirmation package too short: {len(text)} chars"


def test_preflight_manifest_is_readable_text():
    text = _read(PREFLIGHT_PATH)
    assert len(text) > 200, f"preflight manifest too short: {len(text)} chars"


# ═══════════════════════════════════════════════════════════
# B. Approval phrase
# ═══════════════════════════════════════════════════════════


def test_confirmation_contains_approval_phrase():
    text = _read(CONFIRMATION_PATH)
    assert "APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT" in text, (
        "approval phrase missing"
    )


def test_preflight_contains_approval_phrase():
    text = _read(PREFLIGHT_PATH)
    assert "APPROVE_A4_SINGLE_TEMP_H2O_CO2_NO_WRITE_PREFLIGHT" in text, (
        "approval phrase missing"
    )


# ═══════════════════════════════════════════════════════════
# C. NEED_USER_DECISION markers
# ═══════════════════════════════════════════════════════════


def test_confirmation_contains_co2_ppm_decision():
    text = _read(CONFIRMATION_PATH)
    assert "NEED_USER_DECISION_CO2_PPM" in text, (
        "CO2 ppm user decision marker missing"
    )


def test_confirmation_contains_pending_user_confirmation():
    text = _read(CONFIRMATION_PATH)
    count = text.count("PENDING_USER_CONFIRMATION")
    assert count >= 8, f"PENDING_USER_CONFIRMATION count={count}, expected >= 8"


# ═══════════════════════════════════════════════════════════
# D. V1 fallback
# ═══════════════════════════════════════════════════════════


def test_confirmation_contains_v1_fallback():
    text = _read(CONFIRMATION_PATH)
    assert "V1 fallback" in text or "V1 remains" in text, "V1 fallback missing"


def test_preflight_contains_v1_fallback():
    text = _read(PREFLIGHT_PATH)
    assert "V1 fallback" in text, "V1 fallback missing"


# ═══════════════════════════════════════════════════════════
# E. No parameter write
# ═══════════════════════════════════════════════════════════


_NO_WRITE_KEYS = ["ID", "SENCO", "zero", "span", "coefficient", "calibration"]


def test_confirmation_contains_no_parameter_write():
    text = _read(CONFIRMATION_PATH)
    for key in _NO_WRITE_KEYS:
        assert key.lower() in text.lower(), f"no-{key} write missing in confirmation"


def test_preflight_contains_no_parameter_write():
    text = _read(PREFLIGHT_PATH)
    for key in _NO_WRITE_KEYS:
        assert key.lower() in text.lower(), f"no-{key} write missing in preflight"


# ═══════════════════════════════════════════════════════════
# F. CO2 ambient_open deferred
# ═══════════════════════════════════════════════════════════


def test_confirmation_contains_co2_ambient_deferred():
    text = _read(CONFIRMATION_PATH)
    assert "ambient_open" in text.lower() and "deferred" in text.lower(), (
        "CO2 ambient_open deferred not found in confirmation"
    )


def test_preflight_contains_co2_ambient_deferred():
    text = _read(PREFLIGHT_PATH)
    assert "ambient_open" in text.lower() and "deferred" in text.lower(), (
        "CO2 ambient_open deferred not found in preflight"
    )


# ═══════════════════════════════════════════════════════════
# G. Real-machine config not created
# ═══════════════════════════════════════════════════════════


def test_confirmation_declares_real_config_not_created():
    text = _read(CONFIRMATION_PATH)
    assert "NOT CREATED" in text, "confirmation must declare real config NOT CREATED"


def test_preflight_declares_real_config_not_created():
    text = _read(PREFLIGHT_PATH)
    assert "NOT CREATED" in text, "preflight must declare real config NOT CREATED"


# ═══════════════════════════════════════════════════════════
# H. Simulation config must not be used as real config
# ═══════════════════════════════════════════════════════════


def test_preflight_simulation_cannot_be_real_config():
    text = _read(PREFLIGHT_PATH)
    assert "MUST NOT" in text.upper() or "not be used" in text.lower(), (
        "simulation-as-real-config prohibition missing"
    )


# ═══════════════════════════════════════════════════════════
# I. Must NOT contain forbidden statements
# ═══════════════════════════════════════════════════════════


def test_confirmation_does_not_contain_approved_as_conclusion():
    text = _read(CONFIRMATION_PATH).lower()
    lines = _lines(CONFIRMATION_PATH)
    for line in lines:
        if line.startswith("#") or line.startswith(">"):
            continue
        if "decision" in line.lower():
            assert "pass" not in line.lower() or "approved" not in line.lower(), "no APPROVED conclusion allowed"


def test_confirmation_has_no_execute_probe_affirmative():
    text = _read(CONFIRMATION_PATH)
    lines = _lines(CONFIRMATION_PATH)
    for line in lines:
        lowered = line.strip().lower()
        if "execute" in lowered and "probe" in lowered and "do not" not in lowered:
            raise AssertionError(f"affirmative execute-probe: {line.strip()}")


def test_preflight_has_no_execute_probe_affirmative():
    text = _read(PREFLIGHT_PATH)
    lines = _lines(PREFLIGHT_PATH)
    for line in lines:
        lowered = line.strip().lower()
        if "execute" in lowered and "probe" in lowered and "do not" not in lowered:
            raise AssertionError(f"affirmative execute-probe: {line.strip()}")


def test_confirmation_does_not_contain_production_acceptance_ready():
    text = _read(CONFIRMATION_PATH)
    assert "production acceptance ready" not in text.lower(), (
        "production acceptance ready found in confirmation"
    )


def test_preflight_does_not_contain_production_acceptance_ready():
    text = _read(PREFLIGHT_PATH)
    assert "production acceptance ready" not in text.lower(), (
        "production acceptance ready found in preflight"
    )


def test_confirmation_does_not_contain_formal_switch_ready():
    text = _read(CONFIRMATION_PATH)
    assert "formal switch ready" not in text.lower(), (
        "formal switch ready found in confirmation"
    )


def test_preflight_does_not_contain_formal_switch_ready():
    text = _read(PREFLIGHT_PATH)
    assert "formal switch ready" not in text.lower(), (
        "formal switch ready found in preflight"
    )


# ═══════════════════════════════════════════════════════════
# J. Preflight gate count
# ═══════════════════════════════════════════════════════════


def test_preflight_has_7_gate_sections():
    text = _read(PREFLIGHT_PATH)
    gates = ["A. Git Gate", "B. Config Gate", "C. Route Gate",
             "D. Physical Gate", "E. No-Write Gate", "F. Safe-Stop Gate",
             "G. User Confirmation Gate"]
    for gate in gates:
        assert gate in text, f"gate section missing: {gate}"
