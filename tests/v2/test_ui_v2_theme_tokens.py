from gas_calibrator.v2.ui_v2.theme.tokens import THEME


def test_theme_tokens_expose_core_fields() -> None:
    assert THEME.bg.startswith("#")
    assert THEME.accent.startswith("#")
    assert THEME.font_size_md > 0
    assert THEME.spacing_md > 0
