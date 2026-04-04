from pathlib import Path

from gas_calibrator.v2.ui_v2.runtime.release_notes_loader import load_release_notes


def test_release_notes_loader_returns_empty_string_for_missing_file(tmp_path: Path) -> None:
    assert load_release_notes(tmp_path / "missing.md") == ""


def test_release_notes_loader_reads_text_file(tmp_path: Path) -> None:
    source = tmp_path / "release_notes.md"
    source.write_text("# Notes\n\n- item", encoding="utf-8")

    assert "item" in load_release_notes(source)
