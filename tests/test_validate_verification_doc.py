from pathlib import Path
import zipfile
from xml.etree import ElementTree as ET

from gas_calibrator.devices.gas_analyzer import GasAnalyzer
from gas_calibrator.tools import validate_verification_doc


def _write_template_docx(path: Path) -> Path:
    ns = validate_verification_doc.WORD_NS["w"]
    document = ET.Element(f"{{{ns}}}document")
    body = ET.SubElement(document, f"{{{ns}}}body")
    table = ET.SubElement(body, f"{{{ns}}}tbl")
    rows = [
        ["序号", "点位", "标准值", "传感器示值", "误差", "相对误差"],
        ["1", "0", "0.00", "", "", ""],
        ["2", "200", "201.66", "", "", ""],
        ["3", "400", "401.88", "", "", ""],
        ["4", "600", "602.10", "", "", ""],
        ["5", "800", "802.32", "", "", ""],
        ["6", "1000", "1002.54", "", "", ""],
        ["7", "露点温度 2.0℃", "", "", "", ""],
        ["8", "露点温度 25.0℃", "", "", "", ""],
    ]
    for row_values in rows:
        row = ET.SubElement(table, f"{{{ns}}}tr")
        for value in row_values:
            cell = ET.SubElement(row, f"{{{ns}}}tc")
            paragraph = ET.SubElement(cell, f"{{{ns}}}p")
            run = ET.SubElement(paragraph, f"{{{ns}}}r")
            text = ET.SubElement(run, f"{{{ns}}}t")
            text.text = value
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", ET.tostring(document, encoding="utf-8", xml_declaration=True))
    return path


def test_parse_line_marks_legacy_frames_as_mode1() -> None:
    ga = GasAnalyzer("COM1")

    parsed = ga.parse_line("YGAS,006,0562.711,01.213,0.99,0.99,028.73,102.54,0001,2764")

    assert parsed is not None
    assert parsed["mode"] == 1
    assert parsed["co2_ppm"] == 562.711
    assert parsed["h2o_mmol"] == 1.213


def test_derive_humidity_generator_setpoint_matches_expected_points() -> None:
    low = validate_verification_doc.derive_humidity_generator_setpoint(2.0)
    high = validate_verification_doc.derive_humidity_generator_setpoint(25.0)

    assert low["hgen_temp_c"] == 20.0
    assert round(low["hgen_rh_pct"], 2) == 30.19
    assert high["hgen_temp_c"] == 30.0
    assert round(high["hgen_rh_pct"], 3) == 74.639


def test_dewpoint_to_h2o_mmol_per_mol_uses_pressure() -> None:
    value = validate_verification_doc.dewpoint_to_h2o_mmol_per_mol(2.0, 1004.178)

    assert round(value, 3) == 7.03


def test_load_template_spec_reads_expected_rows(tmp_path: Path) -> None:
    spec = validate_verification_doc.load_template_spec(
        _write_template_docx(tmp_path / "verification_template.docx")
    )

    assert [row["nominal_ppm"] for row in spec["co2_rows"]] == [0, 200, 400, 600, 800, 1000]
    assert round(spec["co2_rows"][1]["standard_ppm"], 2) == 201.66
    assert [row["target_dewpoint_c"] for row in spec["h2o_rows"]] == [2.0, 25.0]


def test_set_cell_text_creates_text_node_for_blank_cell() -> None:
    ns = validate_verification_doc.WORD_NS["w"]
    cell = ET.fromstring(f'<w:tc xmlns:w="{ns}"><w:p /></w:tc>')

    validate_verification_doc._set_cell_text(cell, "123.456 ppm")

    texts = cell.findall(".//w:t", validate_verification_doc.WORD_NS)
    assert len(texts) == 1
    assert texts[0].text == "123.456 ppm"
