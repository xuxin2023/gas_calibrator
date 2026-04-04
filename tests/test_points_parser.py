from pathlib import Path

import pandas as pd

from gas_calibrator.data.points import load_points_from_excel, reorder_points, validate_points


def _write_points_xlsx(path: Path) -> None:
    # Keep structure aligned with loader expectation: first 2 rows are header/ignored.
    h2o_text = (
        "10\u2103\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "50%\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "0.1\u2103\uff08\u9732\u70b9\u6e29\u5ea6\uff09 6.0mmol/mol"
    )
    rows = [
        ["hdr", "hdr", "hdr", "hdr", "hdr"],
        ["hdr", "hdr", "hdr", "hdr", "hdr"],
        [10, 0, None, 1000, None],
        [10, 200, None, None, None],  # pressure carry-forward case
        [10, None, h2o_text, 1010, "B"],
        [-20, 400, None, 900, None],
    ]
    df = pd.DataFrame(rows)
    df.to_excel(path, header=False, index=False)


def test_load_points_carry_forward_and_h2o_parse(tmp_path: Path) -> None:
    xlsx = tmp_path / "points.xlsx"
    _write_points_xlsx(xlsx)

    points = load_points_from_excel(xlsx, missing_pressure_policy="carry_forward")

    assert len(points) == 4
    assert points[0].temp_chamber_c == 10.0
    assert points[1].co2_ppm == 200.0
    assert points[1].target_pressure_hpa == 1000.0
    assert points[2].is_h2o_point is True
    assert points[2].co2_ppm is None
    assert points[2].hgen_temp_c == 10.0
    assert points[2].hgen_rh_pct == 50.0
    assert points[2].dewpoint_c == 0.1
    assert points[2].h2o_mmol == 6.0
    assert points[2].co2_group is None


def test_validate_points_require_pressure(tmp_path: Path) -> None:
    xlsx = tmp_path / "points.xlsx"
    _write_points_xlsx(xlsx)

    points = load_points_from_excel(xlsx, missing_pressure_policy="require")
    issues = validate_points(points, missing_pressure_policy="require")

    assert any("missing target pressure" in msg for msg in issues)


def test_reorder_points_water_first_for_high_temp(tmp_path: Path) -> None:
    xlsx = tmp_path / "points.xlsx"
    _write_points_xlsx(xlsx)
    points = load_points_from_excel(xlsx, missing_pressure_policy="carry_forward")

    ordered = reorder_points(points, water_first_temp_gte=10.0)
    same_temp = [p for p in ordered if p.temp_chamber_c == 10.0]

    assert same_temp[0].is_h2o_point is True


def test_reorder_points_water_first_for_zero_degree(tmp_path: Path) -> None:
    xlsx = tmp_path / "points_zero.xlsx"
    h2o_text = (
        "0℃（湿度发生器） "
        "50%（湿度发生器） "
        "-10.0℃（露点温度） 3.0mmol/mol"
    )
    rows = [
        ["hdr", "hdr", "hdr", "hdr", "hdr"],
        ["hdr", "hdr", "hdr", "hdr", "hdr"],
        [0, 0, None, 1100, None],
        [0, None, h2o_text, 1100, None],
    ]
    pd.DataFrame(rows).to_excel(xlsx, header=False, index=False)
    points = load_points_from_excel(xlsx, missing_pressure_policy="carry_forward")

    ordered = reorder_points(points, water_first_temp_gte=0.0)
    same_temp = [p for p in ordered if p.temp_chamber_c == 0.0]

    assert same_temp[0].is_h2o_point is True


def test_reorder_points_orders_temperature_groups_descending_by_default(tmp_path: Path) -> None:
    xlsx = tmp_path / "points_desc.xlsx"
    rows = [
        ["hdr", "hdr", "hdr", "hdr"],
        ["hdr", "hdr", "hdr", "hdr"],
        [-20, 0, None, 1100],
        [40, 0, None, 1100],
        [0, 0, None, 1100],
    ]
    pd.DataFrame(rows).to_excel(xlsx, header=False, index=False)

    points = load_points_from_excel(xlsx, missing_pressure_policy="carry_forward")
    ordered = reorder_points(points, water_first_temp_gte=0.0)

    assert [p.temp_chamber_c for p in ordered] == [40.0, 0.0, -20.0]


def test_reorder_points_can_order_temperature_groups_ascending(tmp_path: Path) -> None:
    xlsx = tmp_path / "points_asc.xlsx"
    rows = [
        ["hdr", "hdr", "hdr", "hdr"],
        ["hdr", "hdr", "hdr", "hdr"],
        [-20, 0, None, 1100],
        [40, 0, None, 1100],
        [0, 0, None, 1100],
    ]
    pd.DataFrame(rows).to_excel(xlsx, header=False, index=False)

    points = load_points_from_excel(xlsx, missing_pressure_policy="carry_forward")
    ordered = reorder_points(points, water_first_temp_gte=0.0, descending_temperatures=False)

    assert [p.temp_chamber_c for p in ordered] == [-20.0, 0.0, 40.0]


def test_load_points_h2o_carry_forward(tmp_path: Path) -> None:
    xlsx = tmp_path / "points.xlsx"
    h2o_text = (
        "20\u2103\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "30%\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "2.0\u2103\uff08\u9732\u70b9\u6e29\u5ea6\uff09 7.0mmol/mol"
    )
    rows = [
        ["hdr", "hdr", "hdr", "hdr", "hdr"],
        ["hdr", "hdr", "hdr", "hdr", "hdr"],
        [20, 0, h2o_text, 1100, None],
        [None, 200, None, 1000, None],
        [None, 400, None, 900, None],
    ]
    pd.DataFrame(rows).to_excel(xlsx, header=False, index=False)

    pts_no_carry = load_points_from_excel(xlsx, missing_pressure_policy="carry_forward")
    pts_carry = load_points_from_excel(
        xlsx,
        missing_pressure_policy="carry_forward",
        carry_forward_h2o=True,
    )

    assert pts_no_carry[1].is_h2o_point is False
    assert pts_carry[1].is_h2o_point is True
    assert pts_carry[1].hgen_temp_c == 20.0
    assert pts_carry[1].hgen_rh_pct == 30.0


def test_h2o_off_marker_breaks_carry_forward(tmp_path: Path) -> None:
    xlsx = tmp_path / "points.xlsx"
    h2o_text = (
        "20\u2103\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "30%\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "2.0\u2103\uff08\u9732\u70b9\u6e29\u5ea6\uff09 7.0mmol/mol"
    )
    rows = [
        ["hdr", "hdr", "hdr", "hdr"],
        ["hdr", "hdr", "hdr", "hdr"],
        [20, 0, h2o_text, 1100],
        [None, 200, None, 1000],     # carry-forward on
        [None, 400, "——", 900],      # explicit non-water marker
        [None, 600, None, 800],       # should remain non-water
    ]
    pd.DataFrame(rows).to_excel(xlsx, header=False, index=False)

    pts = load_points_from_excel(
        xlsx,
        missing_pressure_policy="carry_forward",
        carry_forward_h2o=True,
    )
    assert pts[0].is_h2o_point is True
    assert pts[1].is_h2o_point is True
    assert pts[2].is_h2o_point is False
    assert pts[3].is_h2o_point is False


def test_subzero_points_force_gas_path(tmp_path: Path) -> None:
    xlsx = tmp_path / "points.xlsx"
    h2o_text = (
        "-10\u2103\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "50%\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "-10.0\u2103\uff08\u9732\u70b9\u6e29\u5ea6\uff09 3.0mmol/mol"
    )
    rows = [
        ["hdr", "hdr", "hdr", "hdr"],
        ["hdr", "hdr", "hdr", "hdr"],
        [-10, 0, h2o_text, 1100],
    ]
    pd.DataFrame(rows).to_excel(xlsx, header=False, index=False)

    pts = load_points_from_excel(
        xlsx,
        missing_pressure_policy="carry_forward",
        carry_forward_h2o=True,
    )
    assert len(pts) == 1
    assert pts[0].temp_chamber_c == -10.0
    assert pts[0].is_h2o_point is False
