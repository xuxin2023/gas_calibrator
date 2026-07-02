from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "v1_5_flow_contract" / "v1_5_stage_detail_and_route_optimization.md"


def test_stage_detail_route_optimization_doc_locks_purge_contract():
    text = DOC.read_text(encoding="utf-8")

    assert "CO2 开放流通单点默认 `purge_s = 360s`" in text
    assert "H2O 开放流通单点默认 `purge_s = 720s`" in text
    assert "CO2 首个点、长时间闲置后首点、管路状态未知、刚断电重启后 | 600s" in text
    assert "H2O 首个点、长时间闲置后首点、管路状态未知 | 900s" in text
    assert "到达最小吹扫时间不等于可以采样" in text


def test_stage_detail_route_optimization_doc_preserves_route_and_anchor_boundaries():
    text = DOC.read_text(encoding="utf-8")

    assert "正式样气/水汽路径不能提前关闭" in text
    assert "CO2 零气锚点和 H2O 干气低水锚点分开处理" in text
    assert "不把 S5/S6 混入主拟合" in text
    assert "不直接改现有气路/水路阀控制" in text


def test_stage_detail_route_optimization_doc_separates_n2_and_readiness_from_ratio_only():
    text = DOC.read_text(encoding="utf-8")

    assert "不能自动作为 CO2 零气标准" in text
    assert "不能自动作为 H2O 干气低水锚点" in text
    assert "只看 filtered ratio 稳定是不充分的" in text
    assert "route_open_until_sample_end" in text
    assert "physical_stability_gate_passed" in text


def test_stage_detail_route_optimization_doc_defines_dewpoint_strategy():
    text = DOC.read_text(encoding="utf-8")

    assert "CO2 气路露点策略不是追求越低越好" in text
    assert "各个 CO2 标气点不要求露点完全一致" in text
    assert "水汽状态归一化" in text
    assert "不能用算法掩盖未稳定的湿气记忆" in text
