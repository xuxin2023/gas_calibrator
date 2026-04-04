from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakeGA:
    def __init__(self):
        self._line = (
            "YGAS,097,0658.169,06.783,1240.638,05.191,1.2453,1.2430,"
            "0.7992,0.7992,03178,03948,02538,031.60,031.74,106.06,OK"
        )

    def read_data_passive(self):
        return self._line

    def read_latest_data(self, *args, **kwargs):
        return self._line

    @staticmethod
    def parse_line_mode2(line):
        from gas_calibrator.devices.gas_analyzer import GasAnalyzer

        return GasAnalyzer._parse_mode2(line.split(","), line)


def _point() -> CalibrationPoint:
    return CalibrationPoint(
        index=21,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=2.0,
        h2o_mmol=6.9,
        raw_h2o="demo",
    )


def test_collect_samples_always_contains_mode2_schema(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "stable_count": 1,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        runner = CalibrationRunner(
            cfg,
            {"gas_analyzer": _FakeGA()},
            logger,
            lambda *_: None,
            lambda *_: None,
        )
        rows = runner._collect_samples(_point(), 1, 0.0)
        assert rows and len(rows) == 1
        row = rows[0]
        required = [
            "co2_ppm",
            "h2o_mmol",
            "co2_density",
            "h2o_density",
            "co2_ratio_f",
            "co2_ratio_raw",
            "h2o_ratio_f",
            "h2o_ratio_raw",
            "ref_signal",
            "co2_signal",
            "h2o_signal",
            "chamber_temp_c",
            "case_temp_c",
            "pressure_kpa",
            "status",
            "ga01_co2_ppm",
            "ga01_pressure_kpa",
            "ga01_status",
        ]
        for key in required:
            assert key in row
    finally:
        logger.close()
