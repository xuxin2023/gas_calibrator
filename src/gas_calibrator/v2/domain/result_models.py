from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class PointResult:
    """Aggregated result for a single calibration point."""

    point_index: int
    mean_co2: Optional[float] = None
    mean_h2o: Optional[float] = None
    std_co2: Optional[float] = None
    std_h2o: Optional[float] = None
    chamber_temp_c: Optional[float] = None
    case_temp_c: Optional[float] = None
    sample_count: int = 0
    stable: bool = False
    accepted: bool = True
    notes: str = ""


@dataclass
class RunArtifactManifest:
    """Run artifact manifest."""

    run_id: str
    raw_samples_file: str
    point_results_file: str
    run_summary_file: str
    config_snapshot_file: str
