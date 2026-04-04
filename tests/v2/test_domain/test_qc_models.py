from gas_calibrator.v2.domain.enums import QCLevel
from gas_calibrator.v2.domain.qc_models import QCDecision


def test_qc_decision_defaults() -> None:
    decision = QCDecision(point_index=1, level=QCLevel.WARN, accepted=False)

    assert decision.level is QCLevel.WARN
    assert decision.accepted is False
    assert decision.reasons == []
    assert decision.score == 0.0
