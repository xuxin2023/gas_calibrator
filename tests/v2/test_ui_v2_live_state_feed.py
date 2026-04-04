from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.controllers.live_state_feed import LiveStateFeed

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_live_state_feed_polls_once_and_calls_sink(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    feed = LiveStateFeed(facade, interval_ms=100)
    snapshots = []
    devices = []
    algorithms = []
    reports = []
    timeseries = []
    qc_overview = []
    winners = []
    exports = []
    route_progress = []
    reject_reasons = []
    residuals = []
    analyzer_health = []
    errors = []
    busy = []
    notifications = []
    feed.set_sink(snapshots.append)
    feed.set_devices_sink(devices.append)
    feed.set_algorithms_sink(algorithms.append)
    feed.set_reports_sink(reports.append)
    feed.set_timeseries_sink(timeseries.append)
    feed.set_qc_overview_sink(qc_overview.append)
    feed.set_winner_sink(winners.append)
    feed.set_export_sink(exports.append)
    feed.set_route_progress_sink(route_progress.append)
    feed.set_reject_reason_sink(reject_reasons.append)
    feed.set_residual_sink(residuals.append)
    feed.set_analyzer_health_sink(analyzer_health.append)
    feed.set_error_sink(errors.append)
    feed.set_busy_sink(busy.append)
    feed.set_notification_sink(notifications.append)

    snapshot = feed.poll_once()

    assert snapshots
    assert devices
    assert algorithms
    assert reports
    assert timeseries
    assert qc_overview
    assert winners
    assert exports
    assert route_progress
    assert reject_reasons
    assert residuals
    assert analyzer_health
    assert errors
    assert busy
    assert notifications
    assert snapshots[0]["run"]["run_id"] == facade.session.run_id
    assert snapshot["qc"]["total_points"] == 2
    assert devices[0]["enabled_count"] == 2
    assert winners[0]["winner"] == "amt"
    assert route_progress[0]["route"] == "co2"
