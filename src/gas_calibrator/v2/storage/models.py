from __future__ import annotations

from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CHAR,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import TypeDecorator


RUN_STATUS_VALUES = ("running", "completed", "failed", "aborted")
POINT_STATUS_VALUES = ("pending", "running", "completed", "failed", "skipped")
ROUTE_TYPE_VALUES = ("h2o", "co2")
ALARM_SEVERITY_VALUES = ("info", "warning", "error", "critical")

JSON_VARIANT: TypeEngine = JSON().with_variant(JSONB, "postgresql")


class GUID(TypeDecorator):
    impl = CHAR(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PGUUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if not isinstance(value, UUID):
            value = UUID(str(value))
        return value if dialect.name == "postgresql" else str(value)

    def process_result_value(self, value, dialect):
        if value is None or isinstance(value, UUID):
            return value
        return UUID(str(value))


class Base(DeclarativeBase):
    pass


class SensorRecord(Base):
    __tablename__ = "sensors"
    __table_args__ = (
        UniqueConstraint("device_key", name="uq_sensors_device_key"),
        Index("ix_sensors_legacy_identity", "analyzer_id", "analyzer_serial"),
        Index("ix_sensors_serial", "analyzer_serial"),
        Index("ix_sensors_channel_type", "channel_type"),
    )

    sensor_id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    device_key: Mapped[str] = mapped_column(String(128), nullable=False)
    analyzer_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    analyzer_serial: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    software_version: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    model: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    channel_type: Mapped[str] = mapped_column(String(64), nullable=False, default="co2_h2o_dual")
    metadata_json: Mapped[dict] = mapped_column("metadata", JSON_VARIANT, nullable=False, default=dict)


class RunRecord(Base):
    __tablename__ = "runs"
    __table_args__ = (
        CheckConstraint(
            f"status IN ({', '.join(repr(value) for value in RUN_STATUS_VALUES)})",
            name="ck_runs_status",
        ),
        Index("ix_runs_time_window", "start_time", "end_time"),
        Index("ix_runs_operator", "operator"),
        Index("ix_runs_mode_profile", "run_mode", "profile_name"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    start_time: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    end_time: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    config_hash: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    software_version: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    run_mode: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    route_mode: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    profile_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    profile_version: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    report_family: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    report_templates: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    analyzer_setup: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    operator: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    total_points: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    successful_points: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    failed_points: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    warnings: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    errors: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    points: Mapped[list["PointRecord"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    measurement_frames: Mapped[list["MeasurementFrameRecord"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
    )
    fit_results: Mapped[list["FitResultRecord"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    device_events: Mapped[list["DeviceEventRecord"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    alarms: Mapped[list["AlarmIncidentRecord"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class PointRecord(Base):
    __tablename__ = "points"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_points_run_sequence"),
        CheckConstraint(
            f"route_type IN ({', '.join(repr(value) for value in ROUTE_TYPE_VALUES)})",
            name="ck_points_route_type",
        ),
        CheckConstraint(
            f"status IN ({', '.join(repr(value) for value in POINT_STATUS_VALUES)})",
            name="ck_points_status",
        ),
        Index("ix_points_run_id", "run_id"),
        Index("ix_points_route_status", "route_type", "status"),
        Index("ix_points_group_nominal", "co2_group", "cylinder_nominal_ppm"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    temperature_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    humidity_rh: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pressure_hpa: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    route_type: Mapped[str] = mapped_column(String(16), nullable=False, default="co2")
    co2_target_ppm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    co2_group: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    cylinder_nominal_ppm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    stability_time_s: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_time_s: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    run: Mapped[RunRecord] = relationship(back_populates="points")
    samples: Mapped[list["SampleRecord"]] = relationship(back_populates="point", cascade="all, delete-orphan")
    qc_results: Mapped[list["QCResultRecord"]] = relationship(back_populates="point", cascade="all, delete-orphan")
    measurement_frames: Mapped[list["MeasurementFrameRecord"]] = relationship(
        back_populates="point",
        cascade="all, delete-orphan",
    )


class SampleRecord(Base):
    __tablename__ = "samples"
    __table_args__ = (
        UniqueConstraint("point_id", "analyzer_id", "sample_index", name="uq_samples_point_analyzer_index"),
        Index("ix_samples_point_id", "point_id"),
        Index("ix_samples_timestamp", "timestamp"),
        Index("ix_samples_analyzer", "analyzer_id", "analyzer_serial"),
        Index("ix_samples_sensor_id", "sensor_id"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    point_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("points.id", ondelete="CASCADE"), nullable=False)
    sensor_id: Mapped[Optional[UUID]] = mapped_column(GUID(), ForeignKey("sensors.sensor_id"), nullable=True)
    analyzer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    analyzer_serial: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    sample_index: Mapped[int] = mapped_column(Integer, nullable=False)
    timestamp: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    co2_ppm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    h2o_mmol: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pressure_hpa: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    co2_ratio_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    h2o_ratio_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    co2_ratio_raw: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    h2o_ratio_raw: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    chamber_temp_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    case_temp_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    dewpoint_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    point: Mapped[PointRecord] = relationship(back_populates="samples")


class MeasurementFrameRecord(Base):
    __tablename__ = "measurement_frames"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "point_id",
            "analyzer_label",
            "sample_index",
            "sample_ts",
            name="uq_measurement_frames_natural_key",
        ),
        Index("ix_measurement_frames_run_time", "run_id", "sample_ts"),
        Index("ix_measurement_frames_point_sample", "point_id", "sample_index"),
        Index("ix_measurement_frames_analyzer_time", "analyzer_label", "sample_ts"),
        Index("ix_measurement_frames_sensor_id", "sensor_id"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    point_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("points.id", ondelete="CASCADE"), nullable=False)
    sensor_id: Mapped[Optional[UUID]] = mapped_column(GUID(), ForeignKey("sensors.sensor_id"), nullable=True)
    sample_index: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_ts: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    analyzer_label: Mapped[str] = mapped_column(String(32), nullable=False)
    analyzer_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    analyzer_serial: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    frame_has_data: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    frame_usable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    analyzer_status: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    mode: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    mode2_field_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    co2_ppm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    h2o_mmol: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    co2_ratio_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    h2o_ratio_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    co2_ratio_raw: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    h2o_ratio_raw: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ref_signal: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    co2_signal: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    h2o_signal: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    chamber_temp_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    case_temp_c: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pressure_kpa: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    context_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)

    run: Mapped[RunRecord] = relationship(back_populates="measurement_frames")
    point: Mapped[PointRecord] = relationship(back_populates="measurement_frames")


class QCResultRecord(Base):
    __tablename__ = "qc_results"
    __table_args__ = (
        UniqueConstraint("point_id", "rule_name", "message", name="uq_qc_results_point_rule_message"),
        Index("ix_qc_results_point_id", "point_id"),
        Index("ix_qc_results_passed", "passed"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    point_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("points.id", ondelete="CASCADE"), nullable=False)
    rule_name: Mapped[str] = mapped_column(String(128), nullable=False)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    threshold: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    point: Mapped[PointRecord] = relationship(back_populates="qc_results")


class FitResultRecord(Base):
    __tablename__ = "fit_results"
    __table_args__ = (
        UniqueConstraint("run_id", "analyzer_id", "algorithm", name="uq_fit_results_run_analyzer_algorithm"),
        Index("ix_fit_results_run_id", "run_id"),
        Index("ix_fit_results_analyzer", "analyzer_id"),
        Index("ix_fit_results_sensor_id", "sensor_id"),
        Index("ix_fit_results_run_analyzer", "run_id", "analyzer_id"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    sensor_id: Mapped[Optional[UUID]] = mapped_column(GUID(), ForeignKey("sensors.sensor_id"), nullable=True)
    analyzer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    algorithm: Mapped[str] = mapped_column(String(64), nullable=False)
    coefficients: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    rmse: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    r_squared: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    n_points: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    run: Mapped[RunRecord] = relationship(back_populates="fit_results")


class CoefficientVersionRecord(Base):
    __tablename__ = "coefficient_versions"
    __table_args__ = (
        UniqueConstraint("analyzer_id", "analyzer_serial", "version", name="uq_coefficient_versions_analyzer_version"),
        Index("ix_coefficient_versions_lookup", "analyzer_id", "analyzer_serial", "version"),
        Index("ix_coefficient_versions_deployed", "analyzer_id", "analyzer_serial", "deployed"),
        Index("ix_coefficient_versions_sensor_id", "sensor_id"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    sensor_id: Mapped[Optional[UUID]] = mapped_column(GUID(), ForeignKey("sensors.sensor_id"), nullable=True)
    analyzer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    analyzer_serial: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    coefficients: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    created_at: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    approved: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    approved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    approved_at: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    deployed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    deployed_at: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class DeviceEventRecord(Base):
    __tablename__ = "device_events"
    __table_args__ = (
        Index("ix_device_events_run_id", "run_id"),
        Index("ix_device_events_timestamp", "timestamp"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    device_name: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_data: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    timestamp: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)

    run: Mapped[RunRecord] = relationship(back_populates="device_events")


class AlarmIncidentRecord(Base):
    __tablename__ = "alarms_incidents"
    __table_args__ = (
        UniqueConstraint("run_id", "category", "message", "timestamp", name="uq_alarms_incidents_natural_key"),
        CheckConstraint(
            f"severity IN ({', '.join(repr(value) for value in ALARM_SEVERITY_VALUES)})",
            name="ck_alarms_severity",
        ),
        Index("ix_alarms_incidents_run_id", "run_id"),
        Index("ix_alarms_incidents_severity", "severity"),
        Index("ix_alarms_incidents_timestamp", "timestamp"),
    )

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(GUID(), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    category: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    timestamp: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    resolved_at: Mapped[Optional[object]] = mapped_column(DateTime(timezone=True), nullable=True)

    run: Mapped[RunRecord] = relationship(back_populates="alarms")


class RunIndexRecord(Base):
    __tablename__ = "run_index"
    __table_args__ = (
        UniqueConstraint("run_id", name="uq_run_index_run_id"),
        Index("ix_run_index_run_id", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    timestamp: Mapped[str] = mapped_column(String(64), nullable=False)
    branch: Mapped[str] = mapped_column(String(256), default="")
    head: Mapped[str] = mapped_column(String(64), default="")
    final_decision: Mapped[str] = mapped_column(String(64), default="")
    pressure_points_completed: Mapped[int] = mapped_column(Integer, default=0)
    sample_count_total: Mapped[int] = mapped_column(Integer, default=0)
    attempted_write_count: Mapped[int] = mapped_column(Integer, default=0)
    analyzer_sn: Mapped[str] = mapped_column(String(512), default="")
    config_path: Mapped[str] = mapped_column(Text, default="")
    output_dir: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class ArtifactIndexRecord(Base):
    __tablename__ = "artifact_index"
    __table_args__ = (
        UniqueConstraint("run_id", "artifact_type", "file_hash", name="uq_artifact_index_run_type_hash"),
        Index("ix_artifact_index_run_id", "run_id"),
        Index("ix_artifact_index_type", "artifact_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    artifact_id: Mapped[str] = mapped_column(String(512), nullable=False)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(128), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class RunCoefficientVersion(Base):
    __tablename__ = "coefficient_version"
    __table_args__ = (
        Index("ix_coeffver_run_id", "run_id"),
        Index("ix_coeffver_analyzer_sn", "analyzer_sn"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    analyzer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    analyzer_sn: Mapped[str] = mapped_column(String(64), default="")
    coefficient_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    written_to_device: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class RunAnalyzerRegistry(Base):
    __tablename__ = "analyzer_registry"
    __table_args__ = (
        UniqueConstraint("analyzer_sn", name="uq_analyzer_registry_sn"),
    )

    analyzer_sn: Mapped[str] = mapped_column(String(64), primary_key=True)
    first_seen_run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    first_seen_time: Mapped[str] = mapped_column(String(64), nullable=False)
    last_seen_time: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str] = mapped_column(String(128), default="")
    notes: Mapped[str] = mapped_column(Text, default="")



class StabilityWindowRecord(Base):
    __tablename__ = "stability_windows"
    __table_args__ = (
        Index("ix_stability_windows_run_id", "run_id"),
        Index("ix_stability_windows_timestamp", "timestamp"),
        Index("ix_stability_windows_analyzer_sn", "analyzer_sn"),
        Index("ix_stability_windows_run_sn_window", "run_id", "analyzer_sn", "window_start_time"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    timestamp: Mapped[str] = mapped_column(String(64), nullable=False)
    analyzer_sn: Mapped[str] = mapped_column(String(64), nullable=False)
    state_name: Mapped[str] = mapped_column(String(64), default="")
    window_start_time: Mapped[str] = mapped_column(String(64), default="")
    window_end_time: Mapped[str] = mapped_column(String(64), default="")
    signal_list: Mapped[str] = mapped_column(Text, default="")
    span_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    slope_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    std_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    valid_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    jump_count: Mapped[int] = mapped_column(Integer, default=0)
    hard_threshold_passed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    composite_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    fail_reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class StateTransitionLogRecord(Base):
    __tablename__ = "state_transition_logs"
    __table_args__ = (
        Index("ix_state_transition_logs_run_id", "run_id"),
        Index("ix_state_transition_logs_timestamp", "timestamp"),
        Index("ix_state_transition_logs_analyzer_sn", "analyzer_sn"),
        Index("ix_state_transition_logs_run_time", "run_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    timestamp: Mapped[str] = mapped_column(String(64), nullable=False)
    analyzer_sn: Mapped[str] = mapped_column(String(64), nullable=False)
    from_state: Mapped[str] = mapped_column(String(64), default="")
    to_state: Mapped[str] = mapped_column(String(64), default="")
    trigger: Mapped[str] = mapped_column(String(128), default="")
    decision_context: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class StabilityDecisionRecord(Base):
    __tablename__ = "stability_decisions"
    __table_args__ = (
        Index("ix_stability_decisions_run_id", "run_id"),
        Index("ix_stability_decisions_timestamp", "timestamp"),
        Index("ix_stability_decisions_analyzer_sn", "analyzer_sn"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    timestamp: Mapped[str] = mapped_column(String(64), nullable=False)
    analyzer_sn: Mapped[str] = mapped_column(String(64), nullable=False)
    stability_layer: Mapped[str] = mapped_column(String(64), default="")
    strategy_version: Mapped[str] = mapped_column(String(64), default="")
    signal_snapshot_ref: Mapped[str] = mapped_column(String(256), default="")
    hard_threshold_result: Mapped[str] = mapped_column(String(32), default="")
    composite_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    decision: Mapped[str] = mapped_column(String(32), default="")
    fail_reason: Mapped[str] = mapped_column(Text, default="")
    next_action: Mapped[str] = mapped_column(String(128), default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class RawSignalSnapshotRecord(Base):
    __tablename__ = "raw_signal_snapshots"
    __table_args__ = (
        Index("ix_raw_signal_snapshots_run_id", "run_id"),
        Index("ix_raw_signal_snapshots_timestamp", "timestamp"),
        Index("ix_raw_signal_snapshots_analyzer_sn", "analyzer_sn"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    timestamp: Mapped[str] = mapped_column(String(64), nullable=False)
    analyzer_sn: Mapped[str] = mapped_column(String(64), nullable=False)
    signal_family: Mapped[str] = mapped_column(String(64), default="")
    window_start_time: Mapped[str] = mapped_column(String(64), default="")
    window_end_time: Mapped[str] = mapped_column(String(64), default="")
    frame_count: Mapped[int] = mapped_column(Integer, default=0)
    values_summary: Mapped[str] = mapped_column(Text, default="")
    raw_payload_json: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class ArtifactRecord(Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        UniqueConstraint("artifact_id", name="uq_artifacts_artifact_id"),
        Index("ix_artifacts_run_id", "run_id"),
        Index("ix_artifacts_type", "artifact_type"),
    )

    artifact_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(128), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class ManifestRecord(Base):
    __tablename__ = "manifests"
    __table_args__ = (
        UniqueConstraint("manifest_id", name="uq_manifests_manifest_id"),
        Index("ix_manifests_run_id", "run_id"),
    )

    manifest_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    manifest_data: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class ReviewDigestRecord(Base):
    __tablename__ = "review_digests"
    __table_args__ = (
        UniqueConstraint("digest_id", name="uq_review_digests_digest_id"),
        Index("ix_review_digests_run_id", "run_id"),
    )

    digest_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    reviewer: Mapped[str] = mapped_column(String(128), default="")
    conclusion: Mapped[str] = mapped_column(Text, default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class StandardsMappingRecord(Base):
    __tablename__ = "standards_mappings"
    __table_args__ = (
        UniqueConstraint("mapping_id", name="uq_standards_mappings_mapping_id"),
        Index("ix_standards_mappings_run_id", "run_id"),
        Index("ix_standards_mappings_standard_family", "standard_family"),
    )

    mapping_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    standard_family: Mapped[str] = mapped_column(String(128), default="")
    topic: Mapped[str] = mapped_column(String(256), default="")
    readiness_status: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


class AuditEventRecord(Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_audit_events_event_id"),
        Index("ix_audit_events_run_id", "run_id"),
        Index("ix_audit_events_type", "event_type"),
    )

    event_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(256), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[str] = mapped_column(String(64), nullable=False)


def create_schema_sync(engine: Engine, *, enable_timescaledb: bool = False) -> None:
    Base.metadata.create_all(engine)
    if enable_timescaledb:
        _enable_timescaledb_sync(engine)


async def create_schema_async(engine: AsyncEngine, *, enable_timescaledb: bool = False) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if enable_timescaledb:
            await _enable_timescaledb_async(conn)


def _enable_timescaledb_sync(engine: Engine) -> None:
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        except SQLAlchemyError:
            return
        for table_name, column_name in (
            ("samples", "timestamp"),
            ("measurement_frames", "sample_ts"),
            ("device_events", "timestamp"),
            ("alarms_incidents", "timestamp"),
        ):
            try:
                conn.execute(
                    text(
                        f"SELECT create_hypertable('{table_name}', '{column_name}', if_not_exists => TRUE)"
                    )
                )
            except SQLAlchemyError:
                continue


async def _enable_timescaledb_async(conn) -> None:
    try:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
    except SQLAlchemyError:
        return
    for table_name, column_name in (
        ("samples", "timestamp"),
        ("measurement_frames", "sample_ts"),
        ("device_events", "timestamp"),
        ("alarms_incidents", "timestamp"),
    ):
        try:
            await conn.execute(
                text(
                    f"SELECT create_hypertable('{table_name}', '{column_name}', if_not_exists => TRUE)"
                )
            )
        except SQLAlchemyError:
            continue
