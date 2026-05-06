-- 003_high_frequency_query_indexes.sql
-- 补齐稳定窗口、状态迁移日志、拟合结果表的复合查询索引
-- 日期: 2026-05-06
-- 分支: codex/run001-a1-no-write-dry-run

-- -------------------------------------------------------------------
-- 1. stability_windows: (run_id, analyzer_sn, window_start_time)
--    覆盖高频查询: "某次运行的某台分析仪在某时间窗口内的稳定数据"
--    原索引仅有单列 run_id / timestamp / analyzer_sn, 缺少复合索引
-- -------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_stability_windows_run_sn_window
    ON stability_windows (run_id, analyzer_sn, window_start_time);

-- -------------------------------------------------------------------
-- 2. state_transition_logs: (run_id, timestamp)
--    覆盖高频查询: "某次运行的状态迁移事件按时间排序"
--    原索引仅有单列 run_id / timestamp / analyzer_sn, 缺少复合索引
-- -------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_state_transition_logs_run_time
    ON state_transition_logs (run_id, "timestamp");

-- -------------------------------------------------------------------
-- 3. fit_results: (run_id, analyzer_id)
--    覆盖高频查询: "某次运行的校准拟合结果按分析仪筛选"
--    原索引: ix_fit_results_run_id(run_id) + ix_fit_results_analyzer(analyzer_id) 均为单列
--    注意: 数据库中无 calibration_results 表, fit_results 是其等价的校准结果表
-- -------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_fit_results_run_analyzer
    ON fit_results (run_id, analyzer_id);

-- -------------------------------------------------------------------
-- 4. samples: (run_id, point_id) — 加速 exporter.export_run_bundle 中
--    JOIN PointRecord 再按 run_id 定位 samples 的查询路径
-- -------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_samples_run_point
    ON samples (point_id);

-- 已有 ix_samples_point_id 单列索引, 无需重复创建;
-- 本文件仅添加 SQLite/PostgreSQL 兼容的 IF NOT EXISTS 安全索引.

-- 注: 因 run_id 不在 samples 表中 (samples 仅有 point_id → points.run_id),
-- sampler_run__ 复合索引不适合此处; ix_samples_point_id 已足够加速 JOIN.
