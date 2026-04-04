"""
旧版 CalibrationRunner 适配器

本模块封装旧版的 CalibrationRunner，提供新的接口规范。
这样可以在不修改旧代码的情况下，让新架构使用统一的接口。

使用示例：
    from gas_calibrator.v2.adapters import LegacyCalibrationRunner

    # 使用适配器
    runner = LegacyCalibrationRunner(cfg, devices, logger)
    runner.run()

当 v2 的 CalibrationService 完成后，可以直接替换：
    from gas_calibrator.v2.core import CalibrationService
    runner = CalibrationService(cfg, devices, logger)
    runner.run()  # 接口相同
"""

from typing import Any, Dict, Optional, Callable
import threading
import logging

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.calibration_service import CalibrationService
from gas_calibrator.v2.entry import create_calibration_service_from_config

# 导入旧版 CalibrationRunner
# 注意：这里使用延迟导入，避免循环依赖
_legacy_runner = None


def _get_legacy_runner():
    """延迟导入旧版 CalibrationRunner"""
    global _legacy_runner
    if _legacy_runner is None:
        from gas_calibrator.workflow.runner import CalibrationRunner
        _legacy_runner = CalibrationRunner
    return _legacy_runner


class LegacyCalibrationRunner:
    """
    旧版 CalibrationRunner 的适配器

    封装旧版 CalibrationRunner，提供统一的接口。
    内部调用旧版实现，外部呈现新接口。

    这个类的作用：
    1. 提供与新版 CalibrationService 相同的接口
    2. 内部调用旧版 CalibrationRunner
    3. 便于渐进式迁移，无需一次性重写所有代码
    """

    def __init__(
        self,
        cfg: Dict[str, Any],
        devices: Dict[str, Any],
        logger: Any,
        on_progress: Optional[Callable[[str, float], None]] = None,
        on_status: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        初始化适配器

        Args:
            cfg: 配置字典
            devices: 设备字典
            logger: 日志记录器
            on_progress: 进度回调函数 (phase, progress)
            on_status: 状态回调函数 (status_message)
        """
        self._cfg = cfg
        self._devices = devices
        self._logger = logger
        self._on_progress = on_progress
        self._on_status = on_status

        # 旧版 runner 实例（延迟创建）
        self._legacy_runner: Optional[Any] = None

        # 停止事件
        self._stop_event = threading.Event()

        # 运行状态
        self._is_running = False
        self._current_phase = ""

    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._is_running

    @property
    def current_phase(self) -> str:
        """当前阶段"""
        return self._current_phase

    def start(self) -> None:
        """
        启动校准流程

        这是新接口，内部调用旧版的 run() 方法。
        """
        if self._is_running:
            raise RuntimeError("校准流程已在运行中")

        self._is_running = True
        self._stop_event.clear()

        try:
            # 创建旧版 runner
            CalibrationRunner = _get_legacy_runner()
            self._legacy_runner = CalibrationRunner(
                self._cfg,
                self._devices,
                self._logger
            )

            # 设置停止事件
            self._legacy_runner.stop_event = self._stop_event

            # 运行
            self._legacy_runner.run()

        except Exception as e:
            logging.error(f"校准流程异常: {e}")
            raise
        finally:
            self._is_running = False
            self._legacy_runner = None

    def stop(self) -> None:
        """
        停止校准流程

        设置停止标志，等待当前操作完成后退出。
        """
        self._stop_event.set()

        if self._legacy_runner is not None:
            # 旧版 runner 也有 stop 方法
            if hasattr(self._legacy_runner, 'stop'):
                self._legacy_runner.stop()

    def run(self) -> None:
        """
        运行校准流程（兼容旧接口）

        为了保持与旧版接口的兼容性，保留 run() 方法。
        """
        self.start()

    # =========================================================================
    # 代理旧版 runner 的常用属性和方法
    # =========================================================================

    @property
    def stop_event(self) -> threading.Event:
        """停止事件"""
        return self._stop_event

    @stop_event.setter
    def stop_event(self, event: threading.Event) -> None:
        self._stop_event = event
        if self._legacy_runner is not None:
            self._legacy_runner.stop_event = event

    def get_status(self) -> Dict[str, Any]:
        """
        获取当前状态

        Returns:
            状态字典
        """
        return {
            "is_running": self._is_running,
            "current_phase": self._current_phase,
            "stop_requested": self._stop_event.is_set(),
        }


# =============================================================================
# 工厂函数
# =============================================================================

def create_runner(
    cfg: Dict[str, Any] | AppConfig,
    devices: Dict[str, Any],
    logger: Any,
    use_v2: bool = False
) -> Any:
    """
    创建校准运行器

    根据配置选择使用新版或旧版实现。

    Args:
        cfg: 配置字典
        devices: 设备字典
        logger: 日志记录器
        use_v2: 是否使用 v2 架构

    Returns:
        校准运行器实例
    """
    if use_v2:
        raw_cfg = None if isinstance(cfg, AppConfig) else dict(cfg or {})
        config = cfg if isinstance(cfg, AppConfig) else AppConfig.from_dict(raw_cfg)
        config.features.use_v2 = True
        return create_calibration_service_from_config(
            config,
            raw_cfg=raw_cfg,
            preload_points=False,
        )
    else:
        # 使用旧版
        CalibrationRunner = _get_legacy_runner()
        return CalibrationRunner(cfg, devices, logger)
