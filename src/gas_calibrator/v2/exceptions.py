"""
自定义异常类型

本模块定义了气体分析仪校准系统的异常层次结构。
所有异常都继承自 CalibrationError 基类，便于统一捕获和处理。

使用示例：
    from gas_calibrator.v2.exceptions import (
        CalibrationError,
        DeviceCommunicationError,
        StabilityTimeoutError,
    )

    try:
        device.read()
    except DeviceCommunicationError as e:
        logger.error(f"设备通信失败: {e}")
        raise
"""

from typing import Optional, Any

from gas_calibrator.validation.exceptions import (
    CalibrationError,
    DataError as DataError,
    DataParseError as DataParseError,
    DataValidationError as DataValidationError,
    StabilityError as StabilityError,
    StabilityNotReachedError as StabilityNotReachedError,
    StabilityTimeoutError as StabilityTimeoutError,
)


# =============================================================================
# 设备相关异常
# =============================================================================

class DeviceError(CalibrationError):
    """设备错误基类"""
    pass


class DeviceConnectionError(DeviceError):
    """设备连接错误"""

    def __init__(
        self,
        device: str,
        port: Optional[str] = None,
        reason: Optional[str] = None
    ) -> None:
        message = f"设备连接失败: {device}"
        context = {}
        if port:
            context["port"] = port
        if reason:
            context["reason"] = reason
        super().__init__(message, device=device, context=context)


class DeviceCommunicationError(DeviceError):
    """设备通信错误"""

    def __init__(
        self,
        device: str,
        command: Optional[str] = None,
        response: Optional[str] = None,
        reason: Optional[str] = None
    ) -> None:
        message = f"设备通信失败: {device}"
        context = {}
        if command:
            context["command"] = command
        if response:
            context["response"] = response
        if reason:
            context["reason"] = reason
        super().__init__(message, device=device, context=context)


class DeviceTimeoutError(DeviceError):
    """设备超时错误"""

    def __init__(
        self,
        device: str,
        operation: Optional[str] = None,
        timeout_s: Optional[float] = None
    ) -> None:
        message = f"设备操作超时: {device}"
        context = {}
        if operation:
            context["operation"] = operation
        if timeout_s:
            context["timeout_s"] = timeout_s
        super().__init__(message, device=device, context=context)


class DeviceNotRespondingError(DeviceError):
    """设备无响应错误"""

    def __init__(self, device: str) -> None:
        super().__init__(f"设备无响应: {device}", device=device)


class DeviceNotSupportedError(DeviceError):
    """设备不支持的操作"""

    def __init__(
        self,
        device: str,
        operation: str,
        supported: Optional[list] = None
    ) -> None:
        message = f"设备不支持操作 '{operation}': {device}"
        context = {"operation": operation}
        if supported:
            context["supported_operations"] = ", ".join(supported)
        super().__init__(message, device=device, context=context)


# =============================================================================
# 压力控制相关异常
# =============================================================================

class PressureControlError(CalibrationError):
    """压力控制错误基类"""
    pass


class PressureSetpointError(PressureControlError):
    """压力设定点错误"""

    def __init__(
        self,
        target_hpa: float,
        actual_hpa: Optional[float] = None,
        tolerance_hpa: Optional[float] = None
    ) -> None:
        message = f"压力设定失败: 目标 {target_hpa} hPa"
        context = {"target_hpa": target_hpa}
        if actual_hpa is not None:
            context["actual_hpa"] = actual_hpa
        if tolerance_hpa is not None:
            context["tolerance_hpa"] = tolerance_hpa
        super().__init__(message, context=context)


class PressureLeakError(PressureControlError):
    """压力泄漏错误"""

    def __init__(
        self,
        pressure_drop_hpa: float,
        time_s: float
    ) -> None:
        message = f"检测到压力泄漏: {pressure_drop_hpa} hPa / {time_s} s"
        context = {
            "pressure_drop_hpa": pressure_drop_hpa,
            "time_s": time_s
        }
        super().__init__(message, context=context)


# =============================================================================
# 配置相关异常
# =============================================================================

class ConfigurationError(CalibrationError):
    """配置错误基类"""
    pass


class ConfigurationMissingError(ConfigurationError):
    """配置项缺失"""

    def __init__(self, key: str, section: Optional[str] = None) -> None:
        message = f"配置项缺失: {key}"
        context = {"key": key}
        if section:
            message = f"配置项缺失: {section}.{key}"
            context["section"] = section
        super().__init__(message, context=context)


class ConfigurationInvalidError(ConfigurationError):
    """配置项无效"""

    def __init__(
        self,
        key: str,
        value: Any,
        reason: Optional[str] = None
    ) -> None:
        message = f"配置项无效: {key}={value}"
        context = {"key": key, "value": str(value)}
        if reason:
            context["reason"] = reason
        super().__init__(message, context=context)


# =============================================================================
# 工作流相关异常
# =============================================================================

class WorkflowError(CalibrationError):
    """工作流错误基类"""
    pass


class WorkflowInterruptedError(WorkflowError):
    """工作流被中断"""

    def __init__(
        self,
        phase: Optional[str] = None,
        reason: Optional[str] = None
    ) -> None:
        message = "工作流被中断"
        context = {}
        if phase:
            context["phase"] = phase
        if reason:
            context["reason"] = reason
        super().__init__(message, context=context)


class WorkflowValidationError(WorkflowError):
    """工作流验证错误"""

    def __init__(self, message: str, details: Optional[dict] = None) -> None:
        super().__init__(message, context=details)
