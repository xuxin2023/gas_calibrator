"""Shared validation exception hierarchy."""

from typing import Any, Optional


class CalibrationError(Exception):
    """
    校准错误基类

    所有校准系统异常的基类，提供统一的错误信息格式和上下文支持。

    Attributes:
        message: 错误信息
        device: 相关设备名称（可选）
        context: 额外上下文信息（可选）
    """

    def __init__(
        self,
        message: str,
        device: Optional[str] = None,
        context: Optional[dict] = None
    ) -> None:
        self.message = message
        self.device = device
        self.context = context or {}
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """格式化错误信息"""
        parts = [self.message]
        if self.device:
            parts.append(f"[设备: {self.device}]")
        if self.context:
            ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"[{ctx_str}]")
        return " ".join(parts)

    def to_dict(self) -> dict:
        """转换为字典，便于日志记录和序列化"""
        return {
            "type": self.__class__.__name__,
            "message": self.message,
            "device": self.device,
            "context": self.context,
        }


class StabilityError(CalibrationError):
    """稳定性检测错误基类"""
    pass


class StabilityTimeoutError(StabilityError):
    """稳定性检测超时"""

    def __init__(
        self,
        parameter: str,
        target: Optional[float] = None,
        actual: Optional[float] = None,
        tolerance: Optional[float] = None,
        timeout_s: Optional[float] = None
    ) -> None:
        message = f"稳定性检测超时: {parameter}"
        context = {"parameter": parameter}
        if target is not None:
            context["target"] = target
        if actual is not None:
            context["actual"] = actual
        if tolerance is not None:
            context["tolerance"] = tolerance
        if timeout_s is not None:
            context["timeout_s"] = timeout_s
        super().__init__(message, context=context)


class StabilityNotReachedError(StabilityError):
    """稳定性未达到"""

    def __init__(
        self,
        parameter: str,
        reason: Optional[str] = None
    ) -> None:
        message = f"稳定性未达到: {parameter}"
        context = {"parameter": parameter}
        if reason:
            context["reason"] = reason
        super().__init__(message, context=context)


class DataError(CalibrationError):
    """数据错误基类"""
    pass


class DataParseError(DataError):
    """数据解析错误"""

    def __init__(
        self,
        source: str,
        reason: Optional[str] = None
    ) -> None:
        message = f"数据解析失败: {source}"
        context = {"source": source}
        if reason:
            context["reason"] = reason
        super().__init__(message, context=context)


class DataValidationError(DataError):
    """数据验证错误"""

    def __init__(
        self,
        field: str,
        value: Any,
        expected: Optional[str] = None
    ) -> None:
        message = f"数据验证失败: {field}={value}"
        context = {"field": field, "value": str(value)}
        if expected:
            context["expected"] = expected
        super().__init__(message, context=context)
