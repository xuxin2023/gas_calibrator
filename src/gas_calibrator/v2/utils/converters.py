"""
类型转换工具函数

本模块提供统一的类型转换函数，用于处理设备响应、配置值等数据。
所有函数都提供安全的默认值处理，避免因类型转换失败导致程序崩溃。

使用示例：
    from gas_calibrator.v2.utils import as_float, as_int

    # 安全转换为浮点数
    value = as_float(device_response, default=0.0)

    # 从字符串中提取第一个数字
    temp = parse_first_float("TEMP: 25.3C", default=20.0)
"""

from typing import Any, Optional, TypeVar, Dict, List, Union
import re


T = TypeVar("T")


def as_float(
    value: Any,
    default: Optional[float] = None,
    allow_none: bool = True
) -> Optional[float]:
    """
    安全转换为浮点数

    Args:
        value: 要转换的值，可以是字符串、数字或 None
        default: 转换失败时的默认值，默认为 None
        allow_none: 是否允许返回 None，False 时会返回 default 或 0.0

    Returns:
        转换后的浮点数，或默认值

    Examples:
        >>> as_float("3.14")
        3.14
        >>> as_float("invalid", default=0.0)
        0.0
        >>> as_float(None)
        None
        >>> as_float(None, default=0.0, allow_none=False)
        0.0
    """
    if value is None:
        if allow_none:
            return default
        return default if default is not None else 0.0

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.strip()
        if not value:
            if allow_none:
                return default
            return default if default is not None else 0.0
        try:
            return float(value)
        except ValueError:
            pass

    if default is not None:
        return default
    if allow_none:
        return None
    return 0.0


def as_int(
    value: Any,
    default: Optional[int] = None,
    allow_none: bool = True
) -> Optional[int]:
    """
    安全转换为整数

    Args:
        value: 要转换的值，可以是字符串、数字或 None
        default: 转换失败时的默认值，默认为 None
        allow_none: 是否允许返回 None，False 时会返回 default 或 0

    Returns:
        转换后的整数，或默认值

    Examples:
        >>> as_int("42")
        42
        >>> as_int("3.9")  # 浮点字符串会先转浮点再取整
        3
        >>> as_int("invalid", default=0)
        0
    """
    if value is None:
        if allow_none:
            return default
        return default if default is not None else 0

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        value = value.strip()
        if not value:
            if allow_none:
                return default
            return default if default is not None else 0
        try:
            # 先尝试直接转整数
            return int(value)
        except ValueError:
            try:
                # 再尝试转浮点后取整
                return int(float(value))
            except ValueError:
                pass

    if default is not None:
        return default
    if allow_none:
        return None
    return 0


def as_bool(
    value: Any,
    default: bool = False
) -> bool:
    """
    安全转换为布尔值

    支持多种常见格式：
    - 字符串: "true", "false", "yes", "no", "1", "0", "on", "off"
    - 数字: 1, 0
    - 布尔: True, False

    Args:
        value: 要转换的值
        default: 转换失败时的默认值

    Returns:
        转换后的布尔值

    Examples:
        >>> as_bool("true")
        True
        >>> as_bool("FALSE")
        False
        >>> as_bool("yes")
        True
        >>> as_bool(1)
        True
        >>> as_bool("invalid", default=True)
        True
    """
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        value = value.strip().lower()
        if value in ("true", "yes", "1", "on", "enabled"):
            return True
        if value in ("false", "no", "0", "off", "disabled"):
            return False

    return default


def parse_first_float(
    text: str,
    default: Optional[float] = None,
    allow_none: bool = True
) -> Optional[float]:
    """
    从字符串中提取第一个浮点数

    使用正则表达式匹配字符串中的第一个数字（支持负号和小数点）。

    Args:
        text: 要解析的字符串
        default: 未找到数字时的默认值
        allow_none: 是否允许返回 None

    Returns:
        提取的浮点数，或默认值

    Examples:
        >>> parse_first_float("Temperature: 25.3 C")
        25.3
        >>> parse_first_float("Value is -10.5")
        -10.5
        >>> parse_first_float("No number here", default=0.0)
        0.0
    """
    if not text:
        if allow_none:
            return default
        return default if default is not None else 0.0

    # 匹配可选负号 + 数字 + 可选小数部分
    pattern = r"-?\d+\.?\d*"
    match = re.search(pattern, text)

    if match:
        try:
            return float(match.group())
        except ValueError:
            pass

    if default is not None:
        return default
    if allow_none:
        return None
    return 0.0


def parse_first_int(
    text: str,
    default: Optional[int] = None,
    allow_none: bool = True
) -> Optional[int]:
    """
    从字符串中提取第一个整数

    Args:
        text: 要解析的字符串
        default: 未找到数字时的默认值
        allow_none: 是否允许返回 None

    Returns:
        提取的整数，或默认值

    Examples:
        >>> parse_first_int("Channel: 5, Value: 100")
        5
        >>> parse_first_int("No number", default=0)
        0
    """
    if not text:
        if allow_none:
            return default
        return default if default is not None else 0

    # 匹配可选负号 + 数字
    pattern = r"-?\d+"
    match = re.search(pattern, text)

    if match:
        try:
            return int(match.group())
        except ValueError:
            pass

    if default is not None:
        return default
    if allow_none:
        return None
    return 0


def safe_get(
    data: Dict[str, Any],
    *keys: str,
    default: Optional[T] = None
) -> Union[Any, T]:
    """
    安全地从嵌套字典中获取值

    Args:
        data: 字典数据
        *keys: 键的路径，支持多级嵌套
        default: 未找到时的默认值

    Returns:
        找到的值，或默认值

    Examples:
        >>> config = {"a": {"b": {"c": 42}}}
        >>> safe_get(config, "a", "b", "c")
        42
        >>> safe_get(config, "a", "x", "y", default=0)
        0
        >>> safe_get(config, "missing", default="not found")
        'not found'
    """
    if not data:
        return default

    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        if key not in current:
            return default
        current = current[key]

    return current


def clamp(
    value: float,
    min_value: float,
    max_value: float
) -> float:
    """
    将值限制在指定范围内

    Args:
        value: 要限制的值
        min_value: 最小值
        max_value: 最大值

    Returns:
        限制后的值

    Examples:
        >>> clamp(5, 0, 10)
        5
        >>> clamp(-5, 0, 10)
        0
        >>> clamp(15, 0, 10)
        10
    """
    return max(min_value, min(max_value, value))


def format_number(
    value: Optional[float],
    decimals: int = 2,
    unit: str = ""
) -> str:
    """
    格式化数字为字符串

    Args:
        value: 要格式化的值
        decimals: 小数位数
        unit: 单位字符串

    Returns:
        格式化后的字符串

    Examples:
        >>> format_number(25.567, decimals=1, unit="C")
        '25.6 C'
        >>> format_number(None)
        '--'
    """
    if value is None:
        return "--"
    formatted = f"{value:.{decimals}f}"
    if unit:
        return f"{formatted} {unit}"
    return formatted
