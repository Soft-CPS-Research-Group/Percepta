from typing import Any, Dict, Type


def is_type_compatible(value: Any, expected_type: Type) -> bool:
    """
    Checks whether a single value is compatible with the given expected native type.

    Args:
        value (Any): The value to check.
        expected_type (Type): The native Python type the value is expected to match
            (e.g., float, int, bool, str).

    Returns:
        bool: True if the value's type is compatible with expected_type, False otherwise.
    """
    if value is None:
        return False

    if expected_type in (float, int):
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    elif expected_type is bool:
        return isinstance(value, bool)
    elif expected_type is str:
        return isinstance(value, str)

    return False