UNIT_TYPES = {
    "Kilowatt Hour": float,
    "Celsius": float,
    "Percentage": float,
    "On/Off": bool,
    "String": str
}


def is_type_compatible(value, measurement_unit):
    """
    Checks if a single value is compatible with the expected type of the measurement unit.

    Args:
        value: The value to check.
        measurement_unit: The unit of measurement (used to determine expected type).

    Returns:
        True if compatible, False otherwise.
    """
    expected_type = UNIT_TYPES.get(measurement_unit, float)

    if value is None:
        return False

    if expected_type in [float, int]:
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    elif expected_type == bool:
        return isinstance(value, bool)
    elif expected_type == str:
        return isinstance(value, str)

    return False
