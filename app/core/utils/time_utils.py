# --- Standard Library Imports ---
from datetime import datetime
from typing import Any

# Format of the timestamps of the standard format of the data (see the Adapters), e.g., "2026-08-19 11:00:00 +0100"
TIMESTAMP_FORMAT: str = "%Y-%m-%d %H:%M:%S %z"

# Seconds in each unit of time. Besides the full names, the short forms used in the environments are accepted
_SECONDS_PER_UNIT: dict = {
    "s": 1, "sec": 1, "second": 1, "seconds": 1,
    "min": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600
}


def format_timestamp(moment: datetime) -> str:
    """Writes a moment in the format of the timestamps of the standard format of the data.

    Example:
    --------
    datetime(2026, 8, 19, 11, 0, 0, tzinfo=timezone(timedelta(hours=1))) -> "2026-08-19 11:00:00 +0100"

    Args:
        moment (datetime): The moment. It must have a timezone, otherwise the timestamp would have no offset.

    Returns:
        str: The timestamp.
    """
    return moment.strftime(TIMESTAMP_FORMAT)


def parse_timestamp(timestamp: str) -> datetime:
    """Reads a timestamp of the standard format of the data.

    Example:
    --------
    "2026-08-19 11:00:00 +0100" -> datetime(2026, 8, 19, 11, 0, 0, tzinfo=timezone(timedelta(hours=1)))

    Args:
        timestamp (str): The timestamp, in the format "%Y-%m-%d %H:%M:%S %z".

    Returns:
        datetime: The moment, with its timezone.

    Raises:
        ValueError: If the timestamp is not a string in that format.
    """
    if not isinstance(timestamp, str):
        raise ValueError(f"The timestamp must be a string, and it is {type(timestamp).__name__}.")
    return datetime.strptime(timestamp, TIMESTAMP_FORMAT)


def duration_to_seconds(duration: Any, name: str = "duration") -> float:
    """Converts a duration to seconds.

    Expected Input Structure:
    -------------------------
    {"value": 5, "unit": "min"}         # The unit can be "seconds", "minutes" or "hours", or "sec", "min" or "h"

    Example:
    --------
    {"value": 5, "unit": "min"} -> 300

    Args:
        duration (Any): The duration, as above.
        name (str): Name of the duration, only to build the error messages (e.g., "periodicity").

    Returns:
        float: The duration in seconds.

    Raises:
        ValueError: If it is not a dictionary, its 'value' is not a number above zero, or its 'unit' is unknown.
    """
    if not isinstance(duration, dict):
        raise ValueError(f"'{name}' must be a dictionary with a 'value' and a 'unit'.")

    # A boolean is a number for Python (True == 1), so it is excluded explicitly
    value = duration.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"'{name}.value' must be a number above zero.")

    unit = duration.get("unit")
    if not isinstance(unit, str) or unit.strip().lower() not in _SECONDS_PER_UNIT:
        raise ValueError(f"'{name}.unit' is '{unit}', which is not a known unit. Known: {sorted(_SECONDS_PER_UNIT)}.")
    return value * _SECONDS_PER_UNIT[unit.strip().lower()]
