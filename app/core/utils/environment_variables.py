# --- Standard Library Imports ---
import os
import re
from typing import Any

# A value that is only a placeholder of an environment variable, e.g., "${CW_PASSWORD}"
_PLACEHOLDER = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def expand_environment_variables(value: Any) -> Any:
    """Replaces the placeholders of environment variables in a configuration by the value of the variable.

    It is used for what must not be written in the configuration file (e.g., passwords). A string that is only
    "${NAME}" is replaced by the environment variable NAME. The strings that only contain the placeholder in part, and
    everything that is not a string, are left as they are. The dictionaries and lists are searched at any depth, and the
    value received is not changed.

    Example (with the environment variable CW_PASSWORD set to "secret"):
    --------------------------------------------------------------------
    {"credentials": {"username": "user", "password": "${CW_PASSWORD}"}}
    -> {"credentials": {"username": "user", "password": "secret"}}

    Args:
        value (Any): The configuration, or a part of it.

    Returns:
        Any: A copy of the value with the placeholders replaced.

    Raises:
        ValueError: If a placeholder names an environment variable that is not set (or is empty). The message has the
            name of the variable and where it is used, and never a value.
    """
    return _expand(value, "")


def _expand(value: Any, location: str) -> Any:
    """Does the work of 'expand_environment_variables', keeping the location of the value for the error messages."""
    if isinstance(value, dict):
        return {key: _expand(item, f"{location}.{key}" if location else str(key)) for key, item in value.items()}

    if isinstance(value, list):
        return [_expand(item, f"{location}[{index}]") for index, item in enumerate(value)]

    if isinstance(value, str):
        match = _PLACEHOLDER.match(value)
        if match:
            name = match.group(1)
            variable = os.environ.get(name)

            # Guard Clause 1: Checks that the variable is set, because the application would fail later, in a less
            # clear way (e.g., a login with an empty password).
            if not variable:
                raise ValueError(f"the environment variable '{name}' is not set (used in '{location}').")
            return variable

    return value
