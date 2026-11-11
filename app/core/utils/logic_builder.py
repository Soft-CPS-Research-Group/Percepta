# --- Standard Library Imports ---
from typing import Any, Dict, Optional

# --- Internal Framework Imports ---
from app.core.utils.class_loader import load_class
from app.core.utils.logger import LoggingUtils


def build_logic(
    configuration: Dict[str, Any],
    logic_key: str,
    base_class: type,
    environment: Dict[str, Any],
    logger: LoggingUtils,
    log_prefix: str
) -> Optional[Any]:
    """Creates a logic of an external system (an ML Model or a Target System) from its entry in the configuration.

    Expected `configuration` Data Structure (only the part used here):
    ------------------------------------------------------------------
    {
        "decision_validation_correction_logic": {
            "path": "app.extension.ml_models.model_1.validation_1.Validation1"
        }
    }
    With `logic_key` "decision_validation_correction_logic", it creates `Validation1(environment, logger)`.

    Args:
        configuration (Dict[str, Any]): The entry of the external system, as detailed above.
        logic_key (str): The key of the logic in the entry.
        base_class (type): The class that the logic must extend.
        environment (Dict[str, Any]): The environment, given to the logic.
        logger (LoggingUtils): Logger of the environment, given to the logic.
        log_prefix (str): Prefix of the error messages, to show who is building the logic.

    Returns:
        Optional[Any]: A new instance of the logic, or None if the entry has no such key.

    Raises:
        ValueError: If the path is not valid, the class cannot be loaded, or it does not extend the base class.
    """
    logic_configuration = configuration.get(logic_key)

    # Guard Clause 1: Checks that the logic is configured. If not, there is none, and that is not an error.
    if logic_configuration is None:
        return None

    # Guard Clause 2: Checks that the configuration has a path.
    path = logic_configuration.get("path") if isinstance(logic_configuration, dict) else None
    if not isinstance(path, str) or not path.strip():
        raise ValueError(f"{log_prefix} '{logic_key}' must be a dictionary with a 'path'.")

    # Guard Clause 3: Checks that the path leads to a class that extends the base class.
    logic_class = load_class(path, logic_key, log_prefix, base_class)

    return logic_class(environment, logger)
