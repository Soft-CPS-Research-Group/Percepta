# --- Standard Library Imports ---
import json


def read_json_object(path: str) -> dict:
    """Reads a JSON file that contains a JSON object (a dictionary).

    Args:
        path (str): Path to the file.

    Returns:
        dict: The content of the file.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is not valid JSON, or does not contain a JSON object.

    The messages of the errors have no prefix, so that the caller can add its own
    (e.g., "Application Launcher | Initialization error: ...").
    """
    try:
        # utf-8-sig also accepts files saved with a BOM (e.g., by Windows editors)
        with open(path, "r", encoding="utf-8-sig") as file:
            content = json.load(file)
    except FileNotFoundError as err:
        raise FileNotFoundError(f"file '{path}' does not exist.") from err
    except json.JSONDecodeError as err:
        raise ValueError(f"file '{path}' is not valid JSON: {err}.") from err

    if not isinstance(content, dict):
        raise ValueError(f"file '{path}' must contain a JSON object.")
    return content
