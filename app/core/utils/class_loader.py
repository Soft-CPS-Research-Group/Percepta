# --- Standard Library Imports ---
import importlib
from typing import Optional


def load_class(path: str, name: str, log_prefix: str, base_class: Optional[type] = None) -> type:
    """Imports a class from a path written in a configuration (e.g., the 'path' of a launcher or of a logic).

    The path says the module and the class that is in it. Two ways of writing it are accepted, which differ only in
    the character between the module and the class name: a dot, or a colon. They lead to the same class:

    Examples:
    ---------
    "app.extension.telemetry_systems.cleanwatts.cw_telemetry_launcher.CWTelemetryLauncher"    # With a dot
    "app.extension.telemetry_systems.cleanwatts.cw_telemetry_launcher:CWTelemetryLauncher"    # With a colon

    Both mean: module "app.extension.telemetry_systems.cleanwatts.cw_telemetry_launcher", class
    "CWTelemetryLauncher". The colon makes the limit between module and class explicit, and the dot is the usual way
    of writing a path in Python (the last dot is the one that separates the class).

    Every failure is a ValueError that says which value of the configuration is wrong, so that whoever reads the
    error knows what to fix, and not an ImportError or an AttributeError from the inside of the import.

    Args:
        path (str): Module path and class name, separated by a dot or by a colon.
        name (str): What the path is for, to show in the error messages (e.g., "telemetry_systems.cleanwatts.path").
        log_prefix (str): Prefix of the error messages, to show who is loading the class (e.g., "Data Ingestion
            Launcher |").
        base_class (type, optional): The class that the loaded class must extend (e.g., TelemetrySystemLauncherBase).
            If None, only the existence of the class is checked.

    Returns:
        type: The class (not an instance of it).

    Raises:
        ValueError: If the path is not in the format 'module.ClassName', the module cannot be imported, the module
            has no such class, what was found is not a class, or it does not extend the base class (if one is given).
    """
    # --- Guard Clause 1: the path must be a text with something in it. Anything else cannot be split. ---
    if not isinstance(path, str) or not path.strip():
        raise ValueError(f"{log_prefix} '{name}' must be a non-empty text with the format 'module.ClassName'.")

    # Splits the path at the separator into the module and the class name. 'rpartition' splits at the LAST
    # occurrence, so that the dots inside the module path are kept and only the one before the class name counts.
    # If the path has a colon, that is the separator, otherwise the last dot is.
    # Example: "app.x.launcher.MyLauncher" -> module_path "app.x.launcher", class_name "MyLauncher"
    #          "app.x.launcher:MyLauncher" -> module_path "app.x.launcher", class_name "MyLauncher"
    module_path, _, class_name = path.rpartition(":") if ":" in path else path.rpartition(".")

    # --- Guard Clause 2: both parts are needed. A path with no separator (e.g., "MyLauncher") gives an empty module
    # path, and one that ends in the separator (e.g., "app.x.launcher.") gives an empty class name. ---
    if not module_path or not class_name:
        raise ValueError(f"{log_prefix} '{name}' is '{path}', which is not in the format 'module.ClassName'.")

    # Imports the module. The module may not exist (ImportError), or its name may not be valid (ValueError, e.g., a
    # path that starts with a dot). 'from err' keeps the original error as the cause.
    try:
        module = importlib.import_module(module_path)
    except (ImportError, ValueError) as err:
        raise ValueError(f"{log_prefix} '{name}': could not import the module '{module_path}': {err}") from err

    # --- Guard Clause 3: the module must have the class. 'getattr' with a default of None gives None instead of an
    # error when it does not. ---
    loaded_class = getattr(module, class_name, None)
    if loaded_class is None:
        raise ValueError(f"{log_prefix} '{name}': the module '{module_path}' has no class '{class_name}'.")

    # --- Guard Clause 4: what was found must be a class. A function or a variable with that name would not do. ---
    if not isinstance(loaded_class, type):
        raise ValueError(f"{log_prefix} '{name}': '{class_name}' must be a class.")

    # --- Guard Clause 5: if a base class is required, the class must extend it. Without it, a path that leads to a
    # class of another kind (e.g., the launcher of a Target System where one of a Telemetry System is expected) would
    # only fail later, when it was used. ---
    if base_class is not None and not issubclass(loaded_class, base_class):
        raise ValueError(f"{log_prefix} '{name}': '{class_name}' must extend {base_class.__name__}.")

    return loaded_class
