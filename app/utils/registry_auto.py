import os
import importlib
import inspect
from typing import TypeVar, Type, Dict, Tuple

T = TypeVar('T')

def discover_subclasses(package: str, base_class: Type[T], required_suffix: str) -> Dict[str, Type[T]]:
    """
    Dynamically discovers and registers all subclasses of a given base class within a package.

    Args:
        package (str): The package name to search in (e.g., "some_handlers").
        base_class (Type): The base class to match subclasses against.
        required_suffix (str): Files must end with this suffix to be considered.
    Returns:
        dict[str, Type]: A mapping from derived name (based on filename) to the subclass type.
    """
    registry = {}

    # Get the actual path of the package
    package_path = importlib.import_module(package).__path__[0]

    # Iterate over all files in the package directory
    for filename in os.listdir(package_path):
        # Skip non-Python files
        if not filename.endswith(".py"):
            continue

        # Enforce required suffix (e.g., "_handler.py")
        if not filename.endswith(required_suffix + ".py"):
            continue

        module_name = filename[:-3]  # Remove '.py' extension
        full_module = f"{package}.{module_name}"

        try:
            module = importlib.import_module(full_module)

            # Iterate through all classes in the module
            for _, obj in inspect.getmembers(module, inspect.isclass):
                # Register only subclasses of the base class (excluding the base itself)
                if issubclass(obj, base_class) and obj is not base_class:
                    registry[module_name] = obj

        except Exception as e:
            print(f"[registry] Failed to import {full_module}: {e}")

    return registry
