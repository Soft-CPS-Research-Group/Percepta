# --- Standard Library Imports ---
from typing import Callable, Dict, Optional  # Standard PEP 484 static typing annotations

# --- Internal Framework Imports ---
from app.core.storages.schema_storage.json_schema_storage import JSONSchemaStorage
from app.core.storages.schema_storage.schema_storage_base import SchemaStorageBase
from app.core.utils.labels import SchemaStorageLabel  # Ids of the types of Schema Storage
from app.core.utils.logger import LoggingUtils  # Central telemetry logger instance

# Global dictionary map that holds the unbound factory functions, indexed by the id of the storage type.
_SCHEMA_STORAGE_BUILDERS: Dict[str, Callable[..., SchemaStorageBase]] = {}


def register_schema_storage(storage_type_id: str) -> Callable:
    """
    Global decorator utility. Registers a factory function of a Schema Storage in the central
    _SCHEMA_STORAGE_BUILDERS registry, using the id of the storage type as its key.
    """
    def decorator(func: Callable) -> Callable:
        _SCHEMA_STORAGE_BUILDERS[storage_type_id] = func
        return func
    return decorator


class SchemaStorageLauncher:
    """
    Creates the Schema Storage configured in the Percepta Instance Configuration.

    Each type of Schema Storage has an id (e.g., "json"). Given that id and the connection info, the launcher
    finds the factory registered for it and builds the storage, so whoever asks for a storage does not need
    to know which class implements it.
    """

    # Standardized logging identifier prefix
    _LOG_PREFIX: str = "Schema Storage Launcher |"
    _logger: LoggingUtils  # Injected central logging facility

    def __init__(self, logger: LoggingUtils) -> None:
        """
        Initializes the launcher.

        Args:
            logger (LoggingUtils): Logger of the launcher, used when no other logger is given.
        """
        self._logger = logger

    def build_schema_storage(
            self,
            schema_storage_type_id: str,
            connection_info: dict,
            logger: Optional[LoggingUtils] = None
    ) -> SchemaStorageBase:
        """
        Unified public entrypoint. Fetches the factory registered for the storage type and runs it.

        Expected Input:
        ---------------
        schema_storage_type_id = "json"
        connection_info = {"file_path": "environment_files/environments.json"}

        Args:
            schema_storage_type_id (str): Id of the storage type (e.g., "json").
            connection_info (dict): Settings to access the storage. Their content depends on the type.
            logger (LoggingUtils, optional): Logger given to the storage. Defaults to the logger of the launcher.

        Returns:
            SchemaStorageBase: The Schema Storage, ready to be used.

        Raises:
            ValueError: If the storage type is not registered, or the storage rejects its connection info.
        """
        logger = logger or self._logger
        logger.debug(f"{self._LOG_PREFIX} Resolving Schema Storage type: '{schema_storage_type_id}'...")

        # Guard clause protecting against storage types that were never registered
        if schema_storage_type_id not in _SCHEMA_STORAGE_BUILDERS:
            error_msg = (
                f"{self._LOG_PREFIX} '{schema_storage_type_id}' is not a registered Schema Storage type. "
                f"Available: {sorted(_SCHEMA_STORAGE_BUILDERS)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # The factory is stored unbound, so the launcher instance is passed as its first argument
        unbound_builder = _SCHEMA_STORAGE_BUILDERS[schema_storage_type_id]
        return unbound_builder(self, connection_info, logger)

    @register_schema_storage(SchemaStorageLabel.JSON.value)
    def _build_json_schema_storage(self, connection_info: dict, logger: LoggingUtils) -> JSONSchemaStorage:
        """
        Factory of the JSON Schema Storage, which keeps the environments in a JSON file.

        Args:
            connection_info (dict): Settings of the storage, which must have the 'file_path' of the file.
            logger (LoggingUtils): Logger given to the storage.

        Returns:
            JSONSchemaStorage: The storage, with the file already read.
        """
        return JSONSchemaStorage(connection_info, logger)