# --- Standard Library Imports ---
from typing import Callable, Dict, Optional  # Standard PEP 484 static typing annotations

# --- Internal Framework Imports ---
from app.core.storages.runtime_data_storage.mongodb_runtime_data_storage import MongoDBRuntimeDataStorage
from app.core.storages.runtime_data_storage.runtime_data_storage_base import RuntimeDataStorageBase
from app.core.utils.labels import RuntimeDataStorageLabel  # Ids of the types of Runtime Data Storage
from app.core.utils.logger import LoggingUtils  # Central telemetry logger instance

# Global dictionary map that holds the unbound factory functions, indexed by the id of the storage type.
_RUNTIME_DATA_STORAGE_BUILDERS: Dict[str, Callable[..., RuntimeDataStorageBase]] = {}


def register_runtime_data_storage(storage_type_id: str) -> Callable:
    """
    Global decorator utility. Registers a factory function of a Runtime Data Storage in the central
    _RUNTIME_DATA_STORAGE_BUILDERS registry, using the id of the storage type as its key.
    """
    def decorator(func: Callable) -> Callable:
        _RUNTIME_DATA_STORAGE_BUILDERS[storage_type_id] = func
        return func
    return decorator


class RuntimeDataStorageLauncher:
    """
    Creates the Runtime Data Storage configured in the Percepta Instance Configuration.

    Each type of Runtime Data Storage has an id (e.g., "mongodb"). Given that id and the connection info, the launcher
    finds the factory registered for it and builds the storage, so whoever asks for a storage does not need to know
    which class implements it. Each environment has its own storage.
    """

    # Standardized logging identifier prefix
    _LOG_PREFIX: str = "Runtime Data Storage Launcher |"
    _logger: LoggingUtils  # Injected central logging facility

    def __init__(self, logger: LoggingUtils) -> None:
        """
        Initializes the launcher.

        Args:
            logger (LoggingUtils): Logger of the launcher, used when no other logger is given.
        """
        self._logger = logger

    def build_runtime_data_storage(
            self,
            runtime_data_storage_type_id: str,
            connection_info: dict,
            group: str,
            environment_name: str,
            logger: Optional[LoggingUtils] = None
    ) -> RuntimeDataStorageBase:
        """
        Unified public entrypoint. Fetches the factory registered for the storage type and runs it.

        Expected Input:
        ---------------
        runtime_data_storage_type_id = "mongodb"
        connection_info = {"host": "localhost", "port": 27017}
        group = "living lab"
        environment_name = "R-H-01"

        Args:
            runtime_data_storage_type_id (str): Id of the storage type (e.g., "mongodb").
            connection_info (dict): Settings to access the storage. Their content depends on the type.
            group (str): Group of the environment.
            environment_name (str): Name of the environment whose data the storage keeps.
            logger (LoggingUtils, optional): Logger given to the storage. Defaults to the logger of the launcher.

        Returns:
            RuntimeDataStorageBase: The Runtime Data Storage, ready to be used.

        Raises:
            ValueError: If the storage type is not registered, or the storage rejects its arguments.
        """
        logger = logger or self._logger
        logger.info(f"{self._LOG_PREFIX} Resolving Runtime Data Storage type: '{runtime_data_storage_type_id}'...")

        # Guard clause protecting against storage types that were never registered
        if runtime_data_storage_type_id not in _RUNTIME_DATA_STORAGE_BUILDERS:
            error_msg = (
                f"{self._LOG_PREFIX} '{runtime_data_storage_type_id}' is not a registered Runtime Data Storage type. "
                f"Available: {sorted(_RUNTIME_DATA_STORAGE_BUILDERS)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # The factory is stored unbound, so the launcher instance is passed as its first argument
        unbound_builder = _RUNTIME_DATA_STORAGE_BUILDERS[runtime_data_storage_type_id]
        return unbound_builder(self, connection_info, group, environment_name, logger)

    @register_runtime_data_storage(RuntimeDataStorageLabel.MONGODB.value)
    def _build_mongodb_runtime_data_storage(
            self, connection_info: dict, group: str, environment_name: str, logger: LoggingUtils
    ) -> MongoDBRuntimeDataStorage:
        """
        Factory of the MongoDB Runtime Data Storage, which keeps the data in a MongoDB collection.

        Args:
            connection_info (dict): Settings of the storage, which must have the 'host' and the 'port' of MongoDB.
            group (str): Group of the environment, which is the name of the data base.
            environment_name (str): Name of the environment, which gives the name of the collection.
            logger (LoggingUtils): Logger given to the storage.

        Returns:
            MongoDBRuntimeDataStorage: The storage, not connected yet (it connects when it is first used).
        """
        return MongoDBRuntimeDataStorage(connection_info, group, environment_name, logger)
