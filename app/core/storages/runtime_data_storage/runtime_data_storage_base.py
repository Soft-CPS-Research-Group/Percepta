# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class RuntimeDataStorageBase(ABC):
    """Abstract base class of all Runtime Data Storages.

    A Runtime Data Storage keeps the data that flows through the Inference container of ONE environment, so it can be
    used later (e.g., to retrain the ML Model, or to inspect what it received and decided): the input of the ML Model,
    the decisions it produced and the decisions after the correction, if there was one. How the data is kept (MongoDB,
    files, a relational database...) is up to each implementation.

    The storage only accepts data. It has nothing to return, because Percepta does not read the data back.

    It is not the same as the Schema Storage, even if both use the same technology (e.g., MongoDB). The Schema Storage
    converts the environments kept in a database into a format that Percepta can interpret. This one keeps the data
    that flows through the system.

    Errors: an implementation does NOT hide the errors of the storage. It raises them, and whoever stores the data
    decides what to do. The Inference Service logs them and carries on, because a failure of the storage must never
    prevent the decisions from being dispatched.

    To add a new type of Runtime Data Storage, create a class that inherits from this one, and register it in the
    Runtime Data Storage Launcher with an id (see the README of this folder).
    """

    _LOG_PREFIX: str = "Runtime Data Storage Base |"

    def __init__(self, connection_info: Dict[str, Any], group: str, environment_name: str, logger: LoggingUtils) -> None:
        """Initializes the Runtime Data Storage.

        The implementation validates its own 'connection_info' before it tries to connect to anything, so that an
        invalid configuration is reported clearly.

        Args:
            connection_info (Dict[str, Any]): Settings needed to access the storage. Their content depends on the type
                of storage (e.g., a MongoDB storage needs the host and the port).
            group (str): Group of the environment (e.g., "living lab"), as in the Schema Storage.
            environment_name (str): Name of the environment whose data this storage keeps.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If 'connection_info' is not a dictionary, if 'group' or 'environment_name' is not a non-empty
                string, or if 'logger' is missing.
        """
        # Guard Clause 1: Checks that the logger exists.
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # Guard Clause 2: Checks that the connection info is a dictionary.
        if not isinstance(connection_info, dict):
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'connection_info' must be a dictionary.")

        # Guard Clause 3: Checks that the group and the name of the environment are non-empty strings.
        for name, value in (("group", group), ("environment_name", environment_name)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{self._LOG_PREFIX} Initialization error: '{name}' must be a non-empty string.")

        self._connection_info = connection_info
        self._group = group
        self._environment_name = environment_name
        self._logger = logger

    @abstractmethod
    async def store(self, data: Dict[str, Any]) -> None:
        """Stores the data of one inference.

        Expected Input Structure:
        -------------------------
        data = {
            "model_inputs": {},                 # The input of the ML Model, in the format it expects
            "decisions": {},                    # The decisions of the ML Model, in the format it produces.
                                                # None if the inference failed (the period is stored anyway)
            "corrected_decisions": {},          # The decisions after the correction, in the format of the ML Model.
                                                # None if the ML Model has no Decision Validation and Correction Logic,
                                                # or if the inference failed
            "timestamp": "2026-08-19 11:00:00 +0000"
        }

        The decisions are NOT in the normalised format: they are the ones of the ML Model.

        Args:
            data (Dict[str, Any]): The data of the inference, as detailed above.

        Raises:
            ValueError: If the data does not have the structure above.
            Exception: Any error of the storage (e.g., it is unreachable). It is not hidden, see the class
                documentation.
        """
        raise NotImplementedError()

    @abstractmethod
    async def stop(self) -> None:
        """Releases what the storage holds (e.g., its connection to the database)."""
        raise NotImplementedError()
