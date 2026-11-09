# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Dict

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class RECBridgeAdapterBase(ABC):
    """Abstract base class of the Adapter of the REC Bridge.

    A REC (Renewable Energy Community) is a group of installations that share some of their data, so that each model
    can take the others into account. Which data is shared, and how the data of the others is added to the message of
    the installation, depends on the use case, so it is the Adapter that knows it. It has two jobs:

        1. Extracts from the unified message of the installation the data that is relevant to the REC (the data that
           is sent to the other installations).
        2. Combines the data received from the other installations with the unified message of the installation.

    One instance of the Adapter is created for each environment, and the same instance is used in every period of that
    environment. Nothing is shared between environments.

    To integrate an Adapter, create a class that inherits from this one and implement 'extract_rec_data' and
    'combine_with_received_data'. Its path is in 'rec_bridge.adapter' of the Percepta Instance Configuration.
    """

    _LOG_PREFIX: str = "REC Bridge Adapter |"

    def __init__(self, environment_name: str, environment: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the Adapter.

        Expected Input Structure:
        -------------------------
        environment = {
            "entities": {"EVC01": {"entity_type_id": "ev_charger", "parameters": {...}}},
            "rec": {"community_id": "porto", "installation_id": "lab-01", "peers": ["lab-02"], ...},
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }

        Args:
            environment_name (str): Name of the environment this Adapter works for.
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage. It tells the Adapter
                which parameters are relevant to the REC.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If 'environment_name' is not a non-empty string, 'environment' is not a dictionary or 'logger'
                is missing.
        """
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")
        if not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'environment_name' must be a non-empty string.")
        if not isinstance(environment, dict):
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'environment' must be a dictionary.")

        self._environment_name = environment_name
        self._environment = environment
        self._logger = logger

    @abstractmethod
    def extract_rec_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extracts from the unified message the data that is shared with the other installations.

        Expected Output Structure (the structure is decided by the Adapter, it only has to be JSON):
        ------------------------------------------------------------------------------------------
        {"entities": {"PV01": {"energy_out": {...}}}, "derived_data": {"solar_generation": {...}}}

        Args:
            message (Dict[str, Any]): The unified message of the period.

        Returns:
            Dict[str, Any]: The data relevant to the REC. It may be empty.
        """
        raise NotImplementedError()

    @abstractmethod
    def combine_with_received_data(
            self, message: Dict[str, Any], received_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Combines the data received from the other installations with the unified message.

        Expected Input Structure:
        -------------------------
        received_data = {"lab-02": <what 'extract_rec_data' of that installation returned>}

        Args:
            message (Dict[str, Any]): The unified message of the period. It is not changed.
            received_data (Dict[str, Dict[str, Any]]): The data of each installation that sent it in time, by id of
                the installation. It is empty if none did.

        Returns:
            Dict[str, Any]: The resulting message, which is given to Inference.
        """
        raise NotImplementedError()
