# --- Standard Library Imports ---
from typing import Any, Callable, Dict, List, Optional  # Standard PEP 484 static typing annotations

# --- Internal Framework Imports ---
from app.core.data_processing.data_issue_handling.data_issue_handling_approach_base import DataIssueHandlingApproachBase
from app.core.data_processing.data_issue_handling.duplicated_data_approach import DuplicatedDataApproach
from app.core.data_processing.data_issue_handling.frequency_harmonisation_approach import FrequencyHarmonisationApproach
from app.core.data_processing.data_issue_handling.missing_data_approach import MissingDataApproach
from app.core.data_processing.data_issue_handling.out_of_order_data_approach import OutOfOrderDataApproach
from app.core.utils.labels import DataIssueHandlingApproachLabel  # Ids of the types of approach
from app.core.utils.logger import LoggingUtils  # Central telemetry logger instance

# Global dictionary map that holds the unbound factory functions, indexed by the id of the approach.
_DATA_ISSUE_HANDLING_APPROACH_BUILDERS: Dict[str, Callable[..., DataIssueHandlingApproachBase]] = {}


def register_data_issue_handling_approach(approach_id: str) -> Callable:
    """
    Global decorator utility. Registers a factory function of a Data-Issue Handling Approach in the central
    _DATA_ISSUE_HANDLING_APPROACH_BUILDERS registry, using the id of the approach as its key.
    """
    def decorator(func: Callable) -> Callable:
        _DATA_ISSUE_HANDLING_APPROACH_BUILDERS[approach_id] = func
        return func
    return decorator


class DataIssueHandlingApproachesLauncher:
    """
    Creates the Data-Issue Handling Approaches configured in the Percepta Instance Configuration.

    Each type of approach has an id (e.g., "missing_data"). Given that id, the launcher finds the factory registered
    for it and builds the approach, so whoever asks for an approach does not need to know which class implements it.
    The ids of the approaches that come with Percepta are in DataIssueHandlingApproachLabel
    (app/core/utils/labels.py).

    To add a new Data-Issue Handling Approach, create a class that inherits from this one and register it in the
    Data-Issue Handling Approaches Launcher with an id (see the README of this folder).
    """

    # Standardized logging identifier prefix
    _LOG_PREFIX: str = "Data-Issue Handling Approaches Launcher |"
    _logger: LoggingUtils  # Injected central logging facility

    def __init__(self, logger: LoggingUtils) -> None:
        """
        Initializes the launcher.

        Args:
            logger (LoggingUtils): Logger of the launcher, used when no other logger is given.
        """
        self._logger = logger

    def build_approaches(
            self,
            approach_ids: List[str],
            environment: Dict[str, Any],
            frequency_seconds: float,
            logger: Optional[LoggingUtils] = None
    ) -> List[DataIssueHandlingApproachBase]:
        """
        Builds the approaches of an environment, in the order of the ids. It is the order in which they are applied.

        Expected Input:
        ---------------
        approach_ids = ["missing_data", "duplicated_data", "out_of_order_data", "frequency_harmonisation"]
        environment = {"entities": {...}, ...}                      # As returned by the Schema Storage
        frequency_seconds = 15                                      # Already in seconds

        Args:
            approach_ids (List[str]): Ids of the approaches (the 'data_issue_handling_approaches' of the Percepta
                Instance Configuration).
            environment (Dict[str, Any]): The environment the approaches work for.
            frequency_seconds (float): How often the data is processed, in seconds (e.g., 15 for 15 seconds, 300 for
                5 minutes). It is the length of a period. The InstanceConfiguration gives it already converted.
            logger (LoggingUtils, optional): Logger given to the approaches. Defaults to the logger of the launcher.

        Returns:
            List[DataIssueHandlingApproachBase]: One new approach for each id, in the same order.

        Raises:
            ValueError: If an id is not registered, or an argument has the wrong type.
        """
        return [
            self.build_approach(approach_id, environment, frequency_seconds, logger)
            for approach_id in approach_ids
        ]

    def build_approach(
            self,
            approach_id: str,
            environment: Dict[str, Any],
            frequency_seconds: float,
            logger: Optional[LoggingUtils] = None
    ) -> DataIssueHandlingApproachBase:
        """
        Unified public entrypoint. Fetches the factory registered for the approach and runs it.

        Expected Input:
        ---------------
        approach_id = "missing_data"
        environment = {"entities": {...}, ...}                      # As returned by the Schema Storage
        frequency_seconds = 15                                      # Already in seconds

        Args:
            approach_id (str): Id of the approach (e.g., "missing_data").
            environment (Dict[str, Any]): The environment the approach works for.
            frequency_seconds (float): How often the data is processed, in seconds (e.g., 15 for 15 seconds, 300 for
                5 minutes). It is the length of a period. The InstanceConfiguration gives it already converted.
            logger (LoggingUtils, optional): Logger given to the approach. Defaults to the logger of the launcher.

        Returns:
            DataIssueHandlingApproachBase: A new approach, ready to be used. It is not shared with other environments.

        Raises:
            ValueError: If the id is not registered, an argument has the wrong type, or the factory does not return
                an approach.
        """
        logger = logger or self._logger
        logger.debug(f"{self._LOG_PREFIX} Resolving Data-Issue Handling Approach: '{approach_id}'...")

        # --- Guard Clause 1: the id must be registered ---
        if approach_id not in _DATA_ISSUE_HANDLING_APPROACH_BUILDERS:
            error_msg = (
                f"{self._LOG_PREFIX} '{approach_id}' is not a registered Data-Issue Handling Approach. "
                f"Available: {sorted(_DATA_ISSUE_HANDLING_APPROACH_BUILDERS)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 2: the environment must be a dictionary, and the frequency a number of seconds above zero ---
        if not isinstance(environment, dict):
            raise ValueError(f"{self._LOG_PREFIX} Build error: 'environment' must be a dictionary.")

        # A boolean is a number for Python (True == 1), so it is excluded explicitly
        if isinstance(frequency_seconds, bool) or not isinstance(frequency_seconds, (int, float)) \
                or frequency_seconds <= 0:
            raise ValueError(f"{self._LOG_PREFIX} Build error: 'frequency_seconds' must be a number above zero.")

        # The factory is stored unbound, so the launcher instance is passed as its first argument
        unbound_builder = _DATA_ISSUE_HANDLING_APPROACH_BUILDERS[approach_id]
        approach = unbound_builder(self, environment, frequency_seconds, logger)

        # --- Guard Clause 3: a factory of a new approach could return something else by mistake ---
        if not isinstance(approach, DataIssueHandlingApproachBase):
            raise ValueError(
                f"{self._LOG_PREFIX} Build error: the factory of '{approach_id}' must return a "
                f"{DataIssueHandlingApproachBase.__name__}."
            )
        return approach

    @register_data_issue_handling_approach(DataIssueHandlingApproachLabel.MISSING_DATA.value)
    def _build_missing_data_approach(
            self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils
    ) -> MissingDataApproach:
        """
        Factory of the Missing Data approach, which rebuilds the entities that sent no data in the period.

        Args:
            environment (Dict[str, Any]): The environment, which must have its 'entities'.
            frequency_seconds (float): How often the data is processed, in seconds.
            logger (LoggingUtils): Logger given to the approach.

        Returns:
            MissingDataApproach: The approach, ready to be used.
        """
        return MissingDataApproach(environment, frequency_seconds, logger)

    @register_data_issue_handling_approach(DataIssueHandlingApproachLabel.DUPLICATED_DATA.value)
    def _build_duplicated_data_approach(
            self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils
    ) -> DuplicatedDataApproach:
        """
        Factory of the Duplicated Data approach, which removes the readings that repeat a timestamp.

        Args:
            environment (Dict[str, Any]): The environment. The approach does not use it.
            frequency_seconds (float): How often the data is processed, in seconds. The approach does not use it.
            logger (LoggingUtils): Logger given to the approach.

        Returns:
            DuplicatedDataApproach: The approach, ready to be used.
        """
        return DuplicatedDataApproach(environment, frequency_seconds, logger)

    @register_data_issue_handling_approach(DataIssueHandlingApproachLabel.OUT_OF_ORDER_DATA.value)
    def _build_out_of_order_data_approach(
            self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils
    ) -> OutOfOrderDataApproach:
        """
        Factory of the Out-of-Order Data approach, which sorts the readings of each parameter by timestamp.

        Args:
            environment (Dict[str, Any]): The environment. The approach does not use it.
            frequency_seconds (float): How often the data is processed, in seconds. The approach does not use it.
            logger (LoggingUtils): Logger given to the approach.

        Returns:
            OutOfOrderDataApproach: The approach, ready to be used.
        """
        return OutOfOrderDataApproach(environment, frequency_seconds, logger)

    @register_data_issue_handling_approach(DataIssueHandlingApproachLabel.FREQUENCY_HARMONISATION.value)
    def _build_frequency_harmonisation_approach(
            self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils
    ) -> FrequencyHarmonisationApproach:
        """
        Factory of the Frequency Harmonisation approach, which makes the readings of each parameter match the frequency
        of the Percepta instance.

        Args:
            environment (Dict[str, Any]): The environment, which must have its 'entities'.
            frequency_seconds (float): How often the data is processed, in seconds.
            logger (LoggingUtils): Logger given to the approach.

        Returns:
            FrequencyHarmonisationApproach: The approach, ready to be used.
        """
        return FrequencyHarmonisationApproach(environment, frequency_seconds, logger)
