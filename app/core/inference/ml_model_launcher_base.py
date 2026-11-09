# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

# --- Internal Framework Imports ---
from app.core.inference.ml_model_adapter_base import MLModelAdapterBase
from app.core.inference.decision_validation_correction_logic_base import DecisionValidationCorrectionLogicBase
from app.core.inference.model_execution_error_handling_logic_base import ModelExecutionErrorHandlingLogicBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.utils.logger import LoggingUtils
from app.core.utils.logic_builder import build_logic


@dataclass
class MLModelComponents:
    """What an ML Model launcher returns for one environment.

    Attributes:
        adapter (MLModelAdapterBase): Adapter that maps the unified message into the format of the ML Model and the
            decisions of the model into the normalised format.
        output_handler (OutputHandlerBase): Output Handler used to communicate with the ML Model: it sends the mapped
            data and receives the decisions in return.
        error_handling_logic (ModelExecutionErrorHandlingLogicBase): Logic that handles the errors that occur while
            communicating with the ML Model.
        validation_logic (Optional[DecisionValidationCorrectionLogicBase]): Logic that validates and corrects the
            decisions of the ML Model. None if the model has none integrated.
    """
    adapter: MLModelAdapterBase
    output_handler: OutputHandlerBase
    error_handling_logic: ModelExecutionErrorHandlingLogicBase
    validation_logic: Optional[DecisionValidationCorrectionLogicBase] = None


class MLModelLauncherBase(ABC):
    """Base class for the launchers of ML Models.

    Each ML Model has its own requirements (for example, how to reach it, or what it needs to be given), so integrating
    one means writing a launcher for it. The launcher returns the Adapter, the Output Handler used to communicate with
    the model and its logic. The Inference Service is created by the Inference launcher.

    One launcher instance is shared by all the environments that use the same type of ML Model, even if each
    environment has its own model (each one with its own connection configuration, given to `launch`). For that
    reason, its own logger (`self._logger`) is only for what belongs to the launcher; what belongs to an environment
    uses the logger given to `launch`.
    """

    _LOG_PREFIX: str = "ML Model Launcher |"

    def __init__(self, output_launcher: OutputHandlersLauncher, logger: LoggingUtils) -> None:
        """Initializes the launcher.

        Args:
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers.
            logger (LoggingUtils): Logging interface.

        Raises:
            ValueError: If a dependency is missing.
        """
        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        self._output_launcher = output_launcher
        self._logger = logger

    @abstractmethod
    async def launch(
        self,
        ml_model_id: str,
        ml_model_configuration: dict,
        environment_name: str,
        environment: dict,
        logger: LoggingUtils
    ) -> MLModelComponents:
        """Prepares what is needed to communicate with the ML Model of one environment.

        Expected `ml_model_configuration` Data Structure (the entry of the model in 'external_systems.ml_models'):
        ----------------------------------------------------------------------------------------------------------
        {
            "path": "app.extension.ml_models.model_1.launcher_1.Launcher1",
            "connection_configuration": {
                "protocol_id": "AsyncHTTP",     # Output Handler type
                "connection_info": {}           # Specific to the ML Model
            },
            "model_execution_error_handling_logic": {                   # (dict, optional)
                "path": "app.extension.ml_models.model_1.error_handling_1.ErrorHandling1"
            },
            "decision_validation_correction_logic": {                   # (dict, optional)
                "path": "app.extension.ml_models.model_1.validation_1.Validation1"
            }
        }

        Expected `environment` Data Structure (as returned by the Schema Storage):
        --------------------------------------------------------------------------
        {
            "group": "living lab",
            "entities": {"EVC01": {"entity_type_id": "ev_charger", "actuators": {}}},
            "ml_model_id": "energaize_r_h_01",
            ...
        }

        Args:
            ml_model_id (str): Id of the ML Model in the Percepta Instance Configuration.
            ml_model_configuration (dict): The entry of the model in the Percepta Instance Configuration, as detailed
                above: how to connect to it and, optionally, its logics.
            environment_name (str): Name of the environment.
            environment (dict): The environment, as detailed above.
            logger (LoggingUtils): Logger of the environment, given to everything built for it, so their logs show the
                environment.

        Returns:
            MLModelComponents: The Adapter, the Output Handler (already connected) and the logic of the model.

        Raises:
            ValueError: If the configuration of the model is invalid.
        """
        raise NotImplementedError()

    def _build_logic(
        self,
        ml_model_configuration: dict,
        logic_key: str,
        base_class: type,
        environment: dict,
        logger: LoggingUtils
    ) -> Optional[Any]:
        """Creates a logic of the ML Model from its entry in the Percepta Instance Configuration, if it has one.

        Expected `ml_model_configuration` Data Structure (only the part used here):
        ---------------------------------------------------------------------------
        {
            "decision_validation_correction_logic": {
                "path": "app.extension.ml_models.model_1.validation_1.Validation1"
            }
        }
        With `logic_key` "decision_validation_correction_logic", it creates `Validation1(environment, logger)`.

        Args:
            ml_model_configuration (dict): The entry of the model, as detailed above.
            logic_key (str): The key of the logic in the entry (e.g., "decision_validation_correction_logic").
            base_class (type): The class that the logic must extend.
            environment (dict): The environment, given to the logic.
            logger (LoggingUtils): Logger of the environment, given to the logic.

        Returns:
            Optional[Any]: A new instance of the logic, or None if the entry has no such key.

        Raises:
            ValueError: If the path is not valid, the class cannot be loaded, or it does not extend the base class.
        """
        return build_logic(ml_model_configuration, logic_key, base_class, environment, logger, self._LOG_PREFIX)
