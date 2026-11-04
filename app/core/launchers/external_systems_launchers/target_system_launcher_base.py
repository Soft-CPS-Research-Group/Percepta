# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

# --- Internal Framework Imports ---
from app.core.adapters.target_system_adapter_base import TargetSystemAdapterBase
from app.core.integrated_logic.actuation_error_handling_logic_base import ActuationErrorHandlingLogicBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.utils.logger import LoggingUtils
from app.core.utils.logic_builder import build_logic


@dataclass
class TargetSystemComponents:
    """What a Target System launcher returns for one environment.

    Attributes:
        adapter (TargetSystemAdapterBase): Adapter that maps the decisions into the format of the Target System.
        output_handler (OutputHandlerBase): Output Handler that sends the mapped decisions to the Target System.
        error_handling_logic (ActuationErrorHandlingLogicBase): Logic that handles the errors that occur while
            sending decisions to the Target System.
    """
    adapter: TargetSystemAdapterBase
    output_handler: OutputHandlerBase
    error_handling_logic: ActuationErrorHandlingLogicBase


class TargetSystemLauncherBase(ABC):
    """Base class for the launchers of Target Systems.

    Each Target System has its own requirements (for example, how to log in, or what the decisions must look like), so
    integrating one means writing a launcher for it. The launcher returns the Adapter, the Output Handler used to send
    to the system and its Actuation Error Handling Logic. The Decision Dispatching Service is created by the Decision
    Dispatching launcher.

    One launcher instance is shared by all the environments that use that Target System. For that reason, its own
    logger (`self._logger`) is only for what belongs to the launcher; what belongs to an environment uses the logger
    given to `launch`. What the launcher shares between its environments (e.g., a login session) is its own business,
    and is released in `stop`.
    """

    _LOG_PREFIX: str = "Target System Launcher |"

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
        target_system_id: str,
        target_system_configuration: dict,
        environment_name: str,
        environment: dict,
        logger: LoggingUtils
    ) -> TargetSystemComponents:
        """Prepares what is needed to send decisions to the Target System, for one environment.

        Expected `target_system_configuration` Data Structure (the entry of the system in 'external_systems.target_systems'):
        -------------------------------------------------------------------------------------------------------------------
        {
            "path": "app.extension.target_systems.system_1.launcher_1.Launcher1",
            "connection_configuration": {
                "protocol_id": "AsyncHTTP",     # Output Handler type
                "connection_info": {}           # Specific to the Target System
            },
            "actuation_error_handling_logic": {                         # (dict, optional)
                "path": "app.extension.target_systems.system_1.error_handling_1.ErrorHandling1"
            }
        }

        Expected `environment` Data Structure (as returned by the Schema Storage):
        --------------------------------------------------------------------------
        {
            "group": "living lab",
            "entities": {"B01": {"entity_type_id": "battery", "actuators": {}}},
            "target_systems": {"system_1": {"actuator_mapping": {"act_power_1": "35445"}}},
            ...
        }

        Args:
            target_system_id (str): Id of the Target System in the Percepta Instance Configuration.
            target_system_configuration (dict): The entry of the Target System, as detailed above.
            environment_name (str): Name of the environment.
            environment (dict): The environment, as detailed above.
            logger (LoggingUtils): Logger of the environment, given to everything built for it, so their logs show the
                environment.

        Returns:
            TargetSystemComponents: The Adapter, the Output Handler (already connected) and the Actuation Error
                Handling Logic.

        Raises:
            ValueError: If the configuration of the Target System is invalid.
        """
        raise NotImplementedError()

    async def stop(self) -> None:
        """Releases what the launcher shares between its environments (e.g., a login session).

        It does nothing by default. A launcher that starts something of its own (e.g., a task that renews a token) must
        override it. The Output Handlers returned by `launch` are stopped by the Decision Dispatching launcher, not here.
        """
        return None

    def _build_logic(
        self,
        target_system_configuration: dict,
        logic_key: str,
        base_class: type,
        environment: dict,
        logger: LoggingUtils
    ) -> Optional[Any]:
        """Creates a logic of the Target System from its entry in the Percepta Instance Configuration, if it has one.

        Expected `target_system_configuration` Data Structure (only the part used here):
        --------------------------------------------------------------------------------
        {
            "actuation_error_handling_logic": {
                "path": "app.extension.target_systems.system_1.error_handling_1.ErrorHandling1"
            }
        }
        With `logic_key` "actuation_error_handling_logic", it creates `ErrorHandling1(environment, logger)`.

        Args:
            target_system_configuration (dict): The entry of the Target System, as detailed above.
            logic_key (str): The key of the logic in the entry.
            base_class (type): The class that the logic must extend.
            environment (dict): The environment, given to the logic.
            logger (LoggingUtils): Logger of the environment, given to the logic.

        Returns:
            Optional[Any]: A new instance of the logic, or None if the entry has no such key.

        Raises:
            ValueError: If the path is not valid, the class cannot be loaded, or it does not extend the base class.
        """
        return build_logic(
            target_system_configuration, logic_key, base_class, environment, logger, self._LOG_PREFIX
        )
