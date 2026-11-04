# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.utils.logger import LoggingUtils


@dataclass
class TelemetrySystemComponents:
    """What a Telemetry System launcher returns for one environment.

    Attributes:
        adapter (TelemetryAdapterBase): Adapter that translates the system's messages.
        input_handler (InputHandlerBase): Input Handler that receives the system's messages,
            not yet started.
    """
    adapter: TelemetryAdapterBase
    input_handler: InputHandlerBase


class TelemetrySystemLauncherBase(ABC):
    """Base class for the launchers of Telemetry Systems.

    Each Telemetry System has its own requirements (for example, logging in, or
    refreshing a token), so integrating one means writing a launcher for it. The
    launcher returns the Adapter and a way to build the Input Handler. The service
    and the Output Handler towards the next container are created by the Data
    Ingestion launcher.

    One launcher instance is shared by all the environments that use the same
    Telemetry System, so state that belongs to the system (for example, a login)
    can be kept in the launcher. For that reason, its own logger (`self._logger`) is
    only for what belongs to the system; what belongs to an environment uses the
    logger given to `launch`.
    """

    _LOG_PREFIX: str = "Telemetry System Launcher |"

    def __init__(
        self,
        input_launcher: InputHandlersLauncher,
        output_launcher: OutputHandlersLauncher,
        logger: LoggingUtils
    ) -> None:
        """Initializes the launcher.

        Args:
            input_launcher (InputHandlersLauncher): Factory of Input Handlers.
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers.
            logger (LoggingUtils): Logging interface.

        Raises:
            ValueError: If a dependency is missing.
        """
        if not input_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'input_launcher' dependency is missing.")
        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        self._input_launcher = input_launcher
        self._output_launcher = output_launcher
        self._logger = logger

    @abstractmethod
    async def launch(
        self,
        telemetry_system_id: str,
        connection_configuration: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping: dict,
        settings: dict,
        callback: Callable,
        logger: LoggingUtils
    ) -> TelemetrySystemComponents:
        """Prepares what is needed to receive and adapt the data of one environment.

        Expected `connection_configuration` Data Structure:
        ---------------------------------------------------
        {
            "protocol_id": "AsyncHTTP",
            "connection_info": {}           # Specific to the Telemetry System
        }

        Expected `source_mapping` Data Structure:
        -----------------------------------------
        {
            "37507": {"fetch_key": "37507", "match_key": "37507"},
            "connected_vehicle": None       # Derived by the Adapter, so it has no keys
        }

        Each key is a parameter id of this Telemetry System. "fetch_key" says where to get the data
        (several parameters may share it, so the launcher must request each one only once) and
        "match_key" says how the data is recognised in the message that arrives.

        Expected `settings` Data Structure:
        -----------------------------------
        {
            "timezone": "UTC",
            "frequency": {"value": 15, "unit": "seconds"}
        }

        Args:
            telemetry_system_id (str): Id of the Telemetry System in the Instance Configuration.
            connection_configuration (dict): How to connect to the Telemetry System.
            environment_name (str): Name of the environment.
            environment_specs (dict): Schema of the environment, with only the parameters
                this Telemetry System is responsible for.
            source_mapping (dict): Source mapping of this Telemetry System for this environment,
                already validated by the Data Ingestion launcher.
            settings (dict): Instance settings the launcher may need.
            callback (Callable): Function that the Input Handler calls with each message it receives.
                It is given by the Data Ingestion launcher and only works once the pipeline is
                connected, which happens before the Input Handler is started.
            logger (LoggingUtils): Logger of the environment. It must be given to everything the launcher
                builds for this environment (Adapter, Endpoint Builder and Input Handler), instead of the
                launcher's own logger, so that all their logs show the environment.

        Returns:
            TelemetrySystemComponents: The Adapter and the Input Handler (not started).
        """
        raise NotImplementedError()

    async def stop(self) -> None:
        """Releases what the launcher keeps for the whole Telemetry System (for example,
        a login scheduler). Does nothing by default."""
        return None