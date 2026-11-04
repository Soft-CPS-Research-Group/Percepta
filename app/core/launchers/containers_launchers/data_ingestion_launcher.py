# --- Standard Library Imports ---
import copy
import importlib
from typing import Any, Callable, Dict, List, Optional, Tuple

# --- Internal Framework Imports ---
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.launchers.external_systems_launchers.telemetry_system_launcher_base import TelemetrySystemLauncherBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.services.telemetry_pipeline_service import TelemetryPipelineService
from app.core.utils.instance_configuration import InstanceConfiguration
from app.core.utils.environment_placeholder import replace_environment
from app.core.utils.labels import OutputHandlerLabel
from app.core.utils.logger import LoggingUtils


class _DeferredCallback:
    """Callback whose target is set after it has been given to an Input Handler.

    The Input Handler needs its callback when it is built, but the callback belongs to the
    Telemetry Pipeline Service, which needs the Adapter returned by the same launcher that
    builds the Input Handler. This object is given first and bound to the service afterwards.
    """

    def __init__(self) -> None:
        self._target: Optional[Callable] = None

    def bind(self, target: Callable) -> None:
        """Sets the function that receives the messages.

        Args:
            target (Callable): The function to call (for example, `service.process_message`).
        """
        self._target = target

    async def forward(self, *args, **kwargs) -> Any:
        """Passes a message on to the target.

        This is the function given to the Input Handler. It is a method, not the object itself,
        because the Input Handlers only await a callback that is a coroutine function. A callable
        object is not recognised as one, and would be run in a thread without being awaited.

        Returns:
            Any: What the target returns, so the Input Handler can tell if the message was accepted.

        Raises:
            RuntimeError: If no target was bound yet.
        """
        if self._target is None:
            raise RuntimeError("DataIngestionLauncher | A message arrived before the pipeline was connected.")
        result = self._target(*args, **kwargs)
        if hasattr(result, "__await__"):
            result = await result
        return result


class DataIngestionLauncher:
    """Launcher of the Data Ingestion container.

    It starts the data pipelines of one environment at a time. An environment gets one
    pipeline for each Telemetry System that sends data for it.

    To start a pipeline, it does the following:
    1. Asks the launcher of the Telemetry System for an Adapter and an Input Handler.
    2. Creates a Telemetry Pipeline Service that uses the Adapter and an Output Handler,
       which sends the adapted data to Data Processing.
    3. Connects the Input Handler to the service, and starts it.

    A Telemetry System sends data for an environment if the environment's source mapping
    has an entry for it.
    """

    _LOG_PREFIX: str = "Data Ingestion Launcher |"
    _LOCAL_PROTOCOL_ID: str = OutputHandlerLabel.ASYNC_LOCAL.value      # Output Handler used when Data Processing runs in the same process

    def __init__(
        self,
        instance_configuration: InstanceConfiguration,
        input_launcher: InputHandlersLauncher,
        output_launcher: OutputHandlersLauncher,
        logger: LoggingUtils
    ) -> None:
        """Initializes the Data Ingestion launcher.

        The Percepta Instance Configuration is already validated (see InstanceConfiguration), so its format is not
        checked here. The parts used here are:
        - 'timezone' and 'frequency': passed to the launchers of the Telemetry Systems as settings;
        - 'external_systems.telemetry_systems': the 'path' of the launcher and the 'connection_configuration' of each
          Telemetry System;
        - 'inter_container_communication.data_ingestion_to_data_processing': how to send the data to Data Processing.
          It is only used if Data Processing is not in the same process (see `launch_environment`).

        Args:
            instance_configuration (InstanceConfiguration): The Percepta Instance Configuration, already validated.
            input_launcher (InputHandlersLauncher): Factory of Input Handlers.
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers.
            logger (LoggingUtils): Logging interface.

        Raises:
            ValueError: If a dependency is missing.
        """
        if not instance_configuration:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'instance_configuration' dependency is missing.")
        if not input_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'input_launcher' dependency is missing.")
        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        self._telemetry_systems: Dict[str, dict] = instance_configuration.telemetry_systems
        # Only needed when Data Processing runs in another process (see `launch_environment`)
        self._output_config: Optional[dict] = instance_configuration.data_ingestion_to_data_processing
        self._settings: dict = {
            "timezone": instance_configuration.timezone,
            "frequency": instance_configuration.frequency
        }
        self._input_launcher = input_launcher
        self._output_launcher = output_launcher
        self._logger = logger

        # Launchers of the Telemetry Systems, created on first use and shared by all environments.
        self._telemetry_launchers: Dict[str, TelemetrySystemLauncherBase] = {}

        # What was started, so it can be stopped.
        self._input_handlers: List[InputHandlerBase] = []
        self._output_handlers: List[OutputHandlerBase] = []
        self._rpc_listeners: List[InputHandlerBase] = []

    async def launch_environment(
        self,
        environment_name: str,
        environment: dict,
        next_container_receiver: Optional[Callable] = None,
        logger: Optional[LoggingUtils] = None
    ) -> List[InputHandlerBase]:
        """Launches the Data Ingestion pipelines of one environment, one for each Telemetry System it uses.

        If anything fails, what this call already started is stopped and the error is raised again.

        The Telemetry Systems are independent: if one fails to launch, the error is logged and the
        others are launched anyway. The environment only fails if none of them can be launched.

        Expected `environment` Data Structure (as returned by the Schema Storage):
        --------------------------------------------------------------------------
        {
            "entities": {"EVC01": {"entity_type_id": "ev_charger", "parameters": {}}},
            "telemetry_systems": {
                "cleanwatts": {
                    "source_mapping": {
                        "37507": {"fetch_key": "37507", "match_key": "37507"},
                        "connected_vehicle": None           # Derived by the Adapter, so it has no keys
                    }
                }
            }
        }
        Only the entities and the Telemetry Systems are used. The rest (e.g., 'target_systems', 'ml_model_id')
        is ignored.

        Args:
            environment_name (str): Name of the environment.
            environment (dict): The environment, as detailed above.
            next_container_receiver (Callable, optional): Method of the Data Processing Input Handler
                that receives the data. If given, the data is sent to it directly (same process) and
                the 'inter_container_communication' settings are ignored. Otherwise, the data is sent
                using those settings.
            logger (LoggingUtils, optional): Logger of the environment. It is used by this launcher and
                given to every component built for the environment (Output Handler, Adapters and Input
                Handlers), so all their logs show the environment. Defaults to the logger of the launcher.

        Returns:
            List[InputHandlerBase]: The Input Handlers started, one for each Telemetry System that was launched.

        Raises:
            ValueError: If the environment is invalid, refers to an unknown Telemetry System,
                has an invalid source mapping, or there is no way to reach Data Processing.
            RuntimeError: If none of the Telemetry Systems of the environment could be launched.
        """
        if not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Launch error: the environment name must be a non-empty string.")

        if not isinstance(environment, dict):
            raise ValueError(f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' must be a dictionary.")

        if not isinstance(environment.get("entities"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' must contain 'entities'."
            )

        telemetry_systems = environment.get("telemetry_systems")
        if not isinstance(telemetry_systems, dict) or not telemetry_systems:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' has no 'telemetry_systems'."
            )

        # The environment is the schema of the entities, and the source mapping is kept by Telemetry System
        # Example: {"cleanwatts": {"source_mapping": {"tefhr": {...}}}} -> {"cleanwatts": {"tefhr": {...}}}
        environment_specs = environment
        source_mapping = {system_id: system["source_mapping"] for system_id, system in telemetry_systems.items()}

        unknown = [system_id for system_id in source_mapping if system_id not in self._telemetry_systems]
        if unknown:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: source mapping of environment '{environment_name}' refers to "
                f"telemetry systems that are not configured: {unknown}."
            )

        if next_container_receiver is not None and not callable(next_container_receiver):
            raise ValueError(f"{self._LOG_PREFIX} Launch error: 'next_container_receiver' must be callable.")
        if next_container_receiver is None and self._output_config is None:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: there is no way to reach Data Processing for environment "
                f"'{environment_name}'. Give 'next_container_receiver' or configure "
                f"'inter_container_communication.data_ingestion_to_data_processing'."
            )

        self._validate_source_mapping(environment_name, environment_specs, source_mapping)

        # The logger of the environment is used for everything built here; the launcher's own is the fallback
        logger = logger or self._logger

        # What this call starts, so only that is stopped if it fails
        input_handlers: List[InputHandlerBase] = []
        output_handler: Optional[OutputHandlerBase] = None
        rpc_listener: Optional[InputHandlerBase] = None

        try:
            # One Output Handler per environment, towards Data Processing
            output_handler, rpc_listener = await self._build_output_handler(
                environment_name, next_container_receiver, logger
            )
            self._output_handlers.append(output_handler)
            if rpc_listener:
                self._rpc_listeners.append(rpc_listener)
                await rpc_listener.start()

            failed_systems: List[str] = []

            for system_id, system_source_mapping in source_mapping.items():
                system_input_handler: Optional[InputHandlerBase] = None

                # Outside the try: a launcher that cannot be loaded is a configuration error, not a failure
                # of the Telemetry System, so it stops the environment
                telemetry_launcher = self._get_telemetry_launcher(system_id)

                try:
                    system_specs = self._filter_environment_specs(
                        environment_name, system_id, environment_specs, system_source_mapping
                    )

                    callback = _DeferredCallback()
                    components = await telemetry_launcher.launch(
                        telemetry_system_id=system_id,
                        connection_configuration=self._telemetry_systems[system_id]["connection_configuration"],
                        environment_name=environment_name,
                        environment_specs=system_specs,
                        source_mapping=system_source_mapping,
                        settings=self._settings,
                        callback=callback.forward,
                        logger=logger
                    )
                    # Tracked straight away, so it is stopped if anything fails from here on
                    system_input_handler = components.input_handler
                    input_handlers.append(system_input_handler)
                    self._input_handlers.append(system_input_handler)

                    service = TelemetryPipelineService(
                        adapter=components.adapter,
                        output_handler=output_handler,
                        logger=logger
                    )
                    callback.bind(service.process_message)

                    await system_input_handler.start()

                    # Reaching this point means this Telemetry System is running for the environment:
                    # its Input Handler was built, connected to the pipeline and started
                    logger.info(
                        f"{self._LOG_PREFIX} Telemetry system '{system_id}' launched for environment "
                        f"'{environment_name}'."
                    )
                except Exception as err:
                    # A Telemetry System that fails does not stop the others: only what it started is stopped
                    logger.error(
                        f"{self._LOG_PREFIX} Telemetry system '{system_id}' failed to launch for environment "
                        f"'{environment_name}' ({type(err).__name__}: {err}). Continuing without it."
                    )
                    failed_systems.append(system_id)

                    if system_input_handler is not None:
                        await self._stop_quietly(system_input_handler.stop, logger)
                        input_handlers.remove(system_input_handler)
                        self._input_handlers.remove(system_input_handler)

            # --- Guard Clause: an environment with no telemetry system running is not running at all ---
            if len(failed_systems) == len(source_mapping):
                raise RuntimeError(
                    f"{self._LOG_PREFIX} No telemetry system could be launched for environment "
                    f"'{environment_name}'."
                )

            if failed_systems:
                logger.warning(
                    f"{self._LOG_PREFIX} Environment '{environment_name}' is running without the telemetry "
                    f"systems: {failed_systems}."
                )

            # Telemetry Systems of the source mapping that were launched (the failed ones are left out)
            launched_systems = [system_id for system_id in source_mapping if system_id not in failed_systems]
            logger.info(
                f"{self._LOG_PREFIX} Environment '{environment_name}' is running with the telemetry "
                f"systems: {launched_systems}."
            )
        except Exception:
            logger.error(
                f"{self._LOG_PREFIX} Launch of environment '{environment_name}' failed. "
                f"Stopping what it had started."
            )
            await self._stop_environment(input_handlers, rpc_listener, output_handler, logger)
            raise

        return input_handlers

    async def _build_output_handler(
            self,
            environment_name: str,
            next_container_receiver: Optional[Callable],
            logger: LoggingUtils
    ) -> Tuple[OutputHandlerBase, Optional[InputHandlerBase]]:
        """Builds the Output Handler towards Data Processing.

        If there is a receiver, the Output Handler (AsyncLocal) calls it directly, without waiting
        for its result. Otherwise, it is built from the instance configuration: the 'connection'
        of 'connection_info' is passed as the connection settings and its 'topology' as the
        topology. The '{environment}' placeholder of the topology is replaced by the name of the
        environment, so each environment sends its data to its own exchange.

        Args:
            environment_name (str): Name of the environment the Output Handler is built for.
            next_container_receiver (Callable, optional): Method of the Data Processing Input Handler
                that receives the data.
            logger (LoggingUtils): Logger of the environment, given to the Output Handler.

        Returns:
            Tuple[OutputHandlerBase, Optional[InputHandlerBase]]: The Output Handler and its RPC
                listener, if it has one. Neither is started or tracked here.
        """
        if next_container_receiver is not None:
            # Fire-and-forget: the pipeline does not wait for Data Processing to handle each message
            return await self._output_launcher.build_output_handler(
                handler_type=self._LOCAL_PROTOCOL_ID,
                topology_setup={"rpc_properties": {"enabled": False}},
                server_configurations={"target_callable": next_container_receiver},
                logger=logger
            )

        connection_info = self._output_config.get("connection_info", {})
        topology = self._replace_topology(connection_info.get("topology", {}), environment_name)
        return await self._output_launcher.build_output_handler(
            handler_type=self._output_config["protocol_id"],
            topology_setup=topology,
            server_configurations=connection_info.get("connection", {}),
            logger=logger
        )

    @staticmethod
    def _replace_topology(topology: Dict[str, Any], environment_name: str) -> Dict[str, Any]:
        """Gives the exchanges and queues of a topology the name of the environment.

        The names that are replaced are the 'exchange_name' of each exchange and the 'queue_name' and 'routing_key' of
        each queue. The topology of the configuration is not changed; a copy is returned.

        Example (environment "R-H-01"):
        -------------------------------
        {"exchanges": [{"exchange_name": "percepta.{environment}", "queues": [{"queue_name": "q.{environment}"}]}]}
        -> {"exchanges": [{"exchange_name": "percepta.R-H-01", "queues": [{"queue_name": "q.R-H-01"}]}]}

        Args:
            topology (Dict[str, Any]): The 'topology' of the 'connection_info' of a link.
            environment_name (str): Name of the environment.

        Returns:
            Dict[str, Any]: The topology of the environment.
        """
        topology = copy.deepcopy(topology)
        for exchange in topology.get("exchanges", []):
            if "exchange_name" in exchange:
                exchange["exchange_name"] = replace_environment(exchange["exchange_name"], environment_name)
            for queue in exchange.get("queues", []):
                for key in ("queue_name", "routing_key"):
                    if key in queue:
                        queue[key] = replace_environment(queue[key], environment_name)
        return topology

    async def _stop_environment(
            self,
            input_handlers: List[InputHandlerBase],
            rpc_listener: Optional[InputHandlerBase],
            output_handler: Optional[OutputHandlerBase],
            logger: LoggingUtils
    ) -> None:
        """Stops and forgets what one environment started, leaving the other environments running.

        Args:
            input_handlers (List[InputHandlerBase]): Input Handlers of the environment.
            rpc_listener (InputHandlerBase, optional): RPC listener of the environment's Output Handler.
            output_handler (OutputHandlerBase, optional): Output Handler of the environment.
            logger (LoggingUtils): Logger of the environment, used if a component fails to stop.
        """
        for handler in (*input_handlers, rpc_listener, output_handler):
            if handler is not None:
                await self._stop_quietly(handler.stop, logger)

        for handler in input_handlers:
            if handler in self._input_handlers:
                self._input_handlers.remove(handler)
        if rpc_listener in self._rpc_listeners:
            self._rpc_listeners.remove(rpc_listener)
        if output_handler in self._output_handlers:
            self._output_handlers.remove(output_handler)

    def _get_telemetry_launcher(self, system_id: str) -> TelemetrySystemLauncherBase:
        """Returns the launcher of a Telemetry System, creating it the first time.

        Args:
            system_id (str): Id of the Telemetry System.

        Returns:
            TelemetrySystemLauncherBase: The launcher.
        """
        launcher = self._telemetry_launchers.get(system_id)
        if launcher is None:
            launcher_class = self._load_launcher_class(self._telemetry_systems[system_id]["path"])
            launcher = launcher_class(
                input_launcher=self._input_launcher,
                output_launcher=self._output_launcher,
                logger=self._logger
            )
            self._telemetry_launchers[system_id] = launcher
        return launcher

    @classmethod
    def _load_launcher_class(cls, path: str) -> type:
        """Imports the launcher class at a path.

        Examples:
        ---------
        "app.extension.telemetry_systems.cleanwatts.cw_telemetry_launcher.CWTelemetryLauncher"
        "app.extension.telemetry_systems.cleanwatts.cw_telemetry_launcher:CWTelemetryLauncher"

        Args:
            path (str): Module path and class name, separated by a dot or by a colon.

        Returns:
            type: The launcher class, which must extend TelemetrySystemLauncherBase.

        Raises:
            ValueError: If the class cannot be found or is not a Telemetry System launcher.
        """
        module_path, _, class_name = path.rpartition(":") if ":" in path else path.rpartition(".")
        if not module_path or not class_name:
            raise ValueError(f"{cls._LOG_PREFIX} Invalid launcher path '{path}'.")

        try:
            launcher_class = getattr(importlib.import_module(module_path), class_name)
        except (ImportError, AttributeError) as err:
            raise ValueError(f"{cls._LOG_PREFIX} Could not load launcher '{path}': {err}") from err

        if not (isinstance(launcher_class, type) and issubclass(launcher_class, TelemetrySystemLauncherBase)):
            raise ValueError(
                f"{cls._LOG_PREFIX} Launcher '{path}' must extend TelemetrySystemLauncherBase."
            )
        return launcher_class

    def _validate_source_mapping(self, environment_name: str, environment_specs: dict, source_mapping: dict) -> None:
        """Checks the source mapping of an environment before anything is launched.

        Each Telemetry System maps the ids of the parameters it provides to the keys it needs:
        - "fetch_key": where to get the data from (for example, a tag, an exchange or an environment).
          It is optional, as some Telemetry Systems do not need it (for example, PulseCharge, whose
          exchange comes from the topology template).
        - "match_key": how the data is recognised in the message that arrives. It is mandatory.
        A parameter that the Adapter derives from other parameters (so it has no keys in the
        Telemetry System) is listed with the value None, which means it belongs to the system.

        Expected `source_mapping` Data Structure:
        -----------------------------------------
        {
            "cleanwatts": {
                "37507": {"fetch_key": "37507", "match_key": "37507"},
                "connected_vehicle": None
            },
            "pulsecharge": {
                "soc": {"match_key": "user@example.com"}
            },
            "softcps": {
                "state_of_charge": {"fetch_key": "...", "match_key": "..."}
            }
        }

        Args:
            environment_name (str): Name of the environment, for error messages.
            environment_specs (dict): Schema of the environment.
            source_mapping (dict): Source mapping of the environment, keyed by Telemetry System id.

        Raises:
            ValueError: If a rule is malformed, if a parameter is not in the schema or is in more
                than one entity, or if two Telemetry Systems claim the same parameter.
        """
        # Entities that have each parameter id
        entities_by_parameter: Dict[str, List[str]] = {}
        for entity_id, entity_spec in environment_specs["entities"].items():
            for parameter_id in entity_spec.get("parameters", {}):
                entities_by_parameter.setdefault(parameter_id, []).append(entity_id)

        owner_by_parameter: Dict[str, str] = {}
        for system_id, rules in source_mapping.items():
            if not isinstance(rules, dict) or not rules:
                raise ValueError(
                    f"{self._LOG_PREFIX} Launch error: the source mapping of telemetry system '{system_id}' "
                    f"for environment '{environment_name}' must be a non-empty dictionary."
                )

            for parameter_id, rule in rules.items():
                where = f"telemetry system '{system_id}', parameter '{parameter_id}', environment '{environment_name}'"

                if rule is not None:
                    # 'match_key' is mandatory; 'fetch_key' is optional but, if given, it cannot be empty
                    keys_are_valid = (
                        isinstance(rule, dict)
                        and isinstance(rule.get("match_key"), str) and bool(rule["match_key"].strip())
                        and ("fetch_key" not in rule
                             or (isinstance(rule["fetch_key"], str) and bool(rule["fetch_key"].strip())))
                    )
                    if not keys_are_valid:
                        raise ValueError(
                            f"{self._LOG_PREFIX} Launch error: invalid rule for {where}. It must be None "
                            f"(derived parameter) or have a non-empty 'match_key' and, optionally, "
                            f"a non-empty 'fetch_key'."
                        )

                entities = entities_by_parameter.get(parameter_id, [])
                if not entities:
                    raise ValueError(f"{self._LOG_PREFIX} Launch error: {where} is not in the schema.")
                if len(entities) > 1:
                    raise ValueError(
                        f"{self._LOG_PREFIX} Launch error: {where} is ambiguous, as it exists in "
                        f"several entities: {entities}."
                    )

                if parameter_id in owner_by_parameter:
                    raise ValueError(
                        f"{self._LOG_PREFIX} Launch error: parameter '{parameter_id}' of environment "
                        f"'{environment_name}' is claimed by telemetry systems "
                        f"'{owner_by_parameter[parameter_id]}' and '{system_id}'."
                    )
                owner_by_parameter[parameter_id] = system_id

    def _filter_environment_specs(
            self,
            environment_name: str,
            system_id: str,
            environment_specs: dict,
            source_mapping: dict
    ) -> dict:
        """Keeps only the parameters a Telemetry System is responsible for.

        A parameter belongs to the system if its id is in the system's source mapping
        (including the derived ones, listed with None). Entities left without parameters
        are removed, and so are the actuators, as they belong to Target Systems.

        Expected Output Structure:
        --------------------------
        {"entities": {"EVC01": {"entity_type_id": "ev_charger", "parameters": {"37507": {}}}}}

        Args:
            environment_name (str): Name of the environment, for error messages.
            system_id (str): Id of the Telemetry System, for error messages.
            environment_specs (dict): Schema of the environment.
            source_mapping (dict): Source mapping of the Telemetry System for the environment.

        Returns:
            dict: The schema with only the Telemetry System's parameters.

        Raises:
            ValueError: If no parameter of the schema belongs to the Telemetry System.
        """
        entities = {}
        for entity_id, entity_spec in environment_specs["entities"].items():
            parameters = {
                parameter_id: parameter_spec
                for parameter_id, parameter_spec in entity_spec.get("parameters", {}).items()
                if parameter_id in source_mapping
            }
            if parameters:
                entity = {key: value for key, value in entity_spec.items() if key != "actuators"}
                entity["parameters"] = parameters
                entities[entity_id] = entity

        if not entities:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: the source mapping of telemetry system '{system_id}' "
                f"matches no parameter of environment '{environment_name}'."
            )

        # Only the schema is given to the Telemetry System, not the systems and the model of the environment
        ignored_keys = ("entities", "telemetry_systems", "target_systems", "ml_model_id")
        filtered = {key: value for key, value in environment_specs.items() if key not in ignored_keys}
        filtered["entities"] = entities
        return filtered

    async def stop(self) -> None:
        """Stops everything that was started: Input Handlers first, then the RPC listeners,
        the Output Handlers and the launchers of the Telemetry Systems.

        A failure while stopping one part is logged and does not prevent the others from being stopped.
        """
        for handler in (*self._input_handlers, *self._rpc_listeners, *self._output_handlers):
            await self._stop_quietly(handler.stop)

        for launcher in self._telemetry_launchers.values():
            await self._stop_quietly(launcher.stop)

        self._input_handlers.clear()
        self._rpc_listeners.clear()
        self._output_handlers.clear()
        self._telemetry_launchers.clear()

    async def _stop_quietly(self, stop_function, logger: Optional[LoggingUtils] = None) -> None:
        """Calls a stop function, logging a failure instead of raising it.

        The result is awaited if it is awaitable, as the handlers' stop methods may be synchronous.

        Args:
            stop_function: The stop method to call.
            logger (LoggingUtils, optional): Logger for the failure. Defaults to the logger of the launcher.
        """
        try:
            result = stop_function()
            if hasattr(result, "__await__"):
                await result
        except Exception as err:
            (logger or self._logger).error(f"{self._LOG_PREFIX} Failed to stop a component: {err}")