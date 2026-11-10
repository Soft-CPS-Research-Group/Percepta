# --- Standard Library Imports ---
import copy
import importlib
from typing import Any, Callable, Dict, List, Optional, Tuple

# --- Internal Framework Imports ---
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.decision_dispatching.target_system_launcher_base import (
    TargetSystemComponents,
    TargetSystemLauncherBase
)
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.decision_dispatching.decision_dispatching_service import DecisionDispatchingService
from app.core.decision_dispatching.target_system_resolver import TargetSystemResolver
from app.core.utils.environment_placeholder import replace_environment
from app.core.utils.instance_configuration import InstanceConfiguration
from app.core.utils.labels import InputHandlerLabel, OutputHandlerLabel
from app.core.utils.logger import LoggingUtils


class DecisionDispatchingLauncher:
    """Launcher of the Decision Dispatching container.

    It starts the components of one environment at a time. Each environment gets its own set of components, which is
    not shared with the other environments: the components of each of its Target Systems (the Adapter, the Output
    Handler and the Actuation Error Handling Logic), the Target System Resolver, the Decision Dispatching Service and
    the Input Handler.

    To launch an environment, it does the following:
    1. Finds the Target Systems of the environment (the keys of its 'target_systems') and asks the launcher of each one
       for its components. A Target System can serve several environments, but each one gets its own components.
    2. Creates the Target System Resolver, which uses the 'actuator_mapping' of the environment's Target Systems.
    3. Creates the Decision Dispatching Service with all of the above.
    4. Builds the Input Handler, which gives the decisions that Inference sends to the service, and starts it.

    The launcher of each Target System is created once, when this launcher is created, and receives the whole entry of
    the system in 'external_systems.target_systems' of the Percepta Instance Configuration. What a Target System
    launcher shares between its environments (e.g., a login session) belongs to it, and it is released when this
    launcher stops.

    How the container receives from Inference (step 4) is decided by the 'inter_container_communication' of the Percepta
    Instance Configuration, as in the Inference launcher:
    - "AsyncLocal": the containers run in the same process and call each other directly, in memory. The method that
      receives the decisions is returned by `launch_environment`, and is given to the Inference launcher as its
      'next_container_receiver'.
    - Any other protocol (e.g., "AsyncRabbitMQ"): the Input Handler is built from the configuration of the link. The
      '{environment}' placeholder is replaced by the name of the environment, so each environment has its own exchange
      and queue.
    """

    _LOG_PREFIX: str = "Decision Dispatching Launcher |"

    def __init__(
        self,
        instance_configuration: InstanceConfiguration,
        input_launcher: InputHandlersLauncher,
        output_launcher: OutputHandlersLauncher,
        logger: LoggingUtils
    ) -> None:
        """Initializes the Decision Dispatching launcher.

        The Percepta Instance Configuration is already validated (see InstanceConfiguration), so its format is not
        checked here. The parts used here are:
        - 'external_systems.target_systems': the launcher and the configuration of each Target System;
        - 'inter_container_communication.inference_to_decision_dispatching': how to receive the decisions from
          Inference. It is optional for the InstanceConfiguration, but this launcher needs it.

        Args:
            instance_configuration (InstanceConfiguration): The Percepta Instance Configuration, already validated.
            input_launcher (InputHandlersLauncher): Factory of Input Handlers.
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers. It is also given to the launchers of
                the Target Systems, so they build the Output Handler used to send to the system.
            logger (LoggingUtils): Logging interface.

        Raises:
            ValueError: If a dependency is missing, there is no Target System configured, the link with Inference is not
                configured, or the path of the launcher of a Target System cannot be loaded or leads to a class of the
                wrong type.
        """
        if not instance_configuration:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'instance_configuration' dependency is missing."
            )
        if not input_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'input_launcher' dependency is missing.")
        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # --- Guard Clause 1: there must be at least one Target System, as there is nowhere to send without one ---
        # The InstanceConfiguration does not require it, because an application may launch only some containers.
        target_systems = instance_configuration.target_systems
        if not target_systems:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: there is no Target System. "
                f"Configure 'external_systems.target_systems'."
            )

        # --- Guard Clause 2: the link with Inference must be configured ---
        if instance_configuration.inference_to_decision_dispatching is None:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: there is no way to receive from Inference. "
                f"Configure 'inter_container_communication.inference_to_decision_dispatching'."
            )

        self._input_config: dict = instance_configuration.inference_to_decision_dispatching
        self._input_launcher = input_launcher
        self._output_launcher = output_launcher
        self._logger = logger

        # The launcher of each Target System, created once, and the whole entry of the system in the configuration
        self._target_systems: Dict[str, TargetSystemLauncherBase] = {}
        self._target_system_configurations: Dict[str, dict] = {}
        for target_system_id, target_system in target_systems.items():
            launcher_class = self._load_class(
                target_system["path"], f"target_systems.{target_system_id}.path", TargetSystemLauncherBase
            )
            self._target_systems[target_system_id] = launcher_class(output_launcher, logger)
            self._target_system_configurations[target_system_id] = target_system

        # What was started, so it can be stopped.
        self._input_handlers: List[InputHandlerBase] = []
        self._output_handlers: List[OutputHandlerBase] = []

    async def launch_environment(
        self,
        environment_name: str,
        environment: dict,
        logger: Optional[LoggingUtils] = None
    ) -> Tuple[InputHandlerBase, Optional[Callable[[Any], Any]]]:
        """Launches the Decision Dispatching components of one environment.

        If anything fails, what this call already started is stopped and the error is raised again.

        Expected `environment` Data Structure (as returned by the Schema Storage):
        --------------------------------------------------------------------------
        {
            "entities": {"B01": {"entity_type_id": "battery", "actuators": {...}}},
            "target_systems": {                                         # The Target Systems of the environment
                "cleanwatts": {"actuator_mapping": {"act_power_1": "35445"}}
            },
            ...
        }

        Args:
            environment_name (str): Name of the environment.
            environment (dict): The environment, as detailed above.
            logger (LoggingUtils, optional): Logger of the environment. It is used by this launcher and given to every
                component built for the environment, so all their logs show the environment. Defaults to the logger
                of the launcher.

        Returns:
            Tuple[InputHandlerBase, Optional[Callable[[Any], Any]]]: Two values:
                1. The Input Handler of the environment, already started. The launcher keeps it too, to stop it.
                2. The method that receives the decisions from Inference, if the 'protocol_id' of
                   'inter_container_communication.inference_to_decision_dispatching' is "AsyncLocal" (give it to the
                   Inference launcher as 'next_container_receiver'). It is a coroutine function, and it hands the
                   decisions to the Input Handler. With any other protocol, None: Inference sends over the network, so
                   there is no method to give it.
            Example: `input_handler, dispatching_receiver = await launcher.launch_environment(...)`

        Raises:
            ValueError: If the environment is invalid or one of its Target Systems is not configured in
                the instance.
        """
        if not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Launch error: the environment name must be a non-empty string.")

        if not isinstance(environment, dict) or not isinstance(environment.get("entities"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' must be a dictionary with "
                f"'entities'."
            )

        # --- The Target Systems are optional: without them the decisions are only logged, not sent anywhere ---
        environment_target_systems = environment.get("target_systems") or {}
        if not isinstance(environment_target_systems, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'target_systems' of environment '{environment_name}' must be a "
                f"dictionary."
            )
        if not environment_target_systems:
            (logger or self._logger).warning(
                f"{self._LOG_PREFIX} Environment '{environment_name}' has no 'target_systems': its decisions will "
                f"only be logged, not dispatched."
            )

        # --- Guard Clause 2: every Target System of the environment must be configured in the instance ---
        unknown = sorted(set(environment_target_systems) - set(self._target_systems))
        if unknown:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' uses the Target Systems "
                f"{unknown}, which are not configured in the instance {sorted(self._target_systems)}."
            )

        # The logger of the environment is used for everything built here; the launcher's own is the fallback
        logger = logger or self._logger

        # What this call starts, so only that is stopped if it fails
        input_handler: Optional[InputHandlerBase] = None
        output_handlers: List[OutputHandlerBase] = []

        try:
            # Step 1: the components of each Target System of the environment, from the launcher of that system.
            components: Dict[str, TargetSystemComponents] = {}
            for target_system_id in environment_target_systems:
                components[target_system_id] = await self._target_systems[target_system_id].launch(
                    target_system_id,
                    self._target_system_configurations[target_system_id],
                    environment_name,
                    environment,
                    logger
                )
                output_handlers.append(components[target_system_id].output_handler)
                self._output_handlers.append(components[target_system_id].output_handler)

                # Reaching this point means the components of this Target System were built for the environment
                logger.debug(
                    f"{self._LOG_PREFIX} Target system '{target_system_id}' launched for environment "
                    f"'{environment_name}'."
                )

            # Step 2 and 3
            service = DecisionDispatchingService(
                resolver=TargetSystemResolver(environment_name, environment, logger),
                target_systems=components,
                logger=logger
            )

            # Step 4: the Input Handler that gives the decisions of Inference to the service, built as the
            # configuration says. If the link is local, it is an in-memory one: it has a queue, Inference puts the
            # decisions in it, and the handler gives them to the service one message at a time.
            input_handler = await self._build_input_handler(environment_name, service.receive_message, logger)
            self._input_handlers.append(input_handler)
            await input_handler.start()

            logger.info(
                f"{self._LOG_PREFIX} Environment '{environment_name}' launched with the Target Systems "
                f"{sorted(environment_target_systems) or 'none'}."
            )

            # If the link is not local, Inference sends the decisions over the network, to the Input Handler built
            # from the configuration. There is no method to give it.
            if not self._is_local(self._input_config):
                return input_handler, None

            # --- Guard Clause: a local link must have been built as a local Input Handler ---
            # Only the local Input Handler has the method 'on_message'. It is checked here, inside the 'try', so that,
            # if it is not the case, what was already started is stopped like for any other failure of the launch.
            if not isinstance(input_handler, AsyncLocalInputHandler):
                raise TypeError(
                    f"{self._LOG_PREFIX} Launch error: the Input Handler of a local link must be an "
                    f"{AsyncLocalInputHandler.__name__}, got {type(input_handler).__name__}."
                )

            # The method for Inference: the 'on_message' of the Input Handler. It is a coroutine function, so the
            # local connector of the Output Handler awaits it in the event loop (a normal function would run in
            # another thread, where the queue of the Input Handler cannot be used safely).
            return input_handler, input_handler.on_message
        except Exception:
            logger.error(
                f"{self._LOG_PREFIX} Launch of environment '{environment_name}' failed. "
                f"Stopping what it had started."
            )
            await self._stop_environment(input_handler, output_handlers, logger)
            raise

    @staticmethod
    def _is_local(link_config: dict) -> bool:
        """Tells if a link between two containers is in memory (both containers run in the same process).

        Args:
            link_config (dict): The configuration of the link, with its 'protocol_id'.

        Returns:
            bool: True if the 'protocol_id' is "AsyncLocal".
        """
        return link_config["protocol_id"] == OutputHandlerLabel.ASYNC_LOCAL.value

    async def _build_input_handler(
            self,
            environment_name: str,
            callback: Callable,
            logger: LoggingUtils
    ) -> InputHandlerBase:
        """Builds the Input Handler that receives the decisions of Inference.

        It is built as 'inter_container_communication.inference_to_decision_dispatching' says, which is the same
        configuration that Inference uses to send. If its 'protocol_id' is "AsyncLocal", it is an in-memory Input
        Handler. Otherwise, the 'connection' of its 'connection_info' is passed as the connection settings and its
        'topology' as the topology. The '{environment}' placeholder of the topology is replaced by the name of the
        environment, so each environment has its own exchange and queue.

        The launcher only replaces the placeholder: it adds no default values and does not validate the topology (the
        Input Handler does). Everything the Input Handler needs must be in the configuration, including the queue it
        reads from. Inference only uses the exchange, so it ignores the queues.

        Args:
            environment_name (str): Name of the environment the Input Handler is built for.
            callback (Callable): Method the Input Handler calls with each message it receives.
            logger (LoggingUtils): Logger of the environment, given to the Input Handler.

        Returns:
            InputHandlerBase: The Input Handler. It is not started or tracked here.
        """
        if self._is_local(self._input_config):
            return await self._input_launcher.build_input_handler(
                handler_type=InputHandlerLabel.ASYNC_LOCAL.value,
                topology_setup={},
                callback=callback,
                server_configurations={},
                logger=logger
            )

        connection_info = self._input_config.get("connection_info", {})
        return await self._input_launcher.build_input_handler(
            handler_type=self._input_config["protocol_id"],
            topology_setup=self._replace_topology(connection_info.get("topology", {}), environment_name),
            callback=callback,
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

    def _load_class(self, path: str, name: str, base_class: type) -> type:
        """Loads the launcher class of a Target System from its path.

        Args:
            path (str): Module path and class name, separated by a dot or by a colon (e.g.,
                "app.extension.target_systems.softcps.softcps_target_launcher.SoftCPSTargetLauncher").
            name (str): Name of the value, for error messages.
            base_class (type): The class that it must extend.

        Returns:
            type: The class.

        Raises:
            ValueError: If the path is not valid, the class cannot be loaded or it does not extend the base class.
        """
        module_path, _, class_name = path.rpartition(":") if ":" in path else path.rpartition(".")
        if not module_path or not class_name:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: invalid path '{path}' for '{name}'.")

        try:
            loaded_class = getattr(importlib.import_module(module_path), class_name)
        except (ImportError, AttributeError) as err:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: could not load '{name}' '{path}': {err}"
            ) from err

        if not (isinstance(loaded_class, type) and issubclass(loaded_class, base_class)):
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: '{name}' '{path}' must extend {base_class.__name__}."
            )

        return loaded_class

    async def _stop_environment(
            self,
            input_handler: Optional[InputHandlerBase],
            output_handlers: List[OutputHandlerBase],
            logger: LoggingUtils
    ) -> None:
        """Stops and forgets what one environment started, leaving the other environments running.

        The Input Handler is stopped first (no more decisions arrive), then the Output Handlers.

        Args:
            input_handler (InputHandlerBase, optional): Input Handler of the environment.
            output_handlers (List[OutputHandlerBase]): Output Handlers of the Target Systems of the environment.
            logger (LoggingUtils): Logger of the environment, used if a component fails to stop.
        """
        for component in (input_handler, *output_handlers):
            if component is not None:
                await self._stop_quietly(component.stop, logger)

        if input_handler in self._input_handlers:
            self._input_handlers.remove(input_handler)
        for output_handler in output_handlers:
            if output_handler in self._output_handlers:
                self._output_handlers.remove(output_handler)

    async def stop(self) -> None:
        """Stops everything that was started: the Input Handlers first, then the Output Handlers and the Target Systems.

        A failure while stopping one part is logged and does not prevent the others from being stopped.
        """
        for component in (*self._input_handlers, *self._output_handlers):
            await self._stop_quietly(component.stop)

        # What each Target System launcher shares between its environments (e.g., a login session)
        for target_system_launcher in self._target_systems.values():
            await self._stop_quietly(target_system_launcher.stop)

        self._input_handlers.clear()
        self._output_handlers.clear()

    async def _stop_quietly(self, stop_function: Callable, logger: Optional[LoggingUtils] = None) -> None:
        """Calls a stop function, logging a failure instead of raising it.

        The result is awaited if it is awaitable, as the stop methods may be synchronous.

        Args:
            stop_function (Callable): The stop method to call.
            logger (LoggingUtils, optional): Logger for the failure. Defaults to the logger of the launcher.
        """
        try:
            result = stop_function()
            if hasattr(result, "__await__"):
                await result
        except Exception as err:
            (logger or self._logger).error(f"{self._LOG_PREFIX} Failed to stop a component: {err}")
