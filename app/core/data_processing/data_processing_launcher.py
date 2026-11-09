# --- Standard Library Imports ---
import copy
import importlib
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# --- Internal Framework Imports ---
from app.core.data_processing.data_issue_handling.data_issue_handling_approaches_launcher import DataIssueHandlingApproachesLauncher
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.data_processing.combination_logic_base import CombinationLogicBase
from app.core.data_processing.data_derivation_logic_base import DataDerivationLogicBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.data_processing.data_processing_service import DataProcessingService
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler
from app.core.utils.environment_placeholder import replace_environment
from app.core.utils.instance_configuration import InstanceConfiguration
from app.core.utils.labels import InputHandlerLabel, OutputHandlerLabel
from app.core.utils.logger import LoggingUtils


class DataProcessingLauncher:
    """Launcher of the Data Processing container.

    It starts the components of one environment at a time. Each environment gets its own set of components, which is
    not shared with the other environments: the Input Handler, the Data-Issue Handling Approaches, the Combination
    Logic, the Data-Derivation Logic (if there is one), the Data Processing Service, the Output Handler and the
    Scheduler.

    To launch an environment, it does the following:
    1. Builds the Data-Issue Handling Approaches, the Combination Logic and the Data-Derivation Logic.
    2. Builds the Output Handler, which sends the unified message to the next container (Inference).
    3. Creates the Data Processing Service with all of the above.
    4. Builds the Input Handler, which gives the messages that Data Ingestion sends to the service, and starts it.
    5. Creates the Scheduler, which triggers the service at every period (at fixed points in time), and starts it.
    The Input Handler and the Scheduler are the two entry points of the container: the first one receives data and the
    second one triggers the processing.

    How the container talks to the containers next to it (steps 2 and 4) is always decided by the
    'inter_container_communication' of the Percepta Instance Configuration. The 'protocol_id' of each link says how:
    - "AsyncLocal": the containers run in the same process and call each other directly, in memory. A function cannot
      be written in a configuration file, so the launcher that starts the containers hands over the methods: the
      receiver of Inference is given to `launch_environment` ('next_container_receiver'), and the method that receives the
      data from Data Ingestion is returned by it.
    - Any other protocol (e.g., "AsyncRabbitMQ"): the Input and Output Handlers are built from the configuration of the
      link. The '{environment}' placeholder is replaced by the name of the environment, so each environment has its
      own exchanges and queues.
    The two links are independent: Data Processing may receive in memory and send over the network, or the opposite.
    """

    _LOG_PREFIX: str = "Data Processing Launcher |"

    def __init__(
        self,
        instance_configuration: InstanceConfiguration,
        input_launcher: InputHandlersLauncher,
        output_launcher: OutputHandlersLauncher,
        approaches_launcher: DataIssueHandlingApproachesLauncher,
        logger: LoggingUtils
    ) -> None:
        """Initializes the Data Processing launcher.

        The Percepta Instance Configuration is already validated (see InstanceConfiguration), so its format is not
        checked here. The parts used here are:
        - 'timezone': the timezone in which the points in time of the periods are counted;
        - 'frequency' and 'grace_period': when the Scheduler triggers the service and how long the service waits;
        - 'data_issue_handling_approaches': the ids of the approaches, in the order in which they are applied;
        - 'integrated_logic': the paths of the Combination Logic (mandatory) and of the Data-Derivation Logic
          (optional);
        - 'inter_container_communication.data_ingestion_to_data_processing': how to receive the data from Data
          Ingestion;
        - 'inter_container_communication.data_processing_to_inference': how to send the message to Inference.
        Both are optional for the InstanceConfiguration, but this launcher needs them (see the Raises below).

        Args:
            instance_configuration (InstanceConfiguration): The Percepta Instance Configuration, already validated.
            input_launcher (InputHandlersLauncher): Factory of Input Handlers.
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers.
            approaches_launcher (DataIssueHandlingApproachesLauncher): Factory of Data-Issue Handling Approaches.
            logger (LoggingUtils): Logging interface.

        Raises:
            ValueError: If a dependency is missing, the Combination Logic is not configured, one of the two links
                with the neighbour containers (Data Ingestion and Inference) is not configured, or the path of a
                logic cannot be loaded or leads to a class of the wrong type.
        """
        if not instance_configuration:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'instance_configuration' dependency is missing."
            )
        if not input_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'input_launcher' dependency is missing.")
        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")
        if not approaches_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'approaches_launcher' dependency is missing.")
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # --- Guard Clause 1: the Combination Logic is mandatory, as there is no way to build the message without it ---
        # The InstanceConfiguration does not require it, because the application used to launch only Data Ingestion.
        integrated_logic = instance_configuration.integrated_logic
        if "combination_logic" not in integrated_logic:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'integrated_logic.combination_logic' is not configured. "
                f"Data Processing needs the path of a class that extends CombinationLogicBase."
            )

        # --- Guard Clause 2: both links with the neighbour containers must be configured ---
        # Everything is decided by the configuration ('protocol_id' of each link), so there is no default to fall back
        # to. The InstanceConfiguration does not require them, because an application may launch only Data Ingestion.
        # It is better to find this when the launcher is created than when an environment is being launched.
        if instance_configuration.data_ingestion_to_data_processing is None:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: there is no way to receive from Data Ingestion. "
                f"Configure 'inter_container_communication.data_ingestion_to_data_processing'."
            )
        # The next container is the REC Bridge if the instance uses it, and Inference otherwise
        if instance_configuration.data_processing_output_link is None:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: there is no way to reach the next container. "
                f"Configure 'inter_container_communication.data_processing_to_inference' (or "
                f"'data_processing_to_rec_bridge', if 'rec_bridge' is configured)."
            )

        # --- Guard Clause 3: the classes of the integrated logic must exist ---
        # They are loaded once, here, and not for every environment. What is done for every environment is to create
        # an instance (see `launch_environment`): it cannot be a copy of one instance made here, because each logic
        # receives the environment in its constructor and keeps its own state, which is not shared between environments.
        self._combination_logic_class: type = self._load_logic_class(
            integrated_logic["combination_logic"], CombinationLogicBase, "combination_logic"
        )
        derivation_logic_path = integrated_logic.get("data_derivation_logic")
        self._derivation_logic_class: Optional[type] = None
        if derivation_logic_path is not None:
            self._derivation_logic_class = self._load_logic_class(
                derivation_logic_path, DataDerivationLogicBase, "data_derivation_logic"
            )

        self._approach_ids: List[str] = instance_configuration.data_issue_handling_approaches
        self._frequency_seconds: float = instance_configuration.frequency_seconds
        self._grace_period_seconds: float = instance_configuration.grace_period_seconds
        self._timezone: ZoneInfo = ZoneInfo(instance_configuration.timezone)
        # The instance settings that the Data Processing Service needs (it builds its own ZoneInfo from the timezone)
        self._settings: dict = {
            "timezone": instance_configuration.timezone,
            "frequency_seconds": self._frequency_seconds,
            "grace_period_seconds": self._grace_period_seconds
        }
        self._input_config: dict = instance_configuration.data_ingestion_to_data_processing
        self._output_config: dict = instance_configuration.data_processing_output_link
        self._input_launcher = input_launcher
        self._output_launcher = output_launcher
        self._approaches_launcher = approaches_launcher
        self._logger = logger

        # What was started, so it can be stopped.
        self._schedulers: List[AsyncPeriodicScheduler] = []
        self._input_handlers: List[InputHandlerBase] = []
        self._output_handlers: List[OutputHandlerBase] = []

    async def launch_environment(
        self,
        environment_name: str,
        environment: dict,
        next_container_receiver: Optional[Callable] = None,
        logger: Optional[LoggingUtils] = None
    ) -> Tuple[InputHandlerBase, Optional[Callable[[Any], Awaitable[None]]]]:
        """Launches the Data Processing components of one environment.

        If anything fails, what this call already started is stopped and the error is raised again.

        Expected `environment` Data Structure (as returned by the Schema Storage):
        --------------------------------------------------------------------------
        {
            "entities": {"EVC01": {"entity_type_id": "ev_charger", "parameters": {}}},
            ...                                     # The rest is given as it is to the approaches and to the logic
        }

        Args:
            environment_name (str): Name of the environment.
            environment (dict): The environment, as detailed above.
            next_container_receiver (Callable, optional): Method of the Inference container that receives the unified
                message. It is mandatory if the 'protocol_id' of
                'inter_container_communication.data_processing_to_inference' is "AsyncLocal": the message is then
                sent to it directly, in memory. With any other protocol it is not used, because the message is sent as
                the configuration says.
            logger (LoggingUtils, optional): Logger of the environment. It is used by this launcher and given to every
                component built for the environment, so all their logs show the environment. Defaults to the logger
                of the launcher.

        Returns:
            Tuple[InputHandlerBase, Optional[Callable[[Any], Awaitable[None]]]]: Two values:
                1. The Input Handler of the environment, already started. The launcher keeps it too, to stop it.
                2. The method that receives the data from Data Ingestion, if the 'protocol_id' of
                   'inter_container_communication.data_ingestion_to_data_processing' is "AsyncLocal" (give it to the
                   Data Ingestion launcher as 'next_container_receiver'). It is a coroutine function, and it hands
                   the data to the Input Handler. With any other protocol, None: Data Ingestion sends the data over
                   the network, so there is no method to give it.
            Example: `input_handler, data_processing_receiver = await launcher.launch_environment(...)`

        Raises:
            ValueError: If the environment is invalid, the link to Inference is "AsyncLocal" and there is no
                'next_container_receiver', an id of an approach is not registered, or the constructor of an approach or of
                a logic rejects the environment.
        """
        if not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Launch error: the environment name must be a non-empty string.")

        if not isinstance(environment, dict) or not isinstance(environment.get("entities"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' must be a dictionary with "
                f"'entities'."
            )

        # --- Guard Clause: a local link to Inference needs the method to call ---
        # It cannot be in the configuration, so the launcher that starts the containers has to give it.
        if self._is_local(self._output_config) and (next_container_receiver is None or not callable(next_container_receiver)):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'inter_container_communication.data_processing_to_inference' is "
                f"'{OutputHandlerLabel.ASYNC_LOCAL.value}', so environment '{environment_name}' needs a callable "
                f"'next_container_receiver'."
            )

        # The logger of the environment is used for everything built here; the launcher's own is the fallback
        logger = logger or self._logger

        # What this call starts, so only that is stopped if it fails
        scheduler: Optional[AsyncPeriodicScheduler] = None
        input_handler: Optional[InputHandlerBase] = None
        output_handler: Optional[OutputHandlerBase] = None

        try:
            # Step 1: the logic of the container. The ids of the approaches and the classes of the logics were already
            # checked (by the InstanceConfiguration and by the constructor of this launcher), so what is done here is
            # to create one NEW instance of each for this environment. They are created first, because their
            # constructors can still fail for reasons that depend on the environment (e.g., a parameter that a logic
            # needs is not in the schema), and it is better to find that before opening anything.
            approaches = self._approaches_launcher.build_approaches(
                self._approach_ids, environment, self._frequency_seconds, logger
            )
            combination_logic = self._combination_logic_class(environment, self._settings, logger)
            derivation_logic = None
            if self._derivation_logic_class is not None:
                derivation_logic = self._derivation_logic_class(environment, self._settings, logger)

            # Step 2: the Output Handler towards Inference. If there is a receiver, the Output Handler (AsyncLocal)
            # calls it directly, without waiting for its result (fire-and-forget), as the Data Ingestion launcher does
            # for Data Processing. Otherwise, it is built from the instance configuration.
            output_handler = await self._build_output_handler(environment_name, next_container_receiver, logger)
            self._output_handlers.append(output_handler)

            # Step 3
            service = DataProcessingService(
                approaches=approaches,
                combination_logic=combination_logic,
                data_derivation_logic=derivation_logic,
                output_handler=output_handler,
                settings=self._settings,
                logger=logger
            )

            # Step 4: the Input Handler that gives the messages of Data Ingestion to the service, built as the
            # configuration says. If the link is local, it is an in-memory one: it has a queue, Data Ingestion puts the
            # data in it, and the handler gives it to the service one message at a time.
            input_handler = await self._build_input_handler(environment_name, service.receive_message, logger)
            self._input_handlers.append(input_handler)
            await input_handler.start()

            # Step 5: the Scheduler triggers the service at fixed points in time (e.g., every 10 minutes: at 10:00,
            # 10:10, ...). Each environment has its own Scheduler, so each one has its own job.
            scheduler = AsyncPeriodicScheduler(timezone=self._timezone, logger=logger)
            scheduler.create_job(
                time_interval=int(self._frequency_seconds),
                trigger_callback=service.process_period,
                job_id=f"data_processing_{environment_name}"
            )
            self._schedulers.append(scheduler)
            scheduler.start()

            logger.info(
                f"{self._LOG_PREFIX} Environment '{environment_name}' launched with the approaches "
                f"{self._approach_ids}, a period of {self._frequency_seconds}s and a grace period of "
                f"{self._grace_period_seconds}s."
            )

            # If the link is not local, Data Ingestion sends the data over the network, to the Input Handler built from
            # the configuration. There is no method to give it.
            if not self._is_local(self._input_config):
                return input_handler, None

            # --- Guard Clause: a local link must have been built as a local Input Handler ---
            # Only the local Input Handler has the method 'on_message' (the others, like the RabbitMQ one, receive by
            # themselves and have no such method). It is checked here, inside the 'try', so that, if it is not the
            # case, what was already started is stopped like for any other failure of the launch.
            if not isinstance(input_handler, AsyncLocalInputHandler):
                raise TypeError(
                    f"{self._LOG_PREFIX} Launch error: the Input Handler of a local link must be an "
                    f"{AsyncLocalInputHandler.__name__}, got {type(input_handler).__name__}."
                )

            # The method for Data Ingestion. The Input Handler already has a method for this ('on_message'), but it is
            # not a coroutine function. For a function that is not, the local connector of Data Ingestion's Output
            # Handler runs it in another thread, and the queue of the Input Handler can only be used safely in the
            # thread of the event loop. So this coroutine function is given instead: it runs in the event loop and
            # calls 'on_message'.
            # (The local variable is needed because 'input_handler' starts as None, and a function defined here sees it
            # with that type. 'local_input_handler' is never None.)
            local_input_handler: AsyncLocalInputHandler = input_handler

            async def receive_from_data_ingestion(payload: Any) -> None:
                local_input_handler.on_message(payload)

            return input_handler, receive_from_data_ingestion
        except Exception:
            logger.error(
                f"{self._LOG_PREFIX} Launch of environment '{environment_name}' failed. "
                f"Stopping what it had started."
            )
            await self._stop_environment(scheduler, input_handler, output_handler, logger)
            raise

    @staticmethod
    def _is_local(link_config: dict) -> bool:
        """Tells if a link between two containers is in memory (both containers run in the same process).

        Args:
            link_config (dict): The configuration of the link, with its 'protocol_id' (e.g., {"protocol_id":
                "AsyncRabbitMQ", "connection_info": {...}}).

        Returns:
            bool: True if the 'protocol_id' is "AsyncLocal".
        """
        return link_config["protocol_id"] == OutputHandlerLabel.ASYNC_LOCAL.value

    async def _build_output_handler(
            self,
            environment_name: str,
            next_container_receiver: Optional[Callable],
            logger: LoggingUtils
    ) -> OutputHandlerBase:
        """Builds the Output Handler towards Inference.

        It is built as 'inter_container_communication.data_processing_to_inference' says. If its 'protocol_id' is
        "AsyncLocal", the Output Handler calls the receiver of Inference directly, without waiting for its result.
        Otherwise, the 'connection' of its 'connection_info' is passed as the connection settings and its 'topology' as
        the topology. The '{environment}' placeholder of the topology is replaced by the name of the environment, so
        each environment sends its message to its own exchange.

        Expected Configuration Structure (the case without a receiver):
        ----------------------------------------------------------------
        {
            "protocol_id": "AsyncRabbitMQ",
            "connection_info": {
                "connection": {"host": "localhost", "port": 5672, "vhost": "/", "auth": {...}},
                "topology": {"exchanges": [{"exchange_name": "percepta.data_processing.{environment}", ...}]}
            }
        }

        Args:
            environment_name (str): Name of the environment the Output Handler is built for.
            next_container_receiver (Callable, optional): Method of Inference that receives the message. Only used if the
                link is "AsyncLocal".
            logger (LoggingUtils): Logger of the environment, given to the Output Handler.

        Returns:
            OutputHandlerBase: The Output Handler. It is not started or tracked here.

        Raises:
            ValueError: If the topology enables RPC (see below), or the Output Handler cannot be built.
        """
        if self._is_local(self._output_config):
            output_handler, _ = await self._output_launcher.build_output_handler(
                handler_type=OutputHandlerLabel.ASYNC_LOCAL.value,
                topology_setup={"rpc_properties": {"enabled": False}},
                server_configurations={"target_callable": next_container_receiver},
                logger=logger
            )
            return output_handler

        connection_info = self._output_config.get("connection_info", {})
        topology = self._replace_topology(connection_info.get("topology", {}), environment_name)

        # --- Guard Clause 1: RPC is not used between containers ---
        # With RPC, the Output Handler would also need a listener for the replies, and the service sends the message
        # without waiting for an answer (see `DataProcessingService.process_period`). Better to refuse it here than to
        # leave a listener open that nobody uses.
        for exchange in topology.get("exchanges", []):
            if isinstance(exchange, dict) and (exchange.get("rpc_properties") or {}).get("enabled"):
                raise ValueError(
                    f"{self._LOG_PREFIX} Launch error: 'rpc_properties' is enabled in "
                    f"'inter_container_communication.data_processing_to_inference'. RPC is not supported between "
                    f"containers."
                )

        output_handler, _ = await self._output_launcher.build_output_handler(
            handler_type=self._output_config["protocol_id"],
            topology_setup=topology,
            server_configurations=connection_info.get("connection", {}),
            logger=logger
        )
        return output_handler

    async def _build_input_handler(
            self,
            environment_name: str,
            callback: Callable,
            logger: LoggingUtils
    ) -> InputHandlerBase:
        """Builds the Input Handler that receives the messages of Data Ingestion.

        It is built as 'inter_container_communication.data_ingestion_to_data_processing' says, which is the same
        configuration that Data Ingestion uses to send. If its 'protocol_id' is "AsyncLocal", it is an in-memory Input
        Handler. Otherwise, the 'connection' of its 'connection_info' is passed as the connection settings and its
        'topology' as the topology. The '{environment}' placeholder of the topology is replaced by the name of the
        environment, so each environment has its own exchange and queue.

        The launcher only replaces the placeholder: it adds no default values and does not validate the topology (the
        Input Handler does). Everything the Input Handler needs must be in the configuration, including the queue it
        reads from. Data Ingestion only uses the exchange, so it ignores the queues.

        Expected Configuration Structure (a network protocol):
        ------------------------------------------------------------------
        {
            "protocol_id": "AsyncRabbitMQ",
            "connection_info": {
                "connection": {"host": "localhost", "port": 5672, "vhost": "/", "auth": {...}},
                "topology": {
                    "exchanges": [{
                        "exchange_name": "percepta.data_ingestion.{environment}",
                        "exchange_properties": {"exchange_type": "fanout", "durable": True},
                        "queues": [{
                            "queue_name": "percepta.data_processing.{environment}",
                            "routing_key": "",
                            "queue_properties": {"durable": False, "auto_delete": True},
                            "consume_properties": {"no_ack": False, "requeue": False}
                        }]
                    }]
                }
            }
        }

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

    def _load_logic_class(self, path: str, base_class: type, name: str) -> type:
        """Loads the class of an integrated logic from its path.

        Args:
            path (str): Module path and class name, separated by a dot or by a colon (e.g.,
                "app.extension.integrated_logic.combination_logic.energaize_combination_logic.EnergAIzeCombinationLogic").
            base_class (type): The class that the logic must extend (CombinationLogicBase or
                DataDerivationLogicBase).
            name (str): Name of the type of logic, for error messages (e.g., "combination_logic").

        Returns:
            type: The class. To use it, create an instance for each environment: `logic_class(environment, logger)`.

        Raises:
            ValueError: If the path is not valid, the class cannot be loaded or it does not extend the base class.
        """
        module_path, _, class_name = path.rpartition(":") if ":" in path else path.rpartition(".")
        if not module_path or not class_name:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: invalid path '{path}' for '{name}'.")

        try:
            logic_class = getattr(importlib.import_module(module_path), class_name)
        except (ImportError, AttributeError) as err:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: could not load '{name}' '{path}': {err}"
            ) from err

        if not (isinstance(logic_class, type) and issubclass(logic_class, base_class)):
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: '{name}' '{path}' must extend {base_class.__name__}."
            )

        return logic_class

    async def _stop_environment(
            self,
            scheduler: Optional[AsyncPeriodicScheduler],
            input_handler: Optional[InputHandlerBase],
            output_handler: Optional[OutputHandlerBase],
            logger: LoggingUtils
    ) -> None:
        """Stops and forgets what one environment started, leaving the other environments running.

        The Scheduler is stopped first (no more periods are triggered), then the Input Handler (no more data arrives)
        and then the Output Handler.

        Args:
            scheduler (AsyncPeriodicScheduler, optional): Scheduler of the environment.
            input_handler (InputHandlerBase, optional): Input Handler of the environment.
            output_handler (OutputHandlerBase, optional): Output Handler of the environment.
            logger (LoggingUtils): Logger of the environment, used if a component fails to stop.
        """
        for component in (scheduler, input_handler, output_handler):
            if component is not None:
                await self._stop_quietly(component.stop, logger)

        if scheduler in self._schedulers:
            self._schedulers.remove(scheduler)
        if input_handler in self._input_handlers:
            self._input_handlers.remove(input_handler)
        if output_handler in self._output_handlers:
            self._output_handlers.remove(output_handler)

    async def stop(self) -> None:
        """Stops everything that was started: the Schedulers first, then the Input Handlers and the Output Handlers.

        A failure while stopping one part is logged and does not prevent the others from being stopped.
        """
        for component in (*self._schedulers, *self._input_handlers, *self._output_handlers):
            await self._stop_quietly(component.stop)

        self._schedulers.clear()
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
