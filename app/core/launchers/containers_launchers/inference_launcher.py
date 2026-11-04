# --- Standard Library Imports ---
import copy
import importlib
from typing import Any, Callable, Dict, List, Optional, Tuple

# --- Internal Framework Imports ---
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.launchers.external_systems_launchers.ml_model_launcher_base import MLModelLauncherBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.services.inference_service import InferenceService
from app.core.storages.runtime_data_storage.runtime_data_storage_base import RuntimeDataStorageBase
from app.core.storages.runtime_data_storage.runtime_data_storage_launcher import RuntimeDataStorageLauncher
from app.core.utils.environment_placeholder import replace_environment
from app.core.utils.instance_configuration import InstanceConfiguration
from app.core.utils.labels import InputHandlerLabel, OutputHandlerLabel
from app.core.utils.logger import LoggingUtils


class InferenceLauncher:
    """Launcher of the Inference container.

    It starts the components of one environment at a time. Each environment gets its own set of components, which is
    not shared with the other environments: the components of its ML Model (the Adapter, the Output Handler used to
    communicate with the model and its logic), the Runtime Data Storage, the Output Handler towards Decision
    Dispatching, the Inference Service and the Input Handler.

    To launch an environment, it does the following:
    1. Finds the ML Model of the environment (its 'ml_model_id') and asks the launcher of that model for its components.
       The launcher of the model gets the whole entry of the model, so it can build the logics that the entry names.
    2. Builds the Runtime Data Storage, if the instance has one.
    3. Builds the Output Handler, which sends the decisions to the next container (Decision Dispatching).
    4. Creates the Inference Service with all of the above.
    5. Builds the Input Handler, which gives the messages that Data Processing sends to the service, and starts it.

    The environments can use different ML Models (each one has its own entry in the 'external_systems.ml_models' of the
    Percepta Instance Configuration, with its own connection configuration), but the models of the same type are reached
    in the same way, so they share the launcher class. The launcher of each model is created once, when this launcher
    is created.

    How the container talks to the containers next to it (steps 3 and 5) is always decided by the
    'inter_container_communication' of the Percepta Instance Configuration, as in the Data Processing launcher:
    - "AsyncLocal": the containers run in the same process and call each other directly, in memory. A function cannot
      be written in a configuration file, so the launcher that starts the containers hands over the methods: the
      receiver of the next container is given to `launch_environment` ('next_container_receiver'), and the method that
      receives the messages from Data Processing is returned by it.
    - Any other protocol (e.g., "AsyncRabbitMQ"): the Input and Output Handlers are built from the configuration of the
      link. The '{environment}' placeholder is replaced by the name of the environment, so each environment has its
      own exchanges and queues.
    """

    _LOG_PREFIX: str = "Inference Launcher |"

    def __init__(
        self,
        instance_configuration: InstanceConfiguration,
        input_launcher: InputHandlersLauncher,
        output_launcher: OutputHandlersLauncher,
        runtime_data_storage_launcher: RuntimeDataStorageLauncher,
        logger: LoggingUtils
    ) -> None:
        """Initializes the Inference launcher.

        The Percepta Instance Configuration is already validated (see InstanceConfiguration), so its format is not
        checked here. The parts used here are:
        - 'external_systems.ml_models': the launcher and the connection configuration of each ML Model;
        - 'runtime_data_storage': where to store the data of the inferences (optional);
        - 'inter_container_communication.data_processing_to_inference': how to receive the messages from Data
          Processing;
        - 'inter_container_communication.inference_to_decision_dispatching': how to send the decisions to Decision
          Dispatching.
        The last two are optional for the InstanceConfiguration, but this launcher needs them (see the Raises below).

        Args:
            instance_configuration (InstanceConfiguration): The Percepta Instance Configuration, already validated.
            input_launcher (InputHandlersLauncher): Factory of Input Handlers.
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers. It is also given to the launchers of
                the ML Models, so they build the Output Handler used to communicate with the model.
            runtime_data_storage_launcher (RuntimeDataStorageLauncher): Factory of Runtime Data Storages.
            logger (LoggingUtils): Logging interface.

        Raises:
            ValueError: If a dependency is missing, there is no ML Model configured, one of the two links with the
                neighbour containers (Data Processing and Decision Dispatching) is not configured, or the path of the
                launcher of an ML Model cannot be loaded or leads to a class of the wrong type.
        """
        if not instance_configuration:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'instance_configuration' dependency is missing."
            )
        if not input_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'input_launcher' dependency is missing.")
        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")
        if not runtime_data_storage_launcher:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'runtime_data_storage_launcher' dependency is missing."
            )
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # --- Guard Clause 1: there must be at least one ML Model, as there is no inference without one ---
        # The InstanceConfiguration does not require it, because an application may launch only some containers.
        ml_models = instance_configuration.ml_models
        if not ml_models:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: there is no ML Model. "
                f"Configure 'external_systems.ml_models'."
            )

        # --- Guard Clause 2: both links with the neighbour containers must be configured ---
        # Everything is decided by the configuration ('protocol_id' of each link), so there is no default to fall back
        # to. It is better to find this when the launcher is created than when an environment is being launched.
        if instance_configuration.data_processing_to_inference is None:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: there is no way to receive from Data Processing. "
                f"Configure 'inter_container_communication.data_processing_to_inference'."
            )
        if instance_configuration.inference_to_decision_dispatching is None:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: there is no way to reach Decision Dispatching. "
                f"Configure 'inter_container_communication.inference_to_decision_dispatching'."
            )

        self._input_config: dict = instance_configuration.data_processing_to_inference
        self._output_config: dict = instance_configuration.inference_to_decision_dispatching
        self._runtime_data_storage_config: Optional[dict] = instance_configuration.runtime_data_storage
        self._input_launcher = input_launcher
        self._output_launcher = output_launcher
        self._runtime_data_storage_launcher = runtime_data_storage_launcher
        self._logger = logger

        # --- The launcher of each ML Model ---
        # The class of each launcher is loaded once, here, and not for every environment. The connection configuration
        # is kept with it, because each model has its own.
        # The logics of each model are built by its launcher, from the entry of the model.
        self._ml_models: Dict[str, MLModelLauncherBase] = {}
        self._ml_model_configurations: Dict[str, dict] = {}
        for ml_model_id, ml_model in ml_models.items():
            launcher_class = self._load_class(
                ml_model["path"], f"ml_models.{ml_model_id}.path", MLModelLauncherBase
            )
            self._ml_models[ml_model_id] = launcher_class(output_launcher, logger)
            self._ml_model_configurations[ml_model_id] = ml_model

        if self._runtime_data_storage_config is None:
            self._logger.warning(
                f"{self._LOG_PREFIX} There is no 'runtime_data_storage', so the data of the inferences will not be "
                f"stored."
            )

        # What was started, so it can be stopped.
        self._input_handlers: List[InputHandlerBase] = []
        self._output_handlers: List[OutputHandlerBase] = []
        self._runtime_data_storages: List[RuntimeDataStorageBase] = []

    async def launch_environment(
        self,
        environment_name: str,
        environment: dict,
        next_container_receiver: Optional[Callable] = None,
        logger: Optional[LoggingUtils] = None
    ) -> Tuple[InputHandlerBase, Optional[Callable[[Any], Any]]]:
        """Launches the Inference components of one environment.

        If anything fails, what this call already started is stopped and the error is raised again.

        Expected `environment` Data Structure (as returned by the Schema Storage):
        --------------------------------------------------------------------------
        {
            "group": "living lab",                  # Only needed if the instance has a Runtime Data Storage
            "entities": {"EVC01": {"entity_type_id": "ev_charger", "actuators": {}}},
            "ml_model_id": "energaize_r_h_01",      # The ML Model of the environment
            ...                                     # The rest is given as it is to the components of the ML Model
        }

        Args:
            environment_name (str): Name of the environment.
            environment (dict): The environment, as detailed above.
            next_container_receiver (Callable, optional): Method of the next container (Decision Dispatching) that
                receives the decisions. It is mandatory if the 'protocol_id' of
                'inter_container_communication.inference_to_decision_dispatching' is "AsyncLocal": the decisions are
                then sent to it directly, in memory. With any other protocol it is not used, because the decisions are
                sent as the configuration says.
            logger (LoggingUtils, optional): Logger of the environment. It is used by this launcher and given to every
                component built for the environment, so all their logs show the environment. Defaults to the logger
                of the launcher.

        Returns:
            Tuple[InputHandlerBase, Optional[Callable[[Any], Any]]]: Two values:
                1. The Input Handler of the environment, already started. The launcher keeps it too, to stop it.
                2. The method that receives the messages from Data Processing, if the 'protocol_id' of
                   'inter_container_communication.data_processing_to_inference' is "AsyncLocal" (give it to the Data
                   Processing launcher as 'next_container_receiver'). It is a coroutine function, and it hands the
                   message to the Input Handler. With any other protocol, None: Data Processing sends the messages
                   over the network, so there is no method to give it.
            Example: `input_handler, inference_receiver = await launcher.launch_environment(...)`

        Raises:
            ValueError: If the environment is invalid, its ML Model is not configured, the Runtime Data Storage needs a
                'group' that the environment does not have, the link to Decision Dispatching is "AsyncLocal" and there
                is no 'next_container_receiver', or a component rejects the environment.
        """
        if not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Launch error: the environment name must be a non-empty string.")

        if not isinstance(environment, dict) or not isinstance(environment.get("entities"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' must be a dictionary with "
                f"'entities'."
            )

        # --- Guard Clause 1: the environment must say which ML Model it uses, and the model must be configured ---
        ml_model_id = environment.get("ml_model_id")
        if not isinstance(ml_model_id, str) or ml_model_id not in self._ml_models:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: the 'ml_model_id' of environment '{environment_name}' is "
                f"{ml_model_id!r}, which is not one of the ML Models of the instance {sorted(self._ml_models)}."
            )

        # --- Guard Clause 2: a Runtime Data Storage keeps the data of an environment under its group ---
        group = environment.get("group")
        if self._runtime_data_storage_config is not None and (not isinstance(group, str) or not group.strip()):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' needs a 'group' to store its "
                f"data in the Runtime Data Storage."
            )

        # --- Guard Clause 3: a local link to Decision Dispatching needs the method to call ---
        # It cannot be in the configuration, so the launcher that starts the containers has to give it.
        if self._is_local(self._output_config) and (
                next_container_receiver is None or not callable(next_container_receiver)):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'inter_container_communication.inference_to_decision_dispatching' "
                f"is '{OutputHandlerLabel.ASYNC_LOCAL.value}', so environment '{environment_name}' needs a callable "
                f"'next_container_receiver'."
            )

        # The logger of the environment is used for everything built here; the launcher's own is the fallback
        logger = logger or self._logger

        # What this call starts, so only that is stopped if it fails
        input_handler: Optional[InputHandlerBase] = None
        output_handler: Optional[OutputHandlerBase] = None
        model_output_handler: Optional[OutputHandlerBase] = None
        runtime_data_storage: Optional[RuntimeDataStorageBase] = None

        try:
            # Step 1: the components of the ML Model of the environment, from the launcher of that model.
            components = await self._ml_models[ml_model_id].launch(
                ml_model_id, self._ml_model_configurations[ml_model_id], environment_name, environment, logger
            )
            model_output_handler = components.output_handler
            self._output_handlers.append(model_output_handler)

            # Step 2: the Runtime Data Storage, if the instance has one.
            if self._runtime_data_storage_config is not None:
                runtime_data_storage = self._runtime_data_storage_launcher.build_runtime_data_storage(
                    self._runtime_data_storage_config["runtime_data_storage_type_id"],
                    self._runtime_data_storage_config["connection_info"],
                    group,
                    environment_name,
                    logger
                )
                self._runtime_data_storages.append(runtime_data_storage)

            # Step 3: the Output Handler towards Decision Dispatching. If the link is local, the Output Handler
            # (AsyncLocal) calls the receiver directly, without waiting for its result (fire-and-forget).
            output_handler = await self._build_output_handler(environment_name, next_container_receiver, logger)
            self._output_handlers.append(output_handler)

            # Step 4
            service = InferenceService(
                adapter=components.adapter,
                model_output_handler=model_output_handler,
                error_handling_logic=components.error_handling_logic,
                validation_logic=components.validation_logic,
                runtime_data_storage=runtime_data_storage,
                output_handler=output_handler,
                logger=logger
            )

            # Step 5: the Input Handler that gives the messages of Data Processing to the service, built as the
            # configuration says. If the link is local, it is an in-memory one: it has a queue, Data Processing puts
            # the message in it, and the handler gives it to the service one message at a time.
            input_handler = await self._build_input_handler(environment_name, service.receive_message, logger)
            self._input_handlers.append(input_handler)
            await input_handler.start()

            logger.info(
                f"{self._LOG_PREFIX} Environment '{environment_name}' launched with the ML Model '{ml_model_id}'"
                f"{'' if runtime_data_storage is not None else ' and no Runtime Data Storage'}."
            )

            # If the link is not local, Data Processing sends the messages over the network, to the Input Handler built
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

            # The method for Data Processing. It is a coroutine function so that it runs in the event loop (the queue of
            # the Input Handler can only be used safely there), and it calls 'on_message'.
            local_input_handler: AsyncLocalInputHandler = input_handler

            async def receive_from_data_processing(message: Any) -> None:
                local_input_handler.on_message(message)

            return input_handler, receive_from_data_processing
        except Exception:
            logger.error(
                f"{self._LOG_PREFIX} Launch of environment '{environment_name}' failed. "
                f"Stopping what it had started."
            )
            await self._stop_environment(input_handler, output_handler, model_output_handler, runtime_data_storage,
                                         logger)
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

    async def _build_output_handler(
            self,
            environment_name: str,
            next_container_receiver: Optional[Callable],
            logger: LoggingUtils
    ) -> OutputHandlerBase:
        """Builds the Output Handler towards Decision Dispatching.

        It is built as 'inter_container_communication.inference_to_decision_dispatching' says. If its 'protocol_id' is
        "AsyncLocal", the Output Handler calls the receiver of Decision Dispatching directly, without waiting for its
        result. Otherwise, the 'connection' of its 'connection_info' is passed as the connection settings and its
        'topology' as the topology. The '{environment}' placeholder of the topology is replaced by the name of the
        environment, so each environment sends its decisions to its own exchange.

        Expected Configuration Structure (the case without a receiver):
        ----------------------------------------------------------------
        {
            "protocol_id": "AsyncRabbitMQ",
            "connection_info": {
                "connection": {"host": "localhost", "port": 5672, "vhost": "/", "auth": {...}},
                "topology": {"exchanges": [{"exchange_name": "percepta.inference.{environment}", ...}]}
            }
        }

        Args:
            environment_name (str): Name of the environment the Output Handler is built for.
            next_container_receiver (Callable, optional): Method of Decision Dispatching that receives the decisions.
                Only used if the link is "AsyncLocal".
            logger (LoggingUtils): Logger of the environment, given to the Output Handler.

        Returns:
            OutputHandlerBase: The Output Handler. It is not tracked here.

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
        # With RPC, the Output Handler would also need a listener for the replies, and the service sends the decisions
        # without waiting for an answer. Better to refuse it here than to leave a listener open that nobody uses.
        for exchange in topology.get("exchanges", []):
            if isinstance(exchange, dict) and (exchange.get("rpc_properties") or {}).get("enabled"):
                raise ValueError(
                    f"{self._LOG_PREFIX} Launch error: 'rpc_properties' is enabled in "
                    f"'inter_container_communication.inference_to_decision_dispatching'. RPC is not supported between "
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
        """Builds the Input Handler that receives the messages of Data Processing.

        It is built as 'inter_container_communication.data_processing_to_inference' says, which is the same
        configuration that Data Processing uses to send. If its 'protocol_id' is "AsyncLocal", it is an in-memory Input
        Handler. Otherwise, the 'connection' of its 'connection_info' is passed as the connection settings and its
        'topology' as the topology. The '{environment}' placeholder of the topology is replaced by the name of the
        environment, so each environment has its own exchange and queue.

        The launcher only replaces the placeholder: it adds no default values and does not validate the topology (the
        Input Handler does). Everything the Input Handler needs must be in the configuration, including the queue it
        reads from. Data Processing only uses the exchange, so it ignores the queues.

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
        """Loads the launcher class of an ML Model from its path.

        Args:
            path (str): Module path and class name, separated by a dot or by a colon (e.g.,
                "app.extension.ml_models.energaize.energaize_ml_model_launcher.EnergAIzeMLModelLauncher").
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
            output_handler: Optional[OutputHandlerBase],
            model_output_handler: Optional[OutputHandlerBase],
            runtime_data_storage: Optional[RuntimeDataStorageBase],
            logger: LoggingUtils
    ) -> None:
        """Stops and forgets what one environment started, leaving the other environments running.

        The Input Handler is stopped first (no more messages arrive), then the Output Handlers and the storage.

        Args:
            input_handler (InputHandlerBase, optional): Input Handler of the environment.
            output_handler (OutputHandlerBase, optional): Output Handler towards Decision Dispatching.
            model_output_handler (OutputHandlerBase, optional): Output Handler used to communicate with the ML Model.
            runtime_data_storage (RuntimeDataStorageBase, optional): Runtime Data Storage of the environment.
            logger (LoggingUtils): Logger of the environment, used if a component fails to stop.
        """
        for component in (input_handler, output_handler, model_output_handler, runtime_data_storage):
            if component is not None:
                await self._stop_quietly(component.stop, logger)

        if input_handler in self._input_handlers:
            self._input_handlers.remove(input_handler)
        for output in (output_handler, model_output_handler):
            if output in self._output_handlers:
                self._output_handlers.remove(output)
        if runtime_data_storage in self._runtime_data_storages:
            self._runtime_data_storages.remove(runtime_data_storage)

    async def stop(self) -> None:
        """Stops everything that was started: the Input Handlers first, then the Output Handlers and the storages.

        A failure while stopping one part is logged and does not prevent the others from being stopped.
        """
        for component in (*self._input_handlers, *self._output_handlers, *self._runtime_data_storages):
            await self._stop_quietly(component.stop)

        self._input_handlers.clear()
        self._output_handlers.clear()
        self._runtime_data_storages.clear()

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
