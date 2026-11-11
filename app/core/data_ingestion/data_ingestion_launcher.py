# --- Standard Library Imports ---
import asyncio
import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple

# --- Internal Framework Imports ---
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.data_ingestion.telemetry_system_launcher_base import TelemetrySystemLauncherBase
from app.core.output_handlers.log_output_handler import LogOutputHandler
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.data_ingestion.telemetry_pipeline_service import TelemetryPipelineService
from app.core.utils.instance_configuration import InstanceConfiguration
from app.core.utils.environment_placeholder import replace_environment_in
from app.core.utils.class_loader import load_class
from app.core.utils.labels import OutputHandlerLabel
from app.core.utils.logger import LoggingUtils


class _DeferredCallback:
    """Callback whose target is set after it has been given to an Input Handler.

    Why it exists:
    --------------
    The pieces of a pipeline need each other in a circle, so none of them can be created first:
    - The Input Handler needs its callback (where to send each message) at the moment it is built.
    - That callback is the 'process_message' of the Telemetry Pipeline Service.
    - The service needs the Adapter.
    - The Adapter is only returned by the launcher of the Telemetry System, which is the same launcher that builds
      the Input Handler.
    So the Input Handler must exist before the service, but it needs a callback that only exists after it.

    How it solves it:
    -----------------
    This object is an empty box that looks like a callback. It is given to the Input Handler first, and the real
    target is put in the box later:

        callback = _DeferredCallback()
        components = await telemetry_launcher.launch(callback=callback.forward, ...)   # Input Handler gets 'forward'
        service = TelemetryPipelineService(adapter=components.adapter, ...)            # Now the service can be built
        callback.bind(service.process_message)                                         # The box is filled
        await components.input_handler.start()                                         # Messages only start after this

    From then on, each message that reaches 'forward' is passed on to the service. The Input Handler is started only
    after 'bind', so no message arrives while the box is still empty.
    """

    def __init__(self) -> None:
        # The function that receives the messages. It is None until 'bind' is called.
        self._target: Optional[Callable] = None

        # True if the target is a coroutine function ('async def'). It is checked once, in 'bind', and not in every
        # message.
        self._target_is_async: bool = False

    def bind(self, target: Callable) -> None:
        """Puts the real function in the box, from now on the one that receives the messages.

        Args:
            target (Callable): The function to call with each message (for example, `service.process_message`).
                It can be a coroutine function ('async def') or a normal function.

        Raises:
            TypeError: If the target is not callable.
        """
        if not callable(target):
            raise TypeError(
                f"DataIngestionLauncher | The callback target must be callable, got {type(target).__name__}."
            )

        self._target = target
        self._target_is_async = inspect.iscoroutinefunction(target)

    async def forward(self, *args, **kwargs) -> Any:
        """Passes a message on to the target.

        This is the function that is given to the Input Handler as its callback. It is a method ('async def') and
        not the object itself on purpose: an Input Handler decides how to call its callback by checking if it is a
        coroutine function ('inspect.iscoroutinefunction'). A coroutine function is awaited in the event loop, but an
        object with a '__call__' is not recognised as one, so it would be run in another thread and its result would
        not be awaited. With 'forward', the Input Handler awaits it as it should.

        If the target is a normal function, it is run in another thread so that it does not block the event loop (the
        same as the local connector does with its 'target_callable').

        Returns:
            Any: What the target returns. The Input Handler uses it to decide what to do with the message (e.g., the
                RabbitMQ Input Handler rejects the message if the result is False and acknowledges it otherwise).

        Raises:
            RuntimeError: If 'bind' was not called yet, which means a message arrived before the pipeline was
                connected. It does not happen in the normal order, as the Input Handler is started after 'bind'.
        """
        # Guard Clause: without it, the call below would fail with "'NoneType' object is not callable", which does not
        # say what went wrong.
        if self._target is None:
            raise RuntimeError("DataIngestionLauncher | A message arrived before the pipeline was connected.")

        # The target is called in a different way depending on the kind of function it is. Either way, 'forward' waits
        # until the target ends and returns what it returns (or raises its error), but the event loop is never blocked
        # while it waits: it keeps running the other tasks.
        if self._target_is_async:
            # A coroutine function ('async def', e.g., 'service.process_message'). Calling it does not run its code, it
            # only creates a coroutine. The 'await' runs it in the event loop until its end, and gives the value that
            # it returned. Without the 'await', its code would never run and what was returned would be the coroutine.
            return await self._target(*args, **kwargs)

        # A normal function ('def'). If it was called here, it would stop the whole event loop until it ended (e.g., a
        # function that takes 2 seconds would freeze every task for 2 seconds). So it runs in another thread
        # ('asyncio.to_thread'), and the 'await' waits for the thread to finish while the loop stays free. Because it
        # runs outside the event loop, it must not use the objects that belong to the loop (e.g., an 'asyncio.Queue').
        return await asyncio.to_thread(self._target, *args, **kwargs)


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
    # Output Handler used when Data Processing runs in the same process
    _LOCAL_PROTOCOL_ID: str = OutputHandlerLabel.ASYNC_LOCAL.value

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
          Its 'protocol_id' decides the kind of link: "AsyncLocal" means that Data Processing runs in the same process
          and is called directly (see `launch_environment`), and any other protocol (e.g., "AsyncRabbitMQ") sends
          over the network. If it is not configured, there is nowhere to send the data, and it is only logged (see
          `launch_environment`).

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

        # How to reach Data Processing. It is None if it is not configured, and then the data is only logged
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
                        "37507": {"fetch_key": "37507", "match_key": "37507"}
                    }
                }
            }
        }
        Only the entities and the Telemetry Systems are used. The rest (e.g., 'target_systems', 'ml_model_id')
        is ignored. The environment must have already been validated (see `EnvironmentsConfiguration`): its format, the
        source mapping of each Telemetry System against the schema, and that the Telemetry Systems are configured in
        the instance. Only what Data Ingestion needs for itself is checked here.

        Args:
            environment_name (str): Name of the environment.
            environment (dict): The environment, as detailed above.
            next_container_receiver (Callable, optional): Method of the Data Processing Input Handler
                that receives the data. It is only used, and then it is mandatory, if the 'protocol_id' of
                'inter_container_communication.data_ingestion_to_data_processing' is "AsyncLocal": the data is
                then sent to it directly (same process). With any other protocol it is ignored, and the data is
                sent using the connection settings of the configuration. If the link is not configured at all, the
                data is not sent anywhere and is only logged (see 'LogOutputHandler'), and this is ignored.
            logger (LoggingUtils, optional): Logger of the environment. It is used by this launcher and
                given to every component built for the environment (Output Handler, Adapters and Input
                Handlers), so all their logs show the environment. Defaults to the logger of the launcher.

        Returns:
            List[InputHandlerBase]: The Input Handlers started, one for each Telemetry System that was launched.

        Raises:
            ValueError: If the environment has no Telemetry Systems, or the link to Data Processing is "AsyncLocal"
                and there is no 'next_container_receiver'.
            RuntimeError: If none of the Telemetry Systems of the environment could be launched.
        """
        if not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Launch error: the environment name must be a non-empty string.")

        # --- Guard Clause: Data Ingestion needs at least one Telemetry System, as without one there is nothing to
        # ingest. It is checked here and not in the validation of the environments because an environment may be valid
        # without Telemetry Systems, when the containers that are launched do not need them. ---
        telemetry_systems = environment.get("telemetry_systems")
        if not telemetry_systems:
            raise ValueError(f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' has no 'telemetry_systems'.")

        # The environment is the schema of the entities, and the source mapping is kept by Telemetry System
        # Example: {"cleanwatts": {"source_mapping": {"tefhr": {...}}}} -> {"cleanwatts": {"tefhr": {...}}}
        environment_specs = environment

        # Keeps only the source mapping of each Telemetry System, which says which parameters of the environment that
        # system provides and the keys it needs for each one (the rest of the entry is not used here).
        # Example: {"cleanwatts": {"source_mapping": {"37507": {...}}}, "omie": {"source_mapping": {"price": {...}}}}
        #          -> {"cleanwatts": {"37507": {...}}, "omie": {"price": {...}}}
        source_mapping = {system_id: system["source_mapping"] for system_id, system in telemetry_systems.items()}

        # --- Guard Clause: a local link needs the method to call. It cannot be written in the configuration, so the
        # launcher that starts the containers has to give it. Without a link configured there is no local link, so
        # there is nothing to check (the data is only logged, see '_build_output_handler'). ---
        if (
            self._output_config is not None
            and self._is_local(self._output_config)
            and not callable(next_container_receiver)
        ):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'inter_container_communication.data_ingestion_to_data_processing' "
                f"is '{self._LOCAL_PROTOCOL_ID}', so environment '{environment_name}' needs a callable "
                f"'next_container_receiver'."
            )

        # The logger of the environment is used for everything built here; the launcher's own is the fallback
        logger = logger or self._logger

        # What this call starts, so only that is stopped if it fails
        input_handlers: List[InputHandlerBase] = []
        output_handler: Optional[OutputHandlerBase] = None
        rpc_listener: Optional[InputHandlerBase] = None

        try:
            # Builds the Output Handler that sends the data of this environment towards Data Processing. There is one
            # for the whole environment, not one for each Telemetry System: all the pipelines of the environment (one
            # for each Telemetry System) send through it, so they all reach Data Processing in the same way.
            # It comes with an RPC listener when the handler has to receive replies to what it sends (an Input Handler
            # that listens for them).
            output_handler, rpc_listener = await self._build_output_handler(
                environment_name, next_container_receiver, logger
            )

            # Both are tracked as soon as they are built, so that if anything fails from here on they are stopped
            # (see '_stop_environment' below), and so that 'stop()' stops them when the application ends.
            self._output_handlers.append(output_handler)

            # The RPC listener is only built, not started, so it is started here: from this point it receives the
            # replies. The Output Handler needs no start, since it only acts when a message is sent through it.
            if rpc_listener:
                self._rpc_listeners.append(rpc_listener)
                await rpc_listener.start()

            # Ids of the Telemetry Systems that could not be launched for this environment. A failure of one system
            # does not stop the others, so it is only noted here and checked after the loop.
            # Example: ["telemetry__system_id_1"]
            failed_systems: List[str] = []

            # Starts one pipeline for each Telemetry System of the environment (Input Handler -> Adapter -> Output
            # Handler). They are started one after the other, not at the same time.
            for system_id, system_source_mapping in source_mapping.items():
                # The Input Handler of this system. It stays None until the launcher of the system returns it, so that
                # if something fails before that, the 'except' below knows there is nothing to stop.
                system_input_handler: Optional[InputHandlerBase] = None

                # Gets the launcher of this Telemetry System (it is created the first time and shared by all the
                # environments that use the system). It is outside the 'try' on purpose: a launcher that cannot be
                # loaded is a configuration error (e.g., a wrong 'path'), not a failure of the Telemetry System, so it
                # must stop the whole environment instead of being skipped like the failures handled below.
                telemetry_launcher = self._get_telemetry_launcher(system_id)

                try:
                    # The schema of the environment with only the parameters that this system provides, which is all
                    # that its launcher needs to know. The entities left without parameters are removed.
                    system_specs = self._filter_environment_specs(
                        environment_name, system_id, environment_specs, system_source_mapping
                    )

                    # The empty box that is given to the Input Handler as its callback and filled after the service
                    # exists (see '_DeferredCallback' for why it is needed).
                    callback = _DeferredCallback()

                    # Asks the launcher of the Telemetry System for what it needs: the Adapter, which translates the
                    # messages of the system, and the Input Handler, which receives them. Neither is started.
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

                    # The Input Handler is tracked as soon as it exists, so that it is stopped if anything fails from
                    # here on. It is kept in two lists: the one of this call ('input_handlers'), which is returned and
                    # used to undo this environment, and the one of the launcher ('self._input_handlers'), which
                    # 'stop()' uses to stop everything when the application ends.
                    system_input_handler = components.input_handler
                    input_handlers.append(system_input_handler)
                    self._input_handlers.append(system_input_handler)

                    # The service of this pipeline: it takes each message the Input Handler receives, adapts it with
                    # the Adapter of this system, and sends the result through the Output Handler. Each Telemetry
                    # System has its own service, but the Output Handler is the same for all of them.
                    service = TelemetryPipelineService(
                        adapter=components.adapter,
                        output_handler=output_handler,
                        logger=logger
                    )

                    # Fills the box: from now on, what the Input Handler gives to the callback reaches the service.
                    callback.bind(service.process_message)

                    # Starts the Input Handler, and only now, after the box is filled: from this point messages
                    # arrive, and they need somewhere to go.
                    await system_input_handler.start()

                    # Reaching this point means this Telemetry System is running for the environment:
                    # its Input Handler was built, connected to the pipeline and started
                    logger.debug(
                        f"{self._LOG_PREFIX} Telemetry system '{system_id}' launched for environment "
                        f"'{environment_name}'."
                    )
                except Exception as err:
                    # Any failure of this Telemetry System (its launcher, its Input Handler or its start) ends up here.
                    # The error is logged and not raised, so that the other systems of the environment are still
                    # launched. The type of the error is included because some messages are empty.
                    logger.error(
                        f"{self._LOG_PREFIX} Telemetry system '{system_id}' failed to launch for environment "
                        f"'{environment_name}' ({type(err).__name__}: {err}). Continuing without it."
                    )
                    failed_systems.append(system_id)

                    # Undoes only what this system started: its Input Handler, if it got to exist. It is stopped
                    # quietly (a failure when stopping is logged, not raised, so it does not hide the first error) and
                    # removed from both lists, so that it is not stopped again later or returned as running. The
                    # Adapter and the service of this system are not started, so there is nothing else to stop.
                    if system_input_handler is not None:
                        await self._stop_quietly(system_input_handler.stop, logger)
                        input_handlers.remove(system_input_handler)
                        self._input_handlers.remove(system_input_handler)

            # --- Guard Clause: an environment with no telemetry system running is not running at all ---
            # If every system failed, there is nothing to receive data from, so it is a failure of the environment.
            # The error is caught by the 'except' at the end of the 'try', which undoes the environment.
            if len(failed_systems) == len(source_mapping):
                raise RuntimeError(
                    f"{self._LOG_PREFIX} No telemetry system could be launched for environment "
                    f"'{environment_name}'."
                )

            # If only some failed, the environment runs with the others, and a warning says which are missing, since
            # the error of each one was logged in the middle of the launch and could go unnoticed.
            if failed_systems:
                logger.warning(
                    f"{self._LOG_PREFIX} Environment '{environment_name}' is running without the telemetry "
                    f"systems: {failed_systems}."
                )

            # Telemetry Systems of the source mapping that were launched (the failed ones are left out). The order of
            # the source mapping is kept.
            # Example: source mapping ["cleanwatts", "omie"], "omie" failed -> ["cleanwatts"]
            launched_systems = [system_id for system_id in source_mapping if system_id not in failed_systems]
            logger.info(
                f"{self._LOG_PREFIX} Environment '{environment_name}' is running with the telemetry "
                f"systems: {launched_systems}."
            )
        except Exception:
            # Anything that fails outside the loop of the systems (the Output Handler, the launcher that cannot be
            # loaded, or all the systems failing) ends here, and the environment is undone: everything that this call
            # had started (the Input Handlers, the RPC listener and the Output Handler) is stopped, so that nothing is
            # left running for an environment that failed. The other environments are not affected.
            logger.error(
                f"{self._LOG_PREFIX} Launch of environment '{environment_name}' failed. "
                f"Stopping what it had started."
            )
            await self._stop_environment(input_handlers, rpc_listener, output_handler, logger)

            # The error is raised again, so that whoever launched the environment knows that it failed.
            raise

        # The Input Handlers that are running, one for each Telemetry System that was launched.
        return input_handlers

    @classmethod
    def _is_local(cls, link_config: dict) -> bool:
        """Tells if the link to Data Processing is in memory (both containers run in the same process).

        It is only local if the configuration says so, never because a receiver was given.

        Args:
            link_config (dict): The configuration of the link, with its 'protocol_id' (e.g., {"protocol_id":
                "AsyncRabbitMQ", "connection_info": {...}}).

        Returns:
            bool: True if the 'protocol_id' is "AsyncLocal".
        """
        return link_config["protocol_id"] == cls._LOCAL_PROTOCOL_ID

    async def _build_output_handler(
            self,
            environment_name: str,
            next_container_receiver: Optional[Callable],
            logger: LoggingUtils
    ) -> Tuple[OutputHandlerBase, Optional[InputHandlerBase]]:
        """Builds the Output Handler towards Data Processing.

        If the link is not configured, there is nowhere to send the data, so the Output Handler only logs it
        (see 'LogOutputHandler'). If the link is local (its 'protocol_id' is "AsyncLocal"), the Output Handler calls
        the receiver directly, without waiting for its result. Otherwise, the Output Handler is the one that the
        'protocol_id' of the configuration says (e.g., "AsyncRabbitMQ" or "AsyncHTTP"): the 'connection' of
        'connection_info' is passed as the connection settings and its 'topology' as the topology.

        In the topology, the '{environment}' placeholder is replaced by the name of the environment, so that each
        environment has its own destination instead of all of them sharing one (see 'replace_environment_in').

        Args:
            environment_name (str): Name of the environment the Output Handler is built for.
            next_container_receiver (Callable, optional): Method of the Data Processing Input Handler
                that receives the data. Only used if the link is "AsyncLocal".
            logger (LoggingUtils): Logger of the environment, given to the Output Handler.

        Returns:
            Tuple[OutputHandlerBase, Optional[InputHandlerBase]]: The Output Handler and its RPC
                listener, if it has one. Neither is started or tracked here.
        """
        # --- Case 1: no link configured. There is nowhere to send the data, so a handler that only logs it is used
        # and Data Ingestion can still run (receive, adapt and show the messages) without the next container. The
        # warning is given once per environment, when the handler is built, and not in every message. ---
        if self._output_config is None:
            logger.warning(
                f"{self._LOG_PREFIX} 'inter_container_communication.data_ingestion_to_data_processing' is not "
                f"configured for environment '{environment_name}': the data will only be logged, not sent."
            )
            # The handler is created directly and not asked from the Output Handlers Launcher: it has no connection or
            # configuration to build, and it is not a 'protocol_id' that a configuration can ask for. The second value
            # is the RPC listener, which it does not have.
            return LogOutputHandler(logger=logger), None

        # --- Case 2: local link ('protocol_id' "AsyncLocal"). Data Processing runs in the same process, so there is no
        # network: the Output Handler calls the 'next_container_receiver' directly. ---
        if self._is_local(self._output_config):
            # Fire-and-forget: the pipeline does not wait for Data Processing to handle each message. With RPC
            # enabled, each message would wait for the receiver to finish, and a slow Data Processing would slow down
            # the reception of new messages.
            return await self._output_launcher.build_output_handler(
                handler_type=self._LOCAL_PROTOCOL_ID,
                topology_setup={"rpc_properties": {"enabled": False}},
                # The receiver is a function and cannot be written in a configuration file, so it comes from the
                # launcher that started the containers and is given to the connector as its 'target_callable'.
                server_configurations={"target_callable": next_container_receiver},
                logger=logger
            )

        # --- Case 3: link over the network (e.g., "AsyncRabbitMQ"). The Output Handler is built as the configuration
        # says. ---
        # 'connection_info' has two parts: 'connection' (how to connect to the server) and 'topology' (where to send
        # the data in it). Any of them can be missing.
        connection_info = self._output_config.get("connection_info", {})

        # The topology is given the name of the environment (see 'replace_environment_in'), so that each environment
        # sends its data to its own destination.
        topology = replace_environment_in(connection_info.get("topology", {}), environment_name)

        # The type of Output Handler is the 'protocol_id' of the configuration, which the Output Handlers Launcher
        # resolves to the right class and connector.
        return await self._output_launcher.build_output_handler(
            handler_type=self._output_config["protocol_id"],
            topology_setup=topology,
            server_configurations=connection_info.get("connection", {}),
            logger=logger
        )

    async def _stop_environment(
            self,
            input_handlers: List[InputHandlerBase],
            rpc_listener: Optional[InputHandlerBase],
            output_handler: Optional[OutputHandlerBase],
            logger: LoggingUtils
    ) -> None:
        """Stops and forgets what one environment started, leaving the other environments running.

        It is used when the launch of an environment fails, to undo what that launch had already started. The
        components are stopped in the order in which the data flows, from where it enters to where it leaves: first
        the Input Handlers (so that no new message enters the pipeline), then the RPC listener, and last the Output
        Handler (so that a message that was still being processed can still be sent). A component that fails to stop
        is logged and does not prevent the others from being stopped.

        Args:
            input_handlers (List[InputHandlerBase]): Input Handlers of the environment.
            rpc_listener (InputHandlerBase, optional): RPC listener of the environment's Output Handler.
            output_handler (OutputHandlerBase, optional): Output Handler of the environment.
            logger (LoggingUtils): Logger of the environment, used if a component fails to stop.
        """
        # The '*' in front of 'input_handlers' unpacks the list: it puts its items one by one inside the tuple, instead
        # of putting the list as a single item. It makes one flat tuple, with the Input Handlers first and then the RPC
        # listener and the Output Handler, which is the order in which they are stopped.
        # Example: input_handlers = [A, B], rpc_listener = R, output_handler = O
        #          (*input_handlers, rpc_listener, output_handler) -> (A, B, R, O)
        #          Without the '*': (input_handlers, rpc_listener, output_handler) -> ([A, B], R, O), and the first
        #          'handler' of the loop would be the whole list, which has no 'stop'.
        # Only 'input_handlers' has the '*' because it is the only list here: 'rpc_listener' and 'output_handler' are
        # each a single object (or None), and a '*' on an object that is not a list would give an error.
        # Rule: a list gets a '*' and a single object does not.
        # Stops each component, in the order explained above. The RPC listener and the Output Handler can be None
        # (the launch may have failed before they were built, or the handler has no RPC listener), and a None has
        # nothing to stop. It is stopped quietly: a failure is logged and not raised, because this runs while another
        # error is already going up, and a second error would hide it and leave the other components running.
        for handler in (*input_handlers, rpc_listener, output_handler):
            if handler is not None:
                await self._stop_quietly(handler.stop, logger)

        # Forgets them: removes them from the lists of the launcher, which hold everything that is running. Without
        # this, 'stop()' (when the application ends) would stop them a second time, and the lists would keep
        # references to handlers that no longer work. The 'in' check is there because a component may never have
        # been added to the list (e.g., the launch failed before that point), and 'remove' raises an error if the
        # item is not in the list.
        for handler in input_handlers:
            if handler in self._input_handlers:
                self._input_handlers.remove(handler)
        if rpc_listener in self._rpc_listeners:
            self._rpc_listeners.remove(rpc_listener)
        if output_handler in self._output_handlers:
            self._output_handlers.remove(output_handler)

    def _get_telemetry_launcher(self, system_id: str) -> TelemetrySystemLauncherBase:
        """Returns the launcher of a Telemetry System, creating it the first time.

        There is one launcher for each Telemetry System, shared by all the environments that use it. This way,
        what belongs to the system and not to an environment (e.g., a login, or a token that is refreshed) is done
        once and not once per environment.

        Args:
            system_id (str): Id of the Telemetry System.

        Returns:
            TelemetrySystemLauncherBase: The launcher.

        Raises:
            ValueError: If the launcher cannot be loaded (see 'load_class').
        """
        # The launcher that was already created for this system, if there is one. It is None the first time that an
        # environment asks for it.
        launcher = self._telemetry_launchers.get(system_id)

        if launcher is None:
            # Loads the class of the launcher from its 'path' in the Instance Configuration (e.g., "app.extension.
            # telemetry_systems.cleanwatts.cw_telemetry_launcher.CWTelemetryLauncher"). It is loaded only now, and
            # not when the application starts, so that only the launchers of the systems that are used are imported.
            launcher_class = load_class(
                self._telemetry_systems[system_id]["path"],
                f"telemetry_systems.{system_id}.path",
                self._LOG_PREFIX,
                TelemetrySystemLauncherBase
            )

            # Creates the launcher with the factories of Input Handlers and Output Handlers, which it uses to build
            # what its system needs, and with the logger of the launcher. It is not the logger of an environment,
            # because this launcher is shared by all of them.
            launcher = launcher_class(
                input_launcher=self._input_launcher,
                output_launcher=self._output_launcher,
                logger=self._logger
            )

            # Keeps it, so that the next environment that uses this system gets the same launcher, and so that
            # 'stop()' can stop it when the application ends.
            self._telemetry_launchers[system_id] = launcher

        return launcher

    def _filter_environment_specs(
            self,
            environment_name: str,
            system_id: str,
            environment_specs: dict,
            source_mapping: dict
    ) -> dict:
        """Keeps only the parameters a Telemetry System is responsible for.

        A parameter belongs to the system if its id is in the system's source mapping, or if it derives from a
        parameter that is (its 'derived_from' is the id of one of them). So a derived parameter, which is in no source
        mapping, is handled by the system that provides the parameter it is built from. Entities left without
        parameters are removed, and so are the actuators, as they belong to Target Systems.

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
        # The entities that stay in the schema of this Telemetry System, each with only its parameters
        # Example: {"EVC01": {"entity_type_id": "ev_charger", "parameters": {"37507": {...}}}}
        entities = {}
        for entity_id, entity_spec in environment_specs["entities"].items():
            # Keeps the parameters of this entity that belong to the system. A parameter belongs to it if:
            # - its id is in the source mapping of the system (the system provides it), or
            # - it is derived (it has 'derived_from') from a parameter that is in the source mapping, as the Adapter
            #   of the system is the one that calculates it.
            # The 'isinstance' check is there because 'parameter_spec' is only known to be a dictionary if the schema
            # is valid, and '.get' would fail on anything else (for example, None).
            # Example (source mapping with "37507"): "37507" is kept, and so is "connected_vehicle" (derived from it),
            #                                        but "37506" is not.
            parameters = {
                parameter_id: parameter_spec
                for parameter_id, parameter_spec in entity_spec.get("parameters", {}).items()
                if parameter_id in source_mapping
                or (isinstance(parameter_spec, dict) and parameter_spec.get("derived_from") in source_mapping)
            }

            # Only keeps the entity if some parameter of it belongs to the system. The entity is copied without its
            # actuators (they belong to Target Systems, not to Telemetry Systems), and with the filtered parameters.
            if parameters:
                entity = {key: value for key, value in entity_spec.items() if key != "actuators"}
                entity["parameters"] = parameters
                entities[entity_id] = entity

        # --- Guard Clause: the system must have at least one parameter in the schema. If not, its source mapping
        # points to nothing that exists (for example, ids with typos), and launching it would be pointless. ---
        if not entities:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: the source mapping of telemetry system '{system_id}' "
                f"matches no parameter of environment '{environment_name}'."
            )

        # Copies the schema without the information about the systems and the model, which the Telemetry System does
        # not need, and replaces the entities by the filtered ones.
        ignored_keys = ("telemetry_systems", "target_systems", "ml_model_id")
        filtered = {key: value for key, value in environment_specs.items() if key not in ignored_keys}
        filtered["entities"] = entities
        return filtered

    async def stop(self) -> None:
        """Stops everything that was started: Input Handlers first, then the RPC listeners,
        the Output Handlers and the launchers of the Telemetry Systems.

        A failure while stopping one part is logged and does not prevent the others from being stopped.
        """
        # Stops the handlers, in this order: Input Handlers (so no more data enters), RPC listeners and Output Handlers.
        # The '*' unpacks a list: it puts its items one by one inside the tuple, instead of putting the list as a
        # single item. Here the three attributes are lists, so each one has its own '*', and the three are joined into
        # a single flat sequence of handlers to go through (unlike in '_stop_environment', where only one is a list).
        # Example: input_handlers = [A, B], rpc_listeners = [R], output_handlers = [O]
        #          (*input_handlers, *rpc_listeners, *output_handlers) -> (A, B, R, O)
        #          Without the '*' it would be ([A, B], [R], [O]) and the first 'handler' would be a list,
        #          which has no 'stop'.
        # Each one is stopped one at a time, and '_stop_quietly' makes sure that a failure does not prevent the next
        # from being stopped.
        for handler in (*self._input_handlers, *self._rpc_listeners, *self._output_handlers):
            await self._stop_quietly(handler.stop)

        # Stops the launchers of the Telemetry Systems last, as the handlers above no longer need them
        for launcher in self._telemetry_launchers.values():
            await self._stop_quietly(launcher.stop)

        # Forgets everything that was stopped, so that a second call to 'stop' has nothing to stop again
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
            # Calls the stop function. If it is 'async', this only creates the coroutine and does not run it yet. If
            # it is a normal function, it has already run and the result is whatever it returned (usually None).
            result = stop_function()

            # If the result can be awaited (it is a coroutine), waits for it so the stop really finishes here
            if hasattr(result, "__await__"):
                await result
        except Exception as err:
            # A failure is only logged, never raised, so that it does not prevent the other components from being
            # stopped. It uses the logger that was given (the one of the environment) or, if none, the launcher's.
            (logger or self._logger).error(f"{self._LOG_PREFIX} Failed to stop a component: {err}")