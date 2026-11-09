# --- Standard Library Imports ---
import copy
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.extension.rec_bridge.rec_bridge_adapter_base import RECBridgeAdapterBase
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.extension.rec_bridge.rec_bridge_service import RECBridgeService
from app.core.utils.environment_placeholder import replace_environment
from app.core.utils.instance_configuration import InstanceConfiguration
from app.core.utils.logger import LoggingUtils


class RECBridgeLauncher(ABC):
    """Launcher of the REC Bridge container.

    It starts the components of one environment at a time. Each environment gets its own set of components, which is not
    shared with the other environments: the Adapter, the Output Handler towards the REC, the Output Handler towards
    Inference, the REC Bridge Service and the two Input Handlers (the one for Data Processing and the one for the other
    installations of the REC).

    To launch an environment, it does the following:
    1. Builds the Output Handler towards Inference ('inter_container_communication.rec_bridge_to_inference').
    2. If the environment belongs to a REC (it has a 'rec' block): builds the Adapter and the Output Handler that
       publishes in the exchange of this installation. If it does not, the Bridge only passes the messages on.
    3. Creates the REC Bridge Service with all of the above.
    4. If the environment has peers, builds and starts the Input Handler that receives their data: one queue, bound
       to the exchange of each peer.
    5. Builds and starts the Input Handler that gives the messages of Data Processing to the service
       ('inter_container_communication.data_processing_to_rec_bridge').

    How the REC is organised in RabbitMQ is in the 'topology' of the 'connection_info' of 'rec_bridge' (the broker is
    shared by the installations). It has two parts, with the placeholders '{community_id}', '{installation_id}' and
    '{peer_id}':
    - 'publish': where this installation publishes its data (its own exchange);
    - 'subscribe': what is received from the peers. Its exchanges are templates, repeated for each peer, so each peer
      has its own exchange. The queue has the same name in all of them, so it is ONE queue of this installation bound
      to the exchange of every peer (the Input Handler starts a single consumer for it).

    Expected `environment['rec']` (optional):
    ------------------------------------------
    {
        "community_id": "porto",                # The REC
        "installation_id": "lab-01",            # This installation in the REC
        "peers": ["lab-02", "lab-03"],          # The other installations (may be empty)
        "derived_data_to_share": []             # Used by the Adapter of EnergAIze: the derived data to share (none)
    }

    The links between containers must use a network protocol, as each container runs in its own thread.

    To integrate a REC Bridge, create a class that inherits from this one and implements '_get_adapter_class'. Its path
    is in 'rec_bridge.path' of the Percepta Instance Configuration, which has the same format as the entry of an
    external system: a 'connection_configuration' for the network of the REC (with 'collection_window_seconds' in its
    'connection_info').
    """

    _LOG_PREFIX: str = "REC Bridge Launcher |"

    def __init__(
            self,
            instance_configuration: InstanceConfiguration,
            input_launcher: InputHandlersLauncher,
            output_launcher: OutputHandlersLauncher,
            logger: LoggingUtils
    ) -> None:
        """Initializes the REC Bridge launcher.

        Args:
            instance_configuration (InstanceConfiguration): The Percepta Instance Configuration, already validated.
                The parts used are 'rec_bridge', 'data_processing_to_rec_bridge' and 'rec_bridge_to_inference'.
            input_launcher (InputHandlersLauncher): Factory of Input Handlers.
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers.
            logger (LoggingUtils): Logging interface.

        Raises:
            ValueError: If a dependency is missing, the instance does not use the REC Bridge, or the Adapter class
                cannot be loaded.
        """
        if not instance_configuration:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'instance_configuration' dependency is missing.")
        if not input_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'input_launcher' dependency is missing.")
        if not output_launcher:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'output_launcher' dependency is missing.")
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # --- Guard Clause: the instance must use the REC Bridge, and have both links ---
        if not instance_configuration.rec_bridge:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: there is no 'rec_bridge' in the configuration.")

        rec_bridge = instance_configuration.rec_bridge
        self._input_config: dict = instance_configuration.data_processing_to_rec_bridge
        self._output_config: dict = instance_configuration.rec_bridge_to_inference
        self._network_config: dict = rec_bridge["connection_configuration"]
        self._collection_window_seconds: float = rec_bridge["collection_window_seconds"]
        self._adapter_class: type = self._get_adapter_class()
        if not (isinstance(self._adapter_class, type) and issubclass(self._adapter_class, RECBridgeAdapterBase)):
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: the adapter class must extend "
                f"{RECBridgeAdapterBase.__name__}."
            )
        self._input_launcher = input_launcher
        self._output_launcher = output_launcher
        self._logger = logger

        # What was started, so it can be stopped.
        self._input_handlers: List[InputHandlerBase] = []
        self._output_handlers: List[OutputHandlerBase] = []

    async def launch_environment(
            self,
            environment_name: str,
            environment: dict,
            next_container_receiver: Optional[Any] = None,
            logger: Optional[LoggingUtils] = None
    ) -> InputHandlerBase:
        """Launches the REC Bridge components of one environment.

        If anything fails, what this call already started is stopped and the error is raised again.

        Args:
            environment_name (str): Name of the environment.
            environment (dict): The environment, as returned by the Schema Storage. Its 'rec' is optional.
            next_container_receiver (Any, optional): Not used. The links are over the network, so no container needs
                a method of the next one. It is only here so that all the container launchers have the same
                signature.
            logger (LoggingUtils, optional): Logger of the environment, given to every component built for it.
                Defaults to the logger of the launcher.

        Returns:
            InputHandlerBase: The Input Handler of Data Processing, already started. The launcher keeps it too.

        Raises:
            ValueError: If the environment is not valid, its 'rec' is not well formed, a link between containers is
                "AsyncLocal", or a component rejects the configuration.
        """
        if not isinstance(environment_name, str) or not environment_name.strip():
            raise ValueError(f"{self._LOG_PREFIX} Launch error: the environment name must be a non-empty string.")
        if not isinstance(environment, dict):
            raise ValueError(f"{self._LOG_PREFIX} Launch error: environment '{environment_name}' must be a dictionary.")

        logger = logger or self._logger
        rec = self._read_rec(environment_name, environment)

        started_inputs: List[InputHandlerBase] = []
        started_outputs: List[OutputHandlerBase] = []
        try:
            # Step 1: towards Inference
            output_handler = await self._build_output_handler(self._output_config, environment_name, logger)
            started_outputs.append(output_handler)

            # Step 2: the REC (Adapter and publisher), if the environment belongs to one
            adapter: Optional[RECBridgeAdapterBase] = None
            rec_output_handler: Optional[OutputHandlerBase] = None
            if rec is not None:
                adapter = self._adapter_class(environment_name, environment, logger)
                rec_output_handler = await self._build_rec_output_handler(rec, logger)
                started_outputs.append(rec_output_handler)
            else:
                logger.info(
                    f"{self._LOG_PREFIX} Environment '{environment_name}' has no 'rec', so the REC Bridge only passes "
                    f"its messages on to Inference."
                )

            # Step 3
            service = RECBridgeService(
                adapter=adapter,
                rec_output_handler=rec_output_handler,
                output_handler=output_handler,
                installation_id=rec["installation_id"] if rec else None,
                peers=rec["peers"] if rec else [],
                collection_window_seconds=self._collection_window_seconds,
                logger=logger
            )

            # Step 4: the data of the other installations
            if rec is not None and rec["peers"]:
                peers_input_handler = await self._input_launcher.build_input_handler(
                    handler_type=self._network_config["protocol_id"],
                    topology_setup=self._peers_topology(rec),
                    callback=service.receive_from_installation,
                    server_configurations=self._network_config["connection_info"].get("connection", {}),
                    logger=logger
                )
                started_inputs.append(peers_input_handler)
                await peers_input_handler.start()

            # Step 5: the messages of Data Processing
            input_handler = await self._build_input_handler(environment_name, service.receive_message, logger)
            started_inputs.append(input_handler)
            await input_handler.start()
        except Exception:
            logger.error(
                f"{self._LOG_PREFIX} Launch of environment '{environment_name}' failed. Stopping what it had started."
            )
            for component in (*started_inputs, *started_outputs):
                await self._stop_quietly(component.stop, logger)
            raise

        self._input_handlers.extend(started_inputs)
        self._output_handlers.extend(started_outputs)
        logger.info(
            f"{self._LOG_PREFIX} Environment '{environment_name}' launched"
            + (f" in the REC '{rec['community_id']}' as '{rec['installation_id']}', with the peers {rec['peers']}."
               if rec else " without a REC.")
        )
        return input_handler

    def _read_rec(self, environment_name: str, environment: dict) -> Optional[Dict[str, Any]]:
        """Checks the 'rec' of the environment and returns it, or None if the environment has none."""
        rec = environment.get("rec")
        if rec is None:
            return None
        where = f"{self._LOG_PREFIX} Launch error: the 'rec' of environment '{environment_name}'"
        if not isinstance(rec, dict):
            raise ValueError(f"{where} must be a dictionary.")
        for key in ("community_id", "installation_id"):
            if not isinstance(rec.get(key), str) or not rec[key].strip():
                raise ValueError(f"{where} needs a non-empty string '{key}'.")
        peers = rec.get("peers", [])
        if not isinstance(peers, list) or any(not isinstance(peer, str) or not peer.strip() for peer in peers):
            raise ValueError(f"{where} needs 'peers' to be a list of installation ids.")
        if len(set(peers)) != len(peers) or rec["installation_id"] in peers:
            raise ValueError(f"{where} has repeated peers, or has itself as a peer.")
        return {**rec, "peers": peers}

    @staticmethod
    def _fill(topology: Any, values: Dict[str, str]) -> Any:
        """Replaces the placeholders ('{community_id}', '{installation_id}', '{peer_id}') in every text of a topology.

        Args:
            topology (Any): A topology, or any part of one (dictionaries, lists and texts). It is not changed.
            values (Dict[str, str]): The value of each placeholder, e.g., {"community_id": "porto"}.

        Returns:
            Any: A copy with the placeholders replaced.
        """
        if isinstance(topology, str):
            for name, value in values.items():
                topology = topology.replace("{" + name + "}", value)
            return topology
        if isinstance(topology, dict):
            return {key: RECBridgeLauncher._fill(value, values) for key, value in topology.items()}
        if isinstance(topology, list):
            return [RECBridgeLauncher._fill(value, values) for value in topology]
        return copy.deepcopy(topology)

    async def _build_rec_output_handler(self, rec: Dict[str, Any], logger: LoggingUtils) -> OutputHandlerBase:
        """Builds the Output Handler that publishes the data of this installation in its exchange of the REC.

        The exchange is the 'topology.publish' of the configuration, with '{community_id}' and '{installation_id}'
        replaced.
        """
        topology = self._fill(
            self._network_config["connection_info"]["topology"]["publish"],
            {"community_id": rec["community_id"], "installation_id": rec["installation_id"]}
        )
        output_handler, _ = await self._output_launcher.build_output_handler(
            handler_type=self._network_config["protocol_id"],
            topology_setup=topology,
            server_configurations=self._network_config["connection_info"].get("connection", {}),
            logger=logger
        )
        return output_handler

    def _peers_topology(self, rec: Dict[str, Any]) -> Dict[str, Any]:
        """The topology to receive from the peers: the 'topology.subscribe' of the configuration, once for each peer.

        Each entry of 'exchanges' is a template: it is repeated for each peer, with '{peer_id}' replaced (and
        '{community_id}' and '{installation_id}'). This way each peer has its own exchange, and, as the name of the queue
        does not have the '{peer_id}', all the exchanges are bound to the same queue.
        """
        template = self._network_config["connection_info"]["topology"]["subscribe"]
        exchanges = []
        for peer in rec["peers"]:
            values = {"community_id": rec["community_id"], "installation_id": rec["installation_id"], "peer_id": peer}
            exchanges.extend(self._fill(exchange, values) for exchange in template.get("exchanges", []))
        return {**{key: value for key, value in template.items() if key != "exchanges"}, "exchanges": exchanges}

    async def _build_output_handler(
            self, link_config: dict, environment_name: str, logger: LoggingUtils
    ) -> OutputHandlerBase:
        """Builds the Output Handler of a link between containers (the '{environment}' placeholder is replaced)."""
        self._refuse_local(link_config, "rec_bridge_to_inference")
        connection_info = link_config.get("connection_info", {})
        topology = self._replace_topology(connection_info.get("topology", {}), environment_name)
        output_handler, _ = await self._output_launcher.build_output_handler(
            handler_type=link_config["protocol_id"],
            topology_setup=topology,
            server_configurations=connection_info.get("connection", {}),
            logger=logger
        )
        return output_handler

    async def _build_input_handler(self, environment_name: str, callback: Any, logger: LoggingUtils) -> InputHandlerBase:
        """Builds the Input Handler that receives the messages of Data Processing."""
        self._refuse_local(self._input_config, "data_processing_to_rec_bridge")
        connection_info = self._input_config.get("connection_info", {})
        return await self._input_launcher.build_input_handler(
            handler_type=self._input_config["protocol_id"],
            topology_setup=self._replace_topology(connection_info.get("topology", {}), environment_name),
            callback=callback,
            server_configurations=connection_info.get("connection", {}),
            logger=logger
        )

    def _refuse_local(self, link_config: dict, link_name: str) -> None:
        """The REC Bridge runs in its own thread, so its links must be over the network."""
        if link_config["protocol_id"] == "AsyncLocal":
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'inter_container_communication.{link_name}' uses 'AsyncLocal'. "
                f"Use a network protocol (e.g., 'AsyncRabbitMQ')."
            )

    @staticmethod
    def _replace_topology(topology: Dict[str, Any], environment_name: str) -> Dict[str, Any]:
        """Gives the exchanges and queues of a topology the name of the environment (a copy is returned)."""
        topology = copy.deepcopy(topology)
        for exchange in topology.get("exchanges", []):
            if "exchange_name" in exchange:
                exchange["exchange_name"] = replace_environment(exchange["exchange_name"], environment_name)
            for queue in exchange.get("queues", []):
                for key in ("queue_name", "routing_key"):
                    if key in queue:
                        queue[key] = replace_environment(queue[key], environment_name)
        return topology

    @abstractmethod
    def _get_adapter_class(self) -> type:
        """Gives the class of the Adapter of the REC Bridge (it must extend RECBridgeAdapterBase).

        Like the launcher of an ML Model, the launcher of the REC Bridge of each use case knows its Adapter, so it is
        not in the configuration.
        """
        raise NotImplementedError()

    async def stop(self) -> None:
        """Stops everything that was started: the Input Handlers first, then the Output Handlers.

        A failure while stopping one part is logged and does not prevent the others from being stopped.
        """
        for component in (*self._input_handlers, *self._output_handlers):
            await self._stop_quietly(component.stop)
        self._input_handlers.clear()
        self._output_handlers.clear()

    async def _stop_quietly(self, stop_function: Any, logger: Optional[LoggingUtils] = None) -> None:
        """Calls a stop function (awaiting it if it is awaitable), logging a failure instead of raising it."""
        try:
            result = stop_function()
            if hasattr(result, "__await__"):
                await result
        except Exception as err:
            (logger or self._logger).error(f"{self._LOG_PREFIX} Failed to stop a component: {err}")
