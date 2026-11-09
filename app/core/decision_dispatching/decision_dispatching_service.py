# --- Standard Library Imports ---
import asyncio
import itertools
import json
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.decision_dispatching.target_system_adapter_base import MappedDecision
from app.core.decision_dispatching.target_system_launcher_base import TargetSystemComponents
from app.core.decision_dispatching.target_system_resolver import TargetSystemResolver
from app.core.utils.logger import LoggingUtils


class DecisionDispatchingService:
    """Application Service of the Decision Dispatching container of ONE environment.

    Every time the decisions of a period arrive from Inference (the Input Handler calls 'receive_message'), the
    service sends each decision to the Target System that controls its actuators:

        1. The Target System Resolver determines the Target System of each actuator of the decisions.
        2. For each Target System, the Adapter maps its decisions into the format that the system expects, and the
           Output Handler sends each mapped decision. The Target Systems are served at the same time, and so are the
           mapped decisions of a Target System, so a slow or failing system does not hold the others.
        3. If sending a mapped decision fails, the Actuation Error Handling Logic of that Target System says if it is
           sent again or if its processing ends. When it ends, only that mapped decision is lost: the others are not
           affected.

    If something fails while the decisions of a period are being processed (e.g., an Adapter raises an error), the error
    is logged and only what depends on it is not sent. The service is not stopped, so the next message is processed
    normally.

    NOTE An error is detected either by an exception of the Output Handler or, for the HTTP Output Handler, which
    returns the errors instead of raising them, by a response with the status "error" (see '_find_error'). For that
    reason the HTTP Output Handlers of the Target Systems must have the response enabled ('rpc_properties.enabled').
    """

    _LOG_PREFIX: str = "Decision Dispatching Service |"

    def __init__(
            self,
            resolver: TargetSystemResolver,
            target_systems: Dict[str, TargetSystemComponents],
            logger: LoggingUtils
    ) -> None:
        """Initializes the service.

        Args:
            resolver (TargetSystemResolver): Determines the Target System of each decision.
            target_systems (Dict[str, TargetSystemComponents]): The components of each Target System of the
                environment (Adapter, Output Handler and Actuation Error Handling Logic), by the id of the system.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If the logger is missing.
            TypeError: If another dependency is of the wrong type.
        """
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")
        self._logger = logger

        if not isinstance(resolver, TargetSystemResolver):
            self._raise_type_error("'resolver' must be a TargetSystemResolver.")
        if not isinstance(target_systems, dict) or not all(
                isinstance(components, TargetSystemComponents) for components in target_systems.values()):
            self._raise_type_error("'target_systems' must be a dictionary of TargetSystemComponents.")

        self._resolver = resolver
        self._target_systems = target_systems

        # Identifies the decisions that have no timestamp (see '_period_id')
        self._message_counter = itertools.count(1)

    async def receive_message(self, envelope: Dict[str, Any]) -> None:
        """Sends the decisions of a period to the Target Systems.

        The Input Handler calls this for every message that arrives from Inference.

        Expected Input Structure:
        -------------------------
        envelope = {
            "protocol": "AMQP",
            "source": "exchange/queue",
            "status": None,
            "content_type": "application/json",
            "metadata": {},
            "payload": [                                    # The decisions, or the same list as JSON text/bytes
                {
                    "id": "B01",
                    "entity_type_id": "battery",
                    "actuators": {
                        "act_power_1": {"actuator_type_id": "power", "value": -2.5}
                    },
                    "timestamp": "2026-08-19 11:15:00 +0000"
                }
            ]
        }

        Args:
            envelope (Dict[str, Any]): The message, as given by the Input Handler.
        """
        decisions = self._read_decisions(envelope)

        # Guard Clause 1: A message that cannot be read is skipped.
        if decisions is None:
            return

        period_id: str = self._period_id(decisions)

        try:
            resolved = self._resolver.resolve(decisions)

            # Each Target System is served on its own, at the same time as the others
            await asyncio.gather(*(
                self._dispatch_to_target_system(target_system_id, target_decisions, period_id)
                for target_system_id, target_decisions in resolved.items()
            ))
        except Exception as err:
            self._logger.error(
                f"{self._LOG_PREFIX} Could not process the decisions of the period '{period_id}': "
                f"{type(err).__name__}: {err}"
            )

    async def _dispatch_to_target_system(
            self, target_system_id: str, decisions: List[Dict[str, Any]], period_id: str
    ) -> None:
        """Maps the decisions of one Target System and sends them. It never raises, so it never stops the others."""
        try:
            # Guard Clause 1: The resolver only knows systems of the environment, and the launcher builds the
            # components of every one of them, so this is a failure of the setup.
            components = self._target_systems.get(target_system_id)
            if components is None:
                self._logger.error(
                    f"{self._LOG_PREFIX} There are no components for the Target System '{target_system_id}', so its "
                    f"decisions of the period '{period_id}' were not sent."
                )
                return

            mapped_decisions: List[MappedDecision] = components.adapter.adapt_decisions(decisions)

            await asyncio.gather(*(
                self._send(target_system_id, components, mapped, f"{period_id} | {target_system_id} | {index}")
                for index, mapped in enumerate(mapped_decisions)
            ))
        except Exception as err:
            self._logger.error(
                f"{self._LOG_PREFIX} Could not dispatch the decisions of the period '{period_id}' to the Target "
                f"System '{target_system_id}': {type(err).__name__}: {err}"
            )

    async def _send(
            self,
            target_system_id: str,
            components: TargetSystemComponents,
            mapped: MappedDecision,
            message_id: str
    ) -> bool:
        """Sends one mapped decision. If it fails, the Actuation Error Handling Logic says if it is sent again.

        Args:
            target_system_id (str): Id of the Target System, for the log.
            components (TargetSystemComponents): The components of the Target System.
            mapped (MappedDecision): What to send.
            message_id (str): Identifies the message, so the Actuation Error Handling Logic knows when a new one starts.

        Returns:
            bool: True if the Target System accepted it, False if it was given up.
        """
        while True:
            try:
                response = await components.output_handler.send(mapped.message, **mapped.send_arguments)
                error = self._find_error(response)
            except Exception as err:
                error = err

            if error is None:
                return True

            if not await components.error_handling_logic.should_retry(error, message_id):
                self._logger.error(
                    f"{self._LOG_PREFIX} Gave up sending the message '{message_id}' to the Target System "
                    f"'{target_system_id}': {error}"
                )
                return False

    @staticmethod
    def _find_error(response: Any) -> Optional[Any]:
        """Finds the error in the response of an Output Handler, if there is one.

        The HTTP Output Handler does not raise errors. It returns what each endpoint answered, and an endpoint that
        failed has the status "error". A response that is not in that format (e.g., None, from an Output Handler that
        has nothing to return) is taken as a success.

        Expected Input Structure:
        -------------------------
        {"/actuations": {"status": "success", "code": 200, "data": {...}}}
        {"/actuations": {"status": "error", "error": "Timeout after 5000 ms"}}

        Args:
            response (Any): What the Output Handler returned.

        Returns:
            Optional[Any]: The first error found, or None if there is none.
        """
        if not isinstance(response, dict):
            return None

        for result in response.values():
            if isinstance(result, dict) and result.get("status") == "error":
                return result.get("error", "unknown error")
        return None

    def _read_decisions(self, envelope: Any) -> Optional[List[Dict[str, Any]]]:
        """Gets the decisions from the envelope that the Input Handler delivered.

        The payload is a list when the message came from the same process (in-memory connector), and JSON, as text or
        bytes, when it came from the network (e.g., RabbitMQ). Both are accepted.

        Args:
            envelope (Any): The message, as given by the Input Handler.

        Returns:
            Optional[List[Dict[str, Any]]]: The decisions, or None (with a warning in the log) if they cannot be read.
        """
        payload = envelope.get("payload") if isinstance(envelope, dict) else None

        if isinstance(payload, (bytes, str)):
            try:
                payload = json.loads(payload)
            except ValueError as err:
                self._logger.warning(f"{self._LOG_PREFIX} Skipping a message that is not valid JSON: {err}")
                return None

        # Guard Clause 1: The decisions are a list.
        if not isinstance(payload, list):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping a message whose payload is not a list of decisions, got "
                f"{type(payload).__name__}."
            )
            return None

        return payload

    def _period_id(self, decisions: List[Dict[str, Any]]) -> str:
        """Identifies the period of the decisions: the timestamp of the first one, or a number if it has none."""
        first = decisions[0] if decisions and isinstance(decisions[0], dict) else {}
        timestamp = first.get("timestamp")
        if isinstance(timestamp, str) and timestamp.strip():
            return timestamp
        return f"message-{next(self._message_counter)}"

    def _raise_type_error(self, detail: str) -> None:
        """Logs a wrong type of component and raises it as a TypeError."""
        error_msg = f"{self._LOG_PREFIX} Initialization error: {detail}"
        self._logger.error(error_msg)
        raise TypeError(error_msg)
