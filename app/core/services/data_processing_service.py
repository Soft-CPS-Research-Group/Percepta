# --- Standard Library Imports ---
import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

# --- Internal Framework Imports ---
from app.core.data_issue_handling.data_issue_handling_approach_base import DataIssueHandlingApproachBase
from app.core.integrated_logic.combination_logic_base import CombinationLogicBase
from app.core.integrated_logic.parameter_derivation_logic_base import ParameterDerivationLogicBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import format_timestamp


class DataProcessingService:
    """Application Service of the Data Processing container of ONE environment.

    The service does two jobs, which run at different moments:
    1. It receives the messages that Data Ingestion sends (in the standard format) and accumulates them. This happens
       every time a message arrives. The Input Handler calls 'receive_message'.
    2. It processes what was accumulated. This happens once in every period (see below). The Scheduler calls
       'process_period'. The service waits for the grace period, applies the Data-Issue Handling Approaches (in the
       order they were given), the Combination Logic and, if there is one, the Parameter-Derivation Logic. Then it sends
       the resulting message to the next container (Inference), through the Output Handler, and discards what it
       accumulated, so that the next period starts empty.

    Words used here:
    - Frequency: how often Percepta processes the data (e.g., 10 minutes). The Scheduler triggers the service at fixed
      points in time (at 10:00, 10:10, 10:20, ..., and not 10 minutes after the application started).
    - Period: the time between two triggers. The period that ends at 10:10 is the time after 10:00 up to 10:10.
    - Grace period: extra time that the service waits after the end of the period, to receive the messages that
      belong to the period but arrived late. It is shorter than the frequency.

    What is accumulated, and which period a message belongs to:
    The service accumulates the messages in a structure. A message belongs to the period that is waiting to be
    processed when it arrives, so a message that arrives in the grace period is part of the period that just ended.
    BUT a message that arrives in the grace period may also contain readings of the next period (e.g., a reading with a
    timestamp after the end of the period that just ended). So these messages are accumulated twice: for the period
    that just ended, and for the next one. The service does not decide which readings are useful for each period: it
    keeps all of them, and the approaches decide what to do with the readings that are outside the period being
    processed. A reading outside the period is not useless, because the approaches use it. For example, Frequency
    Harmonisation uses it as a neighbour to work out the value of a missing reading, and it only leaves it out of the
    result if it belongs to a slot that does not touch the period. So it does no harm to keep it in both periods, and
    if it was not kept for the next period, it would be lost.
    Example (frequency of 10 minutes, grace period of 2 seconds): the Scheduler triggers at 10:10:00. A message that
    arrives at 10:10:01 with a reading of 10:10:01 is part of the period that ended at 10:10, where the approaches
    decide if the reading is used. At 10:10:02 that period is processed and its structure is emptied, but the message
    of 10:10:01 was also accumulated for the next period (10:10 to 10:20), where it is used if it belongs there. The
    messages that arrive after 10:10:02 are accumulated only for the next period.

    Expected Structure of the accumulated data:
    -------------------------------------------
    [
        {
            "id": "entity_1_id",
            "entity_type_id": "entity_type_1",
            "parameters": {
                "parameter_1_id": {
                    "parameter_type_id": "parameter_type_1",
                    "values": [                                     # All the readings received in the period
                        {"timestamp": "2026-08-19 10:55:00 +0000", "value": 10.2},
                        {"timestamp": "2026-08-19 11:00:00 +0000", "value": 12.5}
                    ]
                }
            },
            "timestamp": "2026-08-19 11:00:02 +0000"            # Arrival time of the most recent message of the entity
        }
    ]

    This is the input of the first approach. Each approach receives the output of the one before it, and the Combination
    Logic receives the output of the last one.

    If something fails while the period is processed (e.g., an approach raises an error), the error is logged and no
    message is sent for that period. The service is not stopped, so the next period is processed normally.
    """

    _LOG_PREFIX: str = "Data Processing Service |"

    def __init__(
            self,
            approaches: List[DataIssueHandlingApproachBase],
            combination_logic: CombinationLogicBase,
            parameter_derivation_logic: Optional[ParameterDerivationLogicBase],
            output_handler: OutputHandlerBase,
            settings: Dict[str, Any],
            logger: LoggingUtils
    ) -> None:
        """Initializes the service. The settings have already been validated by the InstanceConfiguration.

        Expected `settings` Data Structure:
        -----------------------------------
        {
            "timezone": "UTC",                  # (str) The timezone of the Percepta instance
            "frequency_seconds": 600,           # (float) How often the data is processed, in seconds
            "grace_period_seconds": 2           # (float) How long to wait after the end of a period, in seconds
        }

        Args:
            approaches (List[DataIssueHandlingApproachBase]): The Data-Issue Handling Approaches, in the order in which
                they are applied. It may be empty (no approach is applied).
            combination_logic (CombinationLogicBase): The Combination Logic.
            parameter_derivation_logic (Optional[ParameterDerivationLogicBase]): The Parameter-Derivation Logic, or None
                if the Percepta instance does not have one.
            output_handler (OutputHandlerBase): Sends the resulting message to the next container (Inference).
            settings (Dict[str, Any]): The instance settings the service needs, as detailed above.
                - 'timezone': the points in time at which the periods end (e.g., 10:00, 10:10) are those of this
                  timezone;
                - 'frequency_seconds': how often the data is processed. It divides the hour exactly;
                - 'grace_period_seconds': how long to wait after the end of a period. Zero (or less) means that the
                  period is processed as soon as it ends.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            TypeError: If one of the components is not of the expected type.
            ValueError: If 'settings' is not a dictionary, or one of its keys above is missing.
        """
        self._logger = logger

        # --- Guard Clause 1: each component must be of the expected type ---
        # A wrong type would only fail later, in the middle of a period, so it is better to fail here.
        if not all(isinstance(approach, DataIssueHandlingApproachBase) for approach in approaches):
            self._raise_type_error("every item of 'approaches' must extend DataIssueHandlingApproachBase.")
        if not isinstance(combination_logic, CombinationLogicBase):
            self._raise_type_error("'combination_logic' must extend CombinationLogicBase.")
        if parameter_derivation_logic is not None and not isinstance(
                parameter_derivation_logic, ParameterDerivationLogicBase):
            self._raise_type_error("'parameter_derivation_logic' must extend ParameterDerivationLogicBase.")
        if not isinstance(output_handler, OutputHandlerBase):
            self._raise_type_error("'output_handler' must extend OutputHandlerBase.")

        # --- Guard Clause 2: the settings must have everything the service needs ---
        # Only the presence of the keys is checked: their values were validated when the configuration was read.
        required_settings = ("timezone", "frequency_seconds", "grace_period_seconds")
        if not isinstance(settings, dict) or any(key not in settings for key in required_settings):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'settings' must be a dictionary with "
                f"{list(required_settings)}."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        self._approaches = approaches
        self._combination_logic = combination_logic
        self._parameter_derivation_logic = parameter_derivation_logic
        self._output_handler = output_handler
        self._frequency_seconds = settings["frequency_seconds"]
        self._grace_period_seconds = settings["grace_period_seconds"]
        self._timezone = ZoneInfo(settings["timezone"])

        # The data accumulated for the period that is waiting to be processed, by the id of the entity. It has the
        # structure of the class docstring, but with the entities in a dictionary, so that finding one is fast.
        # Example: {"entity_1_id": {"id": "entity_1_id", "entity_type_id": "entity_type_1", "parameters": {...},
        #                           "timestamp": "2026-08-19 11:00:02 +0000"}}
        self._accumulated: Dict[str, Dict[str, Any]] = {}

        # The data that arrived during the grace period, accumulated again (a separate copy) to start the next period.
        # It is None when there is no grace period running. While it is a dictionary, 'receive_message' adds each
        # message to this structure too. When the grace period ends, it becomes the accumulated data of the next period.
        self._next_period: Optional[Dict[str, Dict[str, Any]]] = None

    async def receive_message(self, envelope: Dict[str, Any]) -> None:
        """Accumulates a message from Data Ingestion. The Input Handler calls this for every message that arrives.

        Expected Input Structure:
        -------------------------
        envelope = {
            "protocol": "AMQP",
            "source": "exchange/queue",
            "status": None,
            "content_type": "application/json",
            "metadata": {},
            "payload": [
                {
                    "id": "entity_1_id",
                    "entity_type_id": "entity_type_1",
                    "parameters": {
                        "parameter_1_id": {
                            "parameter_type_id": "parameter_type_1",
                            "values": [{"timestamp": "2026-08-19 10:55:00 +0000", "value": 10.2}]
                        }
                    },
                    "timestamp": "2026-08-19 10:55:03 +0000"
                }
            ]
        }

        What happens to each entity of the message:
        - If the entity is new in this period, it is added to the accumulated data.
        - If it already exists, the readings of each of its parameters are added at the end of the lists 'values' that
          the service already has, and the 'timestamp' of the entity is replaced by the arrival time of this message.
        - If the message arrives during the grace period, all of the above is done twice: for the period that just
          ended, and for the next period.

        An entity or a parameter that does not have the expected structure is skipped, with a warning in the log, and
        the rest of the message is accumulated. A message whose payload cannot be read is skipped as a whole.

        Args:
            envelope (Dict[str, Any]): The message, as given by the Input Handler.
        """
        entities = self._read_payload(envelope)
        if entities is None:
            return

        # The arrival time, which is the same for all the entities of the message
        arrival = format_timestamp(datetime.now(self._timezone))

        # 'targets' is the list of structures where this message is accumulated. The service has two:
        # - '_accumulated': the period that is waiting to be processed. It always receives the message.
        # - '_next_period': the period that comes after it. It only exists (it is not None) while the grace period is
        #   running, i.e., from the moment 'process_period' is triggered until it takes the accumulated data. Outside
        #   the grace period, it is None.
        # So there are two cases:
        # - Normal moment (no grace period running): targets = [_accumulated]. The message is accumulated once.
        # - Grace period running: targets = [_accumulated, _next_period]. The message is accumulated twice, because
        #   it arrived after the end of the period, but it may still belong to it (a late message), or it may already
        #   have readings of the next one. It is not possible to know which at this point, so it is kept in both. The
        #   approaches of each period then decide what to do with the readings that are outside it: they are not
        #   thrown away by the service, as they can still be useful (e.g., as neighbours to fill a missing reading).
        #   See the class docstring.
        # Example (frequency of 10 minutes, grace period of 2 seconds, trigger at 10:10:00):
        #   10:09:58 -> targets = [_accumulated]                  -> period 10:00-10:10 only
        #   10:10:01 -> targets = [_accumulated, _next_period]    -> period 10:00-10:10 and period 10:10-10:20
        #   10:10:03 -> targets = [_accumulated]                  -> period 10:10-10:20 only (it is the new
        #                                                            '_accumulated' since 10:10:02)
        # Each target has a name, only to say in the logs which structure a message is about: the two have the same
        # message, so without the name the two warnings would be the same.
        targets = [("current period dictionary", self._accumulated)]
        if self._next_period is not None:
            targets.append(("next period dictionary", self._next_period))

        for entity in entities:
            # --- Guard Clause 1: an entity needs an id and a dictionary of parameters ---
            if not isinstance(entity, dict) or not isinstance(entity.get("id"), str) \
                    or not isinstance(entity.get("parameters"), dict):
                self._logger.warning(f"{self._LOG_PREFIX} Skipping an entity without 'id' or 'parameters'.")
                continue

            for target_name, accumulated in targets:
                self._add_entity(accumulated, entity, arrival, target_name)

    def _add_entity(
            self, accumulated: Dict[str, Dict[str, Any]], entity: Dict[str, Any], arrival: str,
            target_name: str = "current period dictionary"
    ) -> None:
        """Adds an entity of a message to an accumulation structure.

        Args:
            accumulated (Dict[str, Dict[str, Any]]): The structure to add to, by the id of the entity. It is changed.
            entity (Dict[str, Any]): The entity of the message. It has already been checked to have an 'id' and a
                dictionary 'parameters'.
            arrival (str): The arrival time of the message, as a timestamp.
            target_name (str): Name of the structure ("current period dictionary" or "next period dictionary"), only for the logs.
        """
        # Finds the entity in 'accumulated', or creates it if it is not there yet. 'dict.setdefault(key, default)' does
        # exactly that, in one step:
        # - If the key exists, it returns the value stored under it. The default is ignored.
        # - If the key does not exist, it stores the default under the key and returns it.
        # The default here is an empty entity: it has the id and the type, and no parameters yet (they are added below).
        # 'stored_entity' is the dictionary that is inside 'accumulated', not a copy, so changing it below changes
        # 'accumulated'.
        # Example 1: "EV1" arrives for the first time in the period.
        #   Before: accumulated = {}
        #   After:  accumulated = {"EV1": {"id": "EV1", "entity_type_id": "ev_charger", "parameters": {}}}
        # Example 2: "EV1" arrives again, in a later message of the same period. It exists, so the empty entity
        # is ignored and the stored one is returned, with the parameters and readings it already has.
        #   Before: accumulated = {"EV1": {"id": "EV1", ..., "parameters": {"power": {"values": [r1, r2]}}}}
        #   After:  the same dictionary, nothing is replaced or lost.
        stored_entity = accumulated.setdefault(
            entity["id"], {"id": entity["id"], "entity_type_id": entity.get("entity_type_id"), "parameters": {}}
        )

        # The timestamp of the entity is the arrival time of the most recent message that had it. So it is replaced
        # every time the entity arrives, and not only when it is created.
        stored_entity["timestamp"] = arrival

        for parameter_id, parameter in entity["parameters"].items():
            # --- Guard Clause 1: a parameter needs a list 'values' ---
            if not isinstance(parameter, dict) or not isinstance(parameter.get("values"), list):
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{parameter_id}' of entity '{entity['id']}' (parameter type: "
                    f"{parameter.get('parameter_type_id') if isinstance(parameter, dict) else None}): "
                    f"not added to the {target_name}, where the readings received for the period are accumulated, "
                    f"because its 'values' is not a list of readings or a dictionary."
                )
                continue

            # Same idea as for the entity: the parameter that is already stored, or a new one with an empty list of
            # readings. The readings of this message are added to the end of that list just below.
            stored_parameter = stored_entity["parameters"].setdefault(
                parameter_id, {"parameter_type_id": parameter.get("parameter_type_id"), "values": []}
            )

            # Each reading is copied ('dict(reading)'), instead of adding the same dictionary. In Python, 'extend'
            # only copies the references. Without 'dict(reading)', the readings of 'stored_parameter["values"]' (the
            # list the service keeps) and of 'parameter["values"]' (the list of the message that was received) would
            # be the SAME dictionaries, and changing a reading in one list would change it in the other:
            #   Without the copy: message [A]  and  service [A]     <- one dictionary A, shared
            #   With the copy:    message [A]  and  service [A']    <- A' is a copy of A, they are independent
            # The approaches do change readings (e.g., Frequency Harmonisation trims the value of a cumulative reading).
            # Two problems are avoided:
            # 1. During the grace period, the message is added to two structures (see 'receive_message'). Without the
            #    copy, both would share the same readings. Example: the reading {"timestamp": "10:05", "value": 10.0} is
            #    trimmed to 6.67 when the period that ended is processed, and the next period would receive it already
            #    with 6.67, instead of 10.0.
            # 2. The message itself is also protected from the approaches: the readings of 'stored_parameter' are
            #    copies, so an approach never changes the message that was received. (When the containers run in the
            #    same process, the Input Handler also gives the service its own copy of the payload, see
            #    'PayloadNormalizer.from_local'; this copy does not replace that one, it is a second line of defence.)
            # Only dictionaries are added: an item of 'values' that is not a reading (e.g., a number) is skipped.
            stored_parameter["values"].extend(
                dict(reading) for reading in parameter["values"] if isinstance(reading, dict)
            )

    async def process_period(self) -> None:
        """Processes the period that just ended. The Scheduler calls this at every trigger (e.g., 10:00, 10:10, ...).

        It works in four steps:
        1. Works out the end of the period (the point in time of the trigger);
        2. Waits for the grace period, if there is one. The messages that arrive meanwhile are accumulated for this
           period and also for the next one;
        3. Takes the accumulated data, applies the approaches, the Combination Logic and the Parameter-Derivation
           Logic. The next period starts with the messages that arrived during the grace period (or empty);
        4. Sends the resulting message through the Output Handler.

        The approaches run even if no message arrived in the period, because some of them can create data (e.g., Missing
        Data rebuilds the entities that did not report). If, after the approaches, there is still no data, there is
        nothing to combine, so nothing is sent.
        """
        period_end = self._current_period_end()
        self._logger.debug(f"{self._LOG_PREFIX} Period ending at {format_timestamp(period_end)} has ended.")

        # Step 2. A grace period of zero or less means that there is nothing to wait for.
        # While the service waits, '_next_period' is a dictionary, and so 'receive_message' also accumulates the
        # messages that arrive in it (see the class docstring).
        if self._grace_period_seconds > 0:
            self._next_period = {}
            try:
                await asyncio.sleep(self._grace_period_seconds)
            except asyncio.CancelledError:
                # The application is stopping while the service waits. The period is not processed, and the job ends
                # quietly (re-raising would make the scheduler print a traceback for a normal shutdown)
                self._logger.debug(
                    f"{self._LOG_PREFIX} Stopped during the grace period, so the period ending at "
                    f"{format_timestamp(period_end)} was not processed."
                )
                return

        # Step 3. The accumulated data is taken, and the data accumulated for the next period (the messages of the
        # grace period, or an empty one) takes its place, in one step, before anything else is done. The service runs in
        # one thread (the event loop), so no message can arrive in between.
        data = list(self._accumulated.values())
        self._accumulated = self._next_period if self._next_period is not None else {}
        self._next_period = None

        try:
            message = self._process(data, period_end)
        except Exception as err:
            self._logger.error(
                f"{self._LOG_PREFIX} Could not process the period ending at {format_timestamp(period_end)}, so no "
                f"message was sent: {type(err).__name__}: {err}"
            )
            return

        if message is None:
            self._logger.warning(
                f"{self._LOG_PREFIX} No data for the period ending at {format_timestamp(period_end)}, so no message "
                f"was sent."
            )
            return

        # Step 4. Hands the message of the period to the Output Handler, which delivers it to the next container
        # (Inference). The message is the final result of the period: the accumulated data was already replaced above
        # (step 3), so the service does not keep anything of this period after this point.
        self._logger.info(
            f"{self._LOG_PREFIX} Message of the period ending at {format_timestamp(period_end)}: "
            f"\n{json.dumps(message, default=str, indent=2)}"
        )
        try:
            await self._output_handler.send(message)
        except Exception as err:
            # 'type(err).__name__' is the name of the error class (e.g., ConnectionError). It is logged with the text of
            # the error because the text alone is sometimes empty or too generic. The end of the period identifies
            # which period lost its message.
            self._logger.error(
                f"{self._LOG_PREFIX} Could not send the message of the period ending at "
                f"{format_timestamp(period_end)}: {type(err).__name__}: {err}"
            )

    def _process(self, data: List[Dict[str, Any]], period_end: datetime) -> Optional[Dict[str, Any]]:
        """Applies the approaches, the Combination Logic and the Parameter-Derivation Logic to the data of a period.

        Args:
            data (List[Dict[str, Any]]): The data accumulated in the period (the structure of the class docstring).
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            Optional[Dict[str, Any]]: The message to send, or None if there is no data to combine.
        """
        # Each approach receives the output of the one before it. That is why the order of the approaches matters.
        for approach in self._approaches:
            data = approach.handle(data, period_end)

        # --- Guard Clause 1: without data there is nothing to combine ---
        if not data:
            return None

        message = self._combination_logic.combine(data, period_end)

        # The Parameter-Derivation Logic is optional. When there is none, the combined message is the final one.
        if self._parameter_derivation_logic is not None:
            message = self._parameter_derivation_logic.derive(message, period_end)

        return message

    def _current_period_end(self) -> datetime:
        """Works out the end of the period that the trigger belongs to.

        The trigger happens at a fixed point in time (e.g., 10:10:00), but the code runs a few milliseconds later (e.g.,
        10:10:00.004). So the current time is rounded DOWN to the last multiple of the frequency inside the hour. This
        works because the frequency divides the hour exactly.
        Example: frequency of 600 seconds (10 minutes) and now = 10:10:00.004 -> 10:10:00.004 is 10 minutes and 0.004
        seconds into the hour -> 600 // 600 x 600 = 600 seconds -> 10:00:00 + 600 seconds = 10:10:00.

        Returns:
            datetime: The end of the period, with the timezone of the Percepta instance.
        """
        now = datetime.now(self._timezone)
        seconds_into_the_hour = now.minute * 60 + now.second
        seconds_of_the_trigger = (seconds_into_the_hour // self._frequency_seconds) * self._frequency_seconds
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(seconds=seconds_of_the_trigger)

    def _read_payload(self, envelope: Any) -> Optional[List[Any]]:
        """Gets the list of entities from the envelope that the Input Handler delivered.

        The payload is a list when the message came from the same process (in-memory connector), and JSON, as text or
        bytes, when it came from the network (e.g., RabbitMQ). Both are accepted.

        This method only checks that there is a list to go through. It does not check the entities inside it: that is
        done for each entity, by 'receive_message' and '_add_entity'. So a failure only loses what is broken:
        - A message that cannot be read (this method returns None) is skipped as a whole, with all of its entities.
          The next messages are accepted normally.
          Example: a message of 3 entities whose text is corrupted -> the 3 entities are lost, but only from this
          message.
        - A message that can be read, but has an entity that is not valid, is accepted without that entity. The other
          entities of the message are accumulated.
          Example: a message with EV1, EV2 and EV3, where EV2 has no 'id' -> EV1 and EV3 are accumulated and EV2 is
          skipped, with a warning in the log.

        Expected Input Structure:
        -------------------------
        envelope = {
            "protocol": "AMQP",
            "source": "exchange/queue",
            "status": None,
            "content_type": "application/json",
            "metadata": {},
            "payload": [                                    # A list (in-memory), or the same list as JSON text/bytes
                {
                    "id": "entity_1_id",
                    "entity_type_id": "entity_type_1",
                    "parameters": {
                        "parameter_1_id": {
                            "parameter_type_id": "parameter_type_1",
                            "values": [{"timestamp": "2026-08-19 10:55:00 +0000", "value": 10.2}]
                        }
                    },
                    "timestamp": "2026-08-19 10:55:03 +0000"
                }
            ]
        }
        # The payload of a message from the network is the same list, as text or bytes:
        # '[{"id": "entity_1_id", "entity_type_id": "entity_type_1", "parameters": {...}, "timestamp": "..."}]'

        Expected Output Structure:
        --------------------------
        [                                                   # The list of the payload, as it came (not checked)
            {
                "id": "entity_1_id",
                "entity_type_id": "entity_type_1",
                "parameters": {...},
                "timestamp": "2026-08-19 10:55:03 +0000"
            }
        ]

        None (with a warning in the log) when the payload cannot be read:
        - the envelope is not a dictionary, or has no 'payload' (e.g., "some text", {"status": 200}): the payload
          is None, so it is not a list;
        - the payload is text/bytes that is not valid JSON (e.g., "not json");
        - the payload is not a list (e.g., a dictionary {"id": "entity_1_id"}, a number or None).

        Args:
            envelope (Any): The message, as given by the Input Handler.

        Returns:
            Optional[List[Any]]: The entities, or None (with a warning in the log) if the payload cannot be read.
        """
        # An envelope that is not a dictionary has no payload. It ends in the Guard Clause 1 below, as None.
        payload = envelope.get("payload") if isinstance(envelope, dict) else None

        # A payload that came from the network is JSON, as text or bytes, so it is turned into a list.
        # Example: '[{"id": "entity_1_id", ...}]' -> [{"id": "entity_1_id", ...}]
        # A payload that came from the same process is already a list, so it goes through untouched.
        if isinstance(payload, (bytes, str)):
            try:
                payload = json.loads(payload)
            except ValueError as err:
                self._logger.warning(f"{self._LOG_PREFIX} Skipping a message that is not valid JSON: {err}")
                return None

        # --- Guard Clause 1: the message must be a list of entities ---
        # Example: a payload {"id": "entity_1_id", ...} (one entity, not inside a list) or a JSON number -> skipped
        if not isinstance(payload, list):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping a message whose payload is not a list of entities, got "
                f"{type(payload).__name__}."
            )
            return None

        return payload

    def _raise_type_error(self, detail: str) -> None:
        """Logs an initialization error and raises it as a TypeError.

        Args:
            detail (str): What is wrong.

        Raises:
            TypeError: Always.
        """
        error_msg = f"{self._LOG_PREFIX} Initialization error: {detail}"
        self._logger.error(error_msg)
        raise TypeError(error_msg)
