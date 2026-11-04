# --- Standard Library Imports ---
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.data_issue_handling.data_issue_handling_approach_base import DataIssueHandlingApproachBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import duration_to_seconds, format_timestamp, parse_timestamp


class MissingDataApproach(DataIssueHandlingApproachBase):
    """Rebuilds the entities that sent no data at all in the period, in any of their parameters.

    What this approach does with each entity of the environment that has parameters:
    - If NONE of its parameters has a reading in the period, the entity is "missing". The approach rebuilds it, with
      all of its parameters.
    - If AT LEAST ONE of its parameters has a reading, the entity is NOT missing, and the approach does nothing to it.
      This is true even if other parameters of the same entity have no readings.

    Why the approach rebuilds the whole entity, and never only one parameter:
    The approach ASSUMES that the parameters of an entity are always sent together, by the same Telemetry System.
    If this is true, when the Telemetry System stops sending data, all the parameters of the entity stop arriving at the
    same time. Then all of them are rebuilt together, from values that were received together.
    Rebuilding the parameters one by one could create values that do not make sense together.
    Example: a washing machine has two parameters, "is_on" and "power", and both come from the same Telemetry System.
    - If the Telemetry System stops sending, neither of them arrives. The approach rebuilds both from the last values
      received (e.g., is_on = on and power = 1200). The two values make sense together, because they were received
      together.
    - If, instead, the approach rebuilt only "power" when it was missing, and it had no previous value, "power" would
      become zero while "is_on" arrived saying that the machine is on. The data would say that the machine is on and
      is using no power.

    When the assumption is NOT true:
    The parameters of an entity can come from different Telemetry Systems.
    Example: "is_on" comes from Telemetry System 1 and "power" comes from Telemetry System 2, and System 2 stops
    sending. In the period, "is_on" arrives but "power" does not. The entity has one reading, so it is NOT missing, and
    this approach does nothing: "power" stays without readings.
    For this reason, this approach may not be right for every use case. If it is not right for yours, do not configure
    it, and integrate an approach made for your use case instead (see the README of this folder).

    How a missing entity is rebuilt, for each of its parameters:
    - The value is the most recent value that the parameter had when it was received. If the parameter never had one
      (for example, right after the application starts), the value is zero.
    - There is only ONE reading, with that value. The approach does not repeat the value to fill all the readings that
      the parameter should have in the period. Completing them is left to whoever needs it, normally the Frequency
      Harmonisation approach, which fills the missing readings of each parameter using its 'fill_operation'.
    - The timestamp of that reading is the timestamp of the FIRST reading that the parameter should have in the
      period. This is the reading closest in time to the most recent value, because that value was received before the
      period started. This way, whoever completes the other readings can carry the value forward from the start of the
      period.
      The first reading is worked out from the periodicity of the parameter (its 'temporal_behavior' in the schema).
      Example: for a period that ends at 10:15 and a periodicity of 5 minutes, the readings of the period are at 10:05,
      10:10 and 10:15, so the first one is at 10:05.
      If the parameter has no periodicity, or reports less often than the frequency, it has only one reading in the
      period, so the timestamp is the end of the period.
    - The timestamp of the entity is the end of the period.

    The rebuilt entity has the same structure as an entity that was received, with no extra field. Nothing in the
    message shows that it was rebuilt. The only trace is a warning in the log.

    Only data that was really received is remembered as "the most recent value". The values that this approach creates
    are never remembered. So, if an entity is missing for a long time, the invented values never replace the last real
    value.

    Example (the period ends at 10:15, and 'id_02' has one parameter with a periodicity of 5 minutes and was never
    received):
    -----------------------------------------------------------------------------------------------------------
    Input:  [{"id": "id_01", ...}]
    Output: [{"id": "id_01", ...},
             {"id": "id_02", "entity_type_id": "entity_type_2",
              "parameters": {"id_param_03": {"parameter_type_id": "parameter_type_3", "values": [
                  {"timestamp": "2026-01-01 10:05:00 +0000", "value": 0.0}]}},
              "timestamp": "2026-01-01 10:15:00 +0000"}]
    """

    _LOG_PREFIX: str = "Missing Data Approach |"

    def __init__(self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils) -> None:
        """Reads the entities with parameters from the schema, and works out where the first reading of each parameter
        falls in a period.

        Only the entities that have parameters are kept, because they are the only ones that can receive data and so
        the only ones that can be missing.

        Args:
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage. Only 'entities' is used.
            frequency_seconds (float): How often the data is processed, in seconds. It is the length of a period.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If the environment has no entities, or the periodicity of a parameter is not valid.
        """
        super().__init__(environment, frequency_seconds, logger)

        # --- Guard Clause 1: the environment must have its entities ---
        entities = environment.get("entities")
        if not isinstance(entities, dict) or not entities:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: the environment has no 'entities'.")

        # What is needed to rebuild each entity, taken from the schema. It is saved once here, and not read again in
        # every period.
        # Entities without parameters are left out. They never receive data, so they can never be missing (e.g., an
        # entity that only has actuators).
        # Example: {"id_02": {"entity_type_id": "entity_type_2", "parameters": {"id_param_03": {
        #               "parameter_type_id": "parameter_type_3", "first_reading_offset_seconds": 600}}}}
        self._entities: Dict[str, Dict[str, Any]] = {}
        for entity_id, entity in entities.items():
            parameters = entity.get("parameters") if isinstance(entity, dict) else None
            if not parameters:
                continue
            self._entities[entity_id] = {
                "entity_type_id": entity.get("entity_type_id"),
                "parameters": {
                    parameter_id: self._read_parameter(entity_id, parameter_id, parameter)
                    for parameter_id, parameter in parameters.items()
                }
            }

        # The most recent value received for each parameter of each entity. It starts empty and it is updated in every
        # period, with the values of the entities that sent data.
        # Example: {"id_02": {"id_param_03": 4.2}}
        self._most_recent_values: Dict[str, Dict[str, Any]] = {}

    def handle(self, data: List[Dict[str, Any]], period_end: datetime) -> List[Dict[str, Any]]:
        """Rebuilds the entities that sent no data in the period, and remembers the values of the other entities.

        Expected Input Structure:
        -------------------------
        data = [
            {
                "id": "id_01",
                "entity_type_id": "entity_type_1",
                "parameters": {
                    "id_param_01": {
                        "parameter_type_id": "parameter_type_1",
                        "values": [{"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.4}]
                    }
                },
                "timestamp": "2026-01-01 10:15:00 +0000"
            }
        ]
        period_end = datetime(2026, 1, 1, 10, 15, 0, tzinfo=timezone.utc)

        Expected Output Structure:
        --------------------------
        The same list, plus one dictionary for each missing entity, in the same structure
        (see the class docstring for an example).

        Args:
            data (List[Dict[str, Any]]): The data received in the period, one dictionary for each entity that sent data.
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            List[Dict[str, Any]]: The data with the missing entities rebuilt. It is the same list that was received,
                changed.
        """
        # The received entities by id, to find each one quickly.
        # Example: {"id_01": {"id": "id_01", ...}}
        received = {entity["id"]: entity for entity in data}

        # Goes through the entities of the schema and separates them in two groups:
        # - the ones that sent data: their values are remembered, to be used in the periods in which they are missing;
        # - the ones that did not: they are collected in 'missing_ids', to be rebuilt below.
        # The values are remembered BEFORE rebuilding, so that only data that was really received is remembered, and
        # never the values created by this approach.
        missing_ids = []
        for entity_id in self._entities:
            if self._has_readings(received.get(entity_id)):
                self._remember_values(received[entity_id])
            else:
                missing_ids.append(entity_id)

        for entity_id in missing_ids:
            rebuilt_entity = self._rebuild_entity(entity_id, period_end)

            # An entity is missing in one of two situations, and each one is handled in a different way:
            # 1. The entity did not arrive at all, so it is not in 'received'. There is nothing to replace, so the
            #    rebuilt entity is added to the end of the list.
            #    Example: data = [id_01]  ->  data = [id_01, id_02 (rebuilt)]
            # 2. The entity arrived, but with no readings (e.g., all its lists 'values' are empty), so it is in
            #    'received'. The list already has an entry with its id, and adding another one would leave two entries
            #    for the same entity. So the empty entry is replaced in the same position: 'data.index' gives the
            #    position of the empty entry, and the rebuilt entity is put there.
            #    Example: data = [id_01, id_02 (empty)]  ->  data = [id_01, id_02 (rebuilt)]
            if entity_id in received:
                data[data.index(received[entity_id])] = rebuilt_entity
            else:
                data.append(rebuilt_entity)

        return data

    def _read_parameter(self, entity_id: str, parameter_id: str, parameter: Any) -> Dict[str, Any]:
        """Works out, from the schema, how many seconds before the end of a period the first reading of a parameter is.

        Expected Input Structure:
        -------------------------
        parameter = {
            "parameter_type_id": "parameter_type_3",
            "temporal_behavior": {"periodicity": {"value": 5, "unit": "min"}, ...}      # (optional)
        }

        Expected Output Structure:
        --------------------------
        {"parameter_type_id": "parameter_type_3", "first_reading_offset_seconds": 600}

        How the offset is worked out:
        The number of readings that the parameter should have in a period is the number of times that its periodicity
        fits in the frequency. The last reading is at the end of the period, and the others are one periodicity apart.
        Example: a frequency of 15 minutes (900 seconds) and a periodicity of 5 minutes (300 seconds) give 3 readings,
        at 10:05, 10:10 and 10:15 for a period that ends at 10:15. The first reading is 10 minutes (600 seconds) before
        the end of the period. This is (3 - 1) x 300.
        A parameter with no periodicity, or that reports less often than the frequency, has 1 reading, so the offset
        is 0.

        Args:
            entity_id (str): Id of the entity. It is only used in the error messages.
            parameter_id (str): Id of the parameter. It is only used in the error messages.
            parameter (Any): The parameter, as it is in the schema.

        Returns:
            Dict[str, Any]: The type of the parameter, and the seconds between its first reading and the end of the
                period.

        Raises:
            ValueError: If the periodicity exists but is not valid (e.g., it has an unknown unit).
        """
        parameter_type_id = parameter.get("parameter_type_id") if isinstance(parameter, dict) else None
        temporal_behavior = parameter.get("temporal_behavior") if isinstance(parameter, dict) else None
        periodicity = temporal_behavior.get("periodicity") if isinstance(temporal_behavior, dict) else None

        # --- Guard Clause 2: a parameter without periodicity has one reading per period ---
        if periodicity is None:
            return {"parameter_type_id": parameter_type_id, "first_reading_offset_seconds": 0}

        try:
            periodicity_seconds = duration_to_seconds(periodicity, "periodicity")
        except ValueError as err:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: parameter '{parameter_id}' of entity '{entity_id}': {err}"
            ) from err

        # Counts how many times the periodicity fits in the frequency (e.g., 900 // 300 = 3). The '//' is a division
        # without decimals. 'max(1, ...)' is used because a parameter that reports less often than the frequency (e.g.,
        # every hour with a frequency of 15 minutes) would give 0, and it still has one reading in the period.
        readings = max(1, int(self._frequency_seconds // periodicity_seconds))
        return {
            "parameter_type_id": parameter_type_id,
            "first_reading_offset_seconds": (readings - 1) * periodicity_seconds
        }

    @staticmethod
    def _has_readings(entity: Optional[Dict[str, Any]]) -> bool:
        """Tells if an entity of the message has at least one reading, in any of its parameters.

        Args:
            entity (Optional[Dict[str, Any]]): The entity as it arrived, or None if it did not arrive.

        Returns:
            bool: True if there is at least one reading. False if the entity did not arrive or has no readings.
        """
        # An entity that did not arrive (None) has no readings
        if not entity:
            return False

        # For each parameter of the entity, 'parameter.get("values")' is its list of readings. A list with readings
        # counts as True. An empty list, or a parameter with no 'values', counts as False.
        # 'any' goes through all of them and returns True as soon as it finds one that is True. If none is True, it
        # returns False.
        # Example: {"p1": {"values": []}, "p2": {"values": [{"timestamp": "...", "value": 3.4}]}}  ->  True
        #          {"p1": {"values": []}, "p2": {"values": []}}                                    ->  False
        return any(parameter.get("values") for parameter in entity.get("parameters", {}).values())

    def _remember_values(self, entity: Dict[str, Any]) -> None:
        """Keeps, for each parameter of an entity that sent data, the value of its most recent reading.

        The most recent reading is the one with the latest timestamp. It is NOT the last one in the list, because this
        approach runs before the readings are sorted, so the list may be out of order. Only one value is kept for each
        parameter: the new value replaces the old one.

        Args:
            entity (Dict[str, Any]): The entity as it arrived.
        """
        # The remembered values of this entity, by parameter.
        # 'setdefault' returns the dictionary of the entity if it already exists. If it does not exist (the first time
        # the entity sends data), it creates an empty one, stores it, and returns it.
        # In both cases, 'remembered' is the same dictionary that is kept in 'self._most_recent_values', so changing it
        # changes what is remembered.
        # Example: self._most_recent_values = {"id_02": {"id_param_03": 4.2}}  ->  remembered = {"id_param_03": 4.2}
        remembered = self._most_recent_values.setdefault(entity["id"], {})

        for parameter_id, parameter in entity.get("parameters", {}).items():
            values = parameter.get("values")

            # A parameter with no readings in this period keeps the value that was remembered before, if there is one.
            # Only the parameters that have readings are updated.
            if not values:
                continue

            # The old value is replaced by the most recent value of this period. Only one value is kept for each
            # parameter, so nothing accumulates.
            remembered[parameter_id] = self._most_recent_value(values)

    @staticmethod
    def _most_recent_value(values: List[Dict[str, Any]]) -> Any:
        """Returns the value of the reading with the latest timestamp.

        Args:
            values (List[Dict[str, Any]]): The readings of a parameter, e.g., [{"timestamp": "...", "value": 1.0}].

        Returns:
            Any: The value of the reading with the latest timestamp. If no timestamp can be read, the value of the last
                reading in the list.
        """
        # 'latest' is the best reading found so far. 'latest_moment' is its timestamp turned into a date and time, so
        # that it can be compared with the timestamps of the other readings.
        # At the start no timestamp was read yet, so 'latest_moment' is None and 'latest' is the last reading of the
        # list. That reading is the one used if no timestamp can be read.
        latest, latest_moment = values[-1], None

        for reading in values:
            # Turns the text of the timestamp (e.g., "2026-01-01 10:15:00 +0000") into a date and time. This is needed
            # because texts cannot be compared as times (e.g., "+0100" and "+0000" would be compared as plain text).
            # If the reading has no 'timestamp' (KeyError), or the timestamp is not in the expected format
            # (ValueError), this reading is skipped. This way, one bad timestamp does not stop the other readings from
            # being compared.
            try:
                moment = parse_timestamp(reading["timestamp"])
            except (KeyError, ValueError):
                continue

            # This reading becomes the best one so far in one of two cases: it is the first one that could be read
            # ('latest_moment' is None), or it is not earlier than the best one so far. Because of the '>=', if two
            # readings have the same timestamp, the one that comes later in the list is kept.
            if latest_moment is None or moment >= latest_moment:
                latest, latest_moment = reading, moment

        return latest["value"]

    def _rebuild_entity(self, entity_id: str, period_end: datetime) -> Dict[str, Any]:
        """Builds the entity that was not received, with its most recent values or with zeros.

        Args:
            entity_id (str): Id of the missing entity.
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            Dict[str, Any]: The entity in the structure of the message, with one reading for each parameter.
        """
        # What the schema says about this entity: its type, and its parameters with the offset of their first reading
        schema = self._entities[entity_id]

        # The most recent value of each parameter of this entity, remembered from the last time it sent data.
        # Example: {"id_param_03": 4.2}
        # It is empty if the entity never sent data (e.g., right after the application starts).
        remembered = self._most_recent_values.get(entity_id, {})

        # Builds the parameters of the rebuilt entity, with one reading each, in the structure of the message:
        # - "parameter_type_id": taken from the schema.
        # - "timestamp": the time of the first reading that the parameter should have in the period. It is the end of
        #   the period minus the offset worked out from the periodicity of the parameter.
        #   Example: the period ends at 10:15 and the first reading is 600 seconds before the end -> 10:05.
        # - "value": the remembered value of the parameter, or 0.0 if there is none. The 0.0 is the second argument of
        #   'get', which is what 'get' returns when the key does not exist.
        parameters = {
            parameter_id: {
                "parameter_type_id": parameter["parameter_type_id"],
                "values": [{
                    "timestamp": format_timestamp(
                        period_end - timedelta(seconds=parameter["first_reading_offset_seconds"])
                    ),
                    "value": remembered.get(parameter_id, 0.0)
                }]
            }
            for parameter_id, parameter in schema["parameters"].items()
        }

        # The only trace that the entity was rebuilt is this warning in the log. For Python, an empty dictionary counts
        # as False. So, when there are no remembered values, the message says that zeros were used.
        used_values = "its most recent values" if remembered else "zeros"
        self._logger.warning(
            f"{self._LOG_PREFIX} Entity '{entity_id}' sent no data in the period that ended at "
            f"{format_timestamp(period_end)}. Rebuilt with {used_values}."
        )

        # The rebuilt entity, in the same structure as an entity that was received. The timestamp of the entity is the
        # end of the period. The readings are not at the end of the period: each one is at the time of the first
        # reading of its parameter.
        return {
            "id": entity_id,
            "entity_type_id": schema["entity_type_id"],
            "parameters": parameters,
            "timestamp": format_timestamp(period_end)
        }
