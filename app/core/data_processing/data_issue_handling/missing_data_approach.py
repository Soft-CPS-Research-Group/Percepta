# --- Standard Library Imports ---
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.data_processing.data_issue_handling.data_issue_handling_approach_base import DataIssueHandlingApproachBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import format_timestamp, parse_timestamp


class MissingDataApproach(DataIssueHandlingApproachBase):
    """Completes the entities with the parameters that did not arrive, using what was received before.

    The approach looks at every entity of the environment that has parameters, and at the parameters that are
    'necessary' (the schema says "necessary": true). There are two situations:

    1. The entity is MISSING: none of its parameters has a reading in the period (it did not arrive, or it arrived with
       every list 'values' empty). The approach rebuilds it. Each parameter receives ALL the readings that the approach
       still remembers for it, with their original timestamps and values. The approach does not place them in time or
       fill anything: that is left to the Frequency Harmonisation approach, which runs after this one.
    2. The entity ARRIVED, with at least one reading in one of its parameters. Its readings are not changed. But each
       necessary parameter that has no readings gets "values": None. None means "this parameter has no value in this
       period". It is what the rest of Percepta turns into a null.
       This approach does not rebuild a parameter that is not necessary, and it does not use remembered readings in
       this situation.

    In both situations, a necessary parameter for which nothing is remembered (for example, right after the
    application starts) has "values": None.

    What the approach remembers:
    The real readings (never the ones that the approach created) of each parameter of each entity that sent data,
    with their timestamps. Not many are kept, so that the memory does not grow without end. It depends on the optional
    'max_reading_age_seconds' of the parameter, in its 'temporal_behavior':
    - If it is set, the readings of every period are added to the ones that are remembered (a reading with the same
      timestamp replaces the old one), and the readings with a timestamp older than 'max_reading_age_seconds' before
      the end of the period are forgotten. A reading in the future is never old.
    - If it is not set, only the readings of the last period in which the parameter had some are remembered: they
      replace the old ones.
    It is kept in memory only, so after a restart nothing is remembered.

    Example (the period ends at 10:15, 'id_02' has the necessary parameter "id_param_03" with 'max_reading_age_seconds'
    600, and it had the readings 10:00 (4.0), 10:08 (4.2) and 10:12 (4.4) when it last sent data):
    ----------------------------------------------------------------------------------------------------------------
    Input:  {"id_01": {...}}
    Output: {"id_01": {...},
             "id_02": {"entity_type_id": "entity_type_2",
                       "parameters": {"id_param_03": {"parameter_type_id": "parameter_type_3", "values": [
                           {"timestamp": "2026-01-01 10:08:00 +0000", "value": 4.2},
                           {"timestamp": "2026-01-01 10:12:00 +0000", "value": 4.4}]}},
                       "timestamp": "2026-01-01 10:15:00 +0000"}}
    The reading of 10:00 is more than 600 seconds before 10:15, so it was forgotten.

    The rebuilt entity has the same structure as an entity that was received, with no extra field. Nothing in the
    message shows that it was rebuilt. The only trace is a warning in the log.
    """

    _LOG_PREFIX: str = "Missing Data Approach |"

    def __init__(self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils) -> None:
        """Reads the entities with parameters from the schema.

        Only the entities that have parameters are kept, because they are the only ones that can receive data and so
        the only ones that can be missing.

        Expected `environment` Data Structure (the parts that are used):
        ---------------------------------------------------------------
        {
            "entities": {
                "id_02": {
                    "entity_type_id": "entity_type_2",
                    "parameters": {
                        "id_param_03": {
                            "parameter_type_id": "parameter_type_3",
                            "necessary": True,                                  # Optional (default False)
                            "temporal_behavior": {"max_reading_age_seconds": 600, ...}    # Optional
                        }
                    }
                }
            }
        }

        Args:
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage. Only 'entities' is used.
            frequency_seconds (float): How often the data is processed, in seconds. It is not used by this approach.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If the environment has no entities, or the 'max_reading_age_seconds' of a parameter is not a
                number greater than zero.
        """
        super().__init__(environment, frequency_seconds, logger)

        # --- Guard Clause 1: the environment must have its entities ---
        entities = environment.get("entities")
        if not isinstance(entities, dict) or not entities:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: the environment has no 'entities'.")

        # What is needed to complete each entity, taken from the schema. It is saved once here, and not read again in
        # every period.
        # Entities without parameters are left out. They never receive data, so they can never be missing (e.g., an
        # entity that only has actuators).
        # Example: {"id_02": {"entity_type_id": "entity_type_2", "parameters": {"id_param_03": {
        #               "parameter_type_id": "parameter_type_3", "necessary": True, "max_age_seconds": 600}}}}
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

        # The readings remembered for each parameter of each entity, by moment, so that a reading with the same
        # timestamp replaces the old one. The reading is a copy with only the timestamp and the value.
        # Example: {"id_02": {"id_param_03": {datetime(2026, 1, 1, 10, 8, tzinfo=utc):
        #               {"timestamp": "2026-01-01 10:08:00 +0000", "value": 4.2}}}}
        self._remembered: Dict[str, Dict[str, Dict[datetime, Dict[str, Any]]]] = {}

    def handle(self, data: Dict[str, Dict[str, Any]], period_end: datetime) -> Dict[str, Dict[str, Any]]:
        """Remembers the readings of the entities that sent data, completes them, and rebuilds the missing entities.

        Expected Input Structure:
        -------------------------
        data = {
            "id_01": {
                "entity_type_id": "entity_type_1",
                "parameters": {
                    "id_param_01": {
                        "parameter_type_id": "parameter_type_1",
                        "values": [{"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.4}]
                    }
                },
                "timestamp": "2026-01-01 10:15:00 +0000"
            }
        }
        period_end = datetime(2026, 1, 1, 10, 15, 0, tzinfo=timezone.utc)

        Expected Output Structure:
        --------------------------
        The same dictionary, with "values": None in the necessary parameters of the entities that arrived without them,
        and one more entry for each missing entity, in the same structure (see the class docstring for an example).

        Args:
            data (Dict[str, Dict[str, Any]]): The data received in the period, by the id of the entity, one entry for
                each entity that sent data.
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            Dict[str, Dict[str, Any]]: The completed data. It is the same dictionary that was received, changed.
        """
        # Goes through the entities of the schema and separates them in two groups:
        # - the ones that sent data: their readings are remembered and their necessary parameters are completed;
        # - the ones that did not: they are collected in 'missing_ids', to be rebuilt below.
        # The readings are remembered BEFORE rebuilding, so that only data that was really received is remembered, and
        # never the readings created by this approach.
        missing_ids = []
        for entity_id in self._entities:
            if self._has_readings(data.get(entity_id)):
                self._remember_readings(entity_id, data[entity_id], period_end)
                self._complete_entity(entity_id, data[entity_id])
            else:
                missing_ids.append(entity_id)

        for entity_id in missing_ids:
            rebuilt_entity = self._rebuild_entity(entity_id, period_end)

            # --- Guard Clause: nothing to rebuild ---
            # No parameter is necessary and none has remembered readings, so the entity would have no parameters.
            if rebuilt_entity is None:
                continue

            # An entity is missing in one of two situations: it did not arrive at all, or it arrived with no readings
            # (e.g., all its lists 'values' are empty). In both, the rebuilt entity is stored under its id: a new entry
            # is added to the end, or the empty entry that was there is replaced in the same position.
            # Example: data = {"id_01": ..., "id_02": (empty)}  ->  data = {"id_01": ..., "id_02": (rebuilt)}
            data[entity_id] = rebuilt_entity

        return data

    def _read_parameter(self, entity_id: str, parameter_id: str, parameter: Any) -> Dict[str, Any]:
        """Reads, from the schema, what this approach needs to know about a parameter.

        Expected Input Structure:
        -------------------------
        parameter = {
            "parameter_type_id": "parameter_type_3",
            "necessary": True,                                      # (optional, default False)
            "temporal_behavior": {"max_reading_age_seconds": 600}   # (optional)
        }

        Expected Output Structure:
        --------------------------
        {"parameter_type_id": "parameter_type_3", "necessary": True, "max_age_seconds": 600}

        Args:
            entity_id (str): Id of the entity. It is only used in the error messages.
            parameter_id (str): Id of the parameter. It is only used in the error messages.
            parameter (Any): The parameter, as it is in the schema.

        Returns:
            Dict[str, Any]: The type of the parameter, if it is necessary (only True counts as necessary) and how old
                a remembered reading can be, in seconds (None if the parameter has no limit set).

        Raises:
            ValueError: If 'max_reading_age_seconds' exists but is not a number greater than zero.
        """
        parameter = parameter if isinstance(parameter, dict) else {}
        temporal_behavior = parameter.get("temporal_behavior")
        max_age = temporal_behavior.get("max_reading_age_seconds") if isinstance(temporal_behavior, dict) else None

        # --- Guard Clause 2: a limit that is set must be a number greater than zero ---
        # 'bool' is a number for Python (True is 1), so it is refused on its own.
        if max_age is not None and (isinstance(max_age, bool) or not isinstance(max_age, (int, float)) or max_age <= 0):
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: parameter '{parameter_id}' of entity '{entity_id}': "
                f"'max_reading_age_seconds' must be a number greater than zero, but it is {max_age!r}."
            )

        return {
            "parameter_type_id": parameter.get("parameter_type_id"),
            "necessary": parameter.get("necessary") is True,
            "max_age_seconds": max_age
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
        # Example: {"p1": {"values": None}, "p2": {"values": [{"timestamp": "...", "value": 3.4}]}}  ->  True
        #          {"p1": {"values": None}, "p2": {"values": []}}                                  ->  False
        return any(parameter.get("values") for parameter in entity.get("parameters", {}).values())

    def _remember_readings(self, entity_id: str, entity: Dict[str, Any], period_end: datetime) -> None:
        """Keeps the readings of the parameters of an entity that sent data, and forgets the ones that are too old.

        A reading that cannot be placed in time (no timestamp, or a timestamp in the wrong format) or that has no value
        (None) is not remembered.

        Args:
            entity_id (str): Id of the entity.
            entity (Dict[str, Any]): The entity as it arrived.
            period_end (datetime): The end of the period, with a timezone.
        """
        # The readings remembered for this entity, by parameter.
        # 'setdefault' returns the dictionary of the entity if it already exists. If it does not exist (the first time
        # the entity sends data), it creates an empty one, stores it, and returns it.
        remembered_entity = self._remembered.setdefault(entity_id, {})
        schema_parameters = self._entities.get(entity_id, {}).get("parameters", {})

        for parameter_id, parameter in entity.get("parameters", {}).items():
            values = parameter.get("values")

            # A parameter with no readings in this period keeps what was remembered before. Only the parameters that
            # have readings are updated.
            if not values:
                continue

            arrived: Dict[datetime, Dict[str, Any]] = {}
            for reading in values:
                # If the reading has no 'timestamp' (KeyError, or TypeError when the reading is not a dictionary), or
                # the timestamp is not in the expected format (ValueError), it cannot be placed in time, so it is
                # skipped. This way, one bad reading does not stop the others from being remembered.
                try:
                    moment = parse_timestamp(reading["timestamp"])
                    value = reading["value"]
                except (KeyError, TypeError, ValueError):
                    continue
                if value is None:
                    continue
                arrived[moment] = {"timestamp": reading["timestamp"], "value": value}

            # --- Guard Clause: nothing in this list can be remembered ---
            if not arrived:
                continue

            max_age = schema_parameters.get(parameter_id, {}).get("max_age_seconds")
            if max_age is None:
                # No limit set: only the readings of the last period with readings are remembered. They replace the old.
                remembered_entity[parameter_id] = arrived
            else:
                # A limit is set: the new readings are added to the old ones (the same timestamp is replaced).
                remembered_entity.setdefault(parameter_id, {}).update(arrived)

        self._forget_old_readings(entity_id, period_end)

    def _forget_old_readings(self, entity_id: str, period_end: datetime) -> None:
        """Removes the remembered readings that are older than the limit of their parameter.

        A parameter with no 'max_reading_age_seconds' has no limit, so none of its readings is removed here (what it
        remembers is already limited to the readings of one period).

        Args:
            entity_id (str): Id of the entity.
            period_end (datetime): The end of the period, with a timezone.
        """
        schema_parameters = self._entities.get(entity_id, {}).get("parameters", {})

        for parameter_id, readings in self._remembered.get(entity_id, {}).items():
            max_age = schema_parameters.get(parameter_id, {}).get("max_age_seconds")
            if max_age is None:
                continue

            # A reading is too old if it happened before this moment. A reading in the future is never too old.
            # Example: the period ends at 10:15 and the limit is 600 seconds -> cutoff 10:05. A reading of 10:00 is
            # removed, one of 10:08 stays.
            cutoff = period_end - timedelta(seconds=max_age)
            for moment in [moment for moment in readings if moment < cutoff]:
                del readings[moment]

    def _complete_entity(self, entity_id: str, entity: Dict[str, Any]) -> None:
        """Gives "values": None to each necessary parameter that an arrived entity did not send.

        The parameters that arrived are not changed. The parameters that are not necessary are not added.

        Args:
            entity_id (str): Id of the entity.
            entity (Dict[str, Any]): The entity as it arrived. It is changed in place.
        """
        parameters = entity.setdefault("parameters", {})

        for parameter_id, parameter_schema in self._entities[entity_id]["parameters"].items():
            if not parameter_schema["necessary"]:
                continue

            # A necessary parameter that did not arrive (not in the entity) or arrived with no readings gets None.
            # None is the way to say "no value in this period".
            if parameter_id not in parameters:
                parameters[parameter_id] = {"parameter_type_id": parameter_schema["parameter_type_id"], "values": None}
            elif not parameters[parameter_id].get("values"):
                parameters[parameter_id]["values"] = None

    def _rebuild_entity(self, entity_id: str, period_end: datetime) -> Optional[Dict[str, Any]]:
        """Builds the entity that was not received, with all the readings that are remembered.

        Args:
            entity_id (str): Id of the missing entity.
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            Optional[Dict[str, Any]]: The entity in the structure of the message, or None if it would have no
                parameters (none is necessary and none has remembered readings).
        """
        # What the schema says about this entity: its type, and its parameters
        schema = self._entities[entity_id]

        # Readings that are too old must not be used, even if this entity was already missing in the periods before.
        self._forget_old_readings(entity_id, period_end)
        remembered = self._remembered.get(entity_id, {})

        # Builds the parameters of the rebuilt entity, in the structure of the message:
        # - the remembered readings of the parameter, in chronological order, as copies (so that nothing that is done
        #   to the message changes what is remembered);
        # - if the parameter has none and is necessary, None ("no value in this period");
        # - if the parameter has none and is not necessary, it is left out.
        parameters = {}
        without_readings = []
        for parameter_id, parameter_schema in schema["parameters"].items():
            readings = [dict(reading) for _, reading in sorted(remembered.get(parameter_id, {}).items())]

            if readings:
                parameters[parameter_id] = {"parameter_type_id": parameter_schema["parameter_type_id"],
                                            "values": readings}
            elif parameter_schema["necessary"]:
                parameters[parameter_id] = {"parameter_type_id": parameter_schema["parameter_type_id"], "values": None}
                without_readings.append(parameter_id)

        # --- Guard Clause: an entity with no parameters is not worth sending ---
        if not parameters:
            return None

        # The only trace that the entity was rebuilt is this warning in the log. It says what it was rebuilt with and
        # what it could not be rebuilt with, so it does not claim to have rebuilt readings that were not remembered.
        rebuilt_with = [parameter_id for parameter_id in parameters if parameter_id not in without_readings]
        rebuilt_text = f" Rebuilt with the remembered readings of: {', '.join(rebuilt_with)}." if rebuilt_with else ""
        without_text = f" No readings remembered for: {', '.join(without_readings)}." if without_readings else ""
        self._logger.warning(
            f"{self._LOG_PREFIX} Entity '{entity_id}' sent no data in the period that ended at "
            f"{format_timestamp(period_end)}.{rebuilt_text}{without_text}"
        )

        # The rebuilt entity, in the same structure as an entity that was received. The timestamp of the entity is the
        # end of the period. The readings keep their own timestamps.
        return {
            "entity_type_id": schema["entity_type_id"],
            "parameters": parameters,
            "timestamp": format_timestamp(period_end)
        }
