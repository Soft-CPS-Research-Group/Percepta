# --- Standard Library Imports ---
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.integrated_logic.combination_logic_base import CombinationLogicBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.type_compatibility import is_type_compatible
from app.core.utils.time_utils import duration_to_seconds, format_timestamp, parse_timestamp
from app.extension.utils.labels import PARAMETER_TYPE_EXPECTED_TYPES, AggregationLabel, Label


class EnergAIzeCombinationLogic(CombinationLogicBase):
    """Combination Logic of the EnergAIze instance.

    It takes the data of the period (already treated by the Data-Issue Handling Approaches) and builds the message
    that the EnergAIze algorithm expects: ONE value (or one series) for each parameter of each entity, together with
    the information that the algorithm needs to interpret it (unit, how the value was aggregated, when it starts and
    how often it repeats).

    How a parameter is reduced to the value of the period depends on its definition in the environment:

        - 'aggregation_operation' is "sum"                -> the sum of the readings of the period
        - 'aggregation_operation' is "average"            -> the average of the readings of the period
        - 'aggregation_operation' is "last_value"  -> the most recent reading of the period
        - no 'temporal_behavior'                          -> the most recent reading of the period, as
                                                             "last_value"
        - 'temporal_behavior' with a null                 -> all the readings of the period, as a series, and the
          'aggregation_operation'                            'aggregation_method' of the message is null

    Only the entities whose type is one of the `Label` of the extension (app/extension/utils/labels.py) go to the
    message. The other entities of the environment (if any) are ignored.
    """

    # The aggregation of the parameters that have no 'temporal_behavior' at all
    _DEFAULT_AGGREGATION: AggregationLabel = AggregationLabel.LAST_VALUE

    def __init__(self, environment: Dict[str, Any], settings: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the logic.

        Expected Input Structure:
        -------------------------
        environment = {
            "group": "group_1",
            "entities": {
                "PV01": {
                    "entity_type_id": "pv_panel",
                    "parameters": {
                        "33711": {
                            "parameter_type_id": "energy",
                            "measurement_unit": "kWh",
                            "temporal_behavior": {
                                "cumulative": True,
                                "periodicity": {"value": 5, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "necessary": True
                        }
                    }
                }
            },
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }
        settings = {
            "timezone": "UTC",                  # The timezone of the periods
            "frequency_seconds": 15,            # The length of a period, in seconds
            "grace_period_seconds": 2           # Wait after the end of a period before processing it
        }

        Args:
            environment (Dict[str, Any]): The environment this logic works for, as returned by the Schema Storage.
            settings (Dict[str, Any]): The settings of the Data Processing container. This logic needs 'frequency_seconds', the length of the period.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If the environment has no 'entities', or if 'frequency_seconds' is missing or not positive.
        """
        super().__init__(environment, settings, logger)

        # Guard Clause 1: Checks that the environment has the 'entities' dictionary.
        if not isinstance(environment.get("entities"), dict):
            raise ValueError("The environment must have an 'entities' dictionary.")

        # Guard Clause 2: Checks that 'frequency_seconds' is a positive number.
        frequency_seconds = settings.get("frequency_seconds")
        if isinstance(frequency_seconds, bool) or not isinstance(frequency_seconds, (int, float)) or frequency_seconds <= 0:
            raise ValueError(f"'frequency_seconds' must be a positive number, and it is {frequency_seconds!r}.")

        self._frequency_seconds: float = frequency_seconds
        self._entities_schema: Dict[str, Any] = environment["entities"]

    def combine(self, data: List[Dict[str, Any]], period_end: datetime) -> Dict[str, Any]:
        """Combines the data of a period into the message of the EnergAIze algorithm.

        Expected Input Structure:
        -------------------------
        data = [
            {
                "id": "PV01",
                "entity_type_id": "pv_panel",
                "generated": False,                                      # Optional. True if the entity was synthesised (default False)
                "parameters": {
                    "33711": {
                        "parameter_type_id": "energy",
                        "values": [
                            {"timestamp": "2026-10-13 16:35:35 +0000", "value": 0.05},
                            {"timestamp": "2026-10-13 16:35:40 +0000", "value": 0.07}
                        ]
                    }
                },
                "timestamp": "2026-10-13 16:35:41 +0000"
            }
        ]
        period_end = datetime(2026, 10, 13, 16, 35, 45, tzinfo=timezone.utc)

        Expected Output Structure:
        --------------------------
        For the input above, if the environment defines the parameter "33711" with the unit "kWh" and the
        'aggregation_operation' "sum", and 'frequency_seconds' is 15, the two readings (0.05 and 0.07) become 0.12:
        {
            "timestamp": "2026-10-13 16:35:45 +0000",
            "entities": [
                {
                    "id": "PV01",
                    "entity_type_id": "pv_panel",
                    "parameters": {
                        "33711": {
                            "parameter_type_id": "energy",
                            "values": [0.12],
                            "measurement_unit": "kWh",
                            "aggregation_method": "sum",
                            "start_date": "2026-10-13 16:35:30 +0000",
                            "frequency_seconds": 15,
                            "generated": False
                        }
                    }
                }
            ]
        }

        Args:
            data (List[Dict[str, Any]]): The data of the period, one dictionary for each entity.
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            Dict[str, Any]: The message of the period.

        Raises:
            ValueError: If 'period_end' has no timezone.
        """
        # Guard Clause 1: Checks that 'period_end' has a timezone.
        if period_end.tzinfo is None:
            raise ValueError("'period_end' must have a timezone.")

        period_start: datetime = period_end - timedelta(seconds=self._frequency_seconds)

        entities: List[Dict[str, Any]] = []
        for entity in data:
            entity_id = entity.get("id")
            entity_type_id = entity.get("entity_type_id")

            # Guard Clause 2: Ignores the entities whose type is not one of the `Label` values.
            if entity_type_id not in {label.value for label in Label}:
                self._logger.debug(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' ignored: type '{entity_type_id}' is not a known type."
                )
                continue

            # Guard Clause 3: Ignores the entities that are not in the environment.
            entity_schema = self._entities_schema.get(entity_id)
            if entity_schema is None:
                self._logger.warning(f"{self._LOG_PREFIX} Entity '{entity_id}' ignored: it is not in the environment.")
                continue

            generated: bool = bool(entity.get("generated", False))

            parameters: Dict[str, Any] = {}
            for parameter_id, parameter in entity.get("parameters", {}).items():
                # Guard Clause 4: Ignores the parameters that are not in the environment.
                parameter_schema = entity_schema.get("parameters", {}).get(parameter_id)
                if parameter_schema is None:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{parameter_id}' of entity '{entity_id}' (parameter type: "
                        f"{parameter.get('parameter_type_id')}): ignored, it is not in the environment."
                    )
                    continue

                parameters[parameter_id] = self._build_parameter(
                    entity_id, parameter_id, parameter, parameter_schema, period_start, generated
                )

            entities.append({"id": entity_id, "entity_type_id": entity_type_id, "parameters": parameters})

        return {"timestamp": format_timestamp(period_end), "entities": entities}

    def _build_parameter(
            self, entity_id: str, parameter_id: str, parameter: Dict[str, Any], parameter_schema: Dict[str, Any],
            period_start: datetime, generated: bool
    ) -> Dict[str, Any]:
        """Builds one parameter of the message.

        Expected Input Structure:
        -------------------------
        parameter = {
            "parameter_type_id": "power",
            "values": [{"timestamp": "2026-10-13 16:35:35 +0000", "value": 2.0}, ...]
        }
        parameter_schema = {
            "parameter_type_id": "power",
            "measurement_unit": "kW",
            "temporal_behavior": {"cumulative": False, "periodicity": {"value": 5, "unit": "min"},
                                  "aggregation_operation": "average", "fill_operation": "linear"}
        }

        Expected Output Structure:
        --------------------------
        {
            "parameter_type_id": "power",
            "values": [2.5],
            "measurement_unit": "kW",
            "aggregation_method": "average",
            "start_date": "2026-10-13 16:35:30 +0000",
            "frequency_seconds": 15,
            "generated": False
        }

        Args:
            entity_id (str): Id of the entity of the parameter (only used in the logs).
            parameter_id (str): Id of the parameter (only used in the logs).
            parameter (Dict[str, Any]): The parameter as it arrives from the Data-Issue Handling Approaches.
            parameter_schema (Dict[str, Any]): The parameter in the environment.
            period_start (datetime): The start of the period.
            generated (bool): If the entity of the parameter was synthesised.

        Returns:
            Dict[str, Any]: The parameter of the message.
        """
        # The readings in time order, because the approaches do not promise it. A reading without a valid timestamp
        # cannot be placed in time, so it is ignored (and logged) instead of losing the whole period.
        timed_readings: List[tuple] = []
        for reading in parameter.get("values") or []:
            try:
                timed_readings.append((parse_timestamp(reading["timestamp"]), reading))
            except (ValueError, KeyError, TypeError):
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{parameter_id}' of entity '{entity_id}' (parameter type: "
                    f"{parameter_schema.get('parameter_type_id')}): reading ignored, it has no valid timestamp."
                )
        readings: List[Dict[str, Any]] = [
            reading for _, reading in sorted(timed_readings, key=lambda timed_reading: timed_reading[0])
        ]
        raw_values: List[Any] = self._valid_values(
            entity_id, parameter_id, parameter_schema.get("parameter_type_id"),
            [reading.get("value") for reading in readings]
        )

        temporal_behavior: Optional[Dict[str, Any]] = parameter_schema.get("temporal_behavior")
        frequency_seconds: float = self._frequency_seconds

        if not temporal_behavior:
            # No temporal behavior: the parameter is a state, so the period is represented by the latest state.
            aggregation: Optional[AggregationLabel] = self._DEFAULT_AGGREGATION
            values: List[Any] = [raw_values[-1] if raw_values else None]
        else:
            operation_id: Optional[str] = temporal_behavior.get("aggregation_operation")
            if operation_id is None:
                # A series (e.g., the prices of the day ahead): it is not reduced, and it repeats with the
                # periodicity of the parameter, not with the one of the period.
                aggregation = None
                values = raw_values
                frequency_seconds = duration_to_seconds(temporal_behavior["periodicity"], "periodicity")
            else:
                # Guard Clause: Checks that the 'aggregation_operation' is one of the `AggregationLabel` values.
                try:
                    aggregation = AggregationLabel(operation_id)
                except ValueError:
                    raise ValueError(f"Unknown aggregation_operation '{operation_id}'.") from None
                values = [self._reduce(aggregation, raw_values)]

        result: Dict[str, Any] = {"parameter_type_id": parameter_schema.get("parameter_type_id"), "values": values}
        if "measurement_unit" in parameter_schema:
            result["measurement_unit"] = parameter_schema["measurement_unit"]
        result["aggregation_method"] = aggregation.value if aggregation else None
        result["start_date"] = format_timestamp(period_start)
        result["frequency_seconds"] = int(frequency_seconds) if float(frequency_seconds).is_integer() \
            else frequency_seconds
        result["generated"] = generated
        return result

    def _valid_values(self, entity_id: str, parameter_id: str, parameter_type_id: Optional[str],
                      values: List[Any]) -> List[Any]:
        """Keeps only the values whose type is the one that the Label expects for the parameter type.

        The expected type of each parameter type is in `PARAMETER_TYPE_EXPECTED_TYPES` (app/extension/utils/labels.py).
        A value that is None is not an error, it only means that there is no reading, so it is dropped silently. A
        value with another type (e.g., the text "abc" for a power) is dropped with a warning, so that it never reaches
        the sum or the average. A number that arrives as text (e.g., "12.5") is accepted and turned into a float, the
        same way `is_type_compatible` accepts it. A parameter type that is not in the mapping (e.g., 'flexibility',
        which is a dictionary) is not validated.

        Examples (for a 'power', which expects a float):
        ------------------------------------------------
        [2.0, "3.5", None, "abc"]  -> [2.0, 3.5]          # "abc" is dropped with a warning, None silently

        Examples (for an 'electric_vehicle', which expects a str):
        ----------------------------------------------------------
        ["2", None, 3]             -> ["2"]               # 3 is dropped with a warning

        Args:
            entity_id (str): Id of the entity (only used in the logs).
            parameter_id (str): Id of the parameter (only used in the logs).
            parameter_type_id (Optional[str]): The type of the parameter, e.g., "power".
            values (List[Any]): The values of the readings, in time order.

        Returns:
            List[Any]: The valid values, in the same order.
        """
        expected_type = PARAMETER_TYPE_EXPECTED_TYPES.get(parameter_type_id)
        if expected_type is None:
            return values

        valid_values: List[Any] = []
        for value in values:
            if value is None:
                continue
            if not is_type_compatible(value, expected_type):
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{parameter_id}' of entity '{entity_id}' "
                    f"(parameter type: {parameter_type_id}): value {value!r} dropped, it expects {expected_type.__name__}."
                )
                continue
            valid_values.append(float(value) if expected_type in (float, int) else value)
        return valid_values

    @staticmethod
    def _reduce(aggregation: AggregationLabel, raw_values: List[Any]) -> Any:
        """Reduces the readings of a period to one value.

        Only the numbers count. A reading that is None (or not a number) is skipped, and if none is left the result
        is None, because there is nothing to sum or to average.

        Examples:
        ---------
        (SUM,  [0.05, 0.07])      -> 0.12
        (AVERAGE, [2.0, 3.0, None])  -> 2.5
        (LAST_VALUE, [1, 2, 3])         -> 3
        (LAST_VALUE, [1, None])         -> None      # The latest reading is the one that counts, even if it is empty
        (SUM,  [])                -> None

        Args:
            aggregation (AggregationLabel): SUM, AVERAGE or LAST_VALUE.
            raw_values (List[Any]): The values of the readings, in time order.

        Returns:
            Any: The value of the period.
        """
        if aggregation == AggregationLabel.LAST_VALUE:
            return raw_values[-1] if raw_values else None

        numbers = [value for value in raw_values if isinstance(value, (int, float)) and not isinstance(value, bool)]
        if not numbers:
            return None
        total = sum(numbers)
        return total if aggregation == AggregationLabel.SUM else total / len(numbers)
