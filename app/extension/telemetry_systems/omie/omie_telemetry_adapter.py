# --- Standard Library Imports ---
import datetime
import json
from typing import Any, Dict, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import duration_to_seconds
from app.extension.utils.labels import PARAMETER_TYPE_EXPECTED_TYPES, Label

# Adaptation strategies registered by entity type id.
_ENTITY_TYPES_STRATEGIES = {}


def register_entity_type_strategy(name: str):
    """Registers an adaptation strategy for an entity type id.

    Expected Input Structure:
    -------------------------
    name = "energy_market"

    Expected Output Structure:
    --------------------------
    A decorator. After `@register_entity_type_strategy("energy_market")` is applied to a function:
    _ENTITY_TYPES_STRATEGIES = {"energy_market": <function _energy_market>}

    Args:
        name (str): Entity type id the strategy handles.

    Returns:
        Callable: A decorator that registers the strategy function.
    """
    def decorator(func):
        _ENTITY_TYPES_STRATEGIES[name] = func
        return func

    return decorator


class OMIETelemetryAdapter(TelemetryAdapterBase):
    """Adapter for OMIE telemetry (the day-ahead spot price of the electricity market).

    Each call to `adapt` handles one envelope, which carries the price of the current 15-minute block and
    of all the next blocks that are known. Unlike the other Telemetry Systems, a reading is not a single
    value: each parameter receives a list of values, one for each block.

    No asset is identified in the envelope. The payload carries the prices of both markets ('pt_spot_price'
    and 'es_spot_price'), and the source mapping says which series (the 'match_key') each parameter wants,
    so the choice between Portugal and Spain is made by the rules, not by the service. Every entity of the
    environment that has such a rule receives its message.
    """

    _entities: dict
    _missing_series_warned: set
    _LOG_PREFIX: str = "OMIE Telemetry Adapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the OMIE adapter.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "EM01": {
                    "entity_type_id": "energy_market",
                    "parameters": {
                        "em01_price": {"parameter_type_id": "energy_price", "measurement_unit": "€/kWh"}
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ------------------------------------------------
        {
            # Parameter id -> series of the payload that holds its prices ('pt_spot_price' or 'es_spot_price').
            # There is no 'fetch_key': the prices are not requested, they are read from the shared service.
            "em01_price": {"match_key": "pt_spot_price", "source_measurement_unit": "€/MWh"}
        }

        Args:
            environment (str): Name of the target environment (e.g., "R-H-03").
            environment_specs (dict): Entity schema for the environment.
            source_mapping_rules (dict): Rules of each parameter, as detailed above.
            logger (LoggingUtils): Logging interface.
            timezone (str): Timezone used to format the timestamps. Defaults to "UTC".

        Raises:
            ValueError: If mandatory parameters are missing or empty.
            TypeError: If input arguments have the wrong type.
        """
        super().__init__(environment, timezone, logger)

        # --- Guard Clause 1: environment_specs must be a dictionary ---
        if not isinstance(environment_specs, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'environment_specs' must be a dictionary "
                f"for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 2: environment_specs must have a non-empty 'entities' dictionary ---
        entities_dict = environment_specs.get("entities")
        if not isinstance(entities_dict, dict) or not entities_dict:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'entities' specification cannot be empty "
                f"for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 3: source_mapping_rules must be a dict ---
        if not isinstance(source_mapping_rules, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'source_mapping_rules' must be a dictionary, "
                f"got {type(source_mapping_rules).__name__} for environment '{environment}'."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        self._entities = entities_dict
        self._source_mapping_rules = source_mapping_rules
        # Checks the units of the parameters and prepares the conversion of the readings to the units of the schema
        self._prepare_unit_conversion(entities_dict, source_mapping_rules)

        # Series of the payload that holds the prices of each parameter
        # Expected `_series_keys` Data Structure:
        # ---------------------------------------
        # {
        #     "em01_price": "pt_spot_price"
        # }
        self._series_keys: Dict[str, str] = {}

        # Parameters that already got a "series not found" warning. The adapter receives one message at every
        # time interval, so without this the same warning would be logged again in every message.
        # Example: after the first warning for 'em01_price' -> {"em01_price"}, and it is not warned about again
        self._missing_series_warned: set = set()

        for entity_id, entity_data in self._entities.items():
            for param_id in entity_data.get("parameters", {}):
                # Rule of this parameter. Example: {"match_key": "pt_spot_price"}
                # A parameter with no rule is not fed by OMIE (it may belong to another Telemetry System)
                rule = source_mapping_rules.get(param_id)
                if rule is None:
                    continue

                # str() accepts numeric values, and strip() removes spaces at the ends, so the key matches the payload's
                # Example: {"match_key": " pt_spot_price "} -> "pt_spot_price"
                match_key = str(rule.get("match_key", "")).strip() if isinstance(rule, dict) else ""
                if not match_key:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): "
                        f"it has no 'match_key' in 'source_mapping_rules'."
                    )
                    continue

                # Example: self._series_keys["em01_price"] = "pt_spot_price"
                self._series_keys[param_id] = match_key

    def adapt(self, message: Union[dict, bytes, str]) -> List[Dict[str, Any]]:
        """Converts an OMIE envelope into canonical entity messages.

        Expected Input Format (`message`):
        ----------------------------------
        {
            "protocol": "Scheduled",
            "source": "omie_spot_price",
            "status": 200,
            "content_type": "application/json",
            "metadata": {},
            "payload": {
                "pt_spot_price": {                              # EUR/MWh, by UTC start of the 15-minute block
                    "2026-10-15T13:30:00+00:00": 84.0,
                    "2026-10-15T13:45:00+00:00": 83.2
                },
                "es_spot_price": {                              # a series that no rule names is ignored
                    "2026-10-15T13:30:00+00:00": 84.0,
                    "2026-10-15T13:45:00+00:00": 83.2
                }
            }
        }

        Expected Output Structure:
        --------------------------
        [
            {
                "id": "EM01",
                "entity_type_id": "energy_market",
                "parameters": {
                    "em01_price": {
                        "parameter_type_id": "energy_price",
                        "values": [
                            {"timestamp": "2026-10-15 13:45:00 +0000", "value": 0.084},   # EUR/kWh
                            {"timestamp": "2026-10-15 14:00:00 +0000", "value": 0.0832}
                        ]
                    }
                },
                "timestamp": "2026-10-15 13:37:42 +0000"
            }
        ]

        Args:
            message (Union[dict, bytes, str]): Raw inbound envelope.

        Returns:
            List[Dict[str, Any]]: One canonical message for each entity that received prices, or an
                empty list if the message could not be processed.
        """
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return []

        # Time of the message, in the adapter timezone. Example (timezone "UTC"): 2026-10-15 13:37:42 +0000
        now_dt = datetime.datetime.now(self._tz)
        timestamp = now_dt.strftime("%Y-%m-%d %H:%M:%S %z")

        # 1. Decode the envelope and keep its payload
        payload = self._normalize_ingress_payload(message)
        if payload is None:
            return []

        messages: List[Dict[str, Any]] = []

        # 2. Every entity of the environment that has prices to receive gets its own message
        # Example: entities = {"EM01": {...}} -> one message for "EM01" (if its parameters have prices)
        for entity_id, entity_data in self._entities.items():
            entity_type_id = entity_data.get("entity_type_id")

            if not entity_type_id:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' has no 'entity_type_id' configured."
                )
                continue

            # 3. Pick the strategy for this entity type. Example: "energy_market" -> self._energy_market
            strategy_func = _ENTITY_TYPES_STRATEGIES.get(entity_type_id)
            if strategy_func is None:
                self._logger.warning(
                    f"{self._LOG_PREFIX} No adaptation strategy registered for entity type '{entity_type_id}'."
                )
                continue

            # 4. Read the prices of each parameter of the entity
            # Example: {"em01_price": {"parameter_type_id": "energy_price", "values": [{...}, {...}]}}
            # An empty result means the payload had no series for this entity: no message is created
            parameters = strategy_func(self, entity_id, payload)
            if not parameters:
                self._logger.debug(f"{self._LOG_PREFIX} No prices found for entity '{entity_id}'.")
                continue

            # 5. Validate the parameters against the schema, and create the message
            # Example: a parameter whose values have the wrong type is dropped by 'parameters_validation'
            entity_parameters = entity_data.get("parameters", {})
            # Converts the readings from the unit of the source to the unit of the schema (see 'source_measurement_unit')
            parameters = self.convert_to_schema_units(parameters)
            parameters = self.parameters_validation(
                parameters, entity_parameters, PARAMETER_TYPE_EXPECTED_TYPES
            )

            messages.append(self._message_creator(parameters, entity_id, entity_type_id, timestamp))

        return messages

    def _normalize_ingress_payload(self, message: Union[dict, bytes, str]) -> Optional[Dict[str, Any]]:
        """Decodes an envelope and returns its payload.

        Envelopes with an error status (not 2xx), or with a payload that is not a valid
        JSON object, are discarded.

        Expected Input Structure (`message`):
        ------------------------------------
        {
            "protocol": "Scheduled",
            "source": "omie_spot_price",
            "status": 200,
            "content_type": "application/json",
            "metadata": {},
            "payload": b'{"pt_spot_price": {"2026-10-15T13:30:00+00:00": 84.0}}'
        }

        Expected Output Structure:
        --------------------------
        {"pt_spot_price": {"2026-10-15T13:30:00+00:00": 84.0}}

        Args:
            message (Union[dict, bytes, str]): Inbound raw envelope.

        Returns:
            Optional[Dict[str, Any]]: The decoded payload. None if the envelope cannot be used.
        """
        if isinstance(message, (bytes, bytearray, str)):
            try:
                message = json.loads(message if isinstance(message, str) else message.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode envelope: {err}")
                return None

        if not isinstance(message, dict):
            return None

        # 1. Discard envelopes with an error status. Example: status 503 -> discarded; 200 -> kept;
        # no status at all -> kept (the check only applies when a number is given)
        status = message.get("status")
        if isinstance(status, int) and not isinstance(status, bool) and not 200 <= status < 300:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope with error status {status}: {message.get('source')}"
            )
            return None

        # 2. Decode the payload. Example: b'{"pt_spot_price": {...}}' or '{"pt_spot_price": {...}}' -> a dict
        payload = message.get("payload")
        if isinstance(payload, (bytes, bytearray, str)):
            try:
                payload = json.loads(payload if isinstance(payload, str) else payload.decode("utf-8"))
            except ValueError as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode payload: {err}")
                return None

        if not isinstance(payload, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope whose payload is not a JSON object: {message.get('source')}"
            )
            return None

        return payload

    def _read_series(self, entity_id: str, payload: dict) -> Dict[str, Dict[str, Any]]:
        """Reads, for each parameter of the entity, the series of prices under the key of its rule.

        OMIE gives each price with the START of the block of time it is valid for, but Percepta gives each reading the
        END of the interval it stands for. So the timestamp of each price is the start of its block plus the length of
        the block, which is the 'periodicity' of the parameter in the environment. Example: with a periodicity of 15
        minutes, the price of the block that starts at 13:30 (valid from 13:30 to 13:45) gets the timestamp 13:45.

        The prices stay in EUR/MWh, as OMIE gives them (the conversion to the unit of the schema is done afterwards,
        in `adapt`), and are sorted by time. A point whose time is not a
        valid date, or whose price is not a number, is skipped (and counted in a single warning for the
        parameter). A parameter whose series is missing, empty or has no valid point is not returned, and neither is
        one with no valid 'periodicity' in the environment (the length of the block would be unknown).
        When the series named by the rule is missing or empty, a warning is logged once for that parameter
        (and again only if the series comes back and fails later), because the cause is usually a wrong 'match_key'.

        Expected Input (`payload`):
        ---------------------------
        {"pt_spot_price": {"2026-10-15T13:45:00+00:00": 83.2, "2026-10-15T13:30:00+00:00": 84.0}}

        Expected Output (the parameter has a periodicity of 15 minutes):
        ----------------------------------------------------------------
        {
            "em01_price": {
                "parameter_type_id": "energy_price",
                "values": [
                    {"timestamp": "2026-10-15 13:45:00 +0000", "value": 84.0},     # EUR/MWh
                    {"timestamp": "2026-10-15 14:00:00 +0000", "value": 83.2}
                ]
            }
        }

        Args:
            entity_id (str): Id of the entity.
            payload (dict): Decoded payload.

        Returns:
            Dict[str, Dict[str, Any]]: The parameters of the entity that received prices.
        """
        parameters: Dict[str, Dict[str, Any]] = {}

        for param_id, param_spec in self._entities.get(entity_id, {}).get("parameters", {}).items():
            # Name of the series this parameter wants. Example: "em01_price" -> "pt_spot_price"
            series_key = self._series_keys.get(param_id)

            # The prices of that series. Example: {"2026-10-15T13:30:00+00:00": 84.0, ...}
            # None when the parameter has no rule, or when the payload does not have the series
            series = payload.get(series_key) if series_key is not None else None

            # --- Guard Clause: the series of the rule must be in the payload ---
            # Nothing in the message matches the 'match_key' of the rule. The usual cause is a wrong 'match_key'
            # in the source mapping, which would otherwise fail silently (no prices, and no error).
            # Example: match_key = "pt_spot_prise" (typo), payload series = ["pt_spot_price", "es_spot_price"]
            if series_key is not None and (not isinstance(series, dict) or not series):
                if param_id not in self._missing_series_warned:
                    self._missing_series_warned.add(param_id)
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): "
                        f"no prices found, the payload has no usable series '{series_key}'. Series received: {sorted(payload)}. "
                        f"Check the 'match_key' of this parameter in the source mapping."
                    )
                continue

            # The series is there again, so a new failure in the future will be warned about again
            self._missing_series_warned.discard(param_id)

            # A parameter with no rule has no series, and is simply not fed by OMIE
            if series is None:
                continue

            parameter_type_id = param_spec.get("parameter_type_id")
            if not parameter_type_id:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): it has no "
                    f"'parameter_type_id' configured. Skipping its prices."
                )
                continue

            # --- Guard Clause: the parameter must have a valid periodicity ---
            # The length of the block is needed to turn its start into its end (see the docstring).
            try:
                block_seconds = duration_to_seconds(
                    (param_spec.get("temporal_behavior") or {}).get("periodicity"), "periodicity"
                )
            except ValueError as err:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): it has no valid periodicity in "
                    f"the environment ({err}). Skipping its prices."
                )
                continue
            block_length = datetime.timedelta(seconds=block_seconds)

            # Valid prices as (block start, EUR/MWh) and how many invalid ones were skipped
            # Example: points = [(datetime(2026, 10, 15, 13, 30, tzinfo=UTC), 84.0)], skipped = 0
            points = []
            skipped = 0

            for raw_time, raw_price in series.items():
                block_start = self._parse_date(raw_time)

                # bool is a subclass of int, so it is excluded explicitly
                if block_start is None or isinstance(raw_price, bool) or not isinstance(raw_price, (int, float)):
                    skipped += 1
                    continue

                # The price stays in the unit of OMIE (EUR/MWh). The conversion to the unit of the schema (EUR/kWh) is
                # done afterwards for all the adapters, with the 'source_measurement_unit' of the source mapping.
                points.append((block_start, raw_price))

            if skipped:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' (parameter type: {self._parameter_type(entity_id, param_id)}): "
                    f"skipped {skipped} invalid price(s)."
                )

            if not points:
                continue

            # Sorted by time, so the order does not depend on the order of the payload
            # Example: [13:45, 13:30] -> [13:30, 13:45]
            points.sort(key=lambda point: point[0])

            parameters[param_id] = {
                "parameter_type_id": parameter_type_id,
                "values": [
                    # The END of the block, in the adapter timezone. Example (periodicity of 15 minutes, timezone
                    # "Europe/Lisbon", summer time): the block that starts at 2026-10-15T13:30:00+00:00 ends at
                    # 13:45 UTC, which is "2026-10-15 14:45:00 +0100"
                    {
                        "timestamp": (block_start + block_length).astimezone(self._tz).strftime("%Y-%m-%d %H:%M:%S %z"),
                        "value": price
                    }
                    for block_start, price in points
                ]
            }

        return parameters

    @register_entity_type_strategy(Label.ENERGY_MARKET.value)
    def _energy_market(self, entity_id: str, payload: dict) -> Dict[str, Dict[str, Any]]:
        """Reads the prices of an energy market entity from the payload.

        Expected Input (`payload`):
        ---------------------------
        {"pt_spot_price": {"2026-10-15T13:30:00+00:00": 84.0}}

        Expected Output (the parameter has a periodicity of 15 minutes, so the block that starts at 13:30 ends at 13:45):
        -------------------------------------------------------------------------------------------------------------
        {
            "em01_price": {
                "parameter_type_id": "energy_price",
                "values": [{"timestamp": "2026-10-15 13:45:00 +0000", "value": 84.0}]      # EUR/MWh
            }
        }

        Args:
            entity_id (str): Id of the energy market entity.
            payload (dict): Decoded payload.

        Returns:
            Dict[str, Dict[str, Any]]: The parameters of the entity that received prices.
        """
        return self._read_series(entity_id, payload)

    @staticmethod
    def _parse_date(raw_date: Any) -> Optional[datetime.datetime]:  # TODO passar isto para um utils
        """Parses a date into a timezone-aware datetime.

        Accepts a datetime, or an ISO 8601 string with or without a timezone. A date with no
        timezone is assumed to be UTC.

        Expected Input Structure:
        -------------------------
        "2026-10-15T13:30:00+00:00"        # also accepted: "2026-10-15T13:30:00Z", "2026-10-15T13:30:00" (UTC)
                                           # or a datetime object

        Expected Output Structure:
        --------------------------
        datetime.datetime(2026, 10, 15, 13, 30, tzinfo=datetime.timezone.utc)
        None                               # when it is not a valid date. Example: "not a date"

        Args:
            raw_date (Any): The date, as received.

        Returns:
            Optional[datetime.datetime]: The parsed datetime, or None if it is not a valid date.
        """
        if isinstance(raw_date, datetime.datetime):
            dt = raw_date
        else:
            try:
                dt = datetime.datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
            except ValueError:
                return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt