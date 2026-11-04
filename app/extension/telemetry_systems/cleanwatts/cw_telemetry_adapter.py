# --- Standard Library Imports ---
import datetime
import json
import re
from typing import Any, Dict, List, Optional, Tuple

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import PARAMETER_TYPE_EXPECTED_TYPES, Label, ParamLabel

# Matches the Cleanwatts tag id in a request URL, e.g. "...&tags=35927&...".
TAGS_PATTERN = re.compile(r"tags=(\d+)")

# How long an EV charger session is kept "active" after the last heartbeat,
# if no new one arrives in the meantime.
SESSION_GRACE_PERIOD = datetime.timedelta(minutes=15)

# Adaptation strategies registered by entity type id.
_LABEL_STRATEGIES = {}


def register_label_strategy(name: str):
    """Registers an adaptation strategy for an entity type id.

    Args:
        name (str): Entity type id the strategy handles.

    Returns:
        Callable: A decorator that registers the strategy function.
    """
    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class CWTelemetryAdapter(TelemetryAdapterBase):
    """Adapter for Cleanwatts telemetry data.

    Each call to `adapt` handles a single parameter's reading, received from one
    HTTP endpoint, and converts it into a canonical entity message.
    """

    _entities: dict
    _LOG_PREFIX: str = "CW Telemetry Adapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the Cleanwatts adapter.

        Expected `environment_specs` Data Structure:
        -------------------------------------------
        {
            "group": "living lab",
            "entities": {
                "EVC01": {
                    "entity_type_id": "ev_charger",
                    "parameters": {
                        "37507": {
                            "parameter_type_id": "session_status",
                            "measurement_unit": "Status",
                            "necessary": true
                        },
                        "41836": {
                            "parameter_type_id": "power",
                            "measurement_unit": "kW",
                            "temporal_behavior": {
                                "cumulative": false,
                                "periodicity": {"value": 5, "unit": "min"},
                                "aggregation_operation": "average",
                                "fill_operation": "linear"
                            },
                            "necessary": true
                        },
                        "connected_vehicle": {
                            "parameter_type_id": "electric_vehicle",
                            "vehicle_id": "EV01",
                            "necessary": true
                        }
                    },
                    "actuators": {
                        "act_power_2": {
                            "type": "Actuation",
                            "measurement_unit": "kW",
                            "actuator_type_id": "power"
                        }
                    }
                }
            }
        }

        Expected `source_mapping_rules` Data Structure:
        ------------------------------------------------
        {
            "37507": {"fetch_key": "37507", "match_key": "37507"},
            "41836": {"fetch_key": "41836", "match_key": "41836"},
            "connected_vehicle": None                # Derived by this adapter, so it has no keys
        }

        The 'match_key' is the Cleanwatts tag id that appears in the request URL of the
        envelope, which is how a reading is matched to its parameter. The 'fetch_key' is
        only used to build the requests, so it is not used here.

        Args:
            environment (str): Name of the target environment (e.g., "R-H-01").
            environment_specs (dict): Entity schema for the environment.
            source_mapping_rules (dict): For each parameter id, its keys, or None if the parameter is derived.
            logger (LoggingUtils): Logging interface.
            timezone (str): Timezone used to timestamp readings. Defaults to "UTC".

        Raises:
            ValueError: If mandatory parameters are missing or empty.
            TypeError: If input arguments have the wrong type.
        """
        super().__init__(environment, timezone, logger)

        # --- Guard Clause: environment_specs must have an 'entities' key ---
        if not isinstance(environment_specs, dict) or "entities" not in environment_specs:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'environment_specs' must be a dictionary "
                f"containing an 'entities' key for environment '{environment}'."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        entities_dict = environment_specs.get("entities")
        if not isinstance(entities_dict, dict) or not entities_dict:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'entities' specification cannot be empty "
                f"for environment '{environment}'."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause: source_mapping_rules must be a dict ---
        if not isinstance(source_mapping_rules, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'source_mapping_rules' must be a dictionary, "
                f"got {type(source_mapping_rules).__name__}."
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

        self._entities = entities_dict
        self._source_mapping_rules = source_mapping_rules

        # Tracks, per EV charger entity, the last time an active charging
        # session was reported, used to bridge short gaps in telemetry.
        # Expected `_last_session_status` Data Structure:
        # -------------------------------------------------
        # {
        #     "EVC01": datetime(...)
        # }
        self._last_session_status: Dict[str, datetime.datetime] = {}

        # Reverse lookup: Cleanwatts tag id -> (entity_id, param_id)
        # Expected `_tag_to_param` Data Structure:
        # ----------------------------------------
        # {
        #     "37507": ("EVC01", "37507"),
        #     "41836": ("EVC01", "41836")
        # }
        self._tag_to_param: Dict[str, Tuple[str, str]] = {}
        for param_id, rule in source_mapping_rules.items():
            # A derived parameter has no tag to be matched with
            if rule is None:
                continue

            str_tag = str(rule["match_key"])
            # Find which entity this parameter belongs to
            for ent_id, ent_info in self._entities.items():
                if param_id in ent_info.get("parameters", {}):
                    self._tag_to_param[str_tag] = (ent_id, param_id)
                    break
            else:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{param_id}' in 'source_mapping_rules' "
                    f"does not belong to any entity."
                )

    def adapt(self, message: dict) -> List[Dict[str, Any]]:
        """Converts a single incoming Cleanwatts envelope into a canonical entity message.

        Each call handles the reading of one parameter, received from one HTTP
        endpoint. The entity and parameter it belongs to are resolved from the
        Cleanwatts tag id present in the request URL.

        Expected Input Format (`message`):
        -----------------------------------
        {
            "protocol": "HTTP",
            "source": "https://innov.cleanwattsdigital.com/api/2.0/data/request/Instant?instantType=avg&tags=35927",
            "status": 200,
            "content_type": "application/json",
            "metadata": {},
            "payload": [{"Date": "2026-08-19T10:00:00Z", "Read": 12.5}]
        }

        Expected Output Structure:
        --------------------------
        [
            {
                "id": "B01",
                "entity_type_id": "battery",
                "parameters": {
                    "35927": {
                        "parameter_type_id": "energy_in",
                        "values": [
                            {"timestamp": "2026-08-19 10:00:00 +0000", "value": 12.5}
                        ]
                    }
                },
                "timestamp": "2026-08-19 10:00:31 +0000"
            }
        ]

        Args:
            message (dict): Raw inbound envelope from the Input Handler, for a single endpoint.

        Returns:
            List[Dict[str, Any]]: A single standardized canonical entity message, or an
                empty list if the message could not be processed.
        """
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return []

        now_dt = datetime.datetime.now(self._tz)
        timestamp = now_dt.strftime("%Y-%m-%d %H:%M:%S %z")

        # 1. Resolve which entity and parameter this message belongs to
        resolved = self._normalize_ingress_payload(message)
        if not resolved:
            return []

        entity_id = resolved["id"]
        entity_type_id = resolved["entity_type_id"]
        param_id = resolved["param_id"]
        parameter_type_id = resolved["parameter_type_id"]
        raw_values = resolved["values"]

        if not entity_type_id:
            self._logger.warning(
                f"{self._LOG_PREFIX} Entity '{entity_id}' has no 'entity_type_id' configured."
            )
            return []

        entity_parameters = self._entities.get(entity_id, {}).get("parameters", {})

        # 2. Pick the strategy for this entity type, if any
        if entity_type_id in _LABEL_STRATEGIES:
            strategy_func = _LABEL_STRATEGIES[entity_type_id]
            parameters = strategy_func(
                self, entity_id, entity_parameters, param_id, parameter_type_id, raw_values, now_dt
            )
        else:
            # No specific strategy: just normalize this parameter's readings
            parameters = {
                param_id: self._build_parameter(entity_id, param_id, parameter_type_id, raw_values)
            }

        # Nothing to report
        if not parameters:
            return []

        # 3. Validate the received parameter's readings
        parameters = self.parameters_validation(
            parameters, entity_parameters, PARAMETER_TYPE_EXPECTED_TYPES
        )

        return [self._message_creator(parameters, entity_id, entity_type_id, timestamp)]

    def _normalize_ingress_payload(self, message: dict) -> Optional[Dict[str, Any]]:
        """Resolves the entity and parameter a single envelope belongs to, from the
        Cleanwatts tag id present in its request URL.

        Envelopes with an error status (not 2xx), or with a payload that is not
        valid JSON, are discarded.

        Expected Input Structure (`message`):
        ------------------------------------
        {
            "protocol": "HTTP",
            "source": "/data/request/Instant?instantType=avg&tags=35927&from=...&to=...",
            "status": 200,
            "content_type": "application/json",
            "metadata": {},
            "payload": [{"Date": "2026-08-17T10:00:00Z", "Read": 22.4}]
        }

        Expected Output Structure:
        --------------------------
        {
            "id": "B01",
            "entity_type_id": "battery",
            "param_id": "35927",
            "parameter_type_id": "energy_in",
            "values": [{"Date": "2026-08-17T10:00:00Z", "Read": 22.4}]
        }

        Args:
            message (dict): Inbound raw envelope, for a single endpoint.

        Returns:
            Optional[Dict[str, Any]]: Entity and parameter metadata, with the raw,
                not yet normalized, readings. None if the envelope has an error
                status, an invalid payload, or a tag that could not be resolved to
                a known entity and parameter.
        """
        if not isinstance(message, dict):
            return None

        status = message.get("status")
        if status is not None and not 200 <= status < 300:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope with error status {status}: {message.get('source')}"
            )
            return None

        source_url = message.get("source", "")
        entity_id, param_id = self._resolve_param_from_url(source_url)

        if not entity_id or not param_id:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope with unresolvable tag in URL: {source_url}"
            )
            return None

        entity_spec = self._entities.get(entity_id, {})
        entity_type_id = entity_spec.get("entity_type_id")
        param_spec = entity_spec.get("parameters", {}).get(param_id, {})
        parameter_type_id = param_spec.get("parameter_type_id", param_id)

        raw_payload = message.get("payload")
        try:
            values = json.loads(raw_payload) if isinstance(raw_payload, (str, bytes, bytearray)) else raw_payload
        except ValueError:
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping envelope with a payload that is not valid JSON: {source_url}"
            )
            return None

        return {
            "id": entity_id,
            "entity_type_id": entity_type_id,
            "param_id": param_id,
            "parameter_type_id": parameter_type_id,
            "values": values
        }

    def _build_values_list(self, parameter_values_list: list) -> list:
        """Converts raw timeseries readings into standardized timestamp-value records.

        Readings that cannot be used are discarded with a warning: readings with an
        invalid or missing date, and readings with no value or a null value.

        Expected Input Data Format (`parameter_values_list`):
        -----------------------------------------------------
        [
            {
                "Date": "2026-08-17T10:00:00Z",  # (str) ISO 8601 or UTC datetime string
                "Read": 22.4                     # (float/int) Measurement value under 'Read' key
            },
            {
                "Date": "2026-08-17 10:15:00",   # (str) Timezone-naive datetime string (assumed UTC)
                "Value": 21.8                    # (float/int) Measurement value under 'Value' key
            }
        ]

        Standardized Output Format (with timezone "UTC"):
        --------------------------------------------------
        [
            {
                "timestamp": "2026-08-17 10:00:00 +0000",  # Localized to self._tz
                "value": 22.4
            },
            {
                "timestamp": "2026-08-17 10:15:00 +0000",
                "value": 21.8
            }
        ]

        Args:
            parameter_values_list (list): Raw readings to convert.

        Returns:
            list: Normalized 'timestamp'/'value' records.
        """
        if not parameter_values_list:
            return []

        result = []
        for item in parameter_values_list:
            if not isinstance(item, dict):
                continue

            # 1. Get the value, discarding readings that have none
            raw_val = item.get("Value")
            if raw_val is None:
                raw_val = item.get("Read")
            if raw_val is None:
                self._logger.warning(f"{self._LOG_PREFIX} Skipping reading with no value: {item}")
                continue

            # 2. Parse and localize the timestamp
            raw_date = item.get("Date")
            if raw_date is None:
                raw_date = item.get("DateUTC")
            if raw_date is None:
                self._logger.warning(f"{self._LOG_PREFIX} Skipping reading with no date: {item}")
                continue

            dt = self._parse_date(raw_date)
            if dt is None:
                # Skip readings with an invalid date, as they cannot be placed in time
                self._logger.warning(
                    f"{self._LOG_PREFIX} Skipping reading with invalid date '{raw_date}'."
                )
                continue

            formatted_ts = dt.astimezone(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

            result.append({
                "timestamp": formatted_ts,
                "value": raw_val
            })

        return result

    def _build_parameter(
            self,
            entity_id: str,
            param_id: str,
            parameter_type_id: str,
            raw_values: Any
    ) -> Dict[str, Any]:
        """Builds the canonical entry for a single parameter from its raw readings.

        If no readings were received (nothing, or an empty list), 'values' is an
        empty list, and parameters_validation reports it as empty. If something was
        received but it cannot be used, the parameter is flagged here as malformed.
        That covers a payload that is not a list of readings, and readings that are
        all discarded (for example, all have invalid dates). Readings that are
        individually invalid are discarded, and the valid ones are kept.

        Args:
            entity_id (str): Id of the entity the parameter belongs to.
            param_id (str): Id of the parameter.
            parameter_type_id (str): Type id of the parameter.
            raw_values (Any): The raw readings received for this parameter.

        Returns:
            Dict[str, Any]: The parameter entry, with its readings or flagged as malformed.
        """
        # Nothing was received: no readings for this parameter
        if raw_values is None or raw_values == []:
            self._logger.warning(
                f"{self._LOG_PREFIX} Parameter '{param_id}' of entity '{entity_id}' has no readings."
            )
            return {"parameter_type_id": parameter_type_id, "values": []}

        # Something was received, but it is not a list of readings
        if not isinstance(raw_values, list):
            self._logger.warning(
                f"{self._LOG_PREFIX} Readings of parameter '{param_id}' of entity '{entity_id}' "
                f"are not a list: {type(raw_values).__name__}."
            )
            return {
                "parameter_type_id": parameter_type_id,
                "values": None,
                "metadata": "Parameter readings malformed"
            }

        values = self._build_values_list(raw_values)
        if not values:
            self._logger.warning(
                f"{self._LOG_PREFIX} All readings of parameter '{param_id}' of entity '{entity_id}' are malformed."
            )
            return {
                "parameter_type_id": parameter_type_id,
                "values": None,
                "metadata": "Parameter readings malformed"
            }

        return {"parameter_type_id": parameter_type_id, "values": values}

    @register_label_strategy(Label.ELECTRIC_VEHICLE_CHARGER.value)
    def _ev_charger(
            self,
            entity_id: str,
            entity_parameters: dict,
            param_id: str,
            parameter_type_id: str,
            raw_values: Any,
            now_dt: datetime.datetime
    ) -> dict:
        """Processes a single reading for an EV charger entity.

        If the reading is for session_status, this updates the charger's session
        state and reports the id of the vehicle configured for this charger while
        the session is active. The session_status itself is not part of the output.
        Otherwise, the reading is just normalized like any other parameter.

        Expected Input (session_status reading):
        -----------------------------------------
        parameter_type_id = "session_status"
        raw_values = [
            {
                "Read": 1,                       # 1 for active charging, 0 for idle
                "DateUTC": "2026-08-17 10:00:00" # Time of the session heartbeat ('Date' is also accepted)
            }
        ]

        Expected Output (session_status reading):
        -------------------------------------------
        {
            "connected_vehicle": {
                "parameter_type_id": "electric_vehicle",
                "values": [
                    {
                        "timestamp": "2026-08-17 10:00:05 +0000",
                        "value": "EV01"         # The configured vehicle's id, or "" if no session is active
                    }
                ]
            }
        }

        Expected Input (any other reading):
        --------------------------------------
        param_id = "41836"
        parameter_type_id = "power"
        raw_values = [{"Date": "2026-08-17T10:00:00Z", "Read": 3.7}]

        Expected Output (any other reading):
        ---------------------------------------
        {
            "41836": {
                "parameter_type_id": "power",
                "values": [
                    {"timestamp": "2026-08-17 10:00:00 +0000", "value": 3.7}
                ]
            }
        }

        Args:
            entity_id (str): Id of the EV charger entity.
            entity_parameters (dict): The entity's parameter definitions.
            param_id (str): Id of the parameter this reading is for.
            parameter_type_id (str): Type id of the parameter this reading is for.
            raw_values (Any): The reading(s) received for this parameter.
            now_dt (datetime.datetime): Current time, timezone-aware.

        Returns:
            dict: The resulting parameter(s) for this reading, or an empty
                dictionary if the entity has no electric_vehicle parameter configured.
        """
        if parameter_type_id != ParamLabel.SESSION_STATUS.value:
            # Not a session_status reading: just normalize it like any other parameter
            return {
                param_id: self._build_parameter(entity_id, param_id, parameter_type_id, raw_values)
            }

        # This reading is the session_status heartbeat: resolve the session state
        session_status = {}
        if isinstance(raw_values, list) and raw_values and isinstance(raw_values[0], dict):
            session_status = raw_values[0]

        # The heartbeat's date may come as 'DateUTC' or as 'Date'.
        date_str = session_status.get("DateUTC") or session_status.get("Date")
        if date_str:
            parsed_dt = self._parse_date(date_str)
            if parsed_dt is None:
                self._logger.warning(f"{self._LOG_PREFIX} Skipping reading with no date: {session_status}")
                return {}
        else:
            self._logger.warning(f"{self._LOG_PREFIX} Skipping reading with no date: {session_status}")
            return {}

        read_status = session_status.get("Read") if session_status.get("Read") is not None else session_status.get("Value", None)

        if read_status == 1:
            # Active session reported: record when this happened (timezone-aware).
            self._last_session_status[entity_id] = parsed_dt

        elif read_status == 0:
            # Explicitly inactive: forget any previous active session
            self._last_session_status.pop(entity_id, None)

        elif read_status is None:
            # No reading this time: if a session was recently active, assume it still is
            last_status_dt = self._last_session_status.get(entity_id)
            if last_status_dt and (now_dt - last_status_dt) <= SESSION_GRACE_PERIOD:
                read_status = 1
            else:
                read_status = 0
                self._last_session_status.pop(entity_id, None)

        # Find the parameter that reports the connected vehicle, and the vehicle's id
        ev_param_id = next(
            (p_id for p_id, p_info in entity_parameters.items()
             if p_info.get("parameter_type_id") == ParamLabel.ELECTRIC_VEHICLE.value),
            None
        )

        if ev_param_id is None:
            self._logger.warning(
                f"{self._LOG_PREFIX} Entity '{entity_id}' has no configured electric_vehicle parameter."
            )
            return {}

        vehicle_id = entity_parameters[ev_param_id].get("vehicle_id")
        if not vehicle_id:
            self._logger.warning(
                f"{self._LOG_PREFIX} Parameter '{ev_param_id}' of entity '{entity_id}' has no 'vehicle_id' configured."
            )
            return {}

        return {
            ev_param_id: {
                "parameter_type_id": ParamLabel.ELECTRIC_VEHICLE.value,
                "values": [{
                    "timestamp": now_dt.strftime("%Y-%m-%d %H:%M:%S %z"),
                    "value": vehicle_id if read_status == 1 else ""
                }]
            }
        }


    def _resolve_param_from_url(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Finds (entity_id, param_id) from the Cleanwatts tag id present in the request URL.

        Args:
            url (str): The request URL the envelope's data came from.

        Returns:
            Tuple[Optional[str], Optional[str]]: The entity id and parameter id this
                tag belongs to, or (None, None) if the URL has no recognized tag.
        """
        if not url:
            return None, None

        match = TAGS_PATTERN.search(url)
        if not match:
            return None, None

        tag_id = match.group(1)
        return self._tag_to_param.get(tag_id, (None, None))

    def _parse_date(self, raw_date: Any) -> Optional[datetime.datetime]:
        """Parses a date string into a timezone-aware datetime.

        This is the single place where dates received from the API are interpreted.
        Accepts ISO 8601 strings, with or without a timezone. A date with no
        timezone is assumed to be UTC.

        Args:
            raw_date (Any): The date, as received.

        Returns:
            Optional[datetime.datetime]: The parsed datetime, or None if it is not a valid date.
        """
        try:
            dt = datetime.datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
        except ValueError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt