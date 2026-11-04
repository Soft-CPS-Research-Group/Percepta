import datetime
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils

# Translation strategies registered by entity label.
_LABEL_STRATEGIES = {}

def register_label_strategy(name):
    """Register a translation strategy for an entity label.

    Args:
        name: Entity label associated with the strategy.

    Returns:
        A decorator that registers the strategy function.
    """

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class CWTranslator(TelemetryAdapterBase):
    """
    Translate Cleanwatts telemetry into canonical entity messages.
    """

    _entities: dict

    _LOG_PREFIX: str = "Cleanwatts Translator |"

    def __init__(self, environment: str,  environment_specs: dict, configurations: dict, output_handler : OutputHandlerBase, logger: LoggingUtils):
        """
        Initialize the translator.

        Args:
            environment: Name of the target environment.
            environment_specs: Entity definitions for the environment.
            configurations: Application configuration values.
            output_handler: Handler used to dispatch translated messages.
            logger: Logger used for operational messages.
        """
        super().__init__(environment, configurations, output_handler, logger)

        self._entities = environment_specs.get('entities')

        self._last_session_status = None


    def _build_result(self, parameter_values_list: list, default_value: float = 0) -> list:
        """
        Transforms raw timeseries telemetry readings into standardized timestamp-value records.

        Expected Input Data Format (`param_values_list`):
        -------------------------------------------------
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

        Standardized Output Format:
        ---------------------------
        [
            {
                "timestamp": "2026-08-17 11:00:00 +0100",  # (str) Localized to self._tz format "%Y-%m-%d %H:%M:%S %z"
                "value": 22.4                              # (float/int) Normalized numeric reading
            },
            {
                "timestamp": "2026-08-17 11:15:00 +0100",
                "value": 21.8
            }
        ]

        Args:
            parameter_values_list (list): Raw list of reading dictionaries to translate.
            default_value (float, optional): Fallback value if neither 'Value' nor 'Read' exists. Defaults to 0.

        Returns:
            list: List of normalized dictionaries containing 'timestamp' and 'value'.
        """
        if not parameter_values_list:
            return []

        result = []
        for item in parameter_values_list:
            if not isinstance(item, dict):
                continue

            # 1. Extract value safely
            raw_val = item.get("Value", item.get("Read", default_value))

            # 2. Parse and localize timestamp
            raw_date = item.get("Date")
            if not raw_date:
                continue

            try:
                # Replaces Z with +00:00 for standard ISO parsing
                date_str = str(raw_date).replace("Z", "+00:00")
                dt = datetime.datetime.fromisoformat(date_str)

                # If timezone-naive, assume UTC; otherwise convert directly to self._tz
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc).astimezone(self._tz)
                else:
                    dt = dt.astimezone(self._tz)

                formatted_ts = dt.strftime("%Y-%m-%d %H:%M:%S %z")
            except Exception:
                # Fallback for unexpected non-ISO string formats
                formatted_ts = str(raw_date)

            # 3. Append normalized canonical record
            result.append({
                "timestamp": formatted_ts,
                "value": raw_val
            })

        return result

    @register_label_strategy("ev_charger")
    def _ev_charger(self, entity_id, messages: dict) -> tuple[dict, list]:
        """
        Processes EV charger sessions and converts them into a standardized dictionary of readings.
        Manages session active states and dynamically attaches or clears associated vehicle metadata.

        Expected Input Data Format (`messages`):
        ----------------------------------------
        {
            "rs_1": [
                {
                    "Read": 1,                       # (int/float, optional) 1 for active charging, 0 for idle (or under key 'Value')
                    "DateUTC": "2026-08-17 10:00:00" # (str, optional) UTC timestamp of the session heartbeat
                }
            ],
            "pow_1": [
                {
                    "Date": "2026-08-17T10:00:00Z",  # (str) ISO or UTC datetime string
                    "Read": 22.4                     # (float/int) Reading value (or under key 'Value')
                }
            ]
        }

        Returned Output Structure:
        --------------------------
        Returns a tuple `(value_dict, ignored_parameters_list)`:
        (
            {
                "rs_1": [
                    {"timestamp": "2026-08-17 11:00:00 +0100", "value": 22.4}
                ],
                "electric_vehicle": "ev_vehicle_id_123"  # Set to configured vehicle ID if active, or "" if inactive
            },
            ["session_status"]                          # Parameters to exclude from base schema validation
        )

        Args:
            entity_id (str): Unique identifier of the EV charger entity.
            messages (dict): Dictionary of parameters and reading lists for the current charging point.

        Returns:
            tuple[dict, list]: Processed parameter records mapped to standardized formats,
                               plus a list of parameter keys to ignore during baseline validation.
        """

        # --- 1. Type Validation Guard: Verify payload conforms to expected dictionary format ---
        if not isinstance(messages, dict):
            raise TypeError(f"{self._LOG_PREFIX} | _ev_charger expected dict, got {type(messages)}")

        # --- 2. Session Isolation: Extract telemetry status and purge it from timeseries mapping ---
        # TODO: Resolve parameter keys from environment specifications.
        session_status_list = messages.pop("session_status", [])
        session_status = {}

        # Unpack the latest state record from the session status buffer if present
        # TODO: Confirm that the first status sample correctly represents the full interval.
        if isinstance(session_status_list, list) and session_status_list:
            first_item = session_status_list[0]
            if isinstance(first_item, dict):
                session_status = first_item

        # --- 3. Operational State Resolution: Resolve binary readiness from 'Read' or 'Value' keys ---
        read_status = session_status.get("Read") if session_status.get("Read") is not None else session_status.get(
            "Value", None)
        current_time = datetime.datetime.now(self._tz)

        # State A: Explicit active charging state received (read_status == 1)
        if read_status == 1:
            date_str = session_status.get("DateUTC")
            if date_str:
                try:
                    self._last_session_status = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # Fallback to local clock baseline on malformed timestamp formats
                    self._last_session_status = current_time
            else:
                self._last_session_status = current_time

        # State B: Implicit state evaluation during intermittent telemetry loss (read_status is None)
        elif read_status is None:
            if self._last_session_status and isinstance(self._last_session_status, datetime.datetime):
                # Maintain active session state if within the 15-minute sliding grace window
                if (current_time - self._last_session_status) <= datetime.timedelta(minutes=15):
                    read_status = 1
                else:
                    # Grace period elapsed: invalidate active state and reset memory cache
                    read_status = 0
                    self._last_session_status = None
            else:
                read_status = 0

        value = {}

        # --- 4. Timeseries Normalization: Process telemetry metrics universally ---
        for param, param_values_list in messages.items():
            if isinstance(param_values_list, list) and param_values_list:
                value[param] = self._build_result(param_values_list)
            else:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Parameter '{param}' of entity '{entity_id}' is missing or empty."
                )
                value[param] = []

        # --- 5. Dynamic Vehicle Association: Bind ID only during active sessions ---
        if read_status == 1:
            # TODO: Resolve parameter keys from environment specifications.
            value['electric_vehicle'] = (
                self._entities.get(entity_id, {})
                .get('parameters', {})
                .get('electric_vehicle', {})
                .get("id", "")
            )
        else:
            value['electric_vehicle'] = ""

        # --- 6. Return Output Tuple ---
        return value, ['session_status']

    async def translate(self, messages : dict) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Expected Input Data Format:
        ---------------------------
        {
            "entity_id": "charger_station_01",           # (str) Target entity unique identifier
            "label": "ev_charger",                       # (str, optional) Strategy tag (e.g. 'ev_charger', 'solar_inverter', etc.)
            "parameters": {                              # (dict) Key-value map of parameter names to raw timeseries lists
                "active_power": [
                    {
                        "Date": "2026-08-17T10:00:00Z",  # (str) ISO or UTC datetime string
                        "Read": 22.4                     # (float/int) Reading value (can also be under key 'Value')
                    },
                    {
                        "Date": "2026-08-17T10:15:00Z",
                        "Read": 21.8
                    }
                ],
                "session_status": [                      # (list, specific to EV charger label strategy)
                    {
                        "Read": 1,                       # (int) 1 for active charging, 0 for idle
                        "DateUTC": "2026-08-17 10:00:00" # (str) UTC timestamp of the status update
                    }
                ]
            }
        }

        Standardized Output Emitted to Output Handler:
        ----------------------------------------------
        [
            {
                "id": "charger_station_01",
                "timestamp": "2026-08-17 10:13:09 +0100",
                "value": {
                    "active_power": [
                        {"timestamp": "2026-08-17 11:00:00 +0100", "value": 22.4},
                        {"timestamp": "2026-08-17 11:15:00 +0100", "value": 21.8}
                    ],
                    "electric_vehicle": "ev_vehicle_id_123"
                }
            }
        ]
        """

        # Generate translation timestamp localized to the configured timezone
        timestamp = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        # --- 1. Type Safety Gate: Ensure incoming payload is a valid dictionary ---
        if not isinstance(messages, dict):
            raise TypeError(f"{self._LOG_PREFIX} | translate expected dict, got {type(messages)}")

        # --- 2. Ingress Unpacking: Extract target identifiers, label strategies, and readings ---
        entity_id = messages.get("entity_id")
        label = messages.get("label")
        parameters_readings = messages.get("parameters", {})

        # Resolve entity schema and expected parameters from environment specifications
        entity_spec = self._entities.get(entity_id, {})
        entities_parameters = entity_spec.get("parameters", {})

        value = {}
        optional_parameters = []

        # --- 3. Translation Strategy Dispatching ---
        if label in _LABEL_STRATEGIES:
            strategy_func = _LABEL_STRATEGIES[label]
            value, optional_parameters = strategy_func(self, entity_id, parameters_readings)
        else:
            # Fallback to generic time-series normalization across all parameter keys
            for param, param_payload in parameters_readings.items():
                if not param:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Encountered unkeyed or missing parameter for entity '{entity_id}'."
                    )
                    continue

                # Extrai a lista do campo 'data' (com suporte a fallback se vier lista direta)
                raw_readings = param_payload.get("data", []) if isinstance(param_payload, dict) else param_payload

                if isinstance(raw_readings, list) and raw_readings:
                    # Normaliza os pontos temporais
                    value[param] = self._build_result(raw_readings)
                else:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Parameter '{param}' of entity '{entity_id}' has empty readings."
                    )
                    value[param] = []

        # Validate translated parameters against environment schema definitions
        TelemetryAdapterBase.parameters_validation(value, entities_parameters, optional_parameters)

        # --- 4. Canonical Outbound Framing ---
        # Wrap translated parameters into standard Percepta Manager ingress format
        new_message = [{
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }]

        # --- 5. Asynchronous Egress Dispatch ---
        # Forward standardized payload to the downstream Output Handler pipeline
        await self._output_handler.send(new_message) # TODO FAZER VERIFICAÇÃO PARA VER SE AINDA ESTÁ ATIVO ANTES DE USAR
