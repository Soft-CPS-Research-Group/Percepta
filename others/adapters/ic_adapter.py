# --- Standard Library Imports ---
import datetime
import json
from typing import Any, Dict, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.utils.data import DataSet
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import Label

# Adaptation strategies registered by provider message attribute.
_LABEL_STRATEGIES = {}


def register_label_strategy(name: str):
    """Register an adaptation strategy for a provider message attribute.

    Args:
        name (str): Provider message attribute associated with the strategy.

    Returns:
        Callable: A decorator that registers the strategy function.
    """
    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class ICTelemetryAdapter(TelemetryAdapterBase):
    """Pure domain adapter for i-charging telemetry data normalization.

    Converts raw i-charging telemetry observation payloads into standardized,
    canonical entity messages without performing any network I/O.
    """

    _entities: dict
    _LOG_PREFIX: str = "ICAdapter |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        configurations: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        super().__init__(environment, timezone, logger)

        # --- Guard Clause: Validate environment_specs ---
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

        self._entities = entities_dict
        self._time_interval = DataSet.calculate_interval(configurations.get("frequency"))

    def adapt(self, message: Union[bytes, str, dict]) -> List[Dict[str, Any]]:
        """Transforms incoming telemetry observation payloads into canonical entity messages."""
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} Skipping empty message payload.")
            return []

        # 1. Parse payload to dictionary if provided as bytes or string
        message_dict: dict = {}
        if isinstance(message, (bytes, bytearray)):
            try:
                parsed_json = json.loads(message.decode("utf-8"))
                message_dict = parsed_json.get("observation", {}) if isinstance(parsed_json, dict) else {}
            except Exception as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode JSON bytes payload: {err}")
                return []
        elif isinstance(message, str):
            try:
                parsed_json = json.loads(message)
                message_dict = parsed_json.get("observation", {}) if isinstance(parsed_json, dict) else {}
            except Exception as err:
                self._logger.error(f"{self._LOG_PREFIX} Failed to decode JSON string payload: {err}")
                return []
        elif isinstance(message, dict):
            message_dict = message.get("observation", message)
        else:
            self._logger.warning(
                f"{self._LOG_PREFIX} Unsupported message format type: '{type(message).__name__}'."
            )
            return []

        if not isinstance(message_dict, dict) or not message_dict:
            return []

        timestamp: str = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")
        canonical_messages: List[Dict[str, Any]] = []

        # 2. Dispatch transformation strategies for mapped observation attributes
        for attr, attr_value in message_dict.items():
            if attr in _LABEL_STRATEGIES:
                strategy_func = _LABEL_STRATEGIES[attr]
                processed_items: list = strategy_func(self, attr_value, timestamp)
                if processed_items:
                    canonical_messages.extend(processed_items)

        self._logger.info(f"{self._LOG_PREFIX} Adapted {len(canonical_messages)} canonical entity messages.")
        return canonical_messages

    @register_label_strategy("charging.session")
    def _ev_charger(self, charging_sessions: list, timestamp: str) -> list:
        """Processes EV charger sessions and converts them into canonical messages."""
        messages: list = []

        if not isinstance(charging_sessions, list):
            return messages

        for charging_session in charging_sessions:
            if not isinstance(charging_session, dict):
                continue

            serial: Optional[str] = charging_session.get("serialnumber")
            plug: Optional[int] = charging_session.get("plug")

            if not serial or plug is None:
                continue

            entity_id: str = f"{serial}_{plug}"
            entity_data: dict = self._entities.get(entity_id)

            if entity_data:
                label: str = entity_data.get("label")

                if label == Label.ELECTRIC_VEHICLE_CHARGER.value:
                    entity_parameters: dict = entity_data.get("parameters", {})

                    value: dict = {
                        "power": [{
                            "timestamp": timestamp,
                            "value": charging_session.get("power")
                        }],
                        "electric_vehicle": charging_session.get("user.id")
                    }

                    TelemetryAdapterBase.parameters_validation(value, entity_parameters, [])
                    messages.append(self._message_creator(value, entity_id, timestamp))
                else:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Entity found, but label mismatch: {entity_id} (label: {label}). Ignored."
                    )
            else:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' not found in configuration specs. Ignored."
                )

        return messages

    @register_label_strategy("pv.production")
    def _pv_panel(self, pv_production: float, timestamp: str) -> list:
        """Processes PV panel production data and creates canonical messages."""
        for entity_id, entity_data in self._entities.items():
            if entity_data.get("label") == Label.PV_PANEL.value:
                entity_parameters: dict = entity_data.get("parameters", {})

                value: dict = {
                    "energy": [{
                        "timestamp": timestamp,
                        "value": pv_production
                    }]
                }

                TelemetryAdapterBase.parameters_validation(value, entity_parameters, [])
                return [self._message_creator(value, entity_id, timestamp)]

        return []

    @register_label_strategy("battery.soc")
    def _battery(self, battery_soc: float, timestamp: str) -> list:
        """Processes battery state-of-charge (SoC) data and creates canonical messages."""
        for entity_id, entity_data in self._entities.items():
            if entity_data.get("label") == Label.BATTERY.value:
                entity_parameters: dict = entity_data.get("parameters", {})

                value: dict = {
                    "SoC": [{
                        "timestamp": timestamp,
                        "value": battery_soc
                    }]
                }

                TelemetryAdapterBase.parameters_validation(value, entity_parameters, [])
                return [self._message_creator(value, entity_id, timestamp)]

        return []

    @register_label_strategy("meter.values")
    def _meter(self, meters_list: list, timestamp: str) -> list:
        """Processes grid meter readings and creates canonical messages."""
        messages: list = []

        if not isinstance(meters_list, list):
            return messages

        hours_factor = self._time_interval / 3600

        for meter in meters_list:
            if not isinstance(meter, dict):
                continue

            entity_id: Optional[str] = meter.get("id")
            if entity_id is None:
                continue

            entity_data: dict = self._entities.get(entity_id)

            if entity_data:
                label: str = entity_data.get("label")
                if label == Label.GRID_METER.value:
                    entity_parameters: dict = entity_data.get("parameters", {})

                    value: dict = {
                        "energy_in_total": [{
                            "timestamp": timestamp,
                            "value": meter.get("l123", 0) * hours_factor
                        }],
                        "energy_in_l1": [{
                            "timestamp": timestamp,
                            "value": meter.get("l1", 0) * hours_factor
                        }],
                        "energy_in_l2": [{
                            "timestamp": timestamp,
                            "value": meter.get("l2", 0) * hours_factor
                        }],
                        "energy_in_l3": [{
                            "timestamp": timestamp,
                            "value": meter.get("l3", 0) * hours_factor
                        }],
                    }

                    TelemetryAdapterBase.parameters_validation(value, entity_parameters, [])
                    messages.append(self._message_creator(value, entity_id, timestamp))
                else:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Entity found, but label mismatch: {entity_id} (label: {label}). Ignored."
                    )
            else:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' not found in configuration specs. Ignored."
                )

        return messages