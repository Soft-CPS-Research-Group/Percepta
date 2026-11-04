# --- Standard Library Imports ---
import asyncio
import datetime
import json
from typing import Any, List, Optional, Union

# --- Internal Framework Imports ---
from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.data import DataSet
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import Label

# Translation strategies registered by provider message attribute.
_LABEL_STRATEGIES = {}


def register_label_strategy(name: str):
    """Register a translation strategy for a provider message attribute.

    Args:
        name: Provider message attribute associated with the strategy.

    Returns:
        A decorator that registers the strategy function.
    """
    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class ICTranslator(TelemetryAdapterBase):
    """Translate i-charging telemetry into canonical entity messages."""

    _entities: dict
    _LOG_PREFIX: str = "ICTranslator"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        configurations: dict,
        output_handler: OutputHandlerBase,
        logger: LoggingUtils
    ) -> None:
        """Initialize the translator.

        Args:
            environment: Name of the target environment.
            environment_specs: Entity definitions for the environment.
            configurations: Application configuration values.
            output_handler: Handler used to dispatch translated messages.
            logger: Logger used for operational messages.
        """
        super().__init__(environment, configurations, output_handler, logger)

        self._entities = environment_specs.get("entities", {})
        self._time_interval = DataSet.calculate_interval(configurations.get("frequency"))

    def process_message(self, message: Union[List[dict], dict, bytes, str]) -> None:
        """Entrypoint callback executed by the ingress InputHandler.

        Normalizes multiple delivery formats (raw bytes, JSON string, dict envelope)
        and safely schedules the asynchronous `translate` coroutine into the event loop.

        Args:
            message (Union[List[dict], dict, bytes, str]): Raw inbound payload from AMQP broker.
        """
        if not message:
            self._logger.debug(f"{self._LOG_PREFIX} | Skipping empty message payload.")
            return

        # 1. Normalize list envelope wrapper if message arrived inside a batch
        raw_target: Any = message[0] if isinstance(message, list) and message else message

        # 2. Extract payload if wrapped in a standard transport envelope (e.g., {"payload": ...})
        if isinstance(raw_target, dict) and "payload" in raw_target:
            raw_target = raw_target["payload"]

        # 3. Serialize to UTF-8 bytes if already a dict or str to ensure a uniform contract for translate()
        payload_bytes: Optional[bytes] = None

        if isinstance(raw_target, (bytes, bytearray)):
            payload_bytes = bytes(raw_target)
        elif isinstance(raw_target, str):
            payload_bytes = raw_target.encode("utf-8")
        elif isinstance(raw_target, dict):
            try:
                payload_bytes = json.dumps(raw_target).encode("utf-8")
            except Exception as err:
                self._logger.error(f"{self._LOG_PREFIX} | Failed to serialize dict payload to JSON bytes: {err}")
                return
        else:
            self._logger.warning(
                f"{self._LOG_PREFIX} | Unsupported message format type: '{type(raw_target).__name__}'."
            )
            return

        # 4. Schedule async translation task in the active event loop
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.translate(payload_bytes))
        except RuntimeError:
            # Fallback if invoked outside an active event loop context
            asyncio.run(self.translate(payload_bytes))

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
                    _entity_parameters: dict = entity_data.get("parameters", {})

                    value: dict = {
                        "power": [{
                            "timestamp": timestamp,
                            "value": charging_session.get("power")
                        }],
                        "electric_vehicle": charging_session.get("user.id")
                    }

                    TelemetryAdapterBase.parameters_validation(value, _entity_parameters, [])
                    messages.append(ICTranslator._message_creator(value, entity_id, timestamp))
                else:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} | Entity found, but label mismatch: {entity_id} (label: {label}). Ignored."
                    )
            else:
                self._logger.warning(
                    f"{self._LOG_PREFIX} | Entity {entity_id} not found in configuration specs. Ignored."
                )

        return messages

    @register_label_strategy("pv.production")
    def _pv_panel(self, pv_production: float, timestamp: str) -> list:
        """Processes PV panel production data and creates canonical messages."""
        for entity_id, entity_data in self._entities.items():
            if entity_data.get("label") == Label.PV_PANEL.value:
                _entity_parameters: dict = entity_data.get("parameters", {})

                value: dict = {
                    "energy": [{
                        "timestamp": timestamp,
                        "value": pv_production
                    }]
                }

                TelemetryAdapterBase.parameters_validation(value, _entity_parameters, [])
                return [ICTranslator._message_creator(value, entity_id, timestamp)]

        return []

    @register_label_strategy("battery.soc")
    def _battery(self, battery_soc: float, timestamp: str) -> list:
        """Processes battery state-of-charge (SoC) data and creates canonical messages."""
        for entity_id, entity_data in self._entities.items():
            if entity_data.get("label") == Label.BATTERY.value:
                _entity_parameters: dict = entity_data.get("parameters", {})

                value: dict = {
                    "SoC": [{
                        "timestamp": timestamp,
                        "value": battery_soc
                    }]
                }

                TelemetryAdapterBase.parameters_validation(value, _entity_parameters, [])
                return [ICTranslator._message_creator(value, entity_id, timestamp)]

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
                    _entity_parameters: dict = entity_data.get("parameters", {})

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

                    TelemetryAdapterBase.parameters_validation(value, _entity_parameters, [])
                    messages.append(ICTranslator._message_creator(value, entity_id, timestamp))
                else:
                    self._logger.warning(
                        f"{self._LOG_PREFIX} | Entity found, but label mismatch: {entity_id} (label: {label}). Ignored."
                    )
            else:
                self._logger.warning(
                    f"{self._LOG_PREFIX} | Entity {entity_id} not found in configuration specs. Ignored."
                )

        return messages

    async def translate(self, message: bytes) -> None:
        """Translates incoming entity messages into standardized format."""
        try:
            parsed_json = json.loads(message.decode("utf-8"))
            message_dict: dict = parsed_json.get("observation", {})
        except Exception as err:
            self._logger.error(f"{self._LOG_PREFIX} | Failed to decode JSON in translate(): {err}")
            return

        if not isinstance(message_dict, dict):
            return

        timestamp: str = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")
        message_list: list = []

        for attr, attr_value in message_dict.items():
            if attr in _LABEL_STRATEGIES:
                func = _LABEL_STRATEGIES[attr]
                attr_processed: list = func(self, attr_value, timestamp)
                if attr_processed:
                    message_list.extend(attr_processed)

        if message_list:
            self._logger.info(f"{self._LOG_PREFIX} | Sending translated messages: {message_list}")
            await self._output_handler.send(message_list)

    @staticmethod
    def _message_creator(value: dict, entity_id: str, timestamp: str) -> dict:
        """Creates a standardized message format for an entity reading."""
        return {
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }