import datetime
import json
from typing import Any, Dict

from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


class PCTranslator(TelemetryAdapterBase):
    """
    Concrete implementation of a translator for PulseCharge (PC) entities.

    Translates PulseCharge telemetry into canonical messages.

    The translator caches the last state-of-charge value for each user when a
    subsequent provider message omits it.
    """

    _LOG_PREFIX: str
    _entities: dict
    _soc_cache: Dict[str, Any]

    _LOG_PREFIX = "PulseCharge Translator |"

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        configurations: dict,
        output_handler: OutputHandlerBase,
        logger: LoggingUtils
    ) -> None:
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

        self._entities = environment_specs.get("entities", {})

        self._soc_cache = {}

    async def translate(self, message: bytes) -> None:
        """
        Translate and dispatch a PulseCharge telemetry payload.

        Args:
            message: Raw JSON bytes received from PulseCharge.
        """
        timestamp: str = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        try:
            message_dict: dict = json.loads(message.decode("utf-8"))
        except Exception as e:
            self._logger.error(f"{self._LOG_PREFIX} Failed to decode incoming JSON payload: {e}")
            return

        self._logger.debug(f"{self._LOG_PREFIX} Processing raw payload: {message_dict}")

        user_id = message_dict.get("user_id")
        if not user_id:
            self._logger.warning(f"{self._LOG_PREFIX} Ingress message missing 'user_id'. Dropping frame.")
            return

        current_soc = message_dict.get("current_soc")

        if current_soc is not None:
            self._soc_cache[user_id] = current_soc
        else:
            current_soc = self._soc_cache.get(user_id)
            self._logger.debug(
                f"{self._LOG_PREFIX} Missing 'current_soc' in frame. Using cached value for user '{user_id}': {current_soc}"
            )

        for entity_id, entity_spec in self._entities.items():
            if entity_spec.get("user_id") == user_id:
                value = {
                    "SoC": current_soc,
                    "flexibility": {
                        "estimated_soc_at_arrival": message_dict.get("estimated_soc_at_arrival", None),
                        "estimated_soc_at_departure": message_dict.get("estimated_soc_at_departure", None),
                        "estimated_time_at_arrival": message_dict.get("estimated_time_at_arrival", ""),
                        "estimated_time_at_departure": message_dict.get("estimated_time_at_departure", ""),
                        "charger": message_dict.get("charger", ""),
                        "mode": message_dict.get("mode", "")
                    }
                }

                canonical_message = self._message_creator(value, entity_id, timestamp)

                await self._output_handler.send([canonical_message]) # TODO FAZER VERIFICAÇÃO PARA VER SE AINDA ESTÁ ATIVO ANTES DE USAR
                break

    @staticmethod
    def _message_creator(value: dict, entity_id: str, timestamp: str) -> dict:
        """
        Build a canonical entity message.

        Args:
            value: Normalized telemetry and flexibility data.
            entity_id: Entity identifier in the environment.
            timestamp: Formatted translation timestamp.

        Returns:
            A canonical entity message.
        """
        return {
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }
