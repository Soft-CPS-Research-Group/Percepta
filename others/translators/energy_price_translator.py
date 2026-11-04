import datetime

from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


class EnergyPriceTranslator(TelemetryAdapterBase):
    """Translate energy-price series into canonical entity messages."""

    _entities: dict

    def __init__(
        self,
        environment: str,
        environment_specs: dict,
        configurations: dict,
        output_handler: OutputHandlerBase,
        logger: LoggingUtils,
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

    async def translate(self, messages: dict) -> None:
        """Translate and dispatch an energy-price message.

        Args:
            messages: Entity metadata and timestamp-keyed price values.

        Raises:
            TypeError: If ``messages`` is not a dictionary.
        """
        timestamp = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        if not isinstance(messages, dict):
            raise TypeError(f"Translator | translate expected dict, got {type(messages)}")

        entity_id = messages.get("entity_id")
        if messages.get("value"):
            value = self._build_values_array(messages["value"])
            new_message = [{
                "id": entity_id,
                "value": {"energy_price": value},
                "timestamp": timestamp,
            }]
            await self._output_handler.send(new_message) # TODO FAZER VERIFICAÇÃO PARA VER SE AINDA ESTÁ ATIVO ANTES DE USAR

    @staticmethod
    def _build_values_array(prices_with_timestamp: dict) -> list:
        """Convert timestamp-keyed prices into canonical value records.

        Args:
            prices_with_timestamp: Price values keyed by datetime objects.

        Returns:
            Timestamp-value records with prices converted from MWh to kWh.
        """
        return [
            {
                "timestamp": date.strftime("%Y-%m-%d %H:%M:%S %z"),
                "value": round(price / 1000, 8),
            }
            for date, price in prices_with_timestamp.items()
        ]
