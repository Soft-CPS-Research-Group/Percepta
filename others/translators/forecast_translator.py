import datetime
import json

from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import Label


# Strategy handlers: links provider forecast types to their translation logic.
_LABEL_STRATEGIES = {}


def register_label_strategy(name):
    """Register a translation strategy for a forecast type.

    Args:
        name: Provider forecast type associated with the strategy.

    Returns:
        A decorator that registers the strategy function.
    """

    def decorator(func):
        _LABEL_STRATEGIES[name] = func
        return func

    return decorator


class ForecastTranslator(TelemetryAdapterBase):
    """Translate consumption and production forecasts into canonical messages."""

    _entities: dict
    _LOG_PREFIX: str = "Forecast Translator |"

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

    def _forecast_message(
        self,
        entity_label: str,
        parameter: str,
        timestamp: str,
        data: list,
    ) -> dict | None:
        """Build and validate a canonical message for a forecast service.

        Args:
            entity_label: Label used to find the configured forecast entity.
            parameter: Canonical parameter name for the forecast values.
            timestamp: Timestamp assigned to the canonical message.
            data: Timestamped forecast values.

        Returns:
            A canonical message, or ``None`` when no matching entity is configured.
        """
        for entity_id, entity_data in self._entities.items():
            if entity_data.get("label") != entity_label:
                continue

            value = {parameter: data}
            TelemetryAdapterBase.parameters_validation(value, entity_data.get("parameters", {}))
            return {
                "id": entity_id,
                "value": value,
                "timestamp": timestamp,
            }

        self._logger.warning(
            f"{self._LOG_PREFIX} No entity configured with label '{entity_label}'."
        )
        return None

    @register_label_strategy("consumption")
    def _consumption_forecast_service(self, timestamp: str, data: list) -> dict | None:
        """Translate consumption forecast values.

        Args:
            timestamp: Timestamp assigned to the canonical message.
            data: Timestamped forecast values.

        Returns:
            A canonical consumption forecast message, if configured.
        """
        return self._forecast_message(
            Label.CONSUMPTION_FORECAST_SERVICE.value,
            "consumption_total",
            timestamp,
            data,
        )

    @register_label_strategy("production")
    def _production_forecast_service(self, timestamp: str, data: list) -> dict | None:
        """Translate production forecast values.

        Args:
            timestamp: Timestamp assigned to the canonical message.
            data: Timestamped forecast values.

        Returns:
            A canonical production forecast message, if configured.
        """
        return self._forecast_message(
            Label.PRODUCTION_FORECAST_SERVICE.value,
            "production_total",
            timestamp,
            data,
        )

    @staticmethod
    def _build_values_array(start_time: datetime.datetime, values: list, delta_minutes: int = 15) -> list:
        """Associate each forecast value with a consecutive timestamp.

        Args:
            start_time: Timestamp of the first forecast value.
            values: Forecast values in chronological order.
            delta_minutes: Interval between consecutive forecast values.

        Returns:
            Canonical timestamp-value records.
        """
        return [
            {
                "timestamp": (start_time + datetime.timedelta(minutes=index * delta_minutes)).strftime(
                    "%Y-%m-%d %H:%M:%S %z"
                ),
                "value": value,
            }
            for index, value in enumerate(values)
        ]

    def _parse_message(self, message: bytes | dict) -> dict | None:
        """Decode a provider payload while supporting pre-decoded messages.

        Args:
            message: JSON-encoded bytes or an already decoded payload.

        Returns:
            The decoded payload, or ``None`` when it cannot be decoded.

        Raises:
            TypeError: If ``message`` is neither bytes nor a dictionary.
        """
        if isinstance(message, dict):
            return message.copy()
        if not isinstance(message, bytes):
            raise TypeError(f"{self._LOG_PREFIX} translate expected bytes or dict, got {type(message)}")

        try:
            decoded_message = json.loads(message.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            self._logger.error(f"{self._LOG_PREFIX} Failed to decode incoming forecast payload: {error}")
            return None

        if not isinstance(decoded_message, dict):
            self._logger.warning(f"{self._LOG_PREFIX} Decoded forecast payload is not a dictionary.")
            return None
        return decoded_message

    async def translate(self, message: bytes | dict) -> None:
        """Translate and dispatch supported forecast series.

        Args:
            message: JSON-encoded bytes or an already decoded provider payload.
        """
        message_dict = self._parse_message(message)
        if message_dict is None:
            return

        target_time = message_dict.pop("target_time", None)
        if not target_time:
            self._logger.warning(f"{self._LOG_PREFIX} Forecast payload is missing 'target_time'.")
            return

        try:
            start_time = datetime.datetime.fromisoformat(str(target_time).replace("Z", "+00:00"))
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
            start_time = start_time.astimezone(self._tz).replace(microsecond=0)
        except (TypeError, ValueError):
            self._logger.warning(f"{self._LOG_PREFIX} Invalid target_time '{target_time}'.")
            return

        message_dict.pop("house_id", None)
        timestamp = start_time.strftime("%Y-%m-%d %H:%M:%S %z")
        translated_messages = []

        for forecast_type, values in message_dict.items():
            strategy_func = _LABEL_STRATEGIES.get(forecast_type)
            if not strategy_func:
                self._logger.warning(
                    f"{self._LOG_PREFIX} No translation strategy registered for '{forecast_type}'."
                )
                continue
            if not isinstance(values, list):
                self._logger.warning(
                    f"{self._LOG_PREFIX} Forecast '{forecast_type}' must contain a list of values."
                )
                continue

            translated_message = strategy_func(
                self,
                timestamp,
                self._build_values_array(start_time, values),
            )
            if translated_message:
                translated_messages.append(translated_message)

        await self._output_handler.send(translated_messages) # TODO FAZER VERIFICAÇÃO PARA VER SE AINDA ESTÁ ATIVO ANTES DE USAR
