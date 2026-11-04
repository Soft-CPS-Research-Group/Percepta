import datetime

from app.core.adapters.telemetry_adapter_base import TelemetryAdapterBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import Label


# Strategy handlers: links environment entity labels to provider-specific logic.
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


class SoftCPSTranslator(TelemetryAdapterBase):
    """Translate SoftCPS telemetry into the canonical entity message format."""

    _entities: dict
    _LOG_PREFIX: str = "SoftCPS Translator |"

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

    def _format_timestamp(self, raw_timestamp: str | None) -> str:
        """Parse a provider timestamp and normalize it to the configured timezone.

        Args:
            raw_timestamp: Provider timestamp in ISO 8601 format.

        Returns:
            A formatted timestamp in the configured timezone.
        """
        if not raw_timestamp:
            return datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        try:
            timestamp = datetime.datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
            return timestamp.astimezone(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")
        except (TypeError, ValueError):
            self._logger.warning(
                f"{self._LOG_PREFIX} Invalid timestamp '{raw_timestamp}'. Using the current time."
            )
            return datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

    @register_label_strategy(Label.GRID_METER.value)
    def _grid_meter(self, entity_id: str, messages: dict) -> dict:
        """Translate grid-meter readings from a SoftCPS payload.

        Args:
            entity_id: Identifier of the grid meter entity.
            messages: Provider readings and timestamp.

        Returns:
            Canonical parameter readings for the grid meter.

        Raises:
            TypeError: If ``messages`` is not a dictionary.
        """
        if not isinstance(messages, dict):
            raise TypeError(f"{self._LOG_PREFIX} grid_meter expected dict, got {type(messages)}")

        entity_data = messages.get(entity_id, {})
        if not isinstance(entity_data, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Grid meter entity '{entity_id}' has invalid readings."
            )
            return {}

        timestamp = self._format_timestamp(messages.get("timestamp"))
        fields = (
            "energy_in_total", "energy_in_l1", "energy_in_l2", "energy_in_l3",
            "energy_out_total", "energy_out_l1", "energy_out_l2", "energy_out_l3",
        )

        return {
            field: [{"timestamp": timestamp, "value": value}]
            for field in fields
            if (value := entity_data.get(field)) is not None
        }

    @register_label_strategy(Label.BATTERY.value)
    def _battery(self, entity_id: str, messages: dict) -> dict:
        """Translate battery window energy values and state of charge.

        Args:
            entity_id: Identifier of the battery entity.
            messages: Provider readings and timestamp.

        Returns:
            Canonical parameter readings for the battery.

        Raises:
            TypeError: If ``messages`` is not a dictionary.
        """
        if not isinstance(messages, dict):
            raise TypeError(f"{self._LOG_PREFIX} battery expected dict, got {type(messages)}")

        timestamp = self._format_timestamp(messages.get("timestamp"))
        values = {
            "energy_in": messages.get("energy_charged_kwh_window"),
            "energy_out": messages.get("energy_discharged_kwh_window"),
        }
        soc = messages.get("soc_pct")
        if soc is not None:
            values["SoC"] = round(soc / 100, 4)

        return {
            parameter: [{"timestamp": timestamp, "value": value}]
            for parameter, value in values.items()
            if value is not None
        }

    async def translate(self, messages: dict) -> None:
        """Translate and dispatch a SoftCPS message.

        Args:
            messages: Entity metadata and provider readings.

        Raises:
            TypeError: If ``messages`` is not a dictionary.
        """
        timestamp = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S %z")

        if not isinstance(messages, dict):
            raise TypeError(f"{self._LOG_PREFIX} translate expected dict, got {type(messages)}")

        entity_id = messages.get("entity_id")
        label = messages.get("label")
        parameters_readings = messages.get("parameters", {})
        entity_spec = self._entities.get(entity_id, {})
        entity_parameters = entity_spec.get("parameters", {})

        value = {}
        strategy_func = _LABEL_STRATEGIES.get(label)
        if strategy_func:
            if isinstance(parameters_readings, dict):
                value = strategy_func(self, entity_id, parameters_readings)
            else:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Entity '{entity_id}' has invalid parameter readings."
                )
        else:
            self._logger.warning(
                f"{self._LOG_PREFIX} No translation strategy registered for label '{label}'."
            )

        TelemetryAdapterBase.parameters_validation(value, entity_parameters)

        new_message = [{
            "id": entity_id,
            "value": value,
            "timestamp": timestamp,
        }]
        await self._output_handler.send(new_message) # TODO FAZER VERIFICAÇÃO PARA VER SE AINDA ESTÁ ATIVO ANTES DE USAR
