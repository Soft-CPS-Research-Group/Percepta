# app/launchers/data_quality_launcher.py
import yaml
from typing import List
from others.entity_handlers.entity_handler_factory import build_entity_handler
from app.core.repositories.irepositories.time_series_repository import TimeSeriesRepository
from app.core.data_quality_handlers.data_quality_handler_base import DataQualityHandlerBase
from app.core.data_quality_handlers import MissingDataHandler
from app.core.utils.logger import LoggingUtils

# Isolated register mapping for data quality filters
DATA_QUALITY_HANDLERS = {}

def register_data_quality_handler(name: str):
    """Decorator to dynamically register a data quality strategy handler."""
    def decorator(func):
        DATA_QUALITY_HANDLERS[name] = func
        return func
    return decorator


class DataQualityLauncher:
    """
    Dedicated factory class responsible for parsing the pipeline manifest
    and instantiating data quality handler chains.
    """

    def __init__(self, time_series_repository: TimeSeriesRepository, environment_specs: dict, configurations : dict, logger: LoggingUtils) -> None:
        """
        Initializes the factory with the dependencies required by the underlying filters.
        """
        self._environment_specs = environment_specs
        self._logger = logger

        self._labels_handlers = build_entity_handler(
            time_series_repository,
            environment_specs,
            configurations,
            logger
        )

    def load_handler_pipeline(self, config_path: str) -> List[DataQualityHandlerBase]:
        """Parses the YAML file and builds the sequential array execution pipeline."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
        except Exception as e:
            self._logger.error(f"DataQualityLauncher | Failed to read configuration stream: {e}")
            raise

        pipeline = []
        for handler_config in config.get('data_quality_pipeline', []):
            handler_name = handler_config.get('name')
            # params = handler_config.get('params', {}) # Can be mapped downstream if needed

            if handler_name in DATA_QUALITY_HANDLERS:
                try:
                    provider_func = DATA_QUALITY_HANDLERS[handler_name]
                    # We invoke the provider, passing 'self' to allow state lookup safely
                    handler_instance = provider_func(self)
                    pipeline.append(handler_instance)
                    self._logger.info(f"DataQualityLauncher | Successfully loaded handler: {handler_name}")
                except Exception as e:
                    self._logger.error(f"DataQualityLauncher | Error initializing handler [{handler_name}]: {e}")
                    raise
            else:
                self._logger.warning(f"DataQualityLauncher | No provider mapped for handler type: {handler_name}")

        return pipeline

    @register_data_quality_handler("MissingDataHandler")
    def _missing_data_provider(self) -> MissingDataHandler:
        """Concrete factory method specialized in bootstrapping the MissingDataHandler infrastructure."""
        return MissingDataHandler(
            logger=self._logger,
            entity_handlers=self._labels_handlers,
            entities=self._environment_specs.get('entities')
        )