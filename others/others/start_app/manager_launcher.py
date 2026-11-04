# launchers/manager_launcher.py

import yaml
from typing import List

from app.core.data_quality_handlers.data_quality_handler_base import DataQualityHandlerBase
from app.core.utils.logger import LoggingUtils
from app.core.data_quality_handlers import MissingDataHandler
from app.core.repositories.irepositories.time_series_repository import TimeSeriesRepository
from app.core.repositories.irepositories import EnvironmentRepository
from others.entity_handlers.entity_handler_factory import build_entity_handler
from app.accumulator_context_factory import AccumulatorContextFactory
from app.start_app.launcher_base import LauncherBase

DATA_QUALITY_HANDLERS = {}

def register_data_quality_handler(name):
    """Decorator to register a label strategy method into the class mapping."""

    def decorator(func):
        DATA_QUALITY_HANDLERS[name] = func
        return func

    return decorator

def build_label_to_ids(environment_specs):
    entities = environment_specs.get('entities')
    label_map = {}
    for entity_id, values in entities.items():
        label = values.get("label")
        if label:
            if label not in label_map:
                label_map[label] = []
            label_map[label].append(entity_id)
    return label_map


# TODO ter launcher genérico?
class ManagerLauncher(LauncherBase):

    def __init__(self, environment, environment_specs, time_series_repository : TimeSeriesRepository, logger : LoggingUtils):
        super().__init__(logger)
        self._environment = environment
        self._environment_specs = environment_specs
        self._time_series_repository = time_series_repository
        self._entity_ids_by_label = build_label_to_ids(environment_specs)
        self._entities_handlers = build_entity_handler(time_series_repository, entity_ids_by_label, self._configurations, manager_logger, self._environment_specs)
        self._manager_logger = LoggingUtils("manager", self._configurations, self._environment)


    def load_handler_pipeline(self, config_path: str) -> List[DataQualityHandlerBase]:
        """
        Loads the pipeline by calling specific providers for each handler.
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
        except Exception as e:
            self._logger.error(f"Launcher | Failed to read config: {e}")
            raise

        pipeline = []

        for handler_config in config.get('data_quality_pipeline', []):
            # We now use a 'type' or 'name' instead of a full module path in YAML
            handler_name = handler_config.get('name')
            params = handler_config.get('params', {})

            if handler_name in DATA_QUALITY_HANDLERS:
                try:
                    # Call the specific provider function
                    provider_func = DATA_QUALITY_HANDLERS[handler_name]
                    handler_instance = provider_func()

                    pipeline.append(handler_instance)
                    self._logger.info(f"Launcher | Successfully loaded handler: {handler_name}")
                except Exception as e:
                    self._logger.error(f"Launcher | Error initializing {handler_name}: {e}")
                    raise
            else:
                self._logger.warning(f"Launcher | No provider found for handler type: {handler_name}")

        return pipeline

    @register_data_quality_handler("MissingDataHandler")
    def missing_data_provider(self):
        """Specific factory for the MissingDataHandler."""
        return MissingDataHandler(
            logger=self._logger,
            entity_handlers=self._entities_handlers,
            entities=self._entities
        )

def start(environment_repository: EnvironmentRepository, repository_factory: RepositoryFactory, configurations: dict, logger):
    """
    Assembles and starts ONE environment.
    Can be called by any script (Central Entrypoint or Standalone Main).
    """

    all_environments = environment_repository.get_environments()
    all_provider_configs = environment_repository.get_all_configurations()
    threads = []


    for current_environment, environment_specs in all_environments.items():
        group = environment_specs.get('group')
        time_series_repository = repository_factory.build_time_series_repository(group, current_environment)
        factory = AccumulatorContextFactory(current_environment, environment_specs, configurations, logger)
        forwarders = factory.build_forwarders(all_provider_configs)
        predictor = factory.build_predictor(forwarders, time_series_repository)
        output_handler = factory.build_output_handler(predictor)
        manager = factory.build_manager(time_series_repository, output_handler)
        accumulator = factory.build_accumulator(manager)
        accumulator.start()

        threads.append(accumulator)

    return threads

# TODO pertence ao launcher genérico?
def build_and_start():
    # Load configuration from file once at startup
    configurations = DataSet.get_schema('./configs/runtime_configurations.json')
    # Initialize logger once for main process
    logger = LoggingUtils(component_name="entrypoint", configurations=configurations)

    repository_factory = RepositoryFactory(configurations, logger)
    environment_repository = repository_factory.build_environment_repository()


    all_environments = environment_repository.get_environments()
    all_provider_configs = environment_repository.get_all_configurations()

    try:
        for current_environment, environment_specs in all_environments.items():
            group = environment_specs.get('group')
            time_series_repository = repository_factory.build_time_series_repository(group, current_environment)
            factory = AccumulatorContextFactory(current_environment, environment_specs, configurations, logger)
            accumulator = start()

            threads.append(accumulator)

            # Loop to keep the main thread alive and monitor the threads


        while threads:
            for thread in threads[:]:  # iterate over a copy of the list
                if not thread.is_alive():
                    threads.remove(thread)
            time.sleep(0.1)

        except KeyboardInterrupt:
        print("KeyboardInterrupt: Stopping threads...")
        for thread in threads:
            thread.stop()  # assuming your accumulator has a stop() method

        for thread in threads:
            thread.join()
        print("All threads stopped.")


