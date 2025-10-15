from .accumulator import Accumulator
from app.manager.manager import Manager
from .predictor import Predictor
from .entity_handlers.entity_handler_factory import build_entity_handler
from .forwarders.decision_forwarder_factory import build_forwarder
from app.utils.logger import LoggingUtils


class AccumulatorContextFactory:
    def __init__(self, environment, environment_specs, configurations, logger):
        self._environment = environment
        self._environment_specs = environment_specs
        self._configurations = configurations
        self._logger = logger

    def _build_label_to_ids(self):
        entities = self._environment_specs.get('entities')
        label_map = {}
        for entity_id, values in entities.items():
            label = values.get("label")
            if label:
                if label not in label_map:
                    label_map[label] = []
                label_map[label].append(entity_id)
        return label_map

    def build_predictor(self, time_series_repository):
        predictor_logger = LoggingUtils("predictor", self._configurations, self._environment)
        forwarders = build_forwarder(self._configurations, predictor_logger)
        return Predictor(self._environment, self._environment_specs, time_series_repository, forwarders, self._configurations, predictor_logger)

    def build_manager(self, time_series_repository, predictor):
        entity_ids_by_label = self._build_label_to_ids()
        manager_logger = LoggingUtils("manager", self._configurations, self._environment)
        entities_handlers = build_entity_handler(time_series_repository, entity_ids_by_label, self._configurations, manager_logger)
        return Manager(self._environment, self._environment_specs, entity_ids_by_label, time_series_repository, predictor, entities_handlers, self._configurations, manager_logger)

    def build_accumulator(self, manager):
        accumulator_logger = LoggingUtils("accumulator", self._configurations, self._environment)
        return Accumulator(self._environment, manager, self._configurations, accumulator_logger)

