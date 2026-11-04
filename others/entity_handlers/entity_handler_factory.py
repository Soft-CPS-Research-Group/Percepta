from .entity_handler_base import EntityHandlerBase
from app.core.utils import discover_subclasses
from typing import Type, Dict

# Dynamically build a registry of all available handlers in the package
ENTITY_HANDLER_REGISTRY: Dict[str, Type[EntityHandlerBase]] = discover_subclasses(
    package="app.entity_handlers",
    base_class=EntityHandlerBase,
    required_suffix="_handler"
)

def _build_label_to_ids(environment_specs):
    entities = environment_specs.get('entities')
    label_map = {}
    for entity_id, values in entities.items():
        label = values.get("label")
        if label:
            if label not in label_map:
                label_map[label] = []
            label_map[label].append(entity_id)
    return label_map

def build_entity_handler(repository, environment_specs, configurations, logger):
    """
    Instantiates all handler classes from the registry using shared dependencies.

    Args:
        repository: shared data repository
        environment_specs:
        configurations: global configuration dictionary
        logger: logger instance used for logging

    Returns:
        dict[str, EntityHandlerBase]: mapping from label to handler instance
    """

    entities_ids = _build_label_to_ids(environment_specs)

    handlers = {}
    for handler_class in ENTITY_HANDLER_REGISTRY.values():
        label = handler_class.label
        handlers[label] = handler_class(
            repository=repository,
            logger=logger,
            entities_ids=entities_ids,
            environment_specs=environment_specs,
            configurations=configurations
        )
    return handlers
