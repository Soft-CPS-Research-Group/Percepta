from .entity_handler_base import EntityHandlerBase
from app.utils.registry_auto import discover_subclasses
from typing import Type, Dict

# Dynamically build a registry of all available handlers in the package
ENTITY_HANDLER_REGISTRY: Dict[str, Type[EntityHandlerBase]] = discover_subclasses(
    package="app.entity_handlers",
    base_class=EntityHandlerBase,
    required_suffix="_handler"
)


def build_entity_handler(repository, entities_ids, configurations, logger):
    """
    Instantiates all handler classes from the registry using shared dependencies.

    Args:
        repository: shared data repository
        entities_ids: dictionary mapping labels to lists of entity IDs
        configurations: global configuration dictionary
        logger: logger instance used for logging

    Returns:
        dict[str, EntityHandlerBase]: mapping from label to handler instance
    """
    handlers = {}
    for handler_class in ENTITY_HANDLER_REGISTRY.values():
        label = handler_class.label
        handlers[label] = handler_class(
            repository=repository,
            logger=logger,
            entities_ids=entities_ids,
            configurations=configurations
        )
    return handlers
