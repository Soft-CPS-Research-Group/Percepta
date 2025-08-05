from .forwarder_base import ForwarderBase
from app.utils.registry_auto import discover_subclasses
from typing import Type, Dict

# Dynamically build a registry of all available forwarders in the package
FORWARDER_REGISTRY: Dict[str, Type[ForwarderBase]] = discover_subclasses(
    package="app.forwarders",
    base_class=ForwarderBase,
    required_suffix="_forwarder"  # exclude files such as __init__.py, base.py, factory.py
)


def build_forwarder(configurations, logger):
    """
    Instantiates all forwarder classes from the registry using shared dependencies.

    Args:
        configurations: global configuration object or dict
        logger: logger instance used for tracking/logging

    Returns:
        dict[str, ]: mapping from label to forwarder instance
    """
    forwarders = {}
    for forwarder_class in FORWARDER_REGISTRY.values():
        provider = forwarder_class.provider
        forwarders[provider] = forwarder_class(
            configurations=configurations,
            logger=logger
        )
    return forwarders
