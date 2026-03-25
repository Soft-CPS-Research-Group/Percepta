from .forwarder_base import ForwarderBase
from app.utils.registry_auto import discover_subclasses
from typing import Type, Dict
from app.repositories.irepositories.environment_repository import EnvironmentRepository

# Dynamically build a registry of all available forwarders in the package
FORWARDER_REGISTRY: Dict[str, Type[ForwarderBase]] = discover_subclasses(
    package="app.forwarders",
    base_class=ForwarderBase,
    required_suffix="_forwarder"  # exclude files such as __init__.py, base.py, factory.py
)


def build_forwarder(environment, environment_specs, all_provider_configs, configurations, logger) -> dict:
    """
    Instantiates all forwarder classes from the registry using shared dependencies.

    Args:
        environment (Environment): Environment to use
        environment_specs (EnvironmentSpec): Environment spec to use
        all_provider_configs (dict): All provider configurations
        configurations: global configuration object or dict
        logger: logger instance used for tracking/logging

    Returns:
        dict[str, ]: mapping from label to forwarder instance
    """

    """
    Agrupa as entidades do environment_specs por provider.

    Returns:
        dict: { 'provider_name': { 'group': '...', 'entities': { ... } } }
    """
    grouped_data = {}
    group_name = environment_specs.get('group')
    entities = environment_specs.get('entities', {})

    for entity_id, entity_info in entities.items():
        provider = entity_info.get('provider')

        if not provider:
            continue  # Ou tratar erro caso o provider seja obrigatório

        # Se o provider ainda não está no dicionário, inicializamos a estrutura
        if provider not in grouped_data:
            grouped_data[provider] = {
                'group': group_name,
                'entities': {}
            }

        # Adicionamos a entidade ao provider correspondente
        grouped_data[provider]['entities'][entity_id] = entity_info

    forwarders = {}
    for forwarder_class in FORWARDER_REGISTRY.values():
        provider = forwarder_class.provider
        provider_configurations = all_provider_configs.get(provider, {})
        if provider_configurations: # Basicamente isto está meio roubado, eu verifico se existe a configuração, se ela exister é pq existe o ambiente mas isto implica que exista sempre configurations nos ficheiros dos providers
            merged_config = {**configurations, **provider_configurations}

            forwarders[provider] = forwarder_class(
                environment=environment,
                environment_specs=grouped_data.get(provider, {}),
                configurations=merged_config,
                logger=logger
            )
    return forwarders
