import json
from app.utils.logger import LoggingUtils

# Strategy handlers: links JSON strategy names to their respective logic methods
_STRATEGIES = {}

def register_strategy(name):
    """Decorator to register a strategy method into the class mapping."""

    def decorator(func):
        _STRATEGIES[name] = func
        return func

    return decorator

class SourceMapper:
    """
    SourceMapper is the address resolution engine of the Percepta framework.

    Its primary responsibility is to translate internal system identities (Environments,
    Entities, or Parameters) into identifiers understood by external Data Providers
    (such as RabbitMQ Exchanges, MQTT Topics, or technical API IDs).

    It acts as an abstraction layer: the rest of the system uses logical names,
    while this class resolves where that data resides in the actual infrastructure.
    """

    _LOG_PREFIX = "| Source Mapper"



    def __init__(self, environment_name: str, environment_specs: dict, source_mapping: dict, logger: LoggingUtils):
        """
        Initializes the mapper with environment-specific rules and specifications.

        Args:
            environment_name (str): String to identify the environment which the data belongs.
            environment_specs (dict): Specifications for the environment, including entities.
            source_mapping: Configuration dictionary containing strategy and override rules.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        self._environment_name : str = environment_name
        self._logger : LoggingUtils = logger

        environment_source_mapping = source_mapping.get(self._environment_name) # TODO está aqui mas não deveria
        # Define the granularity of resolution (Environment, Entity, or Parameter)
        self._strategy : str = environment_source_mapping.get('strategy', 'environment')
        self._rules : dict = environment_source_mapping.get('rules', {})
        self._entities : dict = environment_specs.get('entities', {})


        '''self._logger.info(
            f"{self._LOG_PREFIX} Initialized for '{self._environment_name}' | Strategy: {self._strategy}")'''

    def resolve_address(self) -> dict:
        """
        Main entry point to obtain the addressing map.

        This method merges automatic strategy resolution with explicit manual overrides.
        Explicit rules take precedence over default strategy logic.
        """
        # 1. First, get the handler for the current strategy
        handler = _STRATEGIES.get(self._strategy)

        if not handler:
            self._logger.error(f"{self._LOG_PREFIX} Unsupported strategy: '{self._strategy}'")
            raise ValueError(f"Mapping strategy '{self._strategy}' is not supported.")

        # 2. Get the default mapping based on the strategy
        # We pass 'self' because strategies are stored as functions in the class dict
        resolved_mapping = handler(self)

        # 3. Merge with explicit rules (Overrides)
        # If a key exists in both, self._rules wins.
        if self._rules:
            self._logger.info(f"{self._LOG_PREFIX} Merging default strategy with explicit rules.")
            # The update method replaces existing keys and adds new ones
            resolved_mapping.update(self._rules)

        # Log the final result
        self._logger.debug(
            f"{self._LOG_PREFIX} Final Mapping for '{self._environment_name}':\n{json.dumps(resolved_mapping, indent=4)}")

        return resolved_mapping

    @register_strategy("environment")
    def _environment_strategy(self) -> dict:
        """
        Resolves addresses at the Environment level.
        Ideal for Providers where all data from an installation flows through a single channel.
        """
        return {self._environment_name: self._environment_name}

    @register_strategy("entity")
    def _entity_strategy(self) -> dict:
        """
        Resolves addresses at the Entity level.
        Suitable for IoT devices where each piece of equipment has its own communication channel.
        """
        return {name: name for name in self._entities.keys()}

    @register_strategy("parameter")
    def _parameter_strategy(self) -> dict:
        """
        Returns an empty map to force reliance on explicit 'source_mapping' rules.
        This prevents the Receiver from requesting parameters that lack a
        technical ID (e.g., virtual or internal-only parameters).
        """
        return {}