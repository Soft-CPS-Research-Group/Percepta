# --- Standard Library Imports ---
from abc import ABC, abstractmethod
from typing import Any, Union
from zoneinfo import ZoneInfo

# --- Internal Framework Imports ---
from app.core.utils.logger import LoggingUtils


class EndpointBuilderBase(ABC):
    """Abstract baseline class for all provider endpoint and topology builders.

    Centralizes dependency injection and strict verification for:
    - Operational logger interface
    - Target execution timezone
    - Environment identifier
    - Base topology/blueprint schema
    - Environment entity specifications
    - Source mapping translation rules
    """

    _LOG_PREFIX: str = "EndpointBuilderBase |"

    def __init__(
        self,
        base_topology_template: Union[dict, list],
        environment_name: str,
        environment_specs: dict,
        source_mapping_rules: dict,
        logger: LoggingUtils,
        timezone: str = "UTC"
    ) -> None:
        """Initializes the base builder and enforces mandatory contractual checks.

        Args:
            base_topology_template (Union[dict, list]): Base topology template blueprint.
            environment_name (str): Unique identifier of the target environment.
            environment_specs (dict): Specification dictionary describing environment entities.
            source_mapping_rules (dict): Dictionary mapping entity/parameter keys to hardware descriptors.
            logger (LoggingUtils): Structured telemetry logging interface.
            timezone (str, optional): Target execution timezone. Defaults to "UTC".

        Raises:
            ValueError: If mandatory parameters are missing, empty, or structurally invalid.
            TypeError: If input arguments violate expected types.
        """
        # --- Guard Clause 1: Validate Logger Dependency ---
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        self._logger = logger
        self._tz = ZoneInfo(timezone)
        self._timezone_str = timezone

        # --- Guard Clause 2: Validate environment_name ---
        if not environment_name or not isinstance(environment_name, str) or not environment_name.strip():
            error_msg = f"{self._LOG_PREFIX} Initialization error: 'environment_name' must be a valid non-empty string."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 3: Validate base_topology_template ---
        if not base_topology_template or not isinstance(base_topology_template, (dict, list)):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'base_topology_template' must be a non-empty "
                f"dict or list for environment '{environment_name}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 4: Validate environment_specs ---
        if not isinstance(environment_specs, dict) or "entities" not in environment_specs:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'environment_specs' must be a dictionary "
                f"containing an 'entities' key for environment '{environment_name}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        entities_dict = environment_specs.get("entities")
        if not isinstance(entities_dict, dict) or not entities_dict:
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'entities' specification cannot be empty "
                f"for environment '{environment_name}'."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        # --- Guard Clause 5: Validate source_mapping_rules ---
        if not isinstance(source_mapping_rules, dict):
            error_msg = (
                f"{self._LOG_PREFIX} Initialization error: 'source_mapping_rules' must be a dictionary, "
                f"got {type(source_mapping_rules).__name__}."
            )
            self._logger.error(error_msg)
            raise TypeError(error_msg)

        # Store validated baseline properties
        self._environment_name = environment_name
        self._environment_specs = environment_specs
        self._entities = entities_dict
        self._source_mapping_rules = source_mapping_rules
        self._base_template = base_topology_template

    @abstractmethod
    async def build_endpoints(self) -> Any:
        """Asynchronously builds and returns the resolved topology or dynamic endpoints."""
        pass