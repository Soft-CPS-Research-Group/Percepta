from abc import abstractmethod
from app.utils.logger import LoggingUtils
from typing import Any
from app.utils.measurement_unit_validation import is_type_compatible

def validate_temporal_list(value_list : list, measurement_unit : str):
    """
    Validates a list of dictionaries with 'timestamp' and 'value'.
    Uses `is_type_compatible` for each value.
    Returns True only if all values are compatible.
    """
    if not isinstance(value_list, list):
        return False

    for entry in value_list:
        if not isinstance(entry, dict):
            return False
        val = entry.get("value")
        if not is_type_compatible(val, measurement_unit):
            return False
    return True

class TranslatorBase:
    """
    Abstract base class for implementing a translator service.
    """
    _environment : str # String to identify the environment which the data belongs
    _internal_message_hub_server: dict  # Configuration details for the internal message hub (for example, AMQP server)
    _configurations: dict  # General configurations passed to the translator
    _logger: LoggingUtils  # Logger instance for structured logging

    def __init__(self, environment : str, configurations: dict, logger: LoggingUtils):
        """Initializes the translator with configuration and logging details.

        Args:
            environment (str): String to identify the environment which the data belongs.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        self._environment = environment
        self._internal_message_hub_server = configurations.get('internal_amqp_server').get('server')
        self._configurations = configurations
        self._logger = logger

    @abstractmethod
    def translate(self, data : Any) -> None:
        """
           Translate incoming device messages into a standardized format.

           Args:
               data (Any): Raw data from the source system.
                   Each subclass is responsible for handling its expected type:
                   - bytes (JSON encoded string with "observation")
                   - dict with "messages", "label", "entity_id"
           """
        raise NotImplementedError()

    def _parameters_validation(self, parameters_to_send: dict, entity_parameters: dict, optional_parameters: list = None) -> dict:
        """
        Validates that all required entity parameters are present in messages.
        Adds NaN for missing parameters or empty lists, except for optional ones.
        Validates parameter values and adds metadata if invalid.

        Args:
            parameters_to_send: Dictionary of parameter values to validate.
            entity: Dictionary of entity definitions.
            optional_parameters: List of parameter names that are optional.

        Returns:
            The updated dictionary with missing/invalid parameters handled.
        """
        if optional_parameters is None:
            optional_parameters = []

        for param_name, param_info in entity_parameters.items():
            # Skip optional parameters that are not in the message
            if param_name in optional_parameters and param_name not in parameters_to_send:
                continue

            if param_name not in parameters_to_send:
                # Required parameter missing → add NaN with metadata
                parameters_to_send[param_name] = {
                    "value": float('nan'),
                    "metadata": "Parameter missing in receiver response"
                }
            else:
                param_values = parameters_to_send.get(param_name)
                # Treat empty list as NaN with metadata
                if isinstance(param_values, list) and len(param_values) == 0:
                    parameters_to_send[param_name] = {
                        "value": float('nan'),
                        "metadata": "Parameter sent empty by receiver"
                    }
                    continue
                measurement_unit = param_info.get("measurementUnit")
                if measurement_unit is not None:
                    if not validate_temporal_list(param_values, param_info.get("measurementUnit")):
                        # Invalid values → set NaN with metadata
                        parameters_to_send[param_name] = {
                            "value": float('nan'),
                            "metadata": "Parameter value invalid"
                        }

        return parameters_to_send
