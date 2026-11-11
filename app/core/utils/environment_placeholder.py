from typing import Any


def replace_environment(template: str, environment_name: str) -> str:
    """Replaces the '{environment}' placeholder of a name by the name of the environment.

    It is used to give each environment its own exchange and queues, from a single configuration.

    Examples:
    ---------
    "percepta.data_ingestion.{environment}" with environment "R-H-01" -> "percepta.data_ingestion.R-H-01"
    "" with environment "R-H-01" -> ""                       # Nothing to replace

    Args:
        template (str): A name (or routing key) that may contain '{environment}'.
        environment_name (str): Name of the environment.

    Returns:
        str: The name, with the placeholder replaced.
    """
    return template.replace("{environment}", environment_name)


def replace_environment_in(value: Any, environment_name: str) -> Any:
    """Replaces the '{environment}' placeholder in every text of a structure, whatever its protocol or layout.

    It goes through dictionaries and lists, at any depth, and replaces the placeholder in each text it finds (see
    `replace_environment`). It is used to give each environment its own destination (e.g., its own exchange and
    queues in RabbitMQ) from a single configuration, without assuming the structure of the topology of any protocol.

    Only the values are replaced: the keys of a dictionary are left as they are. What is not a text, a dictionary or
    a list (numbers, booleans, None) is returned as it is. The structure that is given is not changed: a new one is
    returned.

    Example (environment "R-H-01"):
    -------------------------------
    {"exchanges": [{"exchange_name": "percepta.{environment}", "queues": [{"queue_name": "q.{environment}"}]}]}
    -> {"exchanges": [{"exchange_name": "percepta.R-H-01", "queues": [{"queue_name": "q.R-H-01"}]}]}

    {"url": "http://host/{environment}/data", "retries": 3} -> {"url": "http://host/R-H-01/data", "retries": 3}

    Args:
        value (Any): The structure (or text) to replace in, e.g., the 'topology' of a 'connection_info'.
        environment_name (str): Name of the environment.

    Returns:
        Any: A copy of the structure with the placeholder replaced in every text.
    """
    if isinstance(value, str):
        return replace_environment(value, environment_name)
    if isinstance(value, dict):
        return {key: replace_environment_in(item, environment_name) for key, item in value.items()}
    if isinstance(value, list):
        return [replace_environment_in(item, environment_name) for item in value]
    return value
