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
