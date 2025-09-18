import datetime
from zoneinfo import ZoneInfo
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils


class CWTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for CW entities.
    Handles translation of entity messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _labels_functions_mapper : dict # Maps labels to corresponding processing functions

    def __init__(self, environment: str, configurations: dict, logger: LoggingUtils):
        """
        Initializes the CWTranslator.

        Args:
            environment (str): String to identify the environment which the data belongs.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment, configurations, logger)

        # Map each data label to its corresponding processing function
        self._labels_functions_mapper = {
            "ev_charger": self._ev_charger
        }

    def _ev_charger(self, messages: dict) -> dict:
        """
        Processes EV charger sessions and converts them into a standardized dictionary of readings.

        Args:
            messages (dict): Dictionary representing a charging session with various measurements.

        Returns:
            dict: Dictionary where each key maps to its corresponding 'Read' value (or None if not valid).
        """

        # Ensure the input is a dictionary; raise an exception otherwise
        if not isinstance(messages, dict):
            raise TypeError(f"Translator | _ev_charger expected dict, got {type(messages)}")

        # Retrieve the 'session_status' key without modifying the original messages dictionary
        session_status_list = messages.get("session_status", [])
        session_status = {}

        # Extract the first dictionary from 'session_status' list if it exists
        if isinstance(session_status_list, list) and session_status_list:
            first_item = session_status_list[0]
            if isinstance(first_item, dict):
                session_status = first_item

        # Extract the "Read" flag from session_status to determine if the session is valid (ready)
        read_status = session_status.get("Read", 0)

        value = {}

        if read_status == 1:
            # If the session is valid, process each key in the messages dictionary
            for key, item_list in messages.items():
                if isinstance(item_list, list) and item_list:
                    # TODO aqui estou a considerar apenas um último registo, no futuro teremos vários e depois é necessário aplicar harmonização e fazer distinção entre a potência e a energia
                    first_item = item_list[0]
                    # Extract the 'Read' value from the first item if it's a dictionary; otherwise default to 0
                    read_value = first_item.get("Read", 0) if isinstance(first_item, dict) else 0
                    value[key] = read_value
                else:
                    # Default to 0 if the item list is empty or invalid
                    value[key] = 0
        else:
            # If the session is not valid, all measurement values default to 0
            for key in messages:
                value[key] = 0

        # Return the dictionary of processed 'Read' values
        return value

    def translate(self, messages : dict) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
        messages (dict): A dictionary containing the entity identifier,
                        the data type identifier, and the corresponding readings,
                        where keys represent parameters and values are lists of readings.
        """
        if not isinstance(messages, dict):
            raise TypeError(f"Translator | translate expected dict, got {type(messages)}")

        entity_id = messages.get("entity_id")
        label = messages.get("label")
        parameters = messages.get("parameters")

        value = {}

        # Retrieve timezone from configuration, defaulting to UTC if invalid
        tz_name = self._configurations.get("timezone", "UTC")
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            self._logger.warning(f"Translator | Invalid timezone '{tz_name}', falling back to UTC")
            tz = ZoneInfo("UTC")

        # Format timestamp using the configured timezone
        timestamp = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        if label in self._labels_functions_mapper:
            # Use label-specific translation method
            value = self._labels_functions_mapper[label](parameters)
        else:
            # Default translation logic: sum all readings for each parameter
            for key, params in parameters.items():
                if key:
                    total = 0
                    if isinstance(params, list):
                        for reading in params:
                            if isinstance(reading, dict):
                                total += reading.get("Read", 0)
                    value[key] = total
                else:
                    self._logger.warning(f"Translator | No data for {entity_id} entity.")
                    return

        # Construct standardized message with ID, values, and timestamp
        new_message = [{
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }]

        # Send the message to the environment queue
        self.send_message_to_environment_queue(new_message)

