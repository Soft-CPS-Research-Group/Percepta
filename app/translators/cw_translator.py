import datetime
from zoneinfo import ZoneInfo
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils


class CWTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for CW devices.
    Handles translation of device messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _labels_functions_mapper : dict

    def __init__(self, environment: str, configurations: dict, logger: LoggingUtils):
        """
        Initialize the CWTranslator with environment, configuration, and logger.
        Sets up label-function mapping for translation logic.
        """
        super().__init__(environment, configurations, logger)

        # Map labels to their corresponding translation methods
        self._labels_functions_mapper = {
            "ev_charger": self._ev_charger
        }

    def _ev_charger(self, messages : dict) -> dict:
        """
        Process messages for EV chargers with specific translation logic.

        Args:
        messages (dict): Dictionary containing device data, where keys represent
                         measurement types and values are lists of readings.

        Returns:
        dict: Processed values where each key maps to its corresponding "Read" value.
        """
        if not isinstance(messages, dict):
            raise TypeError(f"Cleanwatts | _ev_charger expected dict, got {type(messages)}")

        # Retrieve 'session_status' without removing it from the original dictionary
        session_status_list = messages.get("session_status", [])
        session_status = {}
        if isinstance(session_status_list, list) and session_status_list:
            first_item = session_status_list[0]
            if isinstance(first_item, dict):
                session_status = first_item

        # Extract the "Read" flag from session_status to determine readiness
        read_status = session_status.get("Read", 0)

        value = {}
        if read_status == 1:
            # If the session is valid (read_status == 1), process each measurement key
            for key, item_list in messages.items():
                if isinstance(item_list, list) and item_list:
                    first_item = item_list[0]
                    read_value = first_item.get("Read", 0) if isinstance(first_item, dict) else 0
                    value[key] = read_value
                else:
                    # Default to 0 if the list is empty or invalid
                    value[key] = 0
        else:
            # If the session is not valid, all values default to 0
            for key in messages:
                value[key] = 0

        return value

    def translate(self, messages : dict, label : str, entity_id : str) -> None:
        """
        Translate incoming device messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
        messages (dict): Dictionary containing device data with keys as parameters
                         and values as lists of readings.
        label (str): Identifier for the type of device or data (e.g., "ev_charger").
        entity_id (str): Unique identifier for the device.

        Returns:
        None: The method sends the translated message to the environment queue.
        """
        if not isinstance(messages, dict):
            raise TypeError(f"Cleanwatts | translate expected dict, got {type(messages)}")

        value = {}

        # Retrieve timezone from configuration, defaulting to UTC if invalid
        tz_name = self._configurations.get("timezone", "UTC")
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            self._logger.warning(f"Cleanwatts | Invalid timezone '{tz_name}', falling back to UTC")
            tz = ZoneInfo("UTC")

        # Format timestamp using the configured timezone
        timestamp = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        if label in self._labels_functions_mapper:
            # Use label-specific translation method
            value = self._labels_functions_mapper[label](messages)
        else:
            # Default translation logic: sum all readings for each parameter
            for key, params in messages.items():
                if key:
                    total = 0
                    if isinstance(params, list):
                        for reading in params:
                            if isinstance(reading, dict):
                                total += reading.get("Read", 0)
                    value[key] = total
                else:
                    self._logger.warning(f"Cleanwatts | No data for {entity_id} device.")
                    return

        # Construct standardized message with ID, values, and timestamp
        new_message = [{
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }]

        # Send the message to the environment queue
        self.send_message_to_environment_queue(new_message)

