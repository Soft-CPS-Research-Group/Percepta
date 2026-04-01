import datetime
import pandas as pd
from zoneinfo import ZoneInfo
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils



class CWTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for CW entities.
    Handles translation of entity messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _entities: dict  # Stores entities defined in the environment specifications
    _labels_functions_mapper : dict # Maps labels to corresponding processing functions
    _tz : ZoneInfo

    def __init__(self, environment: str,  environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the CWTranslator.

        Args:
            environment (str): String to identify the environment which the data belongs.
            environment_specs (dict): Environment specifications including entities.
            configurations (dict): General configurations passed to the translator.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment, configurations, logger)

        self._entities = environment_specs.get('entities')

        # Map each data label to its corresponding processing function
        self._labels_functions_mapper = {
            "ev_charger": self._ev_charger
        }

    def _build_result(self, param_values_list, default_value=0):
        if not param_values_list:
            return []

        df = pd.DataFrame(param_values_list)
        if 'Value' not in df.columns:
            df['Value'] = df.get('Read', default_value)

        try:
            # Try to localize and convert timezone normally
            df['timestamp'] = pd.to_datetime(df['Date']).dt.tz_localize('UTC').dt.tz_convert(self._tz)
        except TypeError as e:
            # Handle case where the datetime is already timezone-aware
            print(f"[Warning] Failed to localize timezone: {e}")
            print("Problematic dates:")
            for date_str in df['Date']:
                print(f"  - {date_str}")
            print("Attempting to convert existing timezone instead...")
            df['timestamp'] = pd.to_datetime(df['Date']).dt.tz_convert(self._tz)

        df['timestamp'] = df['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S %z")
        return df[['timestamp', 'Value']].rename(columns={'Value': 'value'}).to_dict(orient='records')

    def _ev_charger(self, entity_id, messages: dict) -> dict:
        """
        Processes EV charger sessions and converts them into a standardized dictionary of readings.

        Args:
            messages (dict): Dictionary representing a charging session with various measurements.

        Returns:
            dict: Dictionary where each param maps to its corresponding 'Read' value (or None if not valid).
        """

        # Ensure the input is a dictionary; raise an exception otherwise
        if not isinstance(messages, dict):
            raise TypeError(f"Translator | _ev_charger expected dict, got {type(messages)}")

        # Retrieve the 'session_status' param without modifying the original messages dictionary
        session_status_list = messages.pop("session_status", []) # TODO aqui poderia estar pop acho eu
        session_status = {}

        # Extract the first dictionary from 'session_status' list if it exists
        if isinstance(session_status_list, list) and session_status_list:
            first_item = session_status_list[0]
            if isinstance(first_item, dict):
                session_status = first_item

        # Extract the "Read" flag from session_status to determine if the session is valid (ready)
        read_status = session_status.get("Read") if session_status.get("Read") else session_status.get("Value", None)
        current_time = datetime.datetime.now()

        if read_status == 1:
            date_str = session_status.get("Date")
            if date_str:
                try:
                    # Convert string to datetime object for math operations
                    self._last_session_status = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # Fallback to current time if string format is invalid
                    self._last_session_status = current_time
            else:
                self._last_session_status = current_time

        # 2. If status is missing (None), check if we are within the 15-minute window
        elif read_status is None:
            if self._last_session_status and isinstance(self._last_session_status, datetime.datetime):
                # Calculate the difference between now and the last recorded '1' status
                if (current_time - self._last_session_status) <= datetime.timedelta(minutes=15):
                    read_status = 1
                else:
                    # Time expired, reset the status and clear memory
                    read_status = 0
                    self._last_session_status = None
            else:
                read_status = 0

        value = {}

        if read_status == 1:
            for param, param_values_list in messages.items():
                if isinstance(param_values_list, list) and param_values_list:
                    value[param] = self._build_result(param_values_list)
                else:
                    self._logger.warning(
                        f"Parameter '{param}' of entity '{entity_id}' is missing."
                    )
                    value[param] = []

            value['electric_vehicle'] = self._entities.get('parameters').get('electric_vehicle').get("id")
        else:
            for param, param_values_list in messages.items():
                value[param] = self._build_result(param_values_list)

            value['electric_vehicle'] = ""

        self._parameters_validation(value, self._entities_parameters, ['session_status'])

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

        # Format timestamp using the configured timezone
        timestamp = datetime.datetime.now(self._tz).strftime("%Y-%m-%d %H:%M:%S")

        if not isinstance(messages, dict):
            raise TypeError(f"Translator | translate expected dict, got {type(messages)}")

        entity_id = messages.get("entity_id")
        label = messages.get("label")
        parameters_readings = messages.get("parameters")

        #print(f"TESTEEEE {messages}")
        self._entities_parameters = self._entities.get(entity_id).get('parameters')

        value = {}

        if label in self._labels_functions_mapper:
            # Use label-specific translation method
            value = self._labels_functions_mapper[label](entity_id, parameters_readings)
        else:
            # Default translation logic: sum all readings for each parameter
            for param, param_values_list in parameters_readings.items():
                if param:
                    if isinstance(param_values_list, list) and param_values_list:
                        value[param] = self._build_result(param_values_list)
                else:
                    self._logger.warning(
                        f"Parameter '{param}' of entity '{entity_id}' is missing."
                    )
                    value[param] = []

            self._parameters_validation(value, self._entities_parameters)

        # Construct standardized message with ID, values, and timestamp
        new_message = [{
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }]

        # Send the message to the environment queue
        self.send_message_to_environment_queue(new_message)


