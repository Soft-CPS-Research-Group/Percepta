import datetime
import json
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils
from app.utils.labels import Label

class ICTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for IC entities.

    Handles translation of entity messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _entities: dict  # Stores entities defined in the environment specifications
    _labels_functions_mapper: dict  # Maps labels to corresponding processing functions

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils) -> None:
        """
        Initializes the ICTranslator.

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
            "pv.production": self._pv_panel,
            "battery.soc": self._battery,
            "meter.values": self._meter,
            "charging.session": self._ev_charger
        }

    def _ev_charger(self, charging_sessions: list, timestamp: str) -> list:
        """
        Processes EV charger sessions and converts them into messages.

        Args:
            charging_sessions (list): List of charging session dictionaries.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List of formatted messages for valid charging sessions.
        """
        messages: list = []

        for charging_session in charging_sessions:
            # Get serial number and plug from the current session
            serial: str = charging_session.get("serialnumber")
            plug: int = charging_session.get("plug")

            # Skip if any of the required fields is missing
            if not serial or plug is None:
                continue

            # Build the entity key (e.g., "AC000001_1")
            entity_id: str = f"{serial}_{plug}"
            #print(f"ENTITIES: {self._entities}\n")
            entity_data: dict = self._entities.get(entity_id)

            if entity_data:
                # Check if the label matches "ev_charger" (double verification: confirms if the id and label matches)
                label: str = entity_data.get("label")

                if label == Label.EV_CHARGER.value:
                    _entity_parameters: dict = entity_data.get('parameters')

                    # Prepare the message data
                    value: dict = {
                        "power": [{
                            "timestamp": timestamp,
                            "value": charging_session.get("power")
                            }],
                        "electric_vehicle": charging_session.get("user.id")
                    }

                    self._parameters_validation(value, _entity_parameters)

                    # Create and add the message
                    messages.append(ICTranslator._message_creator(value, entity_id, timestamp))
                else:
                    # Log error if label does not match
                    self._logger.warning(f"Translator | Entity found, but label mismatch: {entity_id} (label: {label}). The program will ignore this entity.")
            else:
                # Log error if entity is not found in the configuration
                self._logger.warning(f"Translator | Entity {entity_id} not found in the configuration file. The program will ignore this entity.")
                continue

        return messages

    # TODO e se tiver mais do que um PV Panel? Neste momento a ic envia os dados como se só existisse um...
    def _pv_panel(self, pv_production: float, timestamp: str) -> list:
        """
        Processes PV panel production data and creates messages.

        Args:
            pv_production (float): Power produced by the PV panel.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List containing one message per PV panel entity (At this moment, it is considered to be only one PV panel).
        """

        # Iterate over entities dictionary (key=entity_id, value=entity_data)
        for entity_id, entity_data in self._entities.items():
            # Check if this entity has the label "pv_panel"
            if entity_data.get("label") == Label.PV_PANEL.value:
                _entity_parameters: dict = entity_data.get('parameters')

                # Create and return a message using pv_production data and timestamp
                value: dict = {
                    "energy": [{
                        "timestamp": timestamp,
                        "value": pv_production
                    }]
                }

                self._parameters_validation(value, _entity_parameters)

                return [ICTranslator._message_creator(value, entity_id, timestamp)]

        return []

    def _battery(self, battery_soc: float, timestamp: str) -> list:
        """
        Processes battery state-of-charge (SoC) data and creates messages.

        Args:
            battery_soc (float): Current battery SoC.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List containing one message per battery entity (At this moment, it is considered to be only one battery).
        """

        # Iterate over entities dictionary (key=entity_id, value=entity_data)
        for entity_id, entity_data in self._entities.items():
            # Check if this entity has the label "battery"
            if entity_data.get("label") == Label.BATTERY.value:
                _entity_parameters: dict = entity_data.get('parameters')
                # Create and return a message using battery_soc data and timestamp
                # TODO no caso da i-charging nao tenho a energia, o que faço?

                value: dict = {
                    "SoC": [{
                        "timestamp": timestamp,
                        "value": battery_soc
                    }]
                }

                self._parameters_validation(value, _entity_parameters)

                return [ICTranslator._message_creator(value, entity_id, timestamp)]

        return []

    def _meter(self, meters_list: list, timestamp: str) -> list:
        """
        Processes grid meter readings and creates messages.

        Args:
            meters_list (list): List of meter reading dictionaries.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List of formatted messages for each valid grid meter.
        """
        messages: list = []

        for meter in meters_list:
            # Get id from the current meter
            entity_id: str = meter.get("id")

            # Skip if id is None
            if entity_id is None:
                continue

            entity_data: dict = self._entities.get(entity_id)

            if entity_data:
                # Check if the label matches "grid_meter"
                label: str = entity_data.get("label")
                if label == Label.GRID_METER.value:
                    _entity_parameters: dict = entity_data.get('parameters')

                    # Prepare the message data
                    value: dict = {
                        "energy_in_total": [{
                            "timestamp": timestamp,
                            "value": meter.get("l123")
                        }]
                    }

                    self._parameters_validation(value, _entity_parameters)

                    # Create and add the message
                    messages.append(ICTranslator._message_creator(value, entity_id, timestamp))
                else:
                    # Log error if label does not match
                    self._logger.warning(f"Translator | Entity found, but label mismatch: {entity_id} (label: {label}). The program will ignore this entity.")
            else:
                # Log error if entity is not found in the configuration
                self._logger.warning(f"Translator | Entity {entity_id} not found in the configuration file. The program will ignore this entity.")

        return messages

    def translate(self, message: bytes) -> None:
        """
        Translates incoming entity messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
            message (bytes): Dictionary containing i-charging-format environment data, encoded as bytes.
        """
        #print("TRANSLATE CHAMADO\n")
        # Decode the incoming bytes message to a UTF-8 string, then parse it as JSON.
        # Extract the 'observation' key which contains the relevant data.
        message_dict: dict = json.loads(message.decode('utf-8')).get('observation')

        # Generate a timestamp for when the message is being processed.
        timestamp: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Initialize an empty list that will hold the translated messages.
        message_list: list = []
        self._logger.info(f"MESSAGES DICT {message_dict}")
        # Iterate through each attribute in the parsed message dictionary.
        for attr in message_dict:

            # Check if there is a specific processing function mapped for this attribute.
            if attr in self._labels_functions_mapper:
                # Retrieve the function assigned to handle this attribute.
                func = self._labels_functions_mapper[attr]

                # Call the function with the attribute value and the current timestamp.
                # The function is expected to return a list of processed data.
                attr_processed: list = func(message_dict.get(attr), timestamp)

                # Only add the processed attribute data to the message_list if it is not empty.
                # This avoids adding empty lists or dictionaries.
                if attr_processed:
                    message_list.extend(attr_processed)

        self._logger.info(f"MESSAGES LIST {message_list}")

        # Send the final standardized message list to the environment queue.
        self.send_message_to_environment_queue(message_list)

    @staticmethod
    def _message_creator(value: dict, entity_id: str, timestamp: str) -> dict:
        """
        Creates a standardized message format for an entity reading.

        Args:
            value (dict): Dictionary containing entity-specific values.
            entity_id (str): Unique entity identifier.
            timestamp (str): Current timestamp for the message.

        Returns:
            dict: Standardized message dictionary.
        """
        new_message: dict = {
            "id": entity_id,
            "value": value,
            "timestamp": timestamp
        }

        return new_message

# Em caso de erro corro o risco da mensagem ser enviada duas vezes, mas não é um problema
