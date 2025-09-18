import datetime
import json
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils

class ICTranslator(TranslatorRabbitMQBase):
    """
    Concrete implementation of a translator for IC devices.
    Handles translation of device messages into a standardized format,
    applying special rules for specific labels such as EV chargers.
    """

    _entities: dict
    _labels_functions_mapper: dict

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils) -> None:
        """
        Initializes the ICTranslator.

        Args:
            environment (str): Name of the environment.
            environment_specs (dict): Specifications including device entities.
            configurations (dict): Translator configurations.
            logger (LoggingUtils): Logger instance for error/info logging.
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
        Process EV charger sessions and convert them into messages.

        Args:
            charging_sessions (list): List of charging session dictionaries.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List of formatted messages for valid charging sessions.
        """
        messages: list = []

        cs_label: str = "ev_charger"

        for charging_session in charging_sessions:
            # Get serial number and plug from the current session
            serial: str = charging_session.get("serialnumber")
            plug: int = charging_session.get("plug")

            # Skip if any of the required fields is missing
            if not serial or plug is None:
                continue

            # Build the device key (e.g., "AC000001_1")
            device_id: str = f"{serial}_{plug}"
            device_data: dict = self._entities.get(device_id)

            if device_data:
                # Check if the label matches "charging_session"
                label: str = device_data.get("label")
                if label == cs_label:
                    # Prepare the message data
                    value: dict = {
                        "power": charging_session.get("power"),
                        "user_id": charging_session.get("user.id")
                    }
                    # Create and add the message
                    messages.append(ICTranslator._message_creator(value, device_id, timestamp))
                else:
                    # Log error if label does not match
                    self._logger.error(f"ICTranslator: Device found, but label mismatch: {device_id} (label: {label})")
            else:
                # Log error if device is not found in the configuration
                self._logger.error(f"ICTranslator: Device {device_id} not found in the configuration file.")

        return messages

    # TODO e se tiver mais do que um PV Panel? Neste momento a ic envia os dados como se só existisse um...
    def _pv_panel(self, pv_production: float, timestamp: str) -> list:
        """
        Process PV panel production data and create messages.

        Args:
            pv_production (float): Power produced by the PV panel.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List containing one message per PV panel device.
        """
        pv_label: str = "pv_panel"

        # Iterate over devices dictionary (key=device_id, value=device_data)
        for device_id, device_data in self._entities.items():
            # Check if this device has the label "pv_panel"
            if device_data.get("label") == pv_label:
                # Create and return a message using pv_production data and timestamp
                value: dict = {
                    "solar_generation": pv_production
                }
                return [ICTranslator._message_creator(value, device_id, timestamp)]

        return []

    def _battery(self, battery_soc: float, timestamp: str) -> list:
        """
        Process battery state-of-charge (SoC) data and create messages.

        Args:
            battery_soc (float): Current battery state of charge.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List containing one message per battery device.
        """
        pv_label: str = "battery"

        # Iterate over devices dictionary (key=device_id, value=device_data)
        for device_id, device_data in self._entities.items():
            # Check if this device has the label "battery"
            if device_data.get("label") == pv_label:
                # Create and return a message using battery_soc data and timestamp
                # TODO no caso da i-charging nao tenho a energia, o que faço?
                value: dict = {
                    "battery_charging_energy": 0,
                    "state_of_charge": battery_soc
                }
                return [ICTranslator._message_creator(value, device_id, timestamp)]

        return []

    def _meter(self, meters_list: list, timestamp: str) -> list:
        """
        Process grid meter readings and create messages.

        Args:
            meters_list (list): List of meter reading dictionaries.
            timestamp (str): Current timestamp for message creation.

        Returns:
            list: List of formatted messages for each valid grid meter.
        """
        messages: list = []

        gm_label: str = "grid_meter"

        for meter in meters_list:
            # Get id from the current meter
            device_id: str = meter.get("id")

            # Skip if id is None
            if device_id is None:
                continue

            device_data: dict = self._entities.get(device_id)

            if device_data:
                # Check if the label matches "grid_meter"
                label: str = device_data.get("label")
                if label == gm_label:
                    # Prepare the message data
                    value: dict = {
                        "energy_in": meter.get("l123"),
                    }
                    # Create and add the message
                    messages.append(ICTranslator._message_creator(value, device_id, timestamp))
                else:
                    # Log error if label does not match
                    self._logger.error(f"ICTranslator: Device found, but label mismatch: {device_id} (label: {label})")
            else:
                # Log error if device is not found in the configuration
                self._logger.error(f"ICTranslator: Device {device_id} not found in the configuration file.")

        return messages

    def translate(self, message: bytes) -> None:
        """
        Translate incoming device messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
            message (bytes): Dictionary containing i-charging-format environment data, encoded as bytes.
        """
        message_dict: dict = json.loads(message.decode('utf-8')).get('observation')

        timestamp: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message_list: list = []

        for attr in message_dict:
            if attr in self._labels_functions_mapper:
                func = self._labels_functions_mapper[attr]
                attr_processed: list = func(message_dict.get(attr), timestamp)
                # Only append to message_list if attr_processed is not empty (e.g., not an empty list or dict)
                if attr_processed:
                    message_list.extend(attr_processed)

        # Send the message to the environment queue
        self.send_message_to_environment_queue(message_list)

    @staticmethod
    def _message_creator(value: dict, device_id: str, timestamp: str) -> dict:
        """
        Create a standardized message format for a device reading.

        Args:
            value (dict): Dictionary containing device-specific values.
            device_id (str): Unique device identifier.
            timestamp (str): Current timestamp for the message.

        Returns:
            dict: Standardized message dictionary.
        """
        new_message: dict = {
            "id": device_id,
            "value": value,
            "timestamp": timestamp
        }

        return new_message

# Em caso de erro corro o risco da mensagem ser enviada duas vezes, mas não é um problema
