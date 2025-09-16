import datetime
import json
from app.translators.translator_rabbitmq_base import TranslatorRabbitMQBase
from app.utils.logger import LoggingUtils

class ICTranslator(TranslatorRabbitMQBase):

    _entities : dict
    _labels_functions_mapper : dict

    def __init__(self, environment : str, environment_specs : dict, configurations : dict, logger : LoggingUtils):
        super().__init__(environment, configurations, logger)

        self._entities = environment_specs.get('entities')

        self._labels_functions_mapper = {
            "pv.production": self._pv_panel,
            "battery.soc": self._battery,
            "meter.values": self._meter,
            "charging.session": self._ev_charger
        }

    def _ev_charger(self, charging_sessions_list, timestamp):

        messages = []

        cs_label = "ev_charger"

        for charging_session in charging_sessions_list:
            # Get serial number and plug from the current session
            serial = charging_session.get("serialnumber")
            plug = charging_session.get("plug")

            # Skip if any of the required fields is missing
            if not serial or plug is None:
                continue

            # Build the device key (e.g., "AC000001_1")
            device_id = f"{serial}_{plug}"
            device_data = self._entities.get(device_id)

            if device_data:
                # Check if the label matches "charging_session"
                label = device_data.get("label")
                if label == cs_label:
                    # Prepare the message data
                    value = {
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
    def _pv_panel(self, pv_production, timestamp):

        pv_label = "pv_panel"

        # Iterate over devices dictionary (key=device_id, value=device_data)
        for device_id, device_data in self._entities.items():
            # Check if this device has the label "pv_panel"
            if device_data.get("label") == pv_label:
                # Create and return a message using pv_production data and timestamp
                value = {
                    "solar_generation": pv_production
                }
                return [ICTranslator._message_creator(value, device_id, timestamp)]

        return []

    def _battery(self, battery_soc, timestamp):

        pv_label = "battery"

        # Iterate over devices dictionary (key=device_id, value=device_data)
        for device_id, device_data in self._entities.items():
            # Check if this device has the label "battery"
            if device_data.get("label") == pv_label:
                # Create and return a message using battery_soc data and timestamp
                # TODO no caso da i-charging nao tenho a energia, o que faço?
                value = {
                    "battery_charging_energy": 0,
                    "state_of_charge": battery_soc
                }
                return [ICTranslator._message_creator(value, device_id, timestamp)]

        return []

    def _meter(self, meters_list, timestamp):

        messages = []

        gm_label = "grid_meter"

        for meter in meters_list:
            # Get id from the current meter
            device_id = meter.get("id")

            # Skip if id is None
            if device_id is None:
                continue

            device_data = self._entities.get(device_id)

            if device_data:
                # Check if the label matches "grid_meter"
                label = device_data.get("label")
                if label == gm_label:
                    # Prepare the message data
                    value = {
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

    def translate(self, message):
        """
        Translate incoming device messages into a standardized format.
        Handles both generic translation and label-specific logic.

        Args:
        message (dict): Dictionary containing i-charging-format environment data.

        Returns:
        None: The method sends the translated message to the environment queue.
        """
        message = json.loads(message.decode('utf-8')).get('observation')


        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message_list = []

        for attr in message:
            if attr in self._labels_functions_mapper:
                func = self._labels_functions_mapper[attr]
                attr_processed = func(message.get(attr), timestamp)
                # Only append to message_list if attr_processed is not empty (e.g., not an empty list or dict)
                if attr_processed:
                    message_list.extend(attr_processed)

        # Send the message to the environment queue
        self.send_message_to_environment_queue(message_list)

    @staticmethod
    def _message_creator(value, device_id, timestamp):

        new_message = {
            "id": device_id,
            "value": value,
            "timestamp": timestamp
        }

        return new_message


# Em caso de erro corro o risco da mensagem ser enviada duas vezes, mas não é um problema
