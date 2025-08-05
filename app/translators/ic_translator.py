import pika
import datetime
import json
import time

class ICTranslator:
    def __init__(self, environment, environment_specs, configurations, logger):
        self._environment = environment
        self._entities = environment_specs.get('entities')
        self._internal_message_hub_server = configurations.get('internalAMQPServer')
        self._max_reconnect_attempts = configurations.get('maxReconnectAttempts')
        self._logger = logger

    def _message_creator(self, value, device_id, timestamp):

        new_message = {
            "id": device_id,
            "value": value,
            "timestamp": timestamp
        }

        return new_message

    def _ev_charger(self, charging_sessions_list, devices, timestamp):

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
            device_data = devices.get(device_id)

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
    def _pv_panel(self, pv_production, devices, timestamp):

        pv_label = "pv_panel"

        # Iterate over devices dictionary (key=device_id, value=device_data)
        for device_id, device_data in devices.items():
            # Check if this device has the label "pv_panel"
            if device_data.get("label") == pv_label:
                # Create and return a message using pv_production data and timestamp
                value = {
                    "solar_generation": pv_production
                }
                return [ICTranslator._message_creator(value, device_id, timestamp)]

        return []

    def _battery(self, battery_soc, devices, timestamp):

        pv_label = "battery"

        # Iterate over devices dictionary (key=device_id, value=device_data)
        for device_id, device_data in devices.items():
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

    def _meter(self, meters_list, devices, timestamp):

        messages = []

        gm_label = "grid_meter"

        for meter in meters_list:
            # Get id from the current meter
            device_id = meter.get("id")

            # Skip if id is None
            if device_id is None:
                continue

            device_data = devices.get(device_id)

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

    labels_functions_mapper = {
        "pv.production": _pv_panel,
        "battery.soc": _battery,
        "meter.values": _meter,
        "charging.session": _ev_charger
    }

    def translate(self, message):

        message = json.loads(message.decode('utf-8')).get('observation')

        while self._max_reconnect_attempts > 0:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self._internal_message_hub_server.get('host'),
                    port=self._internal_message_hub_server.get('port'), virtual_host=self._internal_message_hub_server.get('vhost'), credentials=pika.PlainCredentials(self._internal_message_hub_server.get('credentials').get('username'), self._internal_message_hub_server.get('credentials').get('password'))
                ))
                channel = connection.channel()
                channel.queue_declare(queue=self._environment, durable=True)

                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                message_list = []

                for attr in message:
                    if attr in ICTranslator.labels_functions_mapper:
                        func = ICTranslator.labels_functions_mapper[attr]
                        attr_processed = func(message.get(attr), self._entities, timestamp)
                        # Only append to message_list if attr_processed is not empty (e.g., not an empty list or dict)
                        if attr_processed:
                            message_list.extend(attr_processed)

                message_bytes = json.dumps(message_list).encode('utf-8')
                channel.basic_publish(exchange='', routing_key=self._environment, body=message_bytes)

                break

            except pika.exceptions.AMQPConnectionError as e:
                self._max_reconnect_attempts -= 1  # Decrement the retry counter
                if self._max_reconnect_attempts == 0:
                    self._logger.error(f"ICTranslator: {self._environment} translator reached maximum reconnection attempts. The message was not sent.")
                else:
                    self._logger.warning(f"ICTranslator: {self._environment} translator lost connection, attempting to reconnect...")
                    time.sleep(5) 
            except Exception as e:
                self._logger.error(f"ICTranslator: An unexpected error occurred: {e}")
                break

# Em caso de erro corro o risco da mensagem ser enviada duas vezes, mas não é um problema
