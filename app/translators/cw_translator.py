import pika
import json
import datetime
import time

class CWTranslator:
    def __init__(self, environment, environment_specs, configurations, logger):
        self._environment = environment
        self._environment_specs = environment_specs
        self._internal_message_hub_server = configurations.get('internalAMQPServer')
        self._max_reconnect_attempts = configurations.get('maxReconnectAttempts')
        self._logger = logger

    def _ev_charger(self, messages):
        # Remove and get the list from 'session_status'; default to empty list if not found
        session_status_list = messages.pop("session_status", [])
        session_status = session_status_list[0] if session_status_list else {}

        # Extract the 'Read' value from the session_status dict; default to 0 if not found
        read_status = session_status.get("Read", 0)

        # Initialize the return dictionary
        value = {}

        # If the read status is 1, process the remaining keys
        if read_status == 1:
            for key, item_list in messages.items():
                # Ensure the item is a list and not empty
                if isinstance(item_list, list) and item_list:
                    first_item = item_list[0]
                    read_value = first_item.get("Read", 0)
                    value[key] = read_value
                else:
                    # Default to 0 if the list is empty or not a list
                    value[key] = 0
        else:
            # If read_status is not 1, set all values to 0
            for key in messages:
                value[key] = 0

        return value

    labels_functions_mapper = {
        "ev_charger": _ev_charger
    }

    def translate(self, messages, label, entity_id):

        while self._max_reconnect_attempts > 0:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self._internal_message_hub_server.get('host'),
                    port=self._internal_message_hub_server.get('port'), virtual_host=self._internal_message_hub_server.get('vhost'), credentials=pika.PlainCredentials(self._internal_message_hub_server.get('credentials').get('username'), self._internal_message_hub_server.get('credentials').get('password'))
                ))
                channel = connection.channel()
                channel.queue_declare(queue=self._environment, durable=True)

                value = {}

                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                if label in CWTranslator.labels_functions_mapper:
                    value = CWTranslator.labels_functions_mapper[label](messages)
                else:
                    # TODO Mudar para nao descartar dados. Meter -1 no energy_in (especifico por parametro)
                    for key, params in messages.items():

                        if key:
                            total = 0
                            if params:
                                for reading in params:
                                    total += reading.get('Read', 0)

                            # If there is a parameter with more than one reading (message) then the values must be added. This is the default case, if there is a label that needs a different approach, it is necessary to create a specific method, as for the ev chargers
                            value[key] = total

                        else:
                            self._logger.warning(f"CWTranslator: No data for {entity_id} device.")
                            return


                new_message = [{
                    "id": entity_id,
                    "value": value,
                    "timestamp": timestamp
                }]

                message_bytes = json.dumps(new_message).encode('utf-8')

                channel.basic_publish(exchange='', routing_key=self._environment, body=message_bytes)

                channel.close()
                connection.close()
                break  # Break out of the retry loop if successful

            except pika.exceptions.AMQPConnectionError as e:
                self._max_reconnect_attempts  -= 1  # Decrement the retry counter
                if self._max_reconnect_attempts  == 0:
                    self._logger.error(f"CWTranslator: {self._environment} translator reached maximum reconnection attempts. The message was not sent.")
                else:
                    self._logger.warning(f"CWTranslator: {self._environment} translator lost connection, attempting to reconnect...")
                    time.sleep(5) 
            except Exception as e:
                self._logger.error(f"CWTranslator: An unexpected error occurred: {e} {self._environment}")
                break
   
        self._logger.info(f"CWTranslator: Translating {self._environment} successfully!")