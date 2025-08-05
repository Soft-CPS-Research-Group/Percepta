import pika
import json
import uuid
import time
from app.utils.data import DataSet


class ICRuntimeRequest:
    def __init__(self, environments, configurations, logger):
        _ic_server = configurations.get('ic_server')
        self._logger = logger
        self._connection_params = pika.ConnectionParameters(host=_ic_server.get('host'), port=_ic_server.get('port'),credentials=pika.PlainCredentials(_ic_server.get('credentials').get('username'), _ic_server.get('credentials').get('password')), heartbeat=_ic_server.get('heartbeat'))
        self._max_reconnect_attempts = configurations.get('maxReconnectAttempts')
        self._connection = None
        self._channel = None
        self._completed = False

        self._message = {
            "type": "runtime",
            "value": {
                "installations": [list(environments.keys())],
                "frequency": DataSet.calculate_interval(configurations.get('frequency'))
            }
        }

    def init(self):
        self._message = json.dumps(self._message)

        self._send_message()

    def _connect(self):
        # Get connection parameters
        self._connection = pika.BlockingConnection(self._connection_params)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue='RPC', durable=True)
        self._returnQueueName = self._channel.queue_declare(queue='', exclusive=True)

    # TODO talvez tornar esta tentativa infinita
    def _send_message(self):
        while self._max_reconnect_attempts > 0 and self._completed == False:
            try:
                self._connect()
                self._channel.basic_publish(
                    exchange='',
                    routing_key='RPC',
                    body=self._message,
                    properties=pika.BasicProperties(
                        reply_to=self._returnQueueName.method.queue,
                        message_id=str(uuid.uuid4())
                    )
                )

                self._channel.basic_consume(
                    queue=self._returnQueueName.method.queue,
                    on_message_callback=self._on_response,
                    auto_ack=True
                )

                # Start consuming
                self._channel.start_consuming()

                self._completed = True
            except pika.exceptions.AMQPConnectionError:
                if self._max_reconnect_attempts == 0:
                    self._logger.error(f"Thread ICRuntimeRequest reached maximum reconnection attempts. Stopping thread.")
                else:
                    self._logger.warning(f"Thread ICRuntimeRequest lost connection, attempting to reconnect...")
                    time.sleep(5)

                self._max_reconnect_attempts -= 1
            except Exception as e:
                self._logger.error(f"Thread ICRuntimeRequest encountered an error: {e}")



    def _on_response(self, ch, method, properties, body):
            self._logger.info(f"Received response: {body.decode()}")
            ch.stop_consuming()
            ch.close()