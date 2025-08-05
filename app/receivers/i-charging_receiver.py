import pika
import threading
import time
from app.ic_runtime_request import ICRuntimeRequest
from app.translators.ic_translator import ICTranslator
from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.receivers.receiver_base import ReceiverBase

class ICReceiver(ReceiverBase):
    provider = "i-charging"

    def __init__(self, environment, environment_specs, configurations, logger):
        super().__init__(environment, environment_specs, configurations, logger)
        self._translator = ICTranslator(environment, environment_specs, configurations, logger)

        _ic_server = configurations.get('ic_server')
        self._connection_params = pika.ConnectionParameters(host=_ic_server.get('host'), port=_ic_server.get('port'),credentials=pika.PlainCredentials(_ic_server.get('credentials').get('username'), _ic_server.get('credentials').get('password')), heartbeat=_ic_server.get('heartbeat'))
        self._connection = None
        self._channel = None

        self._stop_event = threading.Event()
 
    def stop(self):
        self._logger.info(f"ICReceiver: Stopping thread {self._environment}...")
        self._stop_event.set()

    def _callback(self, ch, method, properties, body):
        if self._stop_event.is_set():
            self._channel.stop_consuming()
            self._channel.close()
            self._connection.close()
            self._logger.info(f"ICReceiver: Thread {self._environment} stopped.")
        else:
            self._translator.translate(body)
            self._channel.basic_ack(delivery_tag=method.delivery_tag)

    def _connect(self):
        self._connection = pika.BlockingConnection(self._connection_params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=self._environment, exchange_type='fanout')

        result = self._channel.queue_declare(queue='', exclusive=True)
        self._queue_name = result.method.queue

        self._channel.queue_bind(exchange=self._environment, queue=self._queue_name)
        self._channel.basic_consume(queue=self._queue_name, on_message_callback=self._callback)
        self._channel.start_consuming()
        
    def run(self):
        while self._max_reconnect_attempts > 0 and not self._stop_event.is_set():
            try:
                self._connect()
                
            except pika.exceptions.AMQPConnectionError:
                if self._max_reconnect_attempts == 0:
                    self._logger.error(f"ICReceiver: Thread {self._environment} reached maximum reconnection attempts. Stopping thread.")
                else:
                    self._logger.warning(f"ICReceiver: Thread {self._environment} lost connection, attempting to reconnect...")
                    time.sleep(5)  

                self._max_reconnect_attempts -= 1
            except Exception as e:
                self._logger.error(f"ICReceiver: Thread {self._environment} encountered an error: {e}")

    @classmethod
    def post_start(cls, environments, configurations, logger):
        ICRuntimeRequest(environments, configurations, logger).init()
