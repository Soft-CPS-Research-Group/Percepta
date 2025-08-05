import pika
import threading
import time

class Accumulator(threading.Thread):
    def __init__(self, environment, manager, configurations, logger):
        threading.Thread.__init__(self)

        self._logger = logger
        self._logger.info(f"Accumulator: thread for environment {environment} started with success!")

        _internal_message_hub_server = configurations.get('internalAMQPServer')

        self._environment = environment
        self._manager = manager

        self._connection_params = pika.ConnectionParameters(host=_internal_message_hub_server.get('host'), port=_internal_message_hub_server.get('port'), virtual_host=_internal_message_hub_server.get('vhost'), credentials=pika.PlainCredentials(_internal_message_hub_server.get('credentials').get('username'), _internal_message_hub_server.get('credentials').get('password')), heartbeat=_internal_message_hub_server.get('heartbeat'))
        self._connection = None
        self._channel = None
        self._stop_event = threading.Event()

    def stop(self):
        self._logger.info(f"Accumulator: Stopping thread {self._environment}")
        self._stop_event.set()
    
    def _callback(self, ch, method, properties, body):
        if self._stop_event.is_set():
            self._manager.stop()
            self._channel.stop_consuming()
            self._channel.close()
            self._connection.close()
        else:
            if(self._manager.new_message(body)):
                self._channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                self._channel.basic_nack(delivery_tag=method.delivery_tag)
                self._logger.warning("Accumulator: Error processing RabbitMQ message.")

    def _connect(self):
        self._connection = pika.BlockingConnection(self._connection_params)
        self._channel = self._connection.channel()
        self._channel.queue_declare(self._environment, durable=True)
        self._channel.basic_consume(queue=self._environment, on_message_callback=self._callback)
        self._channel.start_consuming()


    def run(self):
        wait_time = 1
        while not self._stop_event.is_set():
            try:
                self._connect()
            except pika.exceptions.AMQPConnectionError as e:
                self._logger.warning(f"Accumulator: Thread {self._environment} lost connection. Error: {e}. Waiting {wait_time} seconds before attempting to reconnect...")
                time.sleep(wait_time)
                wait_time *= 2
            except KeyboardInterrupt:
                self._logger.info(f"Accumulator: Thread {self._environment} RabbitMQ session manually closed.")
                self.stop()
            except Exception as e:
                self._logger.error(f"Accumulator: Thread {self._environment} encountered an error: {e}")
                break

