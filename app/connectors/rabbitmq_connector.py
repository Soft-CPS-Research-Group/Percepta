import json
from typing import Callable, Dict, Any
from pika.adapters.blocking_connection import BlockingChannel
from pika import PlainCredentials, BlockingConnection, ConnectionParameters, BasicProperties

class RabbitMQError(Exception):
    """Custom exception class for RabbitMQ-related errors."""
    pass


class RabbitMQConnector:
    """
    Wrapper class for managing a RabbitMQ connection, channel, and basic operations.
    Provides methods for connecting, declaring queues, publishing messages, consuming messages,
    stopping consumption safely, and closing the connection cleanly.
    """

    _params: ConnectionParameters  # Connection parameters for RabbitMQ
    _connection: BlockingConnection  # Blocking connection instance
    _channel: BlockingChannel  # Channel instance for sending/receiving messages
    _consuming: bool = False  # Flag to track if consumption is active

    def __init__(self, server_configurations: Dict[str, Any]) -> None:
        """Initialize the RabbitMQ connection parameters."""

        _username: str = server_configurations.get('auth', {}).get('username')
        _password: str = server_configurations.get('auth', {}).get('password')

        self._params = ConnectionParameters(
            host=server_configurations.get("host"),
            port=server_configurations.get("port"),
            virtual_host=server_configurations.get("vhost") or '/',
            credentials=PlainCredentials(_username, _password) if _username else None,
            heartbeat=server_configurations.get("heartbeat") or 60
        )

        self._exchange_conf = server_configurations.get('exchange_conf')
        self._queue_conf = server_configurations.get('queue_conf')


    def connect(self) -> None:
        """Establish a blocking connection and create a channel."""
        self._connection = BlockingConnection(self._params)
        self._channel = self._connection.channel()

    def declare_exchange(self, exchange_name: str = '', exchange_type: str = None, durable: bool = None) -> None:
        if exchange_type is None:
            exchange_type = self._exchange_conf.get('type')
            if exchange_type is None:
                exchange_type = "direct"

        if durable is None:
            durable = self._exchange_conf.get('durable')
            if durable is None:
                durable = False

        """Declare an exchange on the RabbitMQ server."""
        self._channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=durable)

    def declare_queue(self, queue_name: str = '', exchange_name: str = '', durable: bool = True, exclusive: bool = True) -> str:
        if durable is None:
            durable = self._queue_conf.get('durable')
            if durable is None:
                durable = True

        if exclusive is None:
            exclusive = self._queue_conf.get('exclusive')
            if exclusive is None:
                exclusive = True

        """Declare a queue and optionally bind it to an exchange."""
        result = self._channel.queue_declare(queue=queue_name, durable=durable)
        real_queue_name = result.method.queue

        if exchange_name:
            self._channel.queue_bind(exchange=exchange_name, queue=real_queue_name, routing_key=real_queue_name)

        return real_queue_name

    def publish(self, queue_name: str, message: Dict[str, Any], exchange_name: str = "", properties=None) -> None:
        """Publish a message to a specified queue or exchange."""
        basic_props = BasicProperties(**properties) if properties else None

        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=queue_name,
            body=json.dumps(message).encode("utf-8"),
            properties=basic_props,
        )

    def consume(self, queue_name: str, callback: Callable, auto_ack: bool = True) -> None:
        """Start consuming messages from a queue safely tracking the consumption state."""
        self._consuming = True
        try:
            self._channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)
            self._channel.start_consuming()
        except Exception as e:
            raise RabbitMQError(f"Error while consuming messages from queue '{queue_name}': {e}") from e
        finally:
            self._consuming = False

    def ack(self, delivery_tag: int) -> None:
        """Acknowledge a message manually given its delivery tag."""
        if hasattr(self, "_channel") and self._channel and self._channel.is_open:
            self._channel.basic_ack(delivery_tag=delivery_tag)
        else:
            raise RabbitMQError("Cannot ack message: channel is not open.")

    def nack(self, delivery_tag: int,  requeue: bool = False) -> None:
        """Negatively acknowledge a message manually given its delivery tag.
            """
        if hasattr(self, "_channel") and self._channel and self._channel.is_open:
            self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        else:
            raise RabbitMQError("Cannot nack message: channel is not open.")

    def close(self) -> None:
        """Stop consuming messages safely."""
        if getattr(self, "_consuming", False) and self._channel.is_open:
            self._channel.stop_consuming()
            self._consuming = False
        """Close the channel and connection cleanly if they are open."""
        if hasattr(self, "_channel") and self._channel and self._channel.is_open:
            self._channel.close()
        if hasattr(self, "_connection") and self._connection and self._connection.is_open:
            self._connection.close()

    def is_connected(self) -> bool:
        """Returns True if both the connection and the channel are still open."""
        try:
            return self._connection and self._connection.is_open and self._channel and self._channel.is_open
        except AttributeError:
            return False
