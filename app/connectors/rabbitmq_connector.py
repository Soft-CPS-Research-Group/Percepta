import json
from typing import Callable, Dict, Any
from pika.adapters.blocking_connection import BlockingChannel
from pika import PlainCredentials, BlockingConnection, ConnectionParameters, BasicProperties
from pika.spec import Exchange

# Allowed keys for declaring an exchange in RabbitMQ
ALLOWED_EXCHANGE_KEYS = {
    "exchange_type",  # Type of the exchange (e.g., 'direct', 'fanout', 'topic', 'headers')
    "durable",        # If True, the exchange will survive broker restarts
    "auto_delete",    # If True, the exchange is deleted when no longer in use
    "internal",       # If True, the exchange cannot be directly published to by clients
    "arguments"      # Additional optional arguments for the exchange (e.g., for plugins)
}
DEFAULT_EXCHANGE_CONF = {"exchange_type": "direct", "durable": False, "auto_delete": False, "internal": False, "arguments": None}

# Allowed keys for declaring a queue in RabbitMQ
ALLOWED_QUEUE_KEYS = {
    "durable",        # If True, the queue will survive broker restarts
    "exclusive",      # If True, the queue is only accessible by the connection that declared it and deleted on close
    "auto_delete",    # If True, the queue is deleted when no longer used
    "arguments"       # Additional optional arguments for the queue (e.g., TTL, max length)
}

DEFAULT_QUEUE_CONF = {"durable": True, "exclusive": False, "auto_delete": False, "arguments": None}

# Allowed keys for publishing a message to an exchange
ALLOWED_PUBLISH_KEYS = {
    "mandatory",      # If True, the server returns unroutable messages instead of dropping them
    "properties"      # Message properties (e.g., content_type, delivery_mode, headers)
}

DEFAULT_PUBLISH_CONF = {"mandatory": False, "properties": None}

# Allowed keys for consuming messages from a queue
ALLOWED_CONSUME_KEYS = {
    "consumer_tag",   # Identifier for the consumer, can be used to cancel it later
    "exclusive",      # If True, only this consumer can access the queue
    "arguments",      # Additional optional arguments for the consumer
    "auto_ack"        # If True, messages are acknowledged automatically upon delivery
}

DEFAULT_CONSUME_CONF = {"auto_ack": True, "exclusive": False, "arguments": None, "consumer_tag": None}

def validate_keys(conf: dict, allowed_keys: set, process : str) -> None:
    """Raise an error if any invalid keys are found."""
    invalid_keys = set(conf.keys()) - allowed_keys

    if invalid_keys:
        raise ValueError(f"Invalid keys in {process} configuration: {invalid_keys}. "
                         f"Allowed keys are: {allowed_keys}")


class RabbitMQError(Exception):
    """Custom exception class for RabbitMQ-related errors."""
    pass

class RabbitMQConnector:
    """
    Wrapper class for managing a RabbitMQ Blocking Connection, Blocking Channel, and basic operations.
    Provides methods for connecting, declaring queues, publishing messages, consuming messages,
    stopping consumption safely, and closing the connection cleanly.
    """

    _params: ConnectionParameters  # Connection parameters for RabbitMQ
    _connection: BlockingConnection  # Blocking connection instance
    _channel: BlockingChannel  # Channel instance for sending/receiving messages
    _consuming: bool = False  # Flag to track if consumption is active

    def __init__(self, server_configurations: Dict[str, Any]) -> None:
        """Initializes the RabbitMQ connection parameters."""

        # Extract username from server_configurations (nested inside 'auth')
        _username: str = server_configurations.get('auth', {}).get('username')

        # Extract password from server_configurations (nested inside 'auth')
        _password: str = server_configurations.get('auth', {}).get('password')

        # Build connection parameters for RabbitMQ
        self._params = ConnectionParameters(
            host=server_configurations.get("host"),  # RabbitMQ server host
            port=server_configurations.get("port"),  # RabbitMQ server port
            virtual_host=server_configurations.get("vhost") or '/',  # Use provided vhost or default to "/"
            credentials=PlainCredentials(_username, _password) if _username else None,
            # Add credentials if username exists
            heartbeat=server_configurations.get("heartbeat") or 60  # Set heartbeat interval (default: 60s)
        )

        # Load defaults
        self._exchange_conf = server_configurations.get('exchange_conf') or {}
        self._queue_conf = server_configurations.get('queue_conf') or {}
        self._consume_conf = server_configurations.get('consume_conf') or {}
        self._publish_conf = server_configurations.get('publish_conf') or {}

        # Validate defaults immediately
        validate_keys(self._exchange_conf, ALLOWED_EXCHANGE_KEYS, 'exchange defaults')
        validate_keys(self._queue_conf, ALLOWED_QUEUE_KEYS, 'queue defaults')
        validate_keys(self._consume_conf, ALLOWED_CONSUME_KEYS, 'consume defaults')
        validate_keys(self._publish_conf, ALLOWED_PUBLISH_KEYS, 'publish defaults')

    def connect(self) -> None:
        """Establishes a blocking connection and creates a channel."""
        # Create a new blocking connection to RabbitMQ using the provided connection parameters
        self._connection = BlockingConnection(self._params)

        # Open a new channel on the established connection
        self._channel = self._connection.channel()

    def declare_exchange(self, exchange_name: str = '', **kwargs) -> None:
        """
        Declares an exchange with support for default configs (from init)
        and overrides (via kwargs).
        """
        # Merge configs: defaults from init + arguments passed to the method
        exchange_conf = {**DEFAULT_EXCHANGE_CONF, **(self._exchange_conf or {}), **kwargs}
        validate_keys(exchange_conf, ALLOWED_EXCHANGE_KEYS, 'exchange')

        # Declare the exchange with the resolved configuration
        self._channel.exchange_declare(exchange=exchange_name, **exchange_conf)

    def declare_queue(self, queue_name: str = '', exchange_name: str = '', **kwargs) -> str:
        """
        Declares a queue with support for default configs (from init)
        and overrides (via kwargs).
        """
        # Merge configs: defaults from init + arguments passed to the method
        queue_conf = {**DEFAULT_QUEUE_CONF, **(self._queue_conf or {}), **kwargs}
        validate_keys(queue_conf, ALLOWED_QUEUE_KEYS, 'queue')

        # Declare the queue with the resolved configuration
        result = self._channel.queue_declare(queue=queue_name, **queue_conf)
        real_queue_name = result.method.queue

        # Optionally bind the queue to an exchange
        if exchange_name:
            self._channel.queue_bind(
                exchange=exchange_name,
                queue=real_queue_name,
                routing_key=real_queue_name
            )

        return real_queue_name

    def publish(self, queue_name: str, message: Dict[str, Any], exchange_name: str = "", **kwargs) -> None:
        """
        Publishes a message to a specified queue or exchange.
        """
        # Merge default publish configuration with any overrides
        publish_conf = {**DEFAULT_PUBLISH_CONF, **(self._publish_conf or {}), **kwargs}
        validate_keys(publish_conf, ALLOWED_PUBLISH_KEYS, 'publish')


        # Extract properties if provided
        properties = publish_conf.pop("properties", None)
        basic_props = BasicProperties(**properties) if properties else None

        # Publish the message
        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=queue_name,
            body=json.dumps(message).encode("utf-8"),
            properties=basic_props,
            **publish_conf  # other pika parameters like mandatory
        )

    def consume(self, queue_name: str, callback: Callable, **kwargs) -> None:
        """
        Starts consuming messages from a queue while safely tracking the consumption state.
        """
        self._consuming = True

        # Merge default consume configuration with any overrides
        consume_conf = {**DEFAULT_CONSUME_CONF, **(self._consume_conf or {}), **kwargs}
        validate_keys(consume_conf, ALLOWED_CONSUME_KEYS, 'consume')

        # Extract auto_ack separately for clarity, default to True if not set
        auto_ack = consume_conf.pop("auto_ack", True)

        try:
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=auto_ack,
                **consume_conf  # other pika parameters like consumer_tag, exclusive, arguments
            )
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

    def stop_consuming(self) -> None:
        """Stop consuming messages safely."""
        if getattr(self, "_consuming", False) and self._channel.is_open:
            self._channel.stop_consuming()
            self._consuming = False

    def close(self) -> None:
        """Stop consuming messages safely."""
        self.stop_consuming()
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

    def stop_consuming_safely(self) -> None:
        """
        Safely stops the consuming loop from a different thread.
        Uses add_callback_threadsafe to ensure the stop command is executed
        within the I/O loop of the connection's owner thread.
        """
        if self._connection and self._connection.is_open:
            # Schedule the stop command to be executed by the thread that owns the connection
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)