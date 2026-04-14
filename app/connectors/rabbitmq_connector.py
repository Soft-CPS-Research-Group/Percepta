import json
import ssl
from typing import Callable

from charset_normalizer.api import explain_handler
from pika.adapters.blocking_connection import BlockingChannel
from pika import PlainCredentials, BlockingConnection, ConnectionParameters, BasicProperties, SSLOptions

# Allowed keys for declaring an exchange in RabbitMQ
ALLOWED_EXCHANGE_KEYS = {
    "exchange_type",  # Type of the exchange (e.g., 'direct', 'fanout', 'topic', 'headers')
    "durable",        # If True, the exchange will survive broker restarts
    "auto_delete",    # If True, the exchange is deleted when no longer in use
    "internal",       # If True, the exchange cannot be directly published to by clients
    "arguments"       # Additional optional arguments for the exchange (e.g., for plugins)
}
DEFAULT_EXCHANGE_CONF = {"exchange_type": "direct", "durable": False, "auto_delete": False, "internal": False, "arguments": None}

# Allowed keys for declaring a queue in RabbitMQ
ALLOWED_QUEUE_KEYS = {
    "durable",        # If True, the queue will survive broker restarts
    "exclusive"      # If True, the queue is only accessible by the connection that declared it and deleted on close
}

DEFAULT_QUEUE_CONF = {"durable": True, "exclusive": False}

# Allowed keys for publishing a message to an exchange
ALLOWED_PUBLISH_KEYS = {
    "mandatory",      # If True, the server returns unroutable messages instead of dropping them
    "properties"      # Message properties (e.g., content_type, delivery_mode, headers)
}

DEFAULT_PUBLISH_CONF = {"mandatory": False, "properties": None}

# Allowed keys for consuming messages from a queue
ALLOWED_CONSUME_KEYS = {
    "auto_ack"        # If True, messages are acknowledged automatically upon delivery
}

DEFAULT_CONSUME_CONF = {"auto_ack": True}

ALLOWED_SSL_KEYS = {
    "enabled",
    "check_hostname",
    "verify_mode"
}

DEFAULT_SSL_CONF = {"enabled": False, "check_hostname": None, "verify_mode" : None}


def filter_keys(conf: dict, allowed_keys: set, process: str, logger=None) -> dict:
    if not conf:
        return {}

    filtered_conf = {k: v for k, v in conf.items() if k in allowed_keys}

    removed_keys = set(conf.keys()) - allowed_keys
    if removed_keys and logger:
        logger.warning(f"Ignored invalid keys in {process}: {removed_keys}")

    return filtered_conf

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

    def __init__(self, server_configurations: dict) -> None:
        """Initializes the RabbitMQ connection parameters."""

        # Extract username from server_configurations (nested inside 'auth')
        _username: str = server_configurations.get('auth', {}).get('username')

        # Extract password from server_configurations (nested inside 'auth')
        _password: str = server_configurations.get('auth', {}).get('password')

        self._host = server_configurations.get('host')

        # Build connection parameters for RabbitMQ
        self._params = ConnectionParameters(
            host=server_configurations.get("host"),  # RabbitMQ server host
            port=server_configurations.get("port"),  # RabbitMQ server port
            virtual_host=server_configurations.get("vhost") or '/',  # Use provided vhost or default to "/"
            credentials=PlainCredentials(_username, _password) if _username else None,
            ssl_options=self._setup_ssl_options(server_configurations.get("host"), server_configurations.get("ssl_conf")),
            # Add credentials if username exists
            heartbeat=server_configurations.get("heartbeat") or 60  # Set heartbeat interval (default: 60s)
        )

        # Load defaults
        self._exchange_conf = server_configurations.get('exchange_conf') or {}
        self._queue_conf = server_configurations.get('queue_conf') or {}
        self._consume_conf = server_configurations.get('consume_conf') or {}
        self._publish_conf = server_configurations.get('publish_conf') or {}

        # Validate defaults immediately
        # validate_keys(self._exchange_conf, ALLOWED_EXCHANGE_KEYS, 'exchange defaults')
        # validate_keys(self._queue_conf, ALLOWED_QUEUE_KEYS, 'queue defaults')
        # validate_keys(self._consume_conf, ALLOWED_CONSUME_KEYS, 'consume defaults')
        # validate_keys(self._publish_conf, ALLOWED_PUBLISH_KEYS, 'publish defaults')

    def _setup_ssl_options(self, host: str, ssl_conf: dict) -> SSLOptions:
        """Helper to build SSL context based on configuration."""
        if ssl_conf is None or not ssl_conf.get('enabled', False):
            return None


        ssl_conf = {**DEFAULT_SSL_CONF, **(ssl_conf or {})}
        ssl_conf = filter_keys(ssl_conf, ALLOWED_SSL_KEYS, 'ssl defaults')
        context = ssl.create_default_context()

        check_hostname = ssl_conf.get("check_hostname", False)
        verify_mode_str = ssl_conf.get("verify_mode", None)

        context.check_hostname = check_hostname

        if verify_mode_str is None:
            context.verify_mode = ssl.CERT_NONE
        elif str(verify_mode_str).lower() == "required":
            context.verify_mode = ssl.CERT_REQUIRED

        return SSLOptions(context, server_hostname=host)

    def connect(self) -> None:
        """Establishes a blocking connection and creates a channel."""
        # Create a new blocking connection to RabbitMQ using the provided connection parameters
        self._connection = BlockingConnection(self._params)

        # Open a new channel on the established connection
        self._channel = self._connection.channel()

        self._channel.confirm_delivery()

    def declare_exchange(self, exchange_name: str = '', **kwargs) -> None:
        """
        Declares an exchange. Checks if it exists first using passive=True.
        If it doesn't exist, recovers the channel and declares it.
        """
        try:
            # 1. Passive check: does not create, only verifies if exchange exists
            # If it fails, RabbitMQ will force close the channel (404 Not Found)
            self._channel.exchange_declare(exchange=exchange_name, passive=True)
            print(f"Exchange '{exchange_name}' already exists. Skipping declaration.")

        except Exception:
            # 2. Channel recovery: The previous channel is now invalid due to the 404 error
            print(f"Exchange '{exchange_name}' not found or channel closed. Recovering channel...")

            if self._connection and self._connection.is_open:
                self._channel = self._connection.channel()
            else:
                raise RabbitMQError("Connection is closed; cannot recover channel to declare exchange.")

            # 3. Actual declaration
            # Merge configuration: defaults + instance settings + method overrides
            exchange_conf = {**DEFAULT_EXCHANGE_CONF, **(self._exchange_conf or {}), **kwargs}
            filtered_conf = filter_keys(exchange_conf, ALLOWED_EXCHANGE_KEYS, 'exchange')

            try:
                self._channel.exchange_declare(exchange=exchange_name, **filtered_conf)
                print(f"Exchange '{exchange_name}' declared successfully.")
            except Exception as decl_error:
                raise RabbitMQError(f"Critical error declaring exchange '{exchange_name}': {decl_error}")

    def declare_queue(self, queue_name: str = '', exchange_name: str = '', **kwargs) -> str:
        """
        Declares a queue with support for default configs (from init)
        and overrides (via kwargs).
        """
        # Merge configs: defaults from init + arguments passed to the method
        queue_conf = {**DEFAULT_QUEUE_CONF, **(self._queue_conf or {}), **kwargs}

        filtered_conf = filter_keys(queue_conf, ALLOWED_QUEUE_KEYS, 'queue')


        if queue_conf.get("queue_prefix"):
            queue_name = f'{queue_conf.get("queue_prefix")}_{queue_name}'  # TODO pensar se queue_conf é o melhor sitio para ter o bound_queue_prefix


        # Declare the queue with the resolved configuration
        declaration_result = self._channel.queue_declare(queue=queue_name, **filtered_conf)
        real_queue_name = declaration_result.method.queue

        # Optionally bind the queue to an exchange
        if exchange_name:

            self._channel.queue_bind(
                exchange=exchange_name,
                queue=real_queue_name
            )

        return real_queue_name

    def publish(self, routing_key: str, message: dict, exchange_name: str = "", **kwargs) -> None:
        """
        Publishes a message to a specified queue or exchange.
        """
        # Merge default publish configuration with any overrides
        publish_conf = {**DEFAULT_PUBLISH_CONF, **(self._publish_conf or {}), **kwargs}
        filtered_conf = filter_keys(publish_conf, ALLOWED_PUBLISH_KEYS, 'publish')


        # Extract properties if provided
        properties = filtered_conf.pop("properties", None)
        basic_props = BasicProperties(**properties) if properties else None

        # Publish the message
        self._channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=json.dumps(message).encode("utf-8"),
            properties=basic_props,
            **filtered_conf  # other pika parameters like mandatory
        )

    def setup_consumer(self, queue_name: str, callback: Callable, **kwargs) -> None:
        """
        Registers a consumer for a specific queue without starting the I/O loop.
        This allows registering multiple consumers/queues on the same channel
        before calling start_listening().
        """
        # Merge default consume configuration with any overrides
        consume_conf = {**DEFAULT_CONSUME_CONF, **(self._consume_conf or {}), **kwargs}
        filtered_conf = filter_keys(consume_conf, ALLOWED_CONSUME_KEYS, 'consume')
        #print(f"{queue_name} {consume_conf} {filtered_conf}")
        try:
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                **filtered_conf  # other pika parameters like consumer_tag, exclusive, arguments
            )
        except Exception as e:
            raise RabbitMQError(f"Error while setup consumer, queue '{queue_name}': {e}")

    def start_listening(self) -> None:
        """
        Starts the blocking I/O loop to begin consuming messages from all
        previously registered queues.
        """
        self._consuming = True
        try:
            if self._channel and self._channel.is_open:
                self._channel.start_consuming()
            else:
                raise RabbitMQError("Cannot start consuming: channel is closed.")
        except Exception as e:
            raise RabbitMQError(f"Error during the consumption loop: {e}")
        finally:
            self._consuming = False

    def ack(self, delivery_tag: int) -> None:
        """Acknowledge a message manually given its delivery tag."""
        if not self._consume_conf.get("auto_ack"):
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

    def stop_consuming(self) -> None:
        """Stop consuming messages safely."""
        if getattr(self, "_consuming", False) and self._channel.is_open:
            self._channel.stop_consuming()
            self._consuming = False

    def stop_consuming_safely(self) -> None:
        """
        Safely stops the consuming loop from a different thread.
        Uses add_callback_threadsafe to ensure the stop command is executed
        within the I/O loop of the connection's owner thread.
        """
        if self._connection and self._connection.is_open:
            # Schedule the stop command to be executed by the thread that owns the connection
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)