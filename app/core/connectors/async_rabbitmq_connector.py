# --- Standard Library Imports ---
import json
import random
import ssl
import asyncio
from typing import Callable, Optional, Dict, Any

# --- Third-Party Imports ---
import aio_pika
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractQueue, AbstractExchange
from aiormq.exceptions import ChannelAccessRefused, ChannelNotFoundEntity

# --- Configuration Constants ---
DEFAULT_EXCHANGE_CONF = {
    "exchange_type": "direct",
    "durable": False,
    "auto_delete": False,
    "internal": False,
    "arguments": None
}

DEFAULT_QUEUE_CONF = {"durable": True, "exclusive": False}

DEFAULT_PUBLISH_CONF = {"properties": None}

DEFAULT_CONSUME_CONF = {"no_ack": True, "requeue": False}

DEFAULT_SSL_CONF = {"enabled": False, "check_hostname": None, "verify_mode": None}


class RabbitMQAsyncError(Exception):
    """Custom exception class for asynchronous RabbitMQ-related errors."""
    pass


class AsyncRabbitMQConnector:
    """
    Asynchronous wrapper managing an aio-pika connection, channel, and AMQP operations.

    Fully integrated with asyncio event loops for high-throughput non-blocking I/O.
    """

    _LOG_PREFIX: str = "Async RabbitMQ Connector |"

    _connection: Optional[AbstractConnection] = None
    _channel: Optional[AbstractChannel] = None
    _consuming: bool = False

    def __init__(self, server_configurations: dict, logger=None) -> None:
        """
        Initializes physical network transport connection state configurations.

        Args:
            server_configurations (dict): Configuration mapping target endpoint host,
                authentication credentials, heartbeat protocols, and TLS/SSL preferences.
            logger (LoggingUtils): Central logging facility infrastructure dependency.
        """
        self._logger = logger
        self._host = server_configurations.get("host")
        if not self._host:
            raise RabbitMQAsyncError(f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'host' must be provided")

        self._port = server_configurations.get("port", 5672)
        self._vhost = server_configurations.get("vhost") or "/"

        # Extract Authentication Credentials
        auth_conf = server_configurations.get("auth", {})
        self._username = auth_conf.get("username")
        self._password = auth_conf.get("password")

        # In aio-pika connect_robust, network timeouts map to the connection process
        self._timeout = server_configurations.get("timeout", 10)
        self._prefetch_count = server_configurations.get("prefetch_count", 10)
        self._ssl_conf = server_configurations.get("ssl_conf")

        if self._logger:
            self._logger.debug(
                f"{self._LOG_PREFIX} Endpoint transport initialized for host '{self._host}:{self._port}'.")

    def _setup_ssl_context(self) -> Optional[ssl.SSLContext]:
        """
        Creates an asynchronous-safe SSLContext for secure network connections.

        Returns:
            Optional[ssl.SSLContext]: A configured SSL context if enabled, otherwise None.

        Raises:
            RabbitMQAsyncError: If SSL context initialization or configuration fails.
        """
        if self._ssl_conf is None or not self._ssl_conf.get("enabled", False):
            return None

        try:
            ssl_conf = {**DEFAULT_SSL_CONF, **self._ssl_conf}

            context = ssl.create_default_context()
            check_hostname = ssl_conf.get("check_hostname", False)
            context.check_hostname = check_hostname

            verify_mode_str = str(ssl_conf.get("verify_mode", "none")).lower()

            if verify_mode_str == "required":
                context.verify_mode = ssl.CERT_REQUIRED
            else:
                context.verify_mode = ssl.CERT_NONE

            if self._logger:
                self._logger.debug(
                    f"{self._LOG_PREFIX} SSL Context built with hostname_check={context.check_hostname} "
                    f"and verify_mode={context.verify_mode}."
                )
            return context

        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Failed to build SSL context: {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e

    async def connect(self) -> None:
        """
        Establishes the robust non-blocking connection and opens the communication channel.

        Raises:
            RabbitMQAsyncError: If the downstream broker link cannot be secured.
        """
        if self._logger:
            self._logger.debug(f"{self._LOG_PREFIX} Initiating connection to AMQP broker...")

        ssl_context = self._setup_ssl_context()
        use_ssl = ssl_context is not None or self._port == 5671

        try:
            # Build robust connection session using correct library keyword args
            self._connection = await aio_pika.connect_robust(
                host=self._host,
                port=self._port,
                login=self._username,
                password=self._password,
                virtualhost=self._vhost,
                ssl=use_ssl,
                ssl_context=ssl_context,
                timeout=self._timeout
            )

            self._channel = await self._connection.channel()

            # --- Quality of Service (QoS) flow control ---
            await self._channel.set_qos(prefetch_count=self._prefetch_count)
            if self._logger:
                self._logger.debug(f"{self._LOG_PREFIX} QoS configured. Prefetch count set to {self._prefetch_count}.")
                self._logger.debug(f"{self._LOG_PREFIX} Robust connection and channel secured successfully.")


        except asyncio.TimeoutError as e:
            error_msg = (
                f"{self._LOG_PREFIX} Timed out after {self._timeout}s while connecting to RabbitMQ at "
                f"'{self._host}:{self._port}'. The broker may be unreachable (network, VPN, firewall) or slow."
            )

            if self._logger:
                self._logger.error(error_msg)

            raise RabbitMQAsyncError(error_msg) from e

        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Failed to connect asynchronously to RabbitMQ: {type(e).__name__}: {e}"

            if self._logger:
                self._logger.error(error_msg)

            raise RabbitMQAsyncError(error_msg) from e

    async def declare_exchange(self, exchange_name: str, **kwargs) -> AbstractExchange:
        """
        Declares an AMQP exchange asynchronously within the active channel.

        The declaration is idempotent: if the exchange already exists with the same properties,
        nothing changes; if it does not exist, it is created. If it exists with different
        properties, the broker refuses the declaration.

        If 'passive' is true in the properties, the exchange is only checked and never created,
        which does not need permission to configure it. Use it for exchanges that belong to another party.

        Args:
            exchange_name (str): Unique text identifier naming the targeted exchange.
            **kwargs: Arbitrary keyword overrides passed directly onto the declaration primitive.

        Returns:
            AbstractExchange: The proxy object representing the verified AMQP exchange topology.

        Raises:
            RabbitMQAsyncError: If the connector is offline, the exchange does not exist and cannot be
                created (passive), there is no permission to declare it, or the declaration fails.
        """
        if not self.is_connected():
            error_msg = f"{self._LOG_PREFIX} Cannot declare exchange: Connector is offline."
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg)

        exchange_conf = {**DEFAULT_EXCHANGE_CONF, **kwargs}
        exchange_type = exchange_conf.pop("exchange_type", "direct")

        if self._logger:
            self._logger.debug(f"{self._LOG_PREFIX} Declaring exchange '{exchange_name}' of type '{exchange_type}'...")

        try:
            exchange = await self._channel.declare_exchange(
                name=exchange_name,
                type=exchange_type,
                **exchange_conf
            )

            if self._logger:
                self._logger.debug(f"{self._LOG_PREFIX} Exchange '{exchange_name}' verified/declared successfully.")

            return exchange
        except ChannelNotFoundEntity as e:
            if exchange_conf.get("passive"):
                error_msg = (
                    f"{self._LOG_PREFIX} Exchange '{exchange_name}' does not exist on the broker. "
                    f"Its 'exchange_properties' have 'passive' set to true, so the exchange is only checked "
                    f"and not created. To create it, set 'passive' to false in its 'exchange_properties' "
                    f"(the user needs permission to configure exchanges). Broker response: {e}"
                )
            else:
                error_msg = f"{self._LOG_PREFIX} Error declaring exchange '{exchange_name}': {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e
        except ChannelAccessRefused as e:
            error_msg = (
                f"{self._LOG_PREFIX} No permission to create exchange '{exchange_name}'. "
                f"If it belongs to another party, set 'passive' to true in its 'exchange_properties' "
                f"so it is only checked and not created. Broker response: {e}"
            )
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e
        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Error declaring exchange '{exchange_name}': {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e

    async def declare_queue(
            self,
            queue_name: str = "",
            exchange_name: str = "",
            routing_key: str = "",
            **kwargs
    ) -> str:
        """
        Declares, prefixes, and optionally binds a queue asynchronously.

        Args:
            queue_name (str): Target name of the queue. Defaults to an empty string.
            exchange_name (str): Target exchange to bind the declared queue against. Defaults to "".
            routing_key (str): Intercept token linking the queue to the exchange. Defaults to "".
            **kwargs: Configuration parameter overrides.

        Returns:
            str: Queue name.

        Raises:
            RabbitMQAsyncError: If the adapter is disconnected or declaration tasks fail.
        """
        if not self.is_connected():
            error_msg = f"{self._LOG_PREFIX} Cannot declare queue: Connector is offline."
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg)

        queue_conf = {**DEFAULT_QUEUE_CONF, **kwargs}

        if queue_name and queue_conf.get("queue_prefix"):
            queue_name = f"_{queue_name}"

        if queue_conf.get("queue_prefix"):
            queue_name = f'{queue_conf.pop("queue_prefix")}{random.randint(0, 999999):06d}{queue_name}'

        if self._logger:
            self._logger.debug(f"{self._LOG_PREFIX} Declaring queue '{queue_name}'...")

        try:
            queue = await self._channel.declare_queue(name=queue_name, **queue_conf)

            # TODO VALIDAR EXISTÊNCIA DA EXCHANGE
            if exchange_name:
                if self._logger:
                    self._logger.debug(
                        f"{self._LOG_PREFIX} Binding queue '{queue_name}' to exchange '{exchange_name}' with key '{routing_key}'...")
                await queue.bind(exchange=exchange_name, routing_key=routing_key)

            if self._logger:
                self._logger.debug(f"{self._LOG_PREFIX} Queue '{queue_name}' integration complete.")
            return queue.name
        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Failed to declare/bind queue '{queue_name}': {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e

    async def publish(self, routing_key: str, message: dict, exchange_name: str = "", **kwargs) -> None:
        """
        Publishes a serialized JSON payload asynchronously into a target exchange.

        Args:
            routing_key (str): Binding key target used by the exchange to distribute the message.
            message (dict): Source dictionary containing the key-value data payload.
            exchange_name (str): Targeted exchange name. Empty string targets the AMQP Default Exchange.
            **kwargs: Metadata and processing parameter overrides.

        Raises:
            RabbitMQAsyncError: If structural connection interfaces are offline.
        """
        if not self.is_connected():
            error_msg = f"{self._LOG_PREFIX} Cannot publish message: Connector is offline."
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg)

        publish_conf = {**DEFAULT_PUBLISH_CONF, **kwargs}

        properties = publish_conf.pop("properties") or {}
        delivery_mode = aio_pika.DeliveryMode(properties.pop("delivery_mode", 2))

        body_bytes = json.dumps(message).encode("utf-8")
        try:
            amqp_message = aio_pika.Message(
                body=body_bytes,
                delivery_mode=delivery_mode,
                content_type="application/json",
                **properties
            )

            target_exchange_str = exchange_name if exchange_name else "AMQP default"
            if self._logger:
                self._logger.debug(
                    f"{self._LOG_PREFIX} Publishing payload to [{target_exchange_str}] with routing key '{routing_key}'...")


            if exchange_name:
                exchange = await self._channel.get_exchange(exchange_name, ensure=False)
            else:
                exchange = self._channel.default_exchange

            await exchange.publish(
                message=amqp_message,
                routing_key=routing_key,
                **publish_conf
            )
        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Failed to publish message: {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e

    async def setup_consumer(self, queue_name: str, callback: Callable[[aio_pika.abc.AbstractIncomingMessage], Any],
                             **kwargs) -> str:
        """
        Subscribes to an existing queue and registers an asynchronous message handler.

        Args:
            queue_name (str): The specific target queue matching incoming frames.
            callback (Callable): Coroutine executable triggered when a message arrives.
            **kwargs: Consumer specific context arguments overrides.

        Returns:
            str: An identity string representing the server-assigned consumer tag.

        Raises:
            RabbitMQAsyncError: If physical broker pipelines are disconnected.
        """
        if not self.is_connected():
            error_msg = f"{self._LOG_PREFIX} Cannot register consumer: Connector is offline."
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg)

        consume_conf = {**DEFAULT_CONSUME_CONF, **kwargs}
        consume_conf.pop("requeue") # TODO TER DE RETIRAR NÃO FAZ MT SENTIDO VER MELHOR ALTERNATIVA SERÁ QUE CONFIGURAÇÕES DE CONSUMO E DE APROVAÇÃO DE MENSAGENS DEVERIAM FICAR SEPARADAS?

        if self._logger:
            self._logger.debug(f"{self._LOG_PREFIX} Registering consumer callback on queue '{queue_name}'...")

        try:
            queue = await self._channel.get_queue(queue_name, ensure=False)

            consumer_tag = await queue.consume(
                callback=callback,
                **consume_conf
            ) # TODO pode ser util no futuro guardar o consumer_tag para fechar a queue certa
            self._consuming = True # TODO NÃO SEI BEM PARA QUE SERVE ISTO AQUI
            if self._logger:
                self._logger.debug(f"{self._LOG_PREFIX} Consumer successfully established with tag: {consumer_tag}")
            return consumer_tag
        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Failed to setup async consumer on queue '{queue_name}': {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e

    def is_connected(self) -> bool:
        """
        Checks if both the connection and the internal socket channels are alive and open.

        Returns:
            bool: True if connection structures are up, otherwise False.
        """
        return (
                self._connection is not None
                and not self._connection.is_closed
                and self._channel is not None
                and not self._channel.is_closed
                )

    async def acknowledge_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """
        Acknowledges an incoming AMQP message frame cleanly.

        Args:
            message (aio_pika.abc.AbstractIncomingMessage): Incoming AMQP message frame instance to acknowledge.
        """
        try:
            await message.ack()
            if self._logger:
                self._logger.debug(f"{self._LOG_PREFIX} Message frame successfully acknowledged (ACK).")
        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Failed to acknowledge message: {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e

    async def reject_message(self, message: aio_pika.abc.AbstractIncomingMessage, **kwargs) -> None:
        """
        Rejects or negatively acknowledges an AMQP message frame, resolving
        the 'requeue' behavior via passed configuration blocks or general defaults.

        Args:
            message (aio_pika.abc.AbstractIncomingMessage): Incoming AMQP message frame instance to reject.
            **kwargs: Configuration properties mapping parameters like 'requeue'.
        """
        consume_conf = {**DEFAULT_CONSUME_CONF, **kwargs}

        requeue_target = consume_conf.get("requeue", False)

        try:
            await message.nack(requeue=requeue_target)
            if self._logger:
                self._logger.warning(
                    f"{self._LOG_PREFIX} Message frame rejected (NACK). Requeue applied: {requeue_target}"
                )
        except Exception as e:
            error_msg = f"{self._LOG_PREFIX} Failed to reject/nack message: {e}"
            if self._logger:
                self._logger.error(error_msg)
            raise RabbitMQAsyncError(error_msg) from e

    async def close(self) -> None:
        """
        Gracefully tears down active channels and connections without blocking the loop.

        Isolates channel and connection teardowns into separate try blocks to ensure
        that a failing channel closure does not leak the underlying TCP connection socket.
        """
        if self._logger:
            self._logger.debug(f"{self._LOG_PREFIX} Initiating graceful connection teardown...")

        # 1. Attempt isolated channel closure
        if self._channel and not self._channel.is_closed:
            try:
                await self._channel.close()
            except Exception as e:
                if self._logger:
                    self._logger.error(f"{self._LOG_PREFIX} Error while closing AMQP channel: {e}")

        # 2. Attempt isolated connection closure inside a finally block to guarantee execution
        try:
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
        except Exception as e:
            if self._logger:
                self._logger.error(f"{self._LOG_PREFIX} Error while closing AMQP connection: {e}")
        finally:
            # Explicit state reset guaranteed even in case of unhandled errors
            self._channel = None
            self._connection = None
            self._consuming = False

            if self._logger:
                self._logger.debug(f"{self._LOG_PREFIX} Infrastructure teardown completed.")