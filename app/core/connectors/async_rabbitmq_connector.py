# --- Standard Library Imports ---
import json
import random
import ssl
from typing import Awaitable, Callable, Dict, Optional, Any

# --- Third-Party Imports ---
import aio_pika
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractExchange, AbstractQueue


class AsyncRabbitMQConnector:
    """
    Asynchronous wrapper managing an aio-pika connection, channel, and AMQP operations.

    Fully integrated with asyncio event loops for high-throughput non-blocking I/O.

    The connector does not log. Errors are raised and whoever calls it decides what to do with them.
    """

    # The start of the message of every error raised by this class (e.g., "Async RabbitMQ Connector | Cannot publish
    # message: Connector is offline."), so that an error in the logs says where it comes from.
    _LOG_PREFIX: str = "Async RabbitMQ Connector |"

    # The connection to the broker (the TCP connection, like a telephone line) and the channel (a virtual line inside
    # the connection, through which the commands go: declare, publish, consume...). Both are None until 'connect()'
    # creates them and after 'close()'. The messages are sent through the channel, not directly through the connection.
    _connection: Optional[AbstractConnection] = None
    _channel: Optional[AbstractChannel] = None

    def __init__(self, server_configurations: dict) -> None:
        """
        Initializes physical network transport connection state configurations.

        Args:
            server_configurations (dict): The configuration of the broker, the 'connection' of the configuration of
                the system. Only 'host' is mandatory. Example with all the options the connector uses:

                    {
                        "host": "rabbit.cleanwatts.energy",
                        "port": 5671,
                        "vhost": "iEnergyInnov",
                        "heartbeat": 660,
                        "timeout": 10,
                        "prefetch_count": 10,
                        "auth": {"username": "<username>", "password": "<password>"},
                        "ssl_conf": {"enabled": True, "check_hostname": False, "verify_mode": "none"}
                    }

                An option that is not given is not passed to aio-pika, which uses its own default (port 5672, virtual
                host "/", username and password "guest", heartbeat of 60 seconds, no timeout, no limit of messages in
                transit). TLS is only used if 'ssl_conf' is enabled, whatever the port is.

        Raises:
            ValueError: If the configuration is not valid (see '_validate_server_configurations()').
            TypeError: If an option of the configuration has the wrong type (see '_validate_server_configurations()').
        """
        # The configuration is checked before anything is read from it, so the rest of the method can trust it.
        self._validate_server_configurations(server_configurations)

        # The options of the connection. Only 'host' is mandatory. An option that is not in the configuration stays
        # None, and then 'connect()' does not pass it to aio-pika, which uses its own default (e.g., port 5672, virtual
        # host "/", no timeout, no limit of messages in transit).
        self._host = server_configurations.get("host")
        self._port = server_configurations.get("port")
        self._vhost = server_configurations.get("vhost")
        self._heartbeat = server_configurations.get("heartbeat")
        self._timeout = server_configurations.get("timeout")
        self._prefetch_count = server_configurations.get("prefetch_count")
        self._ssl_conf = server_configurations.get("ssl_conf")

        # The consumers that are active, by consumer tag, e.g., {"ctag1.9f3a": <the object of the queue "x">}. A
        # consumer can only be cancelled through the object of the queue that holds it, so 'setup_consumer()' saves it
        # here and 'cancel_consumer()' looks for it by the tag.
        self._consumers: Dict[str, AbstractQueue] = {}

        # Authentication credentials. A null 'auth' counts as no credentials.
        _auth = server_configurations.get("auth") or {}
        self._username = _auth.get("username")
        self._password = _auth.get("password")

    def _validate_server_configurations(self, server_configurations: dict) -> None:
        """
        Checks the configuration of the broker before the connector is created.

        Raises:
            ValueError: If 'host' is not provided, if 'port' is not between 1 and 65535, if 'heartbeat' is not between
                0 and 65534, if 'timeout' is not greater than 0, if 'prefetch_count' is negative, if 'auth' has only
                one of the username and the password, or if 'ssl_conf' has an invalid 'verify_mode' or asks for
                'check_hostname' without verifying the certificate.
            TypeError: If any option has the wrong type (e.g., 'port' is not an integer).
        """
        _host = server_configurations.get("host")
        _port = server_configurations.get("port")
        _heartbeat = server_configurations.get("heartbeat")
        _timeout = server_configurations.get("timeout")
        _prefetch_count = server_configurations.get("prefetch_count")
        _auth = server_configurations.get("auth")
        _ssl_conf = server_configurations.get("ssl_conf")

        # A null 'auth' or 'ssl_conf' is accepted (its type is checked in Guard Clause 2), so the options inside
        # them are read from an empty dictionary in that case.
        _auth_options = _auth if isinstance(_auth, dict) else {}
        _ssl_options = _ssl_conf if isinstance(_ssl_conf, dict) else {}

        # Guard Clause 1: Checks that 'host' was provided.
        if not _host:
            raise ValueError(f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'host' must be provided.")

        # Guard Clause 2: Checks the type of every option. Only 'host' is mandatory; the others (and the options
        # inside 'auth' and 'ssl_conf') are optional: a missing key or a null value is accepted. A boolean is not
        # accepted where a number is expected, even though Python sees True and False as the integers 1 and 0. The
        # message only says the type of the wrong value, and never the value, because it can be a credential.
        for name, value, expected_type, description, optional in (
                ("host", _host, str, "a string", False),
                ("port", _port, int, "an integer", True),
                ("vhost", server_configurations.get("vhost"), str, "a string", True),
                ("heartbeat", _heartbeat, int, "an integer", True),
                ("timeout", _timeout, (int, float), "a number", True),
                ("prefetch_count", _prefetch_count, int, "an integer", True),
                ("auth", _auth, dict, "a dictionary", True),
                ("auth.username", _auth_options.get("username"), str, "a string", True),
                ("auth.password", _auth_options.get("password"), str, "a string", True),
                ("ssl_conf", _ssl_conf, dict, "a dictionary", True),
                ("ssl_conf.enabled", _ssl_options.get("enabled"), bool, "a boolean", True),
                ("ssl_conf.check_hostname", _ssl_options.get("check_hostname"), bool, "a boolean", True),
                ("ssl_conf.verify_mode", _ssl_options.get("verify_mode"), str, "a string", True),
        ):
            if value is None and optional:
                continue
            if isinstance(value, expected_type) and not (isinstance(value, bool) and expected_type is not bool):
                continue
            raise TypeError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: '{name}' must be {description}, and it is "
                f"{type(value).__name__}."
            )

        # Guard Clause 3: Checks that the numbers that were given are in a valid range. A 'prefetch_count' of 0 is
        # valid: in AMQP it means no limit of unacknowledged messages. A 'heartbeat' of 0 is valid too (it turns the
        # heartbeats off), but aio-pika also turns them off if the value is 65535 or more, so those are refused: they
        # would switch them off without the configuration saying so.
        if _port is not None and not 1 <= _port <= 65535:
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'port' must be between 1 and 65535, "
                f"and it is {_port!r}."
            )
        if _heartbeat is not None and not 0 <= _heartbeat < 65535:
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'heartbeat' must be between 0 and 65534, "
                f"and it is {_heartbeat!r}."
            )
        if _timeout is not None and _timeout <= 0:
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'timeout' must be greater than 0, "
                f"and it is {_timeout!r}."
            )
        if _prefetch_count is not None and _prefetch_count < 0:
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'prefetch_count' cannot be negative, "
                f"and it is {_prefetch_count!r}."
            )

        # Guard Clause 4: Checks that 'auth' has both the username and the password, or neither. With only one of
        # them, the connection would be tried with an incomplete login.
        if bool(_auth_options.get("username")) != bool(_auth_options.get("password")):
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'auth' must have both 'username' and "
                f"'password', or neither."
            )

        # Guard Clause 5: Checks 'verify_mode'. Any other text would be read as "none" without warning, which would
        # turn off the verification of the certificate by mistake (e.g., "require" instead of "required").
        _verify_mode = str(_ssl_options.get("verify_mode", "none")).lower()
        if _verify_mode not in ("none", "required"):
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'ssl_conf.verify_mode' must be \"none\" or "
                f"\"required\", and it is {_ssl_options.get('verify_mode')!r}."
            )

        # Guard Clause 6: Checks that 'check_hostname' is only asked together with the verification of the
        # certificate. The hostname is checked against the certificate, so without a certificate to verify
        # ('verify_mode' "none") Python refuses the combination.
        if _ssl_options.get("check_hostname") and _verify_mode != "required":
            raise ValueError(
                f"{self._LOG_PREFIX} Error initializing RabbitMQ setup: 'ssl_conf.check_hostname' needs "
                f"'ssl_conf.verify_mode' to be \"required\"."
            )

# TODO perceber melhor como é feito o setup de ssl e como funciona exatamente
    def _setup_ssl_context(self) -> Optional[ssl.SSLContext]:
        """
        Creates an asynchronous-safe SSLContext for secure network connections.

        Returns:
            Optional[ssl.SSLContext]: A configured SSL context if enabled, otherwise None.
        """
        # Case 1: no 'ssl_conf' in the configuration, or 'enabled' is false or missing, so no SSL context is created and
        # the connection is made without TLS (None is what 'connect()' passes to aio-pika as "no context").
        if self._ssl_conf is None or not self._ssl_conf.get("enabled", False):
            return None

        # Case 2: TLS is enabled. 'create_default_context()' makes a context with the safe settings of Python: it
        # loads the certificates of the authorities that the system trusts (the ones used to check the certificate of
        # the broker), verifies the certificate of the server ('verify_mode' CERT_REQUIRED) and checks that the
        # hostname matches the certificate ('check_hostname' True). The next lines change those two settings to what
        # the configuration asks for.
        context = ssl.create_default_context()

        # Whether the hostname that the connector connects to (e.g., "rabbit.cleanwatts.energy") has to match the one
        # in the certificate of the broker. It is False unless the configuration says otherwise (a null value also
        # counts as False). This line has to come before 'verify_mode': Python does not allow CERT_NONE while
        # 'check_hostname' is True, and the default context starts with it True.
        context.check_hostname = self._ssl_conf.get("check_hostname", False)

        # The text of the verification mode, in lowercase so that "required", "Required" and "REQUIRED" are the same.
        # It is "none" if the configuration does not have 'verify_mode' (or if it is null).
        verify_mode_str = str(self._ssl_conf.get("verify_mode", "none")).lower()

        if verify_mode_str == "required":
            # The certificate of the broker has to be valid and signed by an authority that the system trusts, or the
            # connection is refused.
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            # "none" (the validation in '__init__' refuses any other text): the certificate is not verified, so the
            # traffic is encrypted but the connector cannot be sure of who is at the other end. It is the only way to
            # connect to a broker with a self-signed certificate, if its certificate is not given to the context.
            context.verify_mode = ssl.CERT_NONE

        return context

    async def connect(self) -> None:
        """
        Establishes the robust non-blocking connection and opens the communication channel.

        Raises:
            RuntimeError: If the connector is already connected.
            TimeoutError: If the broker does not answer within the configured timeout.
            aio_pika.exceptions.AMQPException: If the broker refuses the connection (e.g., wrong credentials).

        If it fails after the connection was opened (e.g., the channel cannot be opened), the connection is closed
        and the connector stays as it was before, so 'connect()' can be called again.
        """
        # Guard Clause 1: Checks that the connector is not connected yet. A second 'connect()' would open another
        # connection and channel and the first ones would be lost without being closed (with the connection still
        # open on the broker, and with its consumers still running).
        if self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot connect: Connector is already connected.")

        # The SSL context with the TLS settings of the configuration, or None if the connection is without TLS.
        ssl_context = self._setup_ssl_context()

        # Whether the connection uses TLS (the "amqps" scheme instead of "amqp"). It only depends on the configuration:
        # it does if there is an SSL context ('ssl_conf' enabled), and it does not otherwise. The port is not used to
        # decide it: 5671 is the standard port of AMQP over TLS, but a configuration with that port and without
        # 'ssl_conf' enabled would turn TLS on against what it says (and with the default settings of aio-pika).
        use_ssl = ssl_context is not None

        # Only the options that are in the configuration are passed. The others are left out so that aio-pika uses its
        # own default (e.g., if 'port' is not configured, it connects to 5672). Passing None would not do that: it would
        # replace the default of aio-pika by None.
        # The 'heartbeat' is the interval, in seconds, that the connector proposes to the broker for the heartbeats:
        # small messages that both sides send when the connection is idle, to know that the other side is still there.
        # If a few heartbeats in a row do not arrive, the connection is considered lost (and the robust connection
        # reconnects). Without them, a connection cut by a firewall or a VPN, with no message to show it, could look
        # open for a long time. The default of aio-pika is 60, and 0 turns them off. aio-pika has no argument for it:
        # it goes in the URL of the connection, which is why it is passed here with the other options.
        connection_options = {
            "port": self._port,
            "virtualhost": self._vhost,
            "heartbeat": self._heartbeat,
            "login": self._username,
            "password": self._password,
            "timeout": self._timeout,
        }
        connection_options = {name: value for name, value in connection_options.items() if value is not None}

        # Opens the connection to the broker. 'connect_robust' gives a connection that, if it is lost (e.g., the broker
        # restarts), reconnects by itself and restores what was declared (exchanges, queues, bindings and consumers).
        self._connection = await aio_pika.connect_robust(
            host=self._host,
            ssl=use_ssl,
            ssl_context=ssl_context,
            **connection_options
        )

        # The connection is already open here, so if the next steps fail it has to be closed before the error goes up.
        # Otherwise 'self._connection' would stay open with nobody to close it: 'is_connected()' would be False (there
        # is no channel), so the next 'connect()' would open another connection and this one would be lost.
        try:
            # Opens the channel, the one that every command of the connector goes through.
            self._channel = await self._connection.channel()

            # --- Quality of Service (QoS) flow control ---
            # Without 'prefetch_count' no limit is set (aio-pika's default is 0: the broker sends all the messages of
            # the queue as fast as it can, without waiting for the acknowledgements).
            if self._prefetch_count is not None:
                await self._channel.set_qos(prefetch_count=self._prefetch_count)
        except BaseException:
            # Closes what was opened and puts the connector back to the state before 'connect()' ('close()' ignores
            # the errors of closing, so the error that is raised below is the original one). It catches BaseException
            # so that it also cleans up if the task is cancelled (e.g., a timeout of the caller), which is not an
            # 'Exception'. The error is not handled here, it goes up as it is.
            await self.close()
            raise

    async def declare_exchange(self, exchange_name: str, **kwargs) -> AbstractExchange:
        """
        Declares an AMQP exchange asynchronously within the active channel.

        The declaration is idempotent: if the exchange already exists with the same properties,
        nothing changes; if it does not exist, it is created. If it exists with different
        properties, the broker refuses the declaration.

        If 'passive' is true in the properties, the exchange is only checked and never created,
        which does not need permission to configure it. Use it for exchanges that belong to another party.

        Args:
            exchange_name (str): Name of the exchange, as in the 'exchange_name' of the configuration.
                Examples: "pred.R-H-03", "percepta_local_forecast".
            **kwargs: Options of the exchange, the 'exchange_properties' of the configuration. They go to aio-pika as
                they are, except 'exchange_type', which is passed with the name aio-pika uses, 'type'. If
                'exchange_type' is not given, aio-pika uses its default, "direct". Examples:
                {"exchange_type": "direct", "durable": True} or
                {"exchange_type": "fanout", "durable": True, "passive": True} (only checks it, never creates it).

        Returns:
            AbstractExchange: The proxy object representing the verified AMQP exchange topology.

        Raises:
            RuntimeError: If the connector is offline.
            aiormq.exceptions.ChannelNotFoundEntity: If 'passive' is true and the exchange does not exist.
            aiormq.exceptions.ChannelAccessRefused: If there is no permission to declare the exchange.
        """
        # Guard Clause 1: Checks that the connector is connected. Without a channel there is nothing to declare the
        # exchange on.
        if not self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot declare exchange: Connector is offline.")

        # The configuration calls the type of the exchange 'exchange_type' (e.g., {"exchange_type": "fanout",
        # "durable": True}), but the 'declare_exchange' of aio-pika has no argument with that name: its argument is
        # 'type'. Passing 'exchange_type' as it is would fail with "unexpected keyword argument". So the option is
        # renamed before the call. All the other options (durable, auto_delete, passive...) already have the name that
        # aio-pika uses and go through unchanged.

        # A copy of the options, so that removing 'exchange_type' below does not change the dictionary of the caller
        # (it comes from the configuration, and the input handler uses it again). It only copies the first level of
        # the dictionary (see the explanation in 'publish()'), which is enough because only a key is removed here.
        exchange_conf = dict(kwargs)

        # Takes 'exchange_type' out of the copy and keeps its value (e.g., "fanout"). It is None if the configuration
        # does not have it.
        exchange_type = exchange_conf.pop("exchange_type", None)

        if exchange_type is not None:
            # Puts the value back in the copy with the name aio-pika uses. Example: {"durable": True} becomes
            # {"durable": True, "type": "fanout"}. If there was no 'exchange_type', nothing is added and aio-pika
            # uses its own default ("direct").
            exchange_conf["type"] = exchange_type

        # Declares the exchange on the broker with all the options, e.g., declare_exchange(name="x", durable=True,
        # type="fanout"). Returns the exchange object.
        return await self._channel.declare_exchange(name=exchange_name, **exchange_conf)

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
            queue_name (str): Name of the queue, as in the 'queue_name' of the configuration. With an empty name the
                broker chooses one. Examples: "percepta_local_forecast_R-H-03", "".
            exchange_name (str): The exchange to bind the queue to. Without one the queue is declared but not bound.
                Examples: "pred.R-H-03", "".
            routing_key (str): The label that the messages need to have to reach this queue. "fanout" exchanges ignore
                it. Examples: "q.pred.R-H-03", "".
            **kwargs: Options of the queue, the 'queue_properties' of the configuration. They go to aio-pika as they
                are, except 'queue_prefix', which is not an option of aio-pika: if it is given, the name of the queue
                is built from it (see below). Examples: {"durable": True, "exclusive": False},
                {"durable": False, "auto_delete": True}, {"durable": True, "queue_prefix": "pred_"}.

        Returns:
            str: The name of the queue. It is the one the broker has, which is not the 'queue_name' that was given if
                there is a 'queue_prefix' (or if 'queue_name' is empty, when the broker chooses a name).

        Raises:
            RuntimeError: If the connector is offline.
        """
        # Guard Clause 1: Checks that the connector is connected. Without a channel there is nothing to declare the
        # queue on.
        if not self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot declare queue: Connector is offline.")

        # A copy of the options, so that removing 'queue_prefix' below does not change the dictionary of the caller
        # (it comes from the configuration, and the input handler uses it again). It only copies the first level of
        # the dictionary (see the explanation in 'publish()'), which is enough because only a key is removed here.
        queue_conf = dict(kwargs)

        # 'queue_prefix' is an option of this connector, not of aio-pika, so it is always taken out of the options
        # (even if it is empty or null), otherwise aio-pika would fail with "unexpected keyword argument".
        queue_prefix = queue_conf.pop("queue_prefix", None)

        # With a prefix, the queue gets a name made of: the prefix, a random number of 6 digits, and (if a name was
        # given) an underscore and that name. The random number makes the name different every time, so that each
        # instance of the application has its own queue instead of sharing one.
        # Examples: queue_prefix="pred_", queue_name="forecast" -> "pred_048213_forecast"
        #           queue_prefix="pred_", queue_name=""         -> "pred_048213"
        # Without a prefix, the name is used as it is.
        if queue_prefix:
            suffix = f"_{queue_name}" if queue_name else ""
            queue_name = f"{queue_prefix}{random.randint(0, 999999):06d}{suffix}"

        # Declares the queue on the broker with the options that are left (durable, exclusive, auto_delete...), e.g.,
        # declare_queue(name="pred_048213_forecast", durable=True). Like the exchange, the declaration can be repeated:
        # if the queue already exists with the same options nothing changes, and if it exists with different options
        # the broker refuses. With an empty name the broker creates the queue with a name of its own (e.g.,
        # "amq.gen-JzTY20BRgKO-HjmUJj0wLg"), which is why the name is read from the queue object at the end.
        queue = await self._channel.declare_queue(name=queue_name, **queue_conf)

        # The connector does not check if the exchange exists before the binding, and it is not worth doing:
        #   - The check would be a passive declaration ('get_exchange(name, ensure=True)'), which asks the broker
        #     for the exchange without creating it. If the exchange does not exist, the broker answers with an
        #     error (404 NOT_FOUND), and in AMQP that error closes the channel. So the check would not avoid the
        #     problem: it would only move the error and the channel would be closed anyway, leaving the connector
        #     offline.
        #   - To check without closing the main channel, a temporary channel would have to be opened and closed
        #     just for the test, in every queue, which is a lot of work for something the broker already does.
        #   - Without the check, the binding fails in the same way if the exchange does not exist, with an error
        #     that already says what is missing (e.g., "NOT_FOUND - no exchange 'x' in vhost 'y'").
        #   - In the application the exchange is always declared right before its queues (see the input handler),
        #     so when the binding is made the exchange exists. If its declaration fails, the startup has already
        #     stopped before reaching this point.
        # Binds the queue to the exchange, if one was given. A queue only receives the messages that the exchange
        # sends to it, and the binding is what tells the exchange to do that. The 'routing_key' is the label that the
        # messages need to have to reach this queue (it is ignored by "fanout" exchanges, which send every message to
        # all their queues). Without an 'exchange_name' the queue is declared but not bound.
        if exchange_name:
            await queue.bind(exchange=exchange_name, routing_key=routing_key)

        # Returns the name the queue really has on the broker (see above), so that whoever calls it can consume from
        # the right queue.
        return queue.name

    async def publish(self, routing_key: str, message: dict, exchange_name: str = "", **kwargs) -> None:
        """
        Publishes a message, serialized as JSON, to an exchange.

        Args:
            routing_key (str): The label of the message. The exchange uses it to decide which queues receive it.
                Examples: "q.pred.R-H-03", "energy.porto.nodes.12".
            message (dict): The content of the message. It is converted to JSON and sent as the body.
                Example: {"period_start": "2026-10-08 10:00:00 +0000", "entities": {"ev01": {...}}}.
            exchange_name (str): Name of the exchange. If empty, the message is sent to the AMQP default exchange,
                which delivers it to the queue whose name is the 'routing_key'. Examples: "pred.R-H-03", "".
            **kwargs: Options of the publication.
                'properties' (dict): the properties of the AMQP message, given to 'aio_pika.Message' as they are (the
                connector does not add any). Example: {"message_id": "5b0e...", "delivery_mode": 2,
                "content_type": "application/json", "correlation_id": "c1f3..."}.
                Any other option (e.g., 'mandatory', 'timeout') goes to the 'publish' of aio-pika. Example:
                {"properties": {"message_id": "5b0e..."}, "timeout": 5}.

        Raises:
            RuntimeError: If the connector is offline.
        """
        # Guard Clause 1: Checks that the connector is connected. Without a channel there is nowhere to publish.
        if not self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot publish message: Connector is offline.")

        # A copy of the options, so that removing 'properties' below does not change the dictionary of the caller.
        #
        # How a copy of a dictionary works: in Python, a variable (or a key of a dictionary) does not hold the value, it
        # is an arrow that points to an object that is somewhere else. 'dict(kwargs)' copies the arrows, not the objects
        # they point to: the new dictionary has the same keys, each one pointing to the same object as in the original.
        #
        #   kwargs                               publish_conf (the copy)
        #     "mandatory"  ──►  True      ◄──      "mandatory"
        #     "properties" ──►  {"message_id": "a"}  ◄──  "properties"
        #
        # What this means depends on whether the object can be changed from the inside:
        #   - int, str, bool and None cannot (they have no arrows inside them, there is nothing in them to change).
        #     Giving the key another value only changes where its arrow points, and the arrow of the other dictionary
        #     is not affected. Example: after 'publish_conf["mandatory"] = False' the original still has True.
        #   - dict and list can: a dictionary has arrows inside it (its keys), and a list has them too (its positions).
        #     Changing one of them (e.g., 'd["k"] = 1', 'd.pop("k")', 'items[0] = 2', 'items.append(1)') changes the
        #     object itself, and every arrow that points to it sees the change. Example: after
        #     'publish_conf["properties"]["message_id"] = "b"' the dictionary of the caller also has "b".
        # (Changing where an arrow points, like 'x = 6' after 'x = 5', never changes the old object. Here, the "5" is
        # not touched.) So 'publish_conf["properties"]' is still the dictionary of the caller, not a copy of it.
        publish_conf = dict(kwargs)

        # Takes 'properties' out of the options (it is for the message, not for the publication) and copies it. This
        # second copy is needed because of what is said above: the copy of the options does not copy the dictionaries
        # inside it, and without this one a change to 'properties' here (e.g., a 'pop') would also change the
        # dictionary of the caller. It is empty if there are none.
        properties = dict(publish_conf.pop("properties", None) or {})

        # Builds the AMQP message: the message converted to JSON text and then to bytes (what AMQP sends), plus the
        # properties. No property is added here, so the message only has the ones in the configuration (e.g., without
        # 'content_type' the receiver does not know that the body is JSON, and without 'delivery_mode' the broker
        # treats the message as transient).
        amqp_message = aio_pika.Message(body=json.dumps(message).encode("utf-8"), **properties)

        if exchange_name:
            # Gets the object of the exchange to publish through. With 'ensure=True' (the default), if the exchange was
            # declared by this connector, aio-pika returns the object it already has, without asking the broker. If not,
            # it asks the broker once (a passive declaration, which only checks and never creates) and keeps the object
            # for the next messages. If the exchange does not exist, the broker refuses with an error (404 NOT_FOUND)
            # and closes the channel, here and not in the 'publish' below, which is the same error as there.
            exchange = await self._channel.get_exchange(exchange_name)
        else:
            # No name: the AMQP default exchange, which every channel has.
            exchange = self._channel.default_exchange

        # Sends the message. By default aio-pika waits for the broker to confirm that it received it (publisher
        # confirms), so this line only ends after the confirmation.
        await exchange.publish(
            message=amqp_message,
            routing_key=routing_key,
            **publish_conf
        )

    async def setup_consumer(
            self,
            queue_name: str,
            callback: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[Any]],
            **kwargs
    ) -> str:
        """
        Subscribes to an existing queue and registers an asynchronous message handler.

        Args:
            queue_name (str): The name of an existing queue, the real one returned by 'declare_queue()'.
                Example: "percepta_local_forecast_R-H-03".
            callback (Callable): The coroutine that aio-pika calls with each message that arrives. Example: the
                'functools.partial(self._callback, queue_name="percepta_local_forecast_R-H-03")' of the input
                handler, which is awaited as 'callback(message)'.
            **kwargs: Options of the consumer, the 'consume_properties' of the configuration. They go to aio-pika as
                they are, except 'acknowledgement_properties', which is not an option of the consumer and is ignored
                here (it is used in 'reject_message()'). Examples:
                {"no_ack": False, "acknowledgement_properties": {"requeue": False}}, {"no_ack": True}.

        Returns:
            str: An identity string representing the server-assigned consumer tag.

        Raises:
            RuntimeError: If the connector is offline.
        """
        # Guard Clause 1: Checks that the connector is connected. Without a channel there is nothing to consume from.
        if not self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot register consumer: Connector is offline.")

        # A copy of the options, so that removing 'acknowledgement_properties' below does not change the dictionary of
        # the caller (it comes from the configuration, and the input handler uses it again to reject messages). It only
        # copies the first level of the dictionary (see the explanation in 'publish()'), which is enough because only a
        # key is removed here.
        consume_conf = dict(kwargs)

        # Takes 'acknowledgement_properties' out of the options (it is for rejecting a message, not for the consumer:
        # the consumer of aio-pika has no such option and would fail with "unexpected keyword argument"), like
        # 'publish()' does with 'properties'. It is in the same block of the configuration as 'no_ack'
        # ('consume_properties'), and it is used later by 'reject_message()', which receives the same block.
        # Example: {"no_ack": False, "acknowledgement_properties": {"requeue": False}} leaves {"no_ack": False}.
        consume_conf.pop("acknowledgement_properties", None)

        # Gets the object of the queue to consume from. It has to be the object that 'declare_queue()' created and the
        # channel registered (with 'ensure=True', the default, aio-pika returns that one if it knows the queue). The
        # consumer is saved in the object it is created from, and when the connection is lost and comes back, aio-pika
        # only restores what is in the objects it registered. With 'ensure=False' a new object would be created
        # (only in memory, nothing is asked to the broker) that is not registered, so after a reconnection the queue
        # and its binding would come back, but the consumer would not, and no message would be received.
        # If the queue does not exist, the broker refuses with an error (404 NOT_FOUND) and closes the channel, both
        # here and in the 'consume' below, so nothing is lost by checking it here.
        queue = await self._channel.get_queue(queue_name)

        # Starts the consumer: from now on aio-pika calls the 'callback' with every message that arrives in the queue.
        # It returns the consumer tag, the name of this subscription (e.g., "ctag1.9f3a"), which is needed to cancel it.
        consumer_tag = await queue.consume(
            callback=callback,
            **consume_conf
        )

        # Saves the queue object that holds the consumer, by its tag, so that 'cancel_consumer()' can find it.
        self._consumers[consumer_tag] = queue

        return consumer_tag

    async def cancel_consumer(self, consumer_tag: str) -> None:
        """
        Stops a consumer, so that the broker does not send it any more messages.

        Only that consumer stops: the connection, the channel and the other consumers stay as they are. The messages
        that were already delivered are still in the application, and have to be acknowledged or rejected as usual.
        The queue is not deleted, so its messages wait for a new consumer.

        Args:
            consumer_tag (str): The tag that 'setup_consumer()' returned for the consumer. Example: "ctag1.9f3a8c".

        Raises:
            RuntimeError: If the connector is offline.
            ValueError: If there is no active consumer with that tag (it was never set up, or it was cancelled
                already).
        """
        # Guard Clause 1: Checks that the connector is connected. Without a channel there is no consumer to cancel.
        if not self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot cancel consumer: Connector is offline.")

        # Guard Clause 2: Checks that the tag belongs to a consumer that is active. The tags are only known through
        # 'setup_consumer()', which saves each one with the queue that holds the consumer.
        queue = self._consumers.get(consumer_tag)
        if queue is None:
            raise ValueError(f"{self._LOG_PREFIX} Cannot cancel consumer: there is no active consumer with the tag "
                             f"{consumer_tag!r}.")

        # Asks the broker to stop sending messages to this consumer. The queue object also forgets the consumer, so
        # that aio-pika does not restore it if the connection is lost and comes back.
        await queue.cancel(consumer_tag)

        # The consumer is not active anymore. It is only removed here, after the cancellation worked, so that it can
        # be tried again if it failed.
        del self._consumers[consumer_tag]

    def is_connected(self) -> bool:
        """
        Checks if both the connection and the internal socket channels are alive and open.

        Returns:
            bool: True if connection structures are up, otherwise False.
        """
        # All four have to be true: the connection and the channel exist (they are None before 'connect()' and after
        # 'close()'), and neither of them is closed (the broker or the network can close them on its own).
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
            message (aio_pika.abc.AbstractIncomingMessage): The message that arrived in the callback of the consumer,
                e.g., one with 'body' b'{"a": 1}', 'exchange' "pred.R-H-03" and 'routing_key' "q.pred.R-H-03". It can
                only be acknowledged if the consumer was set up with "no_ack" False.
        """
        # Tells the broker that the message was processed, so that it removes it from the queue. Without it, the broker
        # keeps the message as "unacknowledged" and, if the connection is lost, delivers it again.
        await message.ack()

    async def reject_message(self, message: aio_pika.abc.AbstractIncomingMessage, **kwargs) -> None:
        """
        Rejects or negatively acknowledges an AMQP message frame, resolving
        the 'requeue' behavior via the passed configuration.

        Args:
            message (aio_pika.abc.AbstractIncomingMessage): The message that arrived in the callback of the consumer
                (see 'acknowledge_message()').
            **kwargs: The 'consume_properties' of the queue the message came from. Only its
                'acknowledgement_properties' are used here, and they are given to the 'nack' of aio-pika as they are,
                like the 'properties' of 'publish()'. Its 'requeue' decides what happens to the
                message: True sends it back to the queue (it is the default of aio-pika, if 'requeue' is not given),
                and False drops it, or sends it to the dead letter exchange of the queue if it has one.
                Examples: {"no_ack": False, "acknowledgement_properties": {"requeue": False}}, {}.
        """
        # Only the 'acknowledgement_properties' are used from the options (the rest of the 'consume_properties', e.g.,
        # 'no_ack', is not for rejecting a message). They are copied, like the 'properties' in 'publish()', so that the
        # dictionary of the caller is not changed. If there are none, nothing is passed to 'nack' and aio-pika uses its
        # own defaults, e.g., 'requeue' True: the refused message goes back to the queue.
        acknowledgement_properties = dict(kwargs.get("acknowledgement_properties") or {})

        # Tells the broker that the message was not processed. With 'requeue' True the broker puts it back in the queue;
        # with False it drops it (or sends it to the dead letter exchange of the queue, if there is one).
        await message.nack(**acknowledgement_properties)

    async def close(self) -> None:
        """
        Gracefully tears down active channels and connections without blocking the loop.

        Isolates channel and connection teardowns into separate try blocks to ensure
        that a failing channel closure does not leak the underlying TCP connection socket.
        Errors while closing are ignored on purpose: the connector is being discarded and there is
        nothing left to do with them.
        """
        # 1. Closes the channel first (the line inside the connection). It is only closed if it exists and is still
        # open.
        # Closing it also cancels the consumers that were using it. If this fails, the error is ignored and the
        # connection is closed anyway in the next step, so that its socket is not left open.
        if self._channel and not self._channel.is_closed:
            try:
                await self._channel.close()
            except Exception:
                pass

        # 2. Closes the connection (the TCP connection to the broker), the same way: only if it exists and is open, and
        # ignoring the error. The 'finally' below runs whether this worked or not.
        try:
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
        except Exception:
            pass
        finally:
            # 3. Forgets everything, so that the connector is back to the state it had before 'connect()':
            # 'is_connected()' is False, there are no consumers, and 'connect()' can be called again. It is done even if
            # the closing failed, because after 'close()' the old connection and channel cannot be trusted.
            self._channel = None
            self._connection = None
            self._consumers = {}
