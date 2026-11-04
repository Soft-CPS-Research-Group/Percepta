# --- Standard Library Imports ---
import json
import ssl
from unittest.mock import AsyncMock, MagicMock, patch, ANY

# --- Third-Party Imports ---
import aio_pika
from aio_pika.exceptions import ChannelNotFoundEntity
import pytest

# --- Local Application Imports ---
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector, RabbitMQAsyncError
from app.core.utils.logger import LoggingUtils


# ============================================================================
# UNIT TEST FIXTURES
# ============================================================================

@pytest.fixture
def valid_config():
    """Fixture providing a complete, valid connector configuration dictionary."""
    return {
        "host": "localhost",
        "port": 5672,
        "vhost": "/test_vhost",
        "timeout": 5,
        "prefetch_count": 20,
        "auth": {
            "username": "user_test",
            "password": "pass_test",
        },
        "ssl_conf": {
            "enabled": True,
            "check_hostname": False,
            "verify_mode": "none",
        },
    }


@pytest.fixture
def mock_logger():
    """Provides a real, configured instance of LoggingUtils for unit testing."""
    config = {
        "log_files": {
            "max_size": 1024 * 1024  # 1MB
        }
    }
    return LoggingUtils(
        component_name="async_rabbitmq_connector_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def connector(valid_config, mock_logger):
    """Fixture returning an initialized AsyncRabbitMQConnector instance."""
    return AsyncRabbitMQConnector(server_configurations=valid_config, logger=mock_logger)


# ============================================================================
# 1. INITIALIZATION UNIT TESTS
# ============================================================================

def test_init_raises_error_when_host_missing_unit_test(mock_logger):
    """Verifies that initialization raises RabbitMQAsyncError if 'host' is omitted."""
    invalid_config = {}
    with pytest.raises(RabbitMQAsyncError, match="'host' must be provided"):
        AsyncRabbitMQConnector(server_configurations=invalid_config, logger=mock_logger)


# ============================================================================
# 2. SSL CONTEXT UNIT TESTS
# ============================================================================

def test_setup_ssl_context_disabled_unit_test(connector):
    """Ensures None is returned when SSL configuration is disabled."""
    connector._ssl_conf = {"enabled": False}
    assert connector._setup_ssl_context() is None


@patch("ssl.create_default_context")
def test_setup_ssl_context_enabled_cert_none_unit_test(mock_create_ctx, connector):
    """Verifies SSLContext creation with check_hostname=False and verify_mode='none'."""
    mock_ctx = MagicMock()
    mock_create_ctx.return_value = mock_ctx

    connector._ssl_conf = {"enabled": True, "check_hostname": False, "verify_mode": "none"}
    ctx = connector._setup_ssl_context()

    assert ctx == mock_ctx
    assert mock_ctx.check_hostname is False
    assert mock_ctx.verify_mode == ssl.CERT_NONE


@patch("ssl.create_default_context")
def test_setup_ssl_context_enabled_cert_required_unit_test(mock_create_ctx, connector):
    """Verifies SSLContext creation with check_hostname=True and verify_mode='required'."""
    mock_ctx = MagicMock()
    mock_create_ctx.return_value = mock_ctx

    connector._ssl_conf = {"enabled": True, "check_hostname": True, "verify_mode": "required"}
    ctx = connector._setup_ssl_context()

    assert ctx == mock_ctx
    assert mock_ctx.check_hostname is True
    assert mock_ctx.verify_mode == ssl.CERT_REQUIRED


@patch("ssl.create_default_context")
def test_setup_ssl_context_failure_raises_custom_error_unit_test(mock_create_ctx, connector):
    """Verifies that failures during SSL context construction raise RabbitMQAsyncError."""
    mock_create_ctx.side_effect = Exception("OpenSSL configuration error")
    connector._ssl_conf = {"enabled": True}

    with pytest.raises(RabbitMQAsyncError, match="Failed to build SSL context"):
        connector._setup_ssl_context()


# ============================================================================
# 3. CONNECTION & STATE UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
@patch("aio_pika.connect_robust", new_callable=AsyncMock)
async def test_connect_success_unit_test(mock_connect_robust, connector):
    """Tests successful connection establishment and channel QoS configuration."""
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)

    mock_connection.channel.return_value = mock_channel
    mock_connect_robust.return_value = mock_connection

    await connector.connect()

    mock_connect_robust.assert_called_once_with(
        host="localhost",
        port=5672,
        login="user_test",
        password="pass_test",
        virtualhost="/test_vhost",
        ssl_context=ANY,
        timeout=5,
    )
    mock_channel.set_qos.assert_called_once_with(prefetch_count=20)
    assert connector.is_connected() is True


@pytest.mark.asyncio
@patch("aio_pika.connect_robust", new_callable=AsyncMock)
async def test_connect_fallback_credentials_unit_test(mock_connect_robust, mock_logger):
    """Verifies default parameter fallbacks when authentication and port configurations are omitted."""
    config = {"host": "127.0.0.1"}
    conn = AsyncRabbitMQConnector(server_configurations=config, logger=mock_logger)

    assert conn._host == "127.0.0.1"
    assert conn._port == 5672
    assert conn._vhost == "/"
    assert conn._username is None
    assert conn._password is None
    assert conn._timeout == 10
    assert conn._prefetch_count == 10

    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_connection.channel.return_value = mock_channel
    mock_connect_robust.return_value = mock_connection

    await conn.connect()

    mock_connect_robust.assert_called_once_with(
        host="127.0.0.1",
        port=5672,
        login=None,
        password=None,
        virtualhost="/",
        ssl_context=None,
        timeout=10,
    )


@pytest.mark.asyncio
@patch("aio_pika.connect_robust", side_effect=Exception("Connection refused"))
async def test_connect_failure_raises_custom_error_unit_test(mock_connect_robust, connector):
    """Ensures connection failures raise RabbitMQAsyncError and keep connection state False."""
    with pytest.raises(RabbitMQAsyncError, match="Failed to connect asynchronously to RabbitMQ"):
        await connector.connect()

    assert connector.is_connected() is False


def test_is_connected_states_unit_test(connector):
    """Tests all potential socket/channel state combinations checked by is_connected()."""
    # 1. Initial State (Disconnected)
    assert connector.is_connected() is False

    # 2. Both Connection and Channel Active
    mock_conn = MagicMock(is_closed=False)
    mock_chan = MagicMock(is_closed=False)

    connector._connection = mock_conn
    connector._channel = mock_chan
    assert connector.is_connected() is True

    # 3. Channel Closed
    mock_chan.is_closed = True
    assert connector.is_connected() is False

    # 4. Connection Closed
    mock_chan.is_closed = False
    mock_conn.is_closed = True
    assert connector.is_connected() is False


@pytest.mark.asyncio
async def test_operations_when_offline_raise_error_unit_test(connector):
    """Ensures queue declaration, publishing, and consuming fail fast when offline."""
    assert connector.is_connected() is False

    with pytest.raises(RabbitMQAsyncError, match="Cannot declare queue: Connector is offline"):
        await connector.declare_queue("test_queue")

    with pytest.raises(RabbitMQAsyncError, match="Cannot publish message: Connector is offline"):
        await connector.publish("routing_key", {"data": 123})

    with pytest.raises(RabbitMQAsyncError, match="Cannot register consumer: Connector is offline"):
        await connector.setup_consumer("test_queue", AsyncMock())


# ============================================================================
# 4. TOPOLOGY DECLARATION UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_declare_exchange_success_unit_test(connector):
    """Tests exchange declaration with explicit custom parameters and default fallbacks."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)

    connector._connection = mock_connection
    connector._channel = mock_channel

    # 1. Declaration with explicit custom parameters
    mock_exchange_1 = MagicMock()
    mock_channel.declare_exchange.return_value = mock_exchange_1

    result_1 = await connector.declare_exchange(
        "telemetry_exchange", exchange_type="fanout", durable=True, auto_delete=True
    )

    mock_channel.declare_exchange.assert_called_with(
        name="telemetry_exchange",
        type="fanout",
        durable=True,
        auto_delete=True,
        internal=False,
        arguments=None,
    )
    assert result_1 == mock_exchange_1

    # 2. Declaration using default fallback configuration
    mock_exchange_2 = MagicMock()
    mock_channel.declare_exchange.return_value = mock_exchange_2

    result_2 = await connector.declare_exchange("telemetry_exchange")

    mock_channel.declare_exchange.assert_called_with(
        name="telemetry_exchange",
        type="direct",
        durable=False,
        auto_delete=False,
        internal=False,
        arguments=None,
    )
    assert result_2 == mock_exchange_2
    assert mock_channel.declare_exchange.call_count == 2


@pytest.mark.asyncio
async def test_declare_exchange_failure_raises_custom_error_unit_test(connector):
    """Ensures exceptions during exchange declaration raise RabbitMQAsyncError."""
    mock_channel = AsyncMock(is_closed=False)
    mock_channel.declare_exchange.side_effect = Exception("Declaration failed due to invalid arguments")

    connector._connection = MagicMock(is_closed=False)
    connector._channel = mock_channel

    with pytest.raises(RabbitMQAsyncError, match="Error declaring exchange 'bad_exchange'"):
        await connector.declare_exchange("bad_exchange", exchange_type="topic")


@pytest.mark.asyncio
async def test_declare_queue_with_binding_and_prefix_unit_test(connector):
    """Tests queue declaration with prefixed name formatting and exchange binding."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)

    mock_queue = AsyncMock()
    mock_queue.name = "prefix_123456_db_queue"
    mock_channel.declare_queue.return_value = mock_queue

    connector._connection = mock_connection
    connector._channel = mock_channel

    with patch("random.randint", return_value=123456):
        q_name = await connector.declare_queue(
            queue_name="db_queue",
            exchange_name="telemetry_exchange",
            routing_key="telemetry.#",
            queue_prefix="prefix_",
        )

    mock_channel.declare_queue.assert_called_once_with(
        name="prefix_123456_db_queue", durable=True, exclusive=False
    )
    mock_queue.bind.assert_called_once_with(
        exchange="telemetry_exchange", routing_key="telemetry.#"
    )
    assert q_name == "prefix_123456_db_queue"


@pytest.mark.asyncio
async def test_declare_queue_without_prefix_and_with_binding_unit_test(connector):
    """Verifies queue declaration without prefixing and binds correctly to exchange."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_queue = AsyncMock()
    mock_queue.name = "telemetry_queue"
    mock_channel.declare_queue.return_value = mock_queue

    connector._connection = mock_connection
    connector._channel = mock_channel

    q_name = await connector.declare_queue(
        queue_name="telemetry_queue",
        exchange_name="telemetry_exchange",
        routing_key="telemetry.#",
        durable=True,
    )

    mock_channel.declare_queue.assert_called_once_with(
        name="telemetry_queue",
        durable=True,
        exclusive=False,
    )
    mock_queue.bind.assert_called_once_with(
        exchange="telemetry_exchange",
        routing_key="telemetry.#",
    )
    assert q_name == "telemetry_queue"


@pytest.mark.asyncio
async def test_declare_anonymous_queue_without_prefix_unit_test(connector):
    """Verifies that declaring an anonymous queue passes empty name string to aio-pika."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_queue = AsyncMock()

    # Server-generated random queue name returned by RabbitMQ
    mock_queue.name = "amq.gen-J8zXk1Lp9QsR"
    mock_channel.declare_queue.return_value = mock_queue

    connector._connection = mock_connection
    connector._channel = mock_channel

    q_name = await connector.declare_queue(
        queue_name="",
        exclusive=True,
        auto_delete=True,
    )

    mock_channel.declare_queue.assert_called_once_with(
        name="",
        durable=True,
        exclusive=True,
        auto_delete=True,
    )
    mock_queue.bind.assert_not_called()
    assert q_name == "amq.gen-J8zXk1Lp9QsR"


@pytest.mark.asyncio
async def test_declare_queue_raises_error_when_exchange_does_not_exist_unit_test(connector):
    """
    Verifies that attempting to bind a queue to a non-existent exchange catches
    ChannelNotFoundEntity and raises RabbitMQAsyncError.
    """
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_queue = AsyncMock()

    # Simulate RabbitMQ 404 NOT_FOUND during queue binding
    mock_queue.bind.side_effect = ChannelNotFoundEntity("No exchange 'missing_exchange' in vhost")
    mock_channel.declare_queue.return_value = mock_queue

    connector._connection = mock_connection
    connector._channel = mock_channel

    with pytest.raises(RabbitMQAsyncError, match="Failed to declare/bind queue"):
        await connector.declare_queue(
            queue_name="db_queue",
            exchange_name="missing_exchange",
            routing_key="telemetry.#"
        )

    mock_channel.declare_queue.assert_called_once()
    mock_queue.bind.assert_called_once_with(exchange="missing_exchange", routing_key="telemetry.#")


# ============================================================================
# 5. MESSAGE PUBLISHING UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_publish_message_to_custom_exchange_unit_test(connector):
    """Verifies JSON payload serialization and publishing to a named target exchange."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_exchange = AsyncMock()

    mock_channel.get_exchange.return_value = mock_exchange

    connector._connection = mock_connection
    connector._channel = mock_channel

    payload = {"sensor_id": "meter_01", "voltage": 230.5}

    await connector.publish(
        routing_key="telemetry.data",
        message=payload,
        exchange_name="telemetry_exchange",
        properties={"delivery_mode": 2},
    )

    mock_channel.get_exchange.assert_called_once_with("telemetry_exchange", ensure=False)
    mock_exchange.publish.assert_called_once()

    published_msg = mock_exchange.publish.call_args.kwargs["message"]
    assert published_msg.body == json.dumps(payload).encode("utf-8")
    assert published_msg.content_type == "application/json"
    assert published_msg.delivery_mode == aio_pika.DeliveryMode.PERSISTENT


@pytest.mark.asyncio
async def test_publish_message_to_default_exchange_unit_test(connector):
    """Verifies publishing using the default AMQP exchange when exchange_name is empty."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)

    mock_default_exchange = AsyncMock()
    mock_channel.default_exchange = mock_default_exchange

    connector._connection = mock_connection
    connector._channel = mock_channel

    await connector.publish(routing_key="direct_queue", message={"data": 123})

    mock_default_exchange.publish.assert_called_once()


@pytest.mark.asyncio
async def test_publish_raises_error_when_exchange_does_not_exist_unit_test(connector):
    """Verifies that publishing to an invalid/missing exchange raises RabbitMQAsyncError."""
    mock_channel = AsyncMock(is_closed=False)
    mock_channel.get_exchange.side_effect = ChannelNotFoundEntity("No exchange 'missing_exchange' in vhost")

    connector._connection = MagicMock(is_closed=False)
    connector._channel = mock_channel

    payload = {"sensor_id": "meter_01", "voltage": 230.5}

    with pytest.raises(RabbitMQAsyncError, match="Failed to publish message"):
        await connector.publish(
            routing_key="telemetry.data",
            message=payload,
            exchange_name="missing_exchange"
        )

    mock_channel.get_exchange.assert_called_once_with("missing_exchange", ensure=False)


# ============================================================================
# 6. CONSUMER & MESSAGE ACKNOWLEDGMENT UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_setup_consumer_success_unit_test(connector):
    """Tests consumer callback registration and consumer state tracking."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_queue = AsyncMock()
    mock_queue.consume.return_value = "ctag_12345"
    mock_channel.get_queue.return_value = mock_queue

    connector._connection = mock_connection
    connector._channel = mock_channel

    async def sample_callback(msg):
        pass

    consumer_tag = await connector.setup_consumer(
        queue_name="db_queue", callback=sample_callback, no_ack=False
    )

    mock_channel.get_queue.assert_called_once_with("db_queue", ensure=False)
    mock_queue.consume.assert_called_once_with(callback=sample_callback, no_ack=False)
    assert consumer_tag == "ctag_12345"
    assert connector._consuming is True


@pytest.mark.asyncio
async def test_setup_consumer_failure_raises_custom_error_unit_test(connector):
    """Ensures errors during consumer registration raise RabbitMQAsyncError."""
    mock_connection = MagicMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_channel.get_queue.side_effect = Exception("Queue access denied")

    connector._connection = mock_connection
    connector._channel = mock_channel

    with pytest.raises(RabbitMQAsyncError, match="Failed to setup async consumer on queue 'restricted_queue'"):
        await connector.setup_consumer("restricted_queue", AsyncMock())


@pytest.mark.asyncio
async def test_acknowledge_message_success_unit_test(connector):
    """Validates successful message frame acknowledgment (ACK)."""
    mock_message = AsyncMock()
    await connector.acknowledge_message(mock_message)
    mock_message.ack.assert_called_once()


@pytest.mark.asyncio
async def test_acknowledge_message_failure_raises_error_unit_test(connector):
    """Ensures acknowledgment errors raise RabbitMQAsyncError."""
    mock_message = AsyncMock()
    mock_message.ack.side_effect = Exception("ACK error")

    with pytest.raises(RabbitMQAsyncError, match="Failed to acknowledge message"):
        await connector.acknowledge_message(mock_message)


@pytest.mark.asyncio
async def test_reject_message_requeue_unit_test(connector):
    """Validates message frame rejection (NACK) with requeue=True."""
    mock_message = AsyncMock()
    await connector.reject_message(mock_message, requeue=True)
    mock_message.nack.assert_called_once_with(requeue=True)


@pytest.mark.asyncio
async def test_reject_message_default_no_requeue_unit_test(connector):
    """Validates message frame rejection (NACK) defaulting to requeue=False."""
    mock_message = AsyncMock()
    await connector.reject_message(mock_message)
    mock_message.nack.assert_called_once_with(requeue=False)


@pytest.mark.asyncio
async def test_reject_message_failure_raises_error_unit_test(connector):
    """Ensures errors during rejection raise RabbitMQAsyncError."""
    mock_message = AsyncMock()
    mock_message.nack.side_effect = Exception("NACK frame error")

    with pytest.raises(RabbitMQAsyncError, match="Failed to reject/nack message"):
        await connector.reject_message(mock_message)


# ============================================================================
# 7. TEARDOWN & CLEANUP UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_close_graceful_teardown_unit_test(connector):
    """Ensures graceful shutdown of active channels and connection sockets."""
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)

    connector._connection = mock_connection
    connector._channel = mock_channel

    await connector.close()

    mock_channel.close.assert_called_once()
    mock_connection.close.assert_called_once()


@pytest.mark.asyncio
async def test_close_closes_connection_even_if_channel_fails_unit_test(connector):
    """Ensures that the connection is closed even if closing the channel raises an exception."""
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)

    mock_channel.close.side_effect = Exception("Channel closed unexpectedly by broker")

    connector._connection = mock_connection
    connector._channel = mock_channel

    await connector.close()

    mock_channel.close.assert_called_once()
    mock_connection.close.assert_called_once()

    assert connector.is_connected() is False


@pytest.mark.asyncio
async def test_publish_invalid_message_properties_raises_custom_error_unit_test(connector):
    """
    Verifies that passing invalid message properties in kwargs causes aio_pika.Message
    construction to fail and raises RabbitMQAsyncError.
    """
    mock_channel = AsyncMock(is_closed=False)
    connector._connection = MagicMock(is_closed=False)
    connector._channel = mock_channel

    # Invalid property key that aio_pika.Message does not accept
    invalid_properties = {"properties": {"unsupported_amqp_field": "invalid"}}

    with pytest.raises(RabbitMQAsyncError, match="Failed to publish message"):
        await connector.publish(
            routing_key="test.key",
            message={"data": "test"},
            **invalid_properties
        )


@pytest.mark.asyncio
async def test_close_resilient_when_both_channel_and_connection_fail_unit_test(connector):
    """
    Ensures state is fully reset even if both channel.close() and connection.close()
    raise unexpected exceptions during teardown.
    """
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)

    mock_channel.close.side_effect = Exception("Channel force closed")
    mock_connection.close.side_effect = Exception("Socket reset by peer")

    connector._connection = mock_connection
    connector._channel = mock_channel
    connector._consuming = True

    await connector.close()

    # Verify state cleanup guarantee in finally block
    assert connector._channel is None
    assert connector._connection is None
    assert connector._consuming is False
    assert connector.is_connected() is False