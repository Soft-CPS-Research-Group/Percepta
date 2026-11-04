# --- Standard Library Imports ---
from unittest.mock import AsyncMock, MagicMock, patch

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.input_handlers.async_rabbitmq_input_handler import AsyncRabbitMQInputHandler
from app.core.utils.logger import LoggingUtils


# ============================================================================
# UNIT TEST FIXTURES
# ============================================================================

@pytest.fixture
def mock_logger():
    """Provides a real, configured instance of LoggingUtils for unit testing."""
    config = {
        "log_files": {
            "max_size": 1024 * 1024  # 1MB
        }
    }
    return LoggingUtils(
        component_name="rabbitmq_input_handler_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def valid_topology_setup():
    """Provides a valid topology setup dictionary with exchange and bound queue."""
    return {
        "exchanges": [
            {
                "exchange_name": "telemetry_exchange",
                "exchange_properties": {"exchange_type": "topic", "durable": True},
                "queues": [
                    {
                        "queue_name": "db_persistence_queue",
                        "routing_key": "telemetry.#",
                        "queue_properties": {"durable": True},
                        "consume_properties": {"no_ack": False, "requeue": False}
                    }
                ]
            }
        ]
    }


@pytest.fixture
def mock_connector():
    """Provides an AsyncMock instance for the AsyncRabbitMQConnector dependency."""
    connector = AsyncMock()
    connector.is_connected = MagicMock(return_value=True)
    connector.declare_queue.return_value = "db_persistence_queue"
    return connector


@pytest.fixture
def mock_callback():
    """Provides an AsyncMock instance for the processing callback."""
    callback = AsyncMock()
    callback.return_value = True  # Default to successful processing
    return callback


@pytest.fixture
def handler(valid_topology_setup, mock_callback, mock_connector, mock_logger):
    """Fixture returning an initialized AsyncRabbitMQInputHandler instance."""
    return AsyncRabbitMQInputHandler(
        topology_setup=valid_topology_setup,
        on_message_callback=mock_callback,
        connector=mock_connector,
        logger=mock_logger
    )


@pytest.fixture
def mock_amqp_message():
    """Provides a mock aio_pika AbstractIncomingMessage instance."""
    message = AsyncMock()
    message.proxy.queue.name = "db_persistence_queue"
    message.body = b'{"sensor_id": "meter_01", "voltage": 230}'
    return message


# ============================================================================
# 1. INITIALIZATION & GUARD CLAUSES UNIT TESTS
# ============================================================================

def test_init_success_unit_test(handler, valid_topology_setup):
    """Verifies successful initialization and attribute extraction."""
    assert len(handler._exchanges) == 1
    assert handler._exchanges[0]["exchange_name"] == "telemetry_exchange"
    assert handler._queue_consumers_props == {}


def test_init_raises_key_error_when_topology_missing_unit_test(
    mock_callback, mock_connector, mock_logger
):
    """Verifies that KeyError is raised when topology_setup is missing or empty."""
    with pytest.raises(KeyError, match="topology configuration footprint is required"):
        AsyncRabbitMQInputHandler(
            topology_setup={},
            on_message_callback=mock_callback,
            connector=mock_connector,
            logger=mock_logger
        )


def test_init_raises_value_error_when_exchanges_empty_unit_test(
    mock_callback, mock_connector, mock_logger
):
    """Verifies that ValueError is raised if the 'exchanges' list is missing or empty."""
    invalid_topology = {"exchanges": []}
    with pytest.raises(ValueError, match="'exchanges' must be a non-empty list."):
        AsyncRabbitMQInputHandler(
            topology_setup=invalid_topology,
            on_message_callback=mock_callback,
            connector=mock_connector,
            logger=mock_logger
        )


# ============================================================================
# 2. START & TOPOLOGY SETUP LIFECYCLE UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_start_success_when_connector_already_connected_unit_test(handler, mock_connector):
    """Verifies that start() sets up topology and consumers without calling connect() if already connected."""
    mock_connector.is_connected.return_value = True

    await handler.start()

    mock_connector.connect.assert_not_called()
    mock_connector.declare_exchange.assert_called_once_with(
        exchange_name="telemetry_exchange",
        exchange_type="topic",
        durable=True
    )
    mock_connector.declare_queue.assert_called_once_with(
        queue_name="db_persistence_queue",
        exchange_name="telemetry_exchange",
        routing_key="telemetry.#",
        durable=True
    )
    mock_connector.setup_consumer.assert_called_once_with(
        queue_name="db_persistence_queue",
        callback=handler._callback,
        no_ack=False,
        requeue=False
    )
    assert handler._queue_consumers_props["db_persistence_queue"] == {"no_ack": False, "requeue": False}


@pytest.mark.asyncio
async def test_start_connects_if_connector_is_offline_unit_test(handler, mock_connector):
    """Verifies that start() connects the underlying connector if it is offline."""
    mock_connector.is_connected.return_value = False

    await handler.start()

    mock_connector.connect.assert_called_once()
    mock_connector.setup_consumer.assert_called_once()


@pytest.mark.asyncio
async def test_start_aborts_if_stopped_unit_test(handler, mock_connector):
    """Verifies that start() aborts immediately if the stop event has already been set."""
    handler.stop()
    await handler.start()

    mock_connector.connect.assert_not_called()
    mock_connector.declare_exchange.assert_not_called()
    mock_connector.setup_consumer.assert_not_called()


def test_stop_pipeline_unit_test(handler):
    """Verifies that stop() sets the internal stop_event flag."""
    assert not handler._stop_event.is_set()

    handler.stop()

    assert handler._stop_event.is_set()


# ============================================================================
# 3. MESSAGE CALLBACK & DISPATCHING UNIT TESTS (_callback)
# ============================================================================

@pytest.mark.asyncio
@patch("app.endpoints_builders.payload_normalizer.PayloadNormalizer.from_rabbitmq", new_callable=AsyncMock)
async def test_callback_success_acknowledges_message_unit_test(
    mock_normalizer_from_rabbitmq, handler, mock_connector, mock_callback, mock_amqp_message
):
    """
    Verifies that when the callback processes the normalized envelope successfully (returns True),
    the message is acknowledged (ACK) via the connector.
    """
    handler._queue_consumers_props["db_persistence_queue"] = {"no_ack": False, "requeue": False}
    fake_envelope = {"protocol": "AMQP", "source": "telemetry_exchange/db_persistence_queue", "payload": b"data"}
    mock_normalizer_from_rabbitmq.return_value = fake_envelope
    mock_callback.return_value = True

    await handler._callback(mock_amqp_message)

    mock_normalizer_from_rabbitmq.assert_called_once_with(mock_amqp_message, queue_name="db_persistence_queue")
    mock_callback.assert_called_once_with(fake_envelope)
    mock_connector.acknowledge_message.assert_called_once_with(mock_amqp_message)
    mock_connector.reject_message.assert_not_called()


@pytest.mark.asyncio
@patch("app.endpoints_builders.payload_normalizer.PayloadNormalizer.from_rabbitmq", new_callable=AsyncMock)
async def test_callback_business_failure_rejects_message_unit_test(
    mock_normalizer_from_rabbitmq, handler, mock_connector, mock_callback, mock_amqp_message
):
    """
    Verifies that when the callback returns False (business failure),
    the message is rejected (NACK) with the configured consume properties.
    """
    handler._queue_consumers_props["db_persistence_queue"] = {"no_ack": False, "requeue": True}
    fake_envelope = {"protocol": "AMQP", "source": "telemetry_exchange/db_persistence_queue"}
    mock_normalizer_from_rabbitmq.return_value = fake_envelope
    mock_callback.return_value = False  # Business logic processing failed

    await handler._callback(mock_amqp_message)

    mock_connector.acknowledge_message.assert_not_called()
    mock_connector.reject_message.assert_called_once_with(
        mock_amqp_message, no_ack=False, requeue=True
    )


@pytest.mark.asyncio
@patch("app.endpoints_builders.payload_normalizer.PayloadNormalizer.from_rabbitmq", new_callable=AsyncMock)
async def test_callback_triggers_sync_callback_via_to_thread_unit_test(
    mock_normalizer_from_rabbitmq, valid_topology_setup, mock_connector, mock_logger, mock_amqp_message
):
    """Verifies that synchronous callbacks are offloaded safely using asyncio.to_thread."""
    sync_callback = MagicMock(return_value=True)
    handler_sync = AsyncRabbitMQInputHandler(
        topology_setup=valid_topology_setup,
        on_message_callback=sync_callback,
        connector=mock_connector,
        logger=mock_logger
    )
    handler_sync._queue_consumers_props["db_persistence_queue"] = {"no_ack": False}
    fake_envelope = {"protocol": "AMQP", "source": "telemetry_exchange/db_persistence_queue"}
    mock_normalizer_from_rabbitmq.return_value = fake_envelope

    await handler_sync._callback(mock_amqp_message)

    sync_callback.assert_called_once_with(fake_envelope)
    mock_connector.acknowledge_message.assert_called_once_with(mock_amqp_message)


@pytest.mark.asyncio
async def test_callback_drops_and_rejects_frame_when_stopped_unit_test(
    handler, mock_connector, mock_amqp_message
):
    """
    Verifies that if stop() has been called, incoming frames are rejected immediately
    to preserve data safety and requeue/drop rules.
    """
    handler._queue_consumers_props["db_persistence_queue"] = {"requeue": True}
    handler.stop()

    await handler._callback(mock_amqp_message)

    mock_connector.reject_message.assert_called_once_with(
        mock_amqp_message, requeue=True
    )
    mock_connector.acknowledge_message.assert_not_called()


@pytest.mark.asyncio
@patch("app.endpoints_builders.payload_normalizer.PayloadNormalizer.from_rabbitmq", new_callable=AsyncMock)
async def test_callback_handles_unhandled_exception_and_rejects_unit_test(
    mock_normalizer_from_rabbitmq, handler, mock_connector, mock_callback, mock_amqp_message
):
    """
    Verifies that unhandled exceptions inside the callback or normalizer trigger
    a structural rejection (NACK) to prevent message loss.
    """
    handler._queue_consumers_props["db_persistence_queue"] = {"no_ack": False, "requeue": False}
    mock_normalizer_from_rabbitmq.side_effect = Exception("Normalizer crashed")

    await handler._callback(mock_amqp_message)

    mock_connector.acknowledge_message.assert_not_called()
    mock_connector.reject_message.assert_called_once_with(
        mock_amqp_message, no_ack=False, requeue=False
    )


@pytest.mark.asyncio
async def test_callback_rejects_when_on_message_callback_is_none_unit_test(
    handler, mock_connector, mock_amqp_message
):
    """Verifies that if _on_message_callback becomes None, incoming frames are rejected."""
    handler._queue_consumers_props["db_persistence_queue"] = {"no_ack": False}
    handler._on_message_callback = None

    await handler._callback(mock_amqp_message)

    mock_connector.reject_message.assert_called_once_with(
        mock_amqp_message, no_ack=False
    )