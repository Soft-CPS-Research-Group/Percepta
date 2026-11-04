# --- Standard Library Imports ---
import asyncio
import json

# --- Third-Party Imports ---
import aio_pika
import pytest
import pytest_asyncio
from testcontainers.rabbitmq import RabbitMqContainer

# --- Local Application Imports ---
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector, RabbitMQAsyncError
from app.core.utils.logger import LoggingUtils

# Mark all test coroutines in this module as asyncio-enabled integration tests
pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ============================================================================
# INTEGRATION TEST FIXTURES (CONTAINER & CONNECTOR)
# ============================================================================

@pytest.fixture
def mock_logger():
    """Provides a real, configured instance of LoggingUtils for integration testing."""
    config = {
        "log_files": {
            "max_size": 1024 * 1024  # 1MB
        }
    }
    return LoggingUtils(
        component_name="async_rabbitmq_connector_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture(scope="session")
def rabbitmq_container():
    """Spawns an isolated RabbitMQ Docker container once for the entire test session."""
    with RabbitMqContainer("rabbitmq:3.12-management") as rabbitmq:
        yield rabbitmq


@pytest_asyncio.fixture
async def integration_connector(rabbitmq_container, mock_logger):
    """Provides an active, connected AsyncRabbitMQConnector bound to the test container."""
    config = {
        "host": rabbitmq_container.get_container_host_ip(),
        "port": int(rabbitmq_container.get_exposed_port(5672)),
        "vhost": "/",
        "auth": {
            "username": rabbitmq_container.username,
            "password": rabbitmq_container.password,
        },
        "timeout": 10,
        "prefetch_count": 10,
    }

    connector = AsyncRabbitMQConnector(server_configurations=config, logger=mock_logger)
    await connector.connect()

    yield connector

    # Ensure clean resource teardown after each test execution
    await connector.close()


# ============================================================================
# 1. CONNECTION & LIFECYCLE INTEGRATION TESTS
# ============================================================================

async def test_connect_and_is_connected_integration(integration_connector):
    """Verifies that the connector secures an active AMQP socket connection with the broker."""
    assert integration_connector.is_connected() is True


async def test_close_teardown_integration(integration_connector):
    """Verifies graceful connection teardown against a live RabbitMQ broker."""
    assert integration_connector.is_connected() is True
    await integration_connector.close()
    assert integration_connector.is_connected() is False


# ============================================================================
# 2. TOPOLOGY & DECLARATION INTEGRATION TESTS
# ============================================================================

async def test_declare_exchange_and_queue_integration(integration_connector):
    """Verifies exchange declaration, queue creation, and binding topology on a live broker."""
    exchange_name = "integration_telemetry_exchange"
    exchange = await integration_connector.declare_exchange(
        exchange_name=exchange_name,
        exchange_type="topic",
        durable=False
    )
    assert exchange is not None

    queue_name = await integration_connector.declare_queue(
        queue_name="integration_db_queue",
        exchange_name=exchange_name,
        routing_key="telemetry.#",
        durable=False,
        auto_delete=True
    )
    assert "integration_db_queue" in queue_name


async def test_declare_queue_raises_error_when_exchange_missing_integration(integration_connector):
    """
    Verifies that attempting to bind a queue to a non-existent exchange
    causes the broker to reject the channel operation and raises RabbitMQAsyncError.
    """
    with pytest.raises(RabbitMQAsyncError, match="Failed to declare/bind queue"):
        await integration_connector.declare_queue(
            queue_name="orphan_queue",
            exchange_name="non_existent_exchange_1234",
            routing_key="test.#"
        )


# ============================================================================
# 3. END-TO-END MESSAGING & ROUTING INTEGRATION TESTS
# ============================================================================

async def test_publish_and_consume_end_to_end_integration(integration_connector):
    """
    Verifies the complete messaging pipeline:
    1. Declares exchange and queue topology.
    2. Registers an asynchronous consumer callback.
    3. Publishes a JSON payload.
    4. Confirms that the consumer receives, deserializes, and acknowledges (ACK) the frame.
    """
    exchange_name = "e2e_exchange"
    routing_key = "sensor.temperature"
    queue_name_input = "e2e_queue"

    await integration_connector.declare_exchange(
        exchange_name=exchange_name,
        exchange_type="direct",
        durable=False
    )

    actual_queue_name = await integration_connector.declare_queue(
        queue_name=queue_name_input,
        exchange_name=exchange_name,
        routing_key=routing_key,
        durable=False,
        auto_delete=True
    )

    received_messages = []
    message_received_event = asyncio.Event()

    async def on_message_callback(message: aio_pika.abc.AbstractIncomingMessage):
        async with message.process():
            payload = message.body.decode("utf-8")
            received_messages.append(payload)
            message_received_event.set()

    consumer_tag = await integration_connector.setup_consumer(
        queue_name=actual_queue_name,
        callback=on_message_callback,
        no_ack=False
    )
    assert consumer_tag is not None

    test_payload = {"sensor_id": "temp_sensor_01", "reading_celsius": 24.8}
    await integration_connector.publish(
        routing_key=routing_key,
        message=test_payload,
        exchange_name=exchange_name
    )

    await asyncio.wait_for(message_received_event.wait(), timeout=5.0)

    assert len(received_messages) == 1
    parsed_received = json.loads(received_messages[0])
    assert parsed_received["sensor_id"] == "temp_sensor_01"
    assert parsed_received["reading_celsius"] == 24.8


async def test_fanout_exchange_broadcast_integration(integration_connector):
    """
    Verifies that publishing a single payload to a fanout exchange broadcasts
    the message to all bound queues regardless of the routing key.
    """
    message = {"broadcast": "alert"}
    exchange_name = "fanout_broadcast_exchange"
    await integration_connector.declare_exchange(exchange_name=exchange_name, exchange_type="fanout")

    q1_name = await integration_connector.declare_queue(
        queue_name="fanout_q1", exchange_name=exchange_name, auto_delete=True
    )
    q2_name = await integration_connector.declare_queue(
        queue_name="fanout_q2", exchange_name=exchange_name, auto_delete=True
    )

    received_q1, received_q2 = [], []
    e1, e2 = asyncio.Event(), asyncio.Event()

    async def cb1(msg):
        async with msg.process():
            payload = json.loads(msg.body.decode())
            assert payload == message
            received_q1.append(payload)
            e1.set()

    async def cb2(msg):
        async with msg.process():
            payload = json.loads(msg.body.decode())
            assert payload == message
            received_q2.append(payload)
            e2.set()

    await integration_connector.setup_consumer(queue_name=q1_name, callback=cb1, no_ack=False)
    await integration_connector.setup_consumer(queue_name=q2_name, callback=cb2, no_ack=False)

    await integration_connector.publish(routing_key="", message=message, exchange_name=exchange_name)

    await asyncio.gather(
        asyncio.wait_for(e1.wait(), timeout=5.0),
        asyncio.wait_for(e2.wait(), timeout=5.0)
    )

    assert len(received_q1) == 1
    assert len(received_q2) == 1


# ============================================================================
# 4. ACKNOWLEDGEMENT, REJECTION & DEAD LETTER (DLX) INTEGRATION TESTS
# ============================================================================

async def test_message_rejection_without_requeue_integration(integration_connector):
    """
    Verifies that rejecting a message with requeue=False causes RabbitMQ
    to permanently discard the frame from the queue.
    """
    exchange_name = "nack_exchange"
    routing_key = "test.nack"

    await integration_connector.declare_exchange(exchange_name=exchange_name, exchange_type="direct")
    queue_name = await integration_connector.declare_queue(
        queue_name="nack_queue",
        exchange_name=exchange_name,
        routing_key=routing_key,
        auto_delete=True
    )

    rejection_event = asyncio.Event()

    async def on_message_callback(message: aio_pika.abc.AbstractIncomingMessage):
        await integration_connector.reject_message(message, requeue=False)
        rejection_event.set()

    await integration_connector.setup_consumer(queue_name=queue_name, callback=on_message_callback, no_ack=False)

    await integration_connector.publish(
        routing_key=routing_key,
        message={"status": "fail"},
        exchange_name=exchange_name
    )

    await asyncio.wait_for(rejection_event.wait(), timeout=5.0)
    assert rejection_event.is_set()


async def test_message_rejection_with_requeue_integration(integration_connector):
    """
    Verifies that rejecting a message with requeue=True triggers RabbitMQ to
    re-enqueue and redeliver the frame back to the consumer with redelivered=True.
    """
    exchange_name = "requeue_exchange"
    routing_key = "test.requeue"

    await integration_connector.declare_exchange(exchange_name=exchange_name, exchange_type="direct")
    queue_name = await integration_connector.declare_queue(
        queue_name="requeue_queue",
        exchange_name=exchange_name,
        routing_key=routing_key,
        auto_delete=True
    )

    delivery_count = 0
    requeue_event = asyncio.Event()

    async def on_message_callback(message: aio_pika.abc.AbstractIncomingMessage):
        nonlocal delivery_count
        delivery_count += 1

        if delivery_count == 1:
            # First attempt: Reject and request requeue
            await integration_connector.reject_message(message, requeue=True)
        else:
            # Second attempt: Verify redelivery metadata flag
            assert message.redelivered is True
            await integration_connector.acknowledge_message(message)
            requeue_event.set()

    await integration_connector.setup_consumer(
        queue_name=queue_name,
        callback=on_message_callback,
        no_ack=False
    )

    await integration_connector.publish(
        routing_key=routing_key,
        message={"status": "retry_me"},
        exchange_name=exchange_name
    )

    await asyncio.wait_for(requeue_event.wait(), timeout=5.0)

    assert delivery_count == 2
    assert requeue_event.is_set()


async def test_dead_letter_exchange_routing_integration(integration_connector):
    """
    Verifies that rejecting a message with requeue=False automatically routes
    the unhandled frame into a configured Dead Letter Exchange (DLX).
    """
    dlx_name = "dead_letter_exchange"
    dlx_queue_name = "dead_letter_queue"
    main_queue_name = "work_queue_with_dlx"
    routing_key = "work.item"

    # Setup Dead Letter Exchange and DLX Queue
    await integration_connector.declare_exchange(exchange_name=dlx_name, exchange_type="direct")
    actual_dlx_q = await integration_connector.declare_queue(
        queue_name=dlx_queue_name, exchange_name=dlx_name, routing_key=routing_key, auto_delete=True
    )

    # Setup Main Queue configured with DLX arguments
    actual_main_q = await integration_connector.declare_queue(
        queue_name=main_queue_name,
        arguments={
            "x-dead-letter-exchange": dlx_name,
            "x-dead-letter-routing-key": routing_key
        },
        auto_delete=True
    )

    dlx_received = []
    dlx_event = asyncio.Event()

    async def dlx_callback(msg):
        async with msg.process():
            dlx_received.append(msg.body.decode())
            dlx_event.set()

    async def main_failing_callback(msg):
        await integration_connector.reject_message(msg, requeue=False)

    await integration_connector.setup_consumer(queue_name=actual_dlx_q, callback=dlx_callback, no_ack=False)
    await integration_connector.setup_consumer(queue_name=actual_main_q, callback=main_failing_callback, no_ack=False)

    await integration_connector.publish(routing_key=actual_main_q, message={"task": "poison_pill"})

    await asyncio.wait_for(dlx_event.wait(), timeout=5.0)
    assert len(dlx_received) == 1
    assert "poison_pill" in dlx_received[0]


# ============================================================================
# 5. ADVANCED PROTOCOL & CONFIGURATION INTEGRATION TESTS
# ============================================================================

async def test_qos_prefetch_throttling_integration(integration_connector):
    """
    Verifies that the channel prefetch_count setting limits the number of unacknowledged
    messages delivered to an active consumer.
    """
    exchange_name = "qos_exchange"
    routing_key = "qos.key"

    await integration_connector.declare_exchange(exchange_name=exchange_name, exchange_type="direct")
    queue_name = await integration_connector.declare_queue(
        queue_name="qos_queue", exchange_name=exchange_name, routing_key=routing_key, auto_delete=True
    )

    received_messages = []

    async def unacked_callback(msg):
        received_messages.append(msg)
        # Omit ACK to hold frames in unacknowledged state

    await integration_connector.setup_consumer(queue_name=queue_name, callback=unacked_callback, no_ack=False)

    for i in range(3):
        await integration_connector.publish(routing_key=routing_key, message={"id": i}, exchange_name=exchange_name)

    await asyncio.sleep(0.5)

    # Clean up held frames to allow graceful teardown
    for msg in received_messages:
        await integration_connector.acknowledge_message(msg)

    assert len(received_messages) == 3


async def test_qos_prefetch_strict_throttling_integration(rabbitmq_container, mock_logger):
    """
    Verifies that prefetch_count=1 strictly limits broker deliveries to exactly 1 unacked message.
    """
    config = {
        "host": rabbitmq_container.get_container_host_ip(),
        "port": int(rabbitmq_container.get_exposed_port(5672)),
        "vhost": "/",
        "auth": {"username": rabbitmq_container.username, "password": rabbitmq_container.password},
        "prefetch_count": 1,  # Strict QoS boundary
    }
    strict_connector = AsyncRabbitMQConnector(server_configurations=config, logger=mock_logger)
    await strict_connector.connect()

    try:
        queue_name = await strict_connector.declare_queue(queue_name="strict_qos_queue", auto_delete=True)
        received_messages = []

        async def unacked_callback(msg):
            received_messages.append(msg)

        await strict_connector.setup_consumer(queue_name=queue_name, callback=unacked_callback, no_ack=False)

        for i in range(3):
            await strict_connector.publish(routing_key=queue_name, message={"id": i})

        await asyncio.sleep(0.5)

        # AMQP QoS Guarantee: Exactly 1 message must be delivered while unacknowledged
        assert len(received_messages) == 1

        # Acknowledge first frame -> Broker releases the second frame
        await strict_connector.acknowledge_message(received_messages[0])
        await asyncio.sleep(0.3)
        assert len(received_messages) == 2

        # Acknowledge second frame -> Cleanup remaining
        await strict_connector.acknowledge_message(received_messages[1])
    finally:
        await strict_connector.close()


async def test_message_ttl_expiration_integration(integration_connector):
    """
    Verifies that published messages configured with an expiration TTL automatically
    expire inside the broker queue if no consumer receives them in time.
    """
    queue_name = await integration_connector.declare_queue(queue_name="ttl_queue", auto_delete=True)

    await integration_connector.publish(
        routing_key=queue_name,
        message={"data": "transient"},
        properties={"expiration": 0.2}  # 200 milliseconds TTL
    )

    # Wait for the message to expire in the broker
    await asyncio.sleep(0.5)

    received = []
    event = asyncio.Event()

    async def callback(msg):
        async with msg.process():
            received.append(msg)
            event.set()

    await integration_connector.setup_consumer(queue_name=queue_name, callback=callback, no_ack=False)

    # Expect timeout because the expired message was dropped by RabbitMQ
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(event.wait(), timeout=1.0)

    assert len(received) == 0


async def test_exclusive_queue_resource_locked_integration(rabbitmq_container, integration_connector, mock_logger):
    """
    Verifies AMQP 405 RESOURCE_LOCKED exception when a secondary connection attempts
    to attach a consumer to an exclusive queue owned by another active connection.
    """
    exclusive_queue = await integration_connector.declare_queue(queue_name="exclusive_q", exclusive=True)

    config2 = {
        "host": rabbitmq_container.get_container_host_ip(),
        "port": int(rabbitmq_container.get_exposed_port(5672)),
        "vhost": "/",
        "auth": {"username": rabbitmq_container.username, "password": rabbitmq_container.password},
    }
    secondary_connector = AsyncRabbitMQConnector(server_configurations=config2, logger=mock_logger)
    await secondary_connector.connect()

    try:
        async def dummy_cb(msg):
            pass

        with pytest.raises(RabbitMQAsyncError, match="Failed to setup async consumer"):
            await secondary_connector.setup_consumer(queue_name=exclusive_queue, callback=dummy_cb)
    finally:
        await secondary_connector.close()


# ============================================================================
# 6. INVALID KWARGS & ERROR HANDLING INTEGRATION TESTS
# ============================================================================

async def test_declare_exchange_invalid_kwargs_raises_error_integration(integration_connector):
    """
    Verifies that attempting to re-declare an exchange with conflicting durability
    attributes causes RabbitMQ to raise RabbitMQAsyncError.
    """
    exchange_name = "kwargs_exchange_test"

    await integration_connector.declare_exchange(
        exchange_name=exchange_name,
        exchange_type="direct",
        durable=False
    )

    with pytest.raises(RabbitMQAsyncError, match="Error declaring exchange"):
        await integration_connector.declare_exchange(
            exchange_name=exchange_name,
            exchange_type="direct",
            durable=True
        )


async def test_declare_queue_precondition_failed_on_redeclaration_integration(integration_connector):
    """
    Verifies AMQP 406 PRECONDITION_FAILED when attempting to re-declare an existing
    queue with conflicting durability configuration.
    """
    queue_name = "precondition_queue"

    # Declare queue with durable=False
    await integration_connector.declare_queue(queue_name=queue_name, durable=False, auto_delete=True)

    # Re-declare same queue with conflicting durable=True attribute
    with pytest.raises(RabbitMQAsyncError, match="Failed to declare/bind queue"):
        await integration_connector.declare_queue(queue_name=queue_name, durable=True, auto_delete=False)


async def test_declare_exchange_unsupported_kwargs_raises_error_integration(integration_connector):
    """
    Verifies that passing unsupported parameters to declare_exchange causes the connector
    to reject the declaration and raise RabbitMQAsyncError.
    """
    exchange_name = "kwargs_unsupported_exchange_test"

    with pytest.raises(RabbitMQAsyncError, match="Error declaring exchange"):
        await integration_connector.declare_exchange(
            exchange_name=exchange_name,
            exchange_type="direct",
            invalid_parameter=True
        )


async def test_declare_queue_invalid_kwargs_type_raises_error_integration(integration_connector):
    """
    Verifies that passing invalid AMQP queue arguments (e.g., x-message-ttl as a string)
    causes the broker to reject queue declaration and raises RabbitMQAsyncError.
    """
    with pytest.raises(RabbitMQAsyncError, match="Failed to declare/bind queue"):
        await integration_connector.declare_queue(
            queue_name="invalid_args_queue",
            arguments={"x-message-ttl": "invalid_ttl_type"}
        )


async def test_publish_invalid_kwargs_raises_error_integration(integration_connector):
    """
    Verifies that passing invalid publication properties or message headers raises RabbitMQAsyncError.
    """
    exchange_name = "publish_kwargs_exchange"
    await integration_connector.declare_exchange(exchange_name=exchange_name, exchange_type="direct")

    with pytest.raises(RabbitMQAsyncError, match="Failed to publish message"):
        await integration_connector.publish(
            routing_key="test.key",
            message={"data": "test"},
            exchange_name=exchange_name,
            properties={"expiration": "-1000"}
        )


async def test_setup_consumer_invalid_kwargs_raises_error_integration(integration_connector):
    """
    Verifies that passing unsupported consumer arguments (e.g., x-priority as non-integer)
    causes setup_consumer to fail and raises RabbitMQAsyncError.
    """
    queue_name = await integration_connector.declare_queue(queue_name="consumer_kwargs_queue", auto_delete=True)

    async def dummy_callback(msg):
        pass

    with pytest.raises(RabbitMQAsyncError, match="Failed to setup async consumer"):
        await integration_connector.setup_consumer(
            queue_name=queue_name,
            callback=dummy_callback,
            arguments={"x-priority": "invalid_priority_string"}
        )

async def test_publish_unrouted_message_handles_gracefully_integration(integration_connector):
    """
    Verifies that publishing a message with an unrouted key (no queue bound)
    is handled cleanly by the broker without dropping the connection or raising errors.
    """
    exchange_name = "unrouted_test_exchange"
    await integration_connector.declare_exchange(exchange_name=exchange_name, exchange_type="direct")

    # Publish to exchange with a routing_key that has no queue bound to it
    await integration_connector.publish(
        routing_key="nobody.listening",
        message={"event": "unrouted"},
        exchange_name=exchange_name
    )

    # Confirm the connector remains operational and connected after unrouted delivery
    assert integration_connector.is_connected() is True