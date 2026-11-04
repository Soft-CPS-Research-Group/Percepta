# --- Standard Library Imports ---
from unittest.mock import AsyncMock, patch

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector
from app.core.connectors.async_http_connector import AsyncHTTPConnector
from app.core.input_handlers.async_rabbitmq_input_handler import AsyncRabbitMQInputHandler
from app.core.input_handlers.async_http_input_handler import HTTPInputHandler as AsyncHTTPInputHandler
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler
from app.core.utils.logger import LoggingUtils


# ============================================================================
# INTEGRATION TEST FIXTURES
# ============================================================================

@pytest.fixture
def mock_logger():
    """Provides a real LoggingUtils instance for telemetry capture."""
    config = {"log_files": {"max_size": 1024 * 1024}}
    return LoggingUtils(
        component_name="input_launcher_integration_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def dummy_callback():
    """Provides an async dummy business callback to handle ingested envelopes."""
    async def _callback(envelope: dict) -> bool:
        return True
    return _callback


@pytest.fixture
def rabbitmq_server_config():
    """Provides standard server connection configuration for RabbitMQ."""
    return {
        "host": "localhost",
        "port": 5672,
        "vhost": "/",
        "auth": {"username": "guest", "password": "guest"}
    }


@pytest.fixture
def rabbitmq_topology_config():
    """Provides RabbitMQ topology setup with exchange and queue definitions."""
    return {
        "exchanges": [
            {
                "exchange_name": "telemetry_ingest_exchange",
                "exchange_properties": {"exchange_type": "fanout", "durable": True},
                "queues": [
                    {
                        "queue_name": "telemetry_ingest_queue",
                        "routing_key": "",
                        "consume_properties": {"no_ack": False}
                    }
                ]
            }
        ]
    }


@pytest.fixture
def http_server_config():
    """Provides standard server connection configuration for HTTP polling."""
    return {
        "url": "https://api.cleanwatts.energy",
        "timezone": "UTC",
        "headers": {"Authorization": "Bearer test_token"}
    }


@pytest.fixture
def http_topology_config():
    """Provides HTTP polling topology setup."""
    return {
        "poll_interval_seconds": 10,
        "endpoints": [
            {
                "path": "/v1/telemetry/meter_01",
                "method": "GET",
                "timeout_ms": 3000
            }
        ]
    }


# ============================================================================
# 1. ASYNC RABBITMQ INPUT HANDLER LAUNCHER INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_build_async_rabbitmq_input_handler_integration_test(
        mock_logger, dummy_callback, rabbitmq_server_config, rabbitmq_topology_config
):
    """
    Integration Test: Builds an AsyncRabbitMQ Input Handler.
    Validates physical connection setup, handler initialization, and infrastructure teardown.
    """
    launcher = InputHandlersLauncher(logger=mock_logger)

    with patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock) as mock_connect, \
         patch.object(AsyncRabbitMQConnector, "close", new_callable=AsyncMock) as mock_close, \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        # 1. Build Input Handler through Launcher
        input_handler = await launcher.build_input_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=rabbitmq_topology_config,
            callback=dummy_callback,
            server_configurations=rabbitmq_server_config
        )

        # 2. Verify instance type and connector initialization
        assert isinstance(input_handler, AsyncRabbitMQInputHandler)
        assert input_handler._on_message_callback == dummy_callback
        mock_connect.assert_called_once()
        assert len(launcher._teardown_hooks) == 1

        # 3. Perform infrastructure teardown
        await launcher.shutdown_infrastructure()

        mock_close.assert_called_once()
        assert len(launcher._teardown_hooks) == 0


# ============================================================================
# 2. ASYNC HTTP INPUT HANDLER LAUNCHER INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_build_async_http_input_handler_integration_test(
        mock_logger, dummy_callback, http_server_config, http_topology_config
):
    """
    Integration Test: Builds an AsyncHTTP Input Handler with scheduler support.
    Validates connector initialization, scheduler instantiation, timezone fallback, and teardown.
    """
    launcher = InputHandlersLauncher(logger=mock_logger)

    with patch.object(AsyncHTTPConnector, "connect", new_callable=AsyncMock) as mock_connect, \
         patch.object(AsyncHTTPConnector, "close", new_callable=AsyncMock) as mock_close, \
         patch.object(AsyncHTTPConnector, "is_connected", return_value=True), \
         patch.object(AsyncPeriodicScheduler, "stop") as mock_scheduler_stop:

        # 1. Build Input Handler through Launcher
        input_handler = await launcher.build_input_handler(
            handler_type="AsyncHTTP",
            topology_setup=http_topology_config,
            callback=dummy_callback,
            server_configurations=http_server_config
        )

        # 2. Verify instance type and dependencies
        assert isinstance(input_handler, AsyncHTTPInputHandler)
        assert input_handler._on_message_callback == dummy_callback
        mock_connect.assert_called_once()
        assert len(launcher._teardown_hooks) == 1

        # 3. Perform infrastructure teardown
        await launcher.shutdown_infrastructure()

        mock_scheduler_stop.assert_called_once()
        mock_close.assert_called_once()
        assert len(launcher._teardown_hooks) == 0


@pytest.mark.asyncio
async def test_build_async_http_input_handler_invalid_timezone_fallback_integration_test(
        mock_logger, dummy_callback, http_topology_config
):
    """
    Integration Test: Ensures invalid timezone configurations fall back gracefully to 'UTC'.
    """
    launcher = InputHandlersLauncher(logger=mock_logger)
    invalid_server_config = {
        "url": "https://api.cleanwatts.energy",
        "timezone": "INVALID/TIMEZONE_NAME"
    }

    with patch.object(AsyncHTTPConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncHTTPConnector, "close", new_callable=AsyncMock), \
         patch.object(AsyncHTTPConnector, "is_connected", return_value=True), \
         patch.object(mock_logger, "warning") as spy_warning:

        input_handler = await launcher.build_input_handler(
            handler_type="AsyncHTTP",
            topology_setup=http_topology_config,
            callback=dummy_callback,
            server_configurations=invalid_server_config
        )

        assert isinstance(input_handler, AsyncHTTPInputHandler)
        spy_warning.assert_called_once()
        actual_msg = spy_warning.call_args[0][0]
        assert "Timezone Resolver | Invalid timezone 'INVALID/TIMEZONE_NAME' provided" in actual_msg

        await launcher.shutdown_infrastructure()


# ============================================================================
# 3. LAUNCHER ERROR HANDLING INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_launcher_unregistered_protocol_raises_value_error_integration_test(
        mock_logger, dummy_callback, rabbitmq_server_config, rabbitmq_topology_config
):
    """Integration Test: Validates ValueError when requesting an unregistered protocol identifier."""
    launcher = InputHandlersLauncher(logger=mock_logger)
    unregistered_protocol = "MQTT_INGEST"

    with pytest.raises(ValueError) as exc_info:
        await launcher.build_input_handler(
            handler_type=unregistered_protocol,
            topology_setup=rabbitmq_topology_config,
            callback=dummy_callback,
            server_configurations=rabbitmq_server_config
        )

    expected_error = f"Launcher Core | Execution fail: '{unregistered_protocol}' is not registered in the global registry."
    assert expected_error in str(exc_info.value)


@pytest.mark.asyncio
async def test_launcher_teardown_resilience_on_connector_error_integration_test(
        mock_logger, dummy_callback, rabbitmq_server_config, rabbitmq_topology_config,
        http_server_config, http_topology_config
):
    """
    Integration Test: Ensures shutdown_infrastructure continues executing remaining teardown
    hooks even if one hook raises an exception during execution.
    """
    launcher = InputHandlersLauncher(logger=mock_logger)

    with patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncHTTPConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "close", side_effect=RuntimeError("AMQP Close failed")), \
         patch.object(AsyncHTTPConnector, "close", new_callable=AsyncMock) as mock_http_close, \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True), \
         patch.object(AsyncHTTPConnector, "is_connected", return_value=True):

        # Build two input handlers (RabbitMQ and HTTP)
        await launcher.build_input_handler("AsyncRabbitMQ", rabbitmq_topology_config, dummy_callback, rabbitmq_server_config)
        await launcher.build_input_handler("AsyncHTTP", http_topology_config, dummy_callback, http_server_config)

        assert len(launcher._teardown_hooks) == 2

        # Execute shutdown. RabbitMQ teardown will raise RuntimeError, but HTTP teardown must still run.
        await launcher.shutdown_infrastructure()

        mock_http_close.assert_called_once()
        assert len(launcher._teardown_hooks) == 0