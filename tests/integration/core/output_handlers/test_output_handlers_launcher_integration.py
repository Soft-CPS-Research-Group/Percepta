# --- Standard Library Imports ---
from unittest.mock import AsyncMock, patch

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector
from app.core.connectors.async_http_connector import AsyncHTTPConnector
from app.core.output_handlers import OutputHandlersLauncher
from app.core.output_handlers.async_rabbitmq_output_handler import AsyncRabbitMQOutputHandler, MqPublishMode
from app.core.input_handlers.async_rabbitmq_input_handler import AsyncRabbitMQInputHandler
from app.core.output_handlers.async_http_output_handler import AsyncHTTPOutputHandler, HttpPublishMode
from app.core.utils.logger import LoggingUtils


# ============================================================================
# INTEGRATION TEST FIXTURES
# ============================================================================

@pytest.fixture
def mock_logger():
    """Provides a real LoggingUtils instance for telemetry capture."""
    config = {"log_files": {"max_size": 1024 * 1024}}
    return LoggingUtils(
        component_name="output_launcher_integration_test",
        configurations=config,
        environment="test"
    )


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
def rabbitmq_topology_config_no_rpc():
    """Provides RabbitMQ topology setup without RPC requirements."""
    return {
        "exchanges": [
            {
                "exchange_name": "telemetry_exchange",
                "routing_key": "telemetry.data",
                "exchange_properties": {"exchange_type": "direct", "durable": True},
                "message_properties": {"delivery_mode": 2, "content_type": "application/json"}
            }
        ]
    }


@pytest.fixture
def rabbitmq_topology_config_with_rpc():
    """Provides RabbitMQ topology setup with RPC enabled."""
    return {
        "exchanges": [
            {
                "exchange_name": "command_exchange",
                "routing_key": "device.command",
                "rpc_properties": {
                    "enabled": True,
                    "timeout_ms": 3000,
                    "reply_to": {
                        "exchanges": [
                            {
                                "exchange_name": "rpc_reply_exchange",
                                "queues": [
                                    {
                                        "queue_name": "rpc_reply_queue",
                                        "routing_key": "rpc.reply.key"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }


@pytest.fixture
def http_server_config():
    """Provides standard server connection configuration for HTTP."""
    return {
        "url": "https://api.cleanwatts.energy",
        "headers": {"Authorization": "Bearer test_token"}
    }


@pytest.fixture
def http_topology_config():
    """Provides HTTP topology setup with endpoints."""
    return {
        "endpoints": [
            {
                "path": "/v1/control/actuate",
                "method": "POST",
                "timeout_ms": 2000,
                "rpc_properties": {"enabled": True}
            }
        ]
    }


# ============================================================================
# 1. RABBITMQ ADAPTER INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_build_rabbitmq_handler_without_rpc_integration_test(
        mock_logger, rabbitmq_server_config, rabbitmq_topology_config_no_rpc
):
    """
    Integration Test: Builds a RabbitMQ Output Handler without RPC.
    Validates physical connection setup, handler initialization, message publication,
    and graceful infrastructure shutdown.
    """
    launcher = OutputHandlersLauncher(logger=mock_logger)

    with patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock) as mock_connect, \
         patch.object(AsyncRabbitMQConnector, "declare_exchange", new_callable=AsyncMock) as mock_declare, \
         patch.object(AsyncRabbitMQConnector, "publish", new_callable=AsyncMock) as mock_publish, \
         patch.object(AsyncRabbitMQConnector, "close", new_callable=AsyncMock) as mock_close, \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        # 1. Build Output Handler through Launcher
        output_handler, rpc_input_handler = await launcher.build_output_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=rabbitmq_topology_config_no_rpc,
            server_configurations=rabbitmq_server_config
        )

        # 2. Verify instance types
        assert isinstance(output_handler, AsyncRabbitMQOutputHandler)
        assert rpc_input_handler is None
        assert mock_connect.call_count == 1  # 1 publisher connector connected
        assert len(launcher._teardown_hooks) == 1

        # 3. Publish a message down the pipeline
        payload = {"sensor_id": "sensor_01", "value": 42.5}
        await output_handler.send(message=payload, mode=MqPublishMode.STATIC)

        mock_declare.assert_called_once_with(
            exchange_name="telemetry_exchange",
            exchange_type="direct",
            durable=True
        )
        mock_publish.assert_called_once()
        assert mock_publish.call_args.kwargs["exchange_name"] == "telemetry_exchange"
        assert mock_publish.call_args.kwargs["message"] == payload

        # 4. Perform graceful infrastructure teardown
        await launcher.shutdown_infrastructure()

        mock_close.assert_called_once()
        assert len(launcher._teardown_hooks) == 0


@pytest.mark.asyncio
async def test_build_rabbitmq_handler_with_rpc_integration_test(
        mock_logger, rabbitmq_server_config, rabbitmq_topology_config_with_rpc
):
    """
    Integration Test: Builds a RabbitMQ Output Handler with active RPC configuration.
    Validates two connector physical connections (publisher + RPC receiver), callback bridge binding,
    and dual teardown closure execution.
    """
    launcher = OutputHandlersLauncher(logger=mock_logger)

    with patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock) as mock_connect, \
         patch.object(AsyncRabbitMQConnector, "close", new_callable=AsyncMock) as mock_close, \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        # 1. Build Output Handler through Launcher
        output_handler, rpc_input_handler = await launcher.build_output_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=rabbitmq_topology_config_with_rpc,
            server_configurations=rabbitmq_server_config
        )

        # 2. Verify dual connector establishment (Publisher + RPC Receiver)
        assert isinstance(output_handler, AsyncRabbitMQOutputHandler)
        assert isinstance(rpc_input_handler, AsyncRabbitMQInputHandler)
        assert mock_connect.call_count == 2  # 1 for egress + 1 for RPC reply listener

        # 3. Verify callback bridge mapping
        assert rpc_input_handler._on_message_callback == output_handler.on_rpc_response

        # 4. Perform infrastructure teardown
        await launcher.shutdown_infrastructure()

        assert mock_close.call_count == 2  # Both connectors closed
        assert len(launcher._teardown_hooks) == 0


# ============================================================================
# 2. HTTP ADAPTER INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_build_http_handler_integration_test(
        mock_logger, http_server_config, http_topology_config
):
    """
    Integration Test: Builds an HTTP Output Handler.
    Validates connector initialization, asynchronous request publishing,
    and teardown registration.
    """
    launcher = OutputHandlersLauncher(logger=mock_logger)

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"status": "actuated"})
    mock_response.text = AsyncMock(return_value='{"status": "actuated"}')

    with patch.object(AsyncHTTPConnector, "connect", new_callable=AsyncMock) as mock_connect, \
         patch.object(AsyncHTTPConnector, "request", new_callable=AsyncMock) as mock_send_req, \
         patch.object(AsyncHTTPConnector, "close", new_callable=AsyncMock) as mock_close, \
         patch.object(AsyncHTTPConnector, "is_connected", return_value=True):

        mock_send_req.return_value = mock_response

        output_handler, rpc_input_handler = await launcher.build_output_handler(
            handler_type="AsyncHTTP",
            topology_setup=http_topology_config,
            server_configurations=http_server_config
        )

        assert isinstance(output_handler, AsyncHTTPOutputHandler)
        assert rpc_input_handler is None
        mock_connect.assert_called_once()
        assert len(launcher._teardown_hooks) == 1

        command = {"command": "POWER_ON"}
        result = await output_handler.send(message=command, mode=HttpPublishMode.STATIC)

        mock_send_req.assert_called_once()

        expected_result = {
            "/v1/control/actuate": {
                "status": "success",
                "code": 200,
                "data": {"status": "actuated"}
            }
        }
        assert result == expected_result

        await launcher.shutdown_infrastructure()

        mock_close.assert_called_once()
        assert len(launcher._teardown_hooks) == 0


# ============================================================================
# 3. LAUNCHER ERROR HANDLING INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_launcher_unregistered_protocol_raises_value_error_integration_test(
        mock_logger, rabbitmq_server_config, rabbitmq_topology_config_no_rpc
):
    """Integration Test: Validates ValueError when requesting an unregistered protocol identifier."""
    launcher = OutputHandlersLauncher(logger=mock_logger)
    unregistered_protocol = "MQTT_PROT"

    with pytest.raises(ValueError) as exc_info:
        await launcher.build_output_handler(
            handler_type=unregistered_protocol,
            topology_setup=rabbitmq_topology_config_no_rpc,
            server_configurations=rabbitmq_server_config
        )

    expected_error = f"Launcher Core | Execution fail: '{unregistered_protocol}' is not registered in the global registry."
    assert expected_error in str(exc_info.value)

@pytest.mark.asyncio
async def test_launcher_teardown_resilience_on_connector_error_integration_test(
        mock_logger, rabbitmq_server_config, rabbitmq_topology_config_no_rpc
):
    """
    Integration Test: Ensures shutdown_infrastructure continues executing remaining teardown
    hooks even if one hook raises an exception during execution.
    """
    launcher = OutputHandlersLauncher(logger=mock_logger)

    valid_http_topology = {
        "endpoints": [
            {
                "path": "/v1/health",
                "method": "POST"
            }
        ]
    }

    with patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncHTTPConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "close", side_effect=RuntimeError("AMQP Close failed")), \
         patch.object(AsyncHTTPConnector, "close", new_callable=AsyncMock) as mock_http_close, \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True), \
         patch.object(AsyncHTTPConnector, "is_connected", return_value=True):

        # Build two handlers (RabbitMQ and HTTP)
        await launcher.build_output_handler("AsyncRabbitMQ", rabbitmq_topology_config_no_rpc, rabbitmq_server_config)
        await launcher.build_output_handler("AsyncHTTP", valid_http_topology, {"url": "http://localhost"})

        assert len(launcher._teardown_hooks) == 2

        # Execute shutdown. RabbitMQ teardown will raise RuntimeError, but HTTP teardown must still run.
        await launcher.shutdown_infrastructure()

        mock_http_close.assert_called_once()
        assert len(launcher._teardown_hooks) == 0