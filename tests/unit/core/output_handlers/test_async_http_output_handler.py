# --- Standard Library Imports ---
import asyncio
from unittest.mock import AsyncMock, patch

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.output_handlers.async_http_output_handler import (
    AsyncHTTPOutputHandler,
    HttpPublishMode,
)
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
        component_name="http_output_handler_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def valid_topology_setup():
    """Provides a valid output topology setup dictionary with static egress endpoints."""
    return {
        "endpoints": [
            {
                "path": "/v1/telemetry/meter_01",
                "method": "POST",
                "timeout_ms": 3000,
                "headers": {"Content-Type": "application/json"},
                "rpc_properties": {"enabled": True}
            },
            {
                "path": "/v1/telemetry/meter_02",
                "method": "PUT",
                "timeout_ms": 4000,
                "headers": {"Content-Type": "application/json"},
                "rpc_properties": {"enabled": True}
            }
        ]
    }


@pytest.fixture
def mock_connector():
    """Provides an AsyncMock instance for the AsyncHTTPConnector dependency."""
    connector = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"status": "received"})
    mock_response.text = AsyncMock(return_value='{"status": "received"}')
    connector.request.return_value = mock_response
    return connector


@pytest.fixture
def handler(valid_topology_setup, mock_connector, mock_logger):
    """Fixture returning an initialized AsyncHTTPOutputHandler instance."""
    return AsyncHTTPOutputHandler(
        topology_setup=valid_topology_setup,
        connector=mock_connector,
        logger=mock_logger
    )


# ============================================================================
# 1. INITIALIZATION & GUARD CLAUSES UNIT TESTS
# ============================================================================

def test_init_success_unit_test(handler, valid_topology_setup, mock_connector):
    """Verifies successful initialization and attribute state."""
    assert handler._topology_setup == valid_topology_setup
    assert handler._publisher_connector == mock_connector
    assert not handler._stop_event.is_set()


def test_init_raises_key_error_when_topology_missing_unit_test(mock_connector, mock_logger):
    """Verifies that KeyError is raised with correct message when topology_setup is missing."""
    expected_error = f"The topology configuration footprint is required for {AsyncHTTPOutputHandler.__name__}."

    with pytest.raises(KeyError, match=expected_error):
        AsyncHTTPOutputHandler(
            topology_setup={},
            connector=mock_connector,
            logger=mock_logger
        )


def test_init_raises_value_error_when_connector_missing_unit_test(valid_topology_setup, mock_logger):
    """Verifies that ValueError is raised with correct message when connector is missing."""
    expected_error = "Initialization error: 'connector' dependency is missing."

    with pytest.raises(ValueError, match=expected_error):
        AsyncHTTPOutputHandler(
            topology_setup=valid_topology_setup,
            connector=None,  # type: ignore
            logger=mock_logger
        )


def test_stop_pipeline_unit_test(handler):
    """Verifies that stop() transitions the internal stop flag."""
    assert not handler._stop_event.is_set()

    handler.stop()

    assert handler._stop_event.is_set()


@pytest.mark.asyncio
async def test_send_rejects_when_stopped_unit_test(handler, mock_connector):
    """Verifies that send() aborts and returns None if the handler is stopped."""
    handler.stop()

    result = await handler.send(message={"active_power": 120.5})

    assert result is None
    mock_connector.request.assert_not_called()


# ============================================================================
# 2. TARGET RESOLUTION & PUBLISH MODES UNIT TESTS (send)
# ============================================================================

@pytest.mark.asyncio
async def test_send_static_mode_dispatches_only_topology_endpoints_unit_test(
        handler, mock_connector
):
    """Verifies that STATIC mode targets strictly the endpoints configured in topology_setup."""
    message = {"sensor": "meter_01", "reading": 230.1}

    result = await handler.send(message=message, mode=HttpPublishMode.STATIC)

    assert result is not None
    assert "/v1/telemetry/meter_01" in result
    assert "/v1/telemetry/meter_02" in result
    assert len(result) == 2

    expected_response = {
        "status": "success",
        "code": 200,
        "data": {"status": "received"}
    }
    assert result["/v1/telemetry/meter_01"] == expected_response
    assert result["/v1/telemetry/meter_02"] == expected_response

    assert mock_connector.request.call_count == 2


@pytest.mark.asyncio
async def test_send_override_mode_targets_only_dynamic_endpoints_unit_test(
        handler, mock_connector
):
    """Verifies that OVERRIDE mode ignores static topology and uses dynamic endpoints."""
    message = {"sensor": "meter_03"}
    dynamic_endpoints = [
        {
            "path": "/v1/dynamic/override_target",
            "method": "POST",
            "rpc_properties": {"enabled": True}
        }
    ]

    result = await handler.send(
        message=message,
        dynamic_endpoints=dynamic_endpoints,
        mode=HttpPublishMode.OVERRIDE
    )

    assert result is not None
    assert "/v1/dynamic/override_target" in result
    assert "/v1/telemetry/meter_01" not in result
    assert mock_connector.request.call_count == 1


@pytest.mark.asyncio
async def test_send_override_mode_falls_back_to_static_when_dynamic_empty_unit_test(
        handler, mock_connector
):
    """Verifies that OVERRIDE mode falls back to static topology if dynamic endpoints list is empty."""
    message = {"sensor": "meter_01"}

    result = await handler.send(
        message=message,
        dynamic_endpoints=[],
        mode=HttpPublishMode.OVERRIDE
    )

    assert result is not None
    assert "/v1/telemetry/meter_01" in result
    assert "/v1/telemetry/meter_02" in result
    assert mock_connector.request.call_count == 2


@pytest.mark.asyncio
async def test_send_additive_mode_combines_static_and_dynamic_endpoints_unit_test(
        handler, mock_connector
):
    """Verifies that ADDITIVE mode targets both static topology and dynamic endpoints."""
    message = {"sensor": "meter_01"}
    dynamic_endpoints = [
        {
            "path": "/v1/telemetry/meter_03",
            "method": "POST",
            "rpc_properties": {"enabled": True}
        }
    ]

    result = await handler.send(
        message=message,
        dynamic_endpoints=dynamic_endpoints,
        mode=HttpPublishMode.ADDITIVE
    )

    assert result is not None
    assert len(result) == 3
    assert "/v1/telemetry/meter_01" in result
    assert "/v1/telemetry/meter_02" in result
    assert "/v1/telemetry/meter_03" in result
    assert mock_connector.request.call_count == 3


@pytest.mark.asyncio
async def test_send_returns_none_when_no_valid_endpoints_resolved_unit_test(
        mock_connector, mock_logger
):
    """Verifies that send() returns None if resolved endpoints contain no valid path entries."""

    expected_error = "Topology validation error: Mandatory string 'path' is missing or empty at index"

    with pytest.raises(ValueError, match=expected_error):
        AsyncHTTPOutputHandler(
            topology_setup={"endpoints": [{"path": ""}]},
            connector=mock_connector,
            logger=mock_logger
        )


# ============================================================================
# 3. ENDPOINT DISPATCHING & RPC MODES UNIT TESTS (_dispatch_to_endpoint)
# ============================================================================

@pytest.mark.asyncio
async def test_dispatch_raises_value_error_for_unsupported_http_method_unit_test(handler):
    """Verifies that attempting unsupported HTTP verbs raises ValueError with exact error message."""
    invalid_endpoint_block = {"path": "/v1/telemetry", "method": "GET"}

    await handler._dispatch_to_endpoint(invalid_endpoint_block, message={"data": 123})
    with patch.object(handler._logger, "warning") as spy_warning:
        await handler._dispatch_to_endpoint(invalid_endpoint_block, message={"data": 123})

        actual_msg = spy_warning.call_args[0][0]

        assert "Unsupported HTTP method requested" in actual_msg

@pytest.mark.asyncio
async def test_dispatch_rpc_enabled_returns_json_response_unit_test(handler, mock_connector):
    """Verifies RPC enabled execution returning structured JSON payload."""
    endpoint_block = {
        "path": "/v1/telemetry/rpc",
        "method": "POST",
        "timeout_ms": 2000,
        "headers": {"Content-Type": "application/json"},
        "rpc_properties": {"enabled": True}
    }

    mock_resp = AsyncMock()
    mock_resp.status = 201
    mock_resp.json = AsyncMock(return_value={"acknowledged": True, "id": 101})
    mock_connector.request.return_value = mock_resp

    result = await handler._dispatch_to_endpoint(endpoint_block, message={"voltage": 230})

    assert result == {
        "status": "success",
        "code": 201,
        "data": {"acknowledged": True, "id": 101}
    }
    mock_connector.request.assert_called_once_with(
        method="POST",
        endpoint="/v1/telemetry/rpc",
        timeout=2.0,
        headers={"Content-Type": "application/json"},
        json={"voltage": 230}
    )

@pytest.mark.asyncio
async def test_dispatch_rpc_enabled_unsupported_method_defaults_to_post_and_returns_json_unit_test(handler, mock_connector):
    """
    Verifies that requesting an unsupported HTTP method (e.g. GET) during RPC execution
    logs a warning, defaults gracefully to POST, and processes the JSON response correctly.
    """
    endpoint_block = {
        "path": "/v1/telemetry/rpc",
        "method": "GET",
        "timeout_ms": 2000,
        "headers": {"Content-Type": "application/json"},
        "rpc_properties": {"enabled": True}
    }

    mock_resp = AsyncMock()
    mock_resp.status = 201
    mock_resp.json = AsyncMock(return_value={"acknowledged": True, "id": 101})
    mock_connector.request.return_value = mock_resp

    result = await handler._dispatch_to_endpoint(endpoint_block, message={"voltage": 230})

    assert result == {
        "status": "success",
        "code": 201,
        "data": {"acknowledged": True, "id": 101}
    }
    mock_connector.request.assert_called_once_with(
        method="POST",
        endpoint="/v1/telemetry/rpc",
        timeout=2.0,
        headers={"Content-Type": "application/json"},
        json={"voltage": 230}
    )

@pytest.mark.asyncio
async def test_dispatch_rpc_enabled_falls_back_to_plain_text_when_json_fails_unit_test(
        handler, mock_connector
):
    """Verifies RPC enabled execution falling back to plain text parsing when response is not JSON."""
    endpoint_block = {
        "path": "/v1/telemetry/text",
        "method": "POST",
        "rpc_properties": {"enabled": True}
    }

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.side_effect = Exception("Invalid JSON format")
    mock_resp.text = AsyncMock(return_value="OK_ACK_TEXT")
    mock_connector.request.return_value = mock_resp

    result = await handler._dispatch_to_endpoint(endpoint_block, message={"voltage": 230})

    assert result == {
        "status": "success",
        "code": 200,
        "data": "OK_ACK_TEXT"
    }


@pytest.mark.asyncio
async def test_dispatch_rpc_enabled_captures_network_exception_unit_test(handler, mock_connector):
    """Verifies that network exceptions in RPC mode are captured and formatted as error dictionaries."""
    endpoint_block = {
        "path": "/v1/telemetry/fail",
        "method": "POST",
        "rpc_properties": {"enabled": True}
    }
    mock_connector.request.side_effect = TimeoutError("HTTP request timeout after 3000ms")

    with patch.object(handler._logger, "error") as spy_warning:
        result = await handler._dispatch_to_endpoint(endpoint_block, message={"voltage": 230})

        actual_msg = spy_warning.call_args[0][0]

        assert "Request failure on path" in actual_msg

        assert result == {
                "status": "error",
                "error": "HTTP request timeout after 3000ms"
            }

@pytest.mark.asyncio
async def test_dispatch_fire_and_forget_returns_none_and_triggers_background_task_unit_test(
        handler, mock_connector
):
    """Verifies that RPC disabled mode returns None immediately and dispatches background request."""
    endpoint_block = {
        "path": "/v1/telemetry/async",
        "method": "POST",
        "rpc_properties": {"enabled": False}
    }

    result = await handler._dispatch_to_endpoint(endpoint_block, message={"voltage": 230})

    assert result is None

    # Yield control to the asyncio event loop tick (sleep=0), allowing the
    # non-blocking background task created via 'asyncio.create_task()' to execute
    # before asserting the underlying network connector calls.
    await asyncio.sleep(0)

    mock_connector.request.assert_called_once()


# ============================================================================
# 4. BACKGROUND FIRE-AND-FORGET & DISPATCHER HELPER UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_fire_and_forget_request_success_unit_test(handler, mock_connector):
    """Verifies successful execution of isolated background fire-and-forget request."""
    await handler._fire_and_forget_request(
        method="POST",
        endpoint="/v1/telemetry/bg",
        data={"sensor": "meter_01"},
        headers={"HeaderKey": "HeaderVal"},
        timeout_ms=3000
    )

    mock_connector.request.assert_called_once_with(
        method="POST",
        endpoint="/v1/telemetry/bg",
        timeout=3.0,
        headers={"HeaderKey": "HeaderVal"},
        json={"sensor": "meter_01"}
    )


@pytest.mark.asyncio
async def test_fire_and_forget_request_handles_exception_gracefully_unit_test(
        handler, mock_connector
):
    """Verifies that background task exceptions are caught without breaking the loop."""
    mock_connector.request.side_effect = Exception("Socket connection dropped")
    with patch.object(handler._logger, "error") as spy_warning:

        await handler._fire_and_forget_request(
            method="PUT",
            endpoint="/v1/telemetry/bg_err",
            data="raw_payload",
            headers=None,
            timeout_ms=5000
        )

        actual_msg = spy_warning.call_args[0][0]

        assert "[Background Fire-and-Forget] Outbound task" in actual_msg

    mock_connector.request.assert_called_once()


@pytest.mark.asyncio
async def test_perform_request_routes_put_and_non_dict_data_unit_test(handler, mock_connector):
    """Verifies that perform_request handles PUT requests and serializes bytes/raw data using 'data' kwarg."""
    await handler._perform_request(
        method="PUT",
        endpoint="/v1/telemetry/binary",
        data=b"\x00\x01\x02",
        headers={"Content-Type": "application/octet-stream"},
        timeout_ms=4000
    )

    mock_connector.request.assert_called_once_with(
        method="PUT",
        endpoint="/v1/telemetry/binary",
        timeout=4.0,
        headers={"Content-Type": "application/octet-stream"},
        data=b"\x00\x01\x02"
    )