# --- Standard Library Imports ---
import asyncio

# --- Third-Party Imports ---
from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest
import pytest_asyncio

# --- Local Application Imports ---
from app.core.connectors.async_http_connector import AsyncHTTPConnector, HTTPErrorWrapper
from app.core.utils.logger import LoggingUtils

# Mark all test coroutines in this module as asyncio-enabled integration tests
pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ============================================================================
# INTEGRATION FIXTURES (REAL HTTP SERVER & CONNECTOR)
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
        component_name="async_http_connector_test",
        configurations=config,
        environment="test"
    )


@pytest_asyncio.fixture
async def http_test_server():
    """
    Spawns an isolated local HTTP web server in memory to validate real socket I/O.
    """
    app = web.Application()

    async def handle_get_json(request):
        return web.json_response({"status": "ok", "message": "Telemetry fetched"})

    async def handle_get_text(request):
        return web.Response(text="<html><body>Plain Text Content</body></html>", content_type="text/html")

    async def handle_post_echo(request):
        data = await request.json()
        auth_header = request.headers.get("Authorization")
        custom_header = request.headers.get("X-Custom-Header")
        return web.json_response({
            "received_payload": data,
            "auth_header": auth_header,
            "custom_header": custom_header
        })

    async def handle_error_500(request):
        return web.Response(text="Internal Error Details", status=500)

    async def handle_slow_route(request):
        await asyncio.sleep(1.0)
        return web.json_response({"status": "delayed"})

    app.router.add_get("/v1/telemetry", handle_get_json)
    app.router.add_get("/v1/html_text", handle_get_text)
    app.router.add_post("/v1/echo", handle_post_echo)
    app.router.add_get("/v1/server_error", handle_error_500)
    app.router.add_get("/v1/slow_route", handle_slow_route)

    server = TestServer(app)
    await server.start_server()

    yield server

    await server.close()


@pytest_asyncio.fixture
async def integration_connector(http_test_server, mock_logger):
    """Provides an active AsyncHTTPConnector bound to the running local TestServer."""
    config = {
        "url": str(http_test_server.make_url("")),
        "verify_ssl": False,
        "headers": {
            "X-Custom-Header": "IntegrationTestValue"
        }
    }

    connector = AsyncHTTPConnector(server_configurations=config, logger=mock_logger)
    await connector.connect()

    yield connector

    await connector.close()


# ============================================================================
# 1. CONNECTION & LIFECYCLE INTEGRATION TESTS
# ============================================================================

async def test_connect_and_is_connected_integration(integration_connector):
    """Verifies that the connector establishes a real persistent ClientSession pool."""
    assert integration_connector.is_connected() is True


async def test_close_teardown_integration(integration_connector):
    """Verifies graceful session teardown against a real HTTP server."""
    assert integration_connector.is_connected() is True
    await integration_connector.close()
    assert integration_connector.is_connected() is False


# ============================================================================
# 2. END-TO-END HTTP REQUEST & RESPONSE INTEGRATION TESTS
# ============================================================================

async def test_get_json_response_integration(integration_connector):
    """Verifies executing a real async GET request and parsing JSON payload."""
    response = await integration_connector.request("GET", "/v1/telemetry")

    assert isinstance(response, dict)
    assert response["status"] == "ok"
    assert response["message"] == "Telemetry fetched"


async def test_get_raw_text_fallback_integration(integration_connector):
    """Verifies fallback to raw string text when the server responds with HTML/Plain text."""
    response = await integration_connector.request("GET", "/v1/html_text")

    assert isinstance(response, str)
    assert "<html><body>Plain Text Content</body></html>" in response


async def test_post_json_payload_and_global_headers_integration(integration_connector):
    """
    Verifies POSTing a JSON payload and ensures custom global headers are sent over the wire.
    """
    payload = {"sensor_id": "meter_99", "reading": 230.4}
    response = await integration_connector.request("POST", "/v1/echo", json=payload)

    assert response["received_payload"] == payload
    assert response["custom_header"] == "IntegrationTestValue"


async def test_dynamic_header_update_integration(integration_connector):
    """
    Verifies updating headers dynamically affects subsequent real HTTP requests over the wire.
    """
    integration_connector.update_headers({"Authorization": "Bearer dyn_token_777"})

    payload = {"test": "header_update"}
    response = await integration_connector.request("POST", "/v1/echo", json=payload)

    assert response["auth_header"] == "Bearer dyn_token_777"


# ============================================================================
# 3. TIMEOUT & ERROR HANDLING INTEGRATION TESTS
# ============================================================================

async def test_http_server_error_500_raises_custom_error_integration(integration_connector):
    """
    Verifies that real 500-range HTTP response status codes cause HTTPErrorWrapper to be raised.
    """
    with pytest.raises(HTTPErrorWrapper, match="status code 500"):
        await integration_connector.request("GET", "/v1/server_error")


async def test_request_timeout_integration(integration_connector):
    """
    Verifies that exceeding the configured timeout threshold raises HTTPErrorWrapper.
    """
    with pytest.raises(HTTPErrorWrapper, match="Timeout during async GET"):
        await integration_connector.request("GET", "/v1/slow_route", timeout=0.2)


async def test_connection_refused_integration(mock_logger):
    """
    Verifies that attempting to issue a request to an offline endpoint raises HTTPErrorWrapper.
    """
    config = {"url": "http://127.0.0.1:59999"}  # Closed port
    offline_connector = AsyncHTTPConnector(server_configurations=config, logger=mock_logger)
    await offline_connector.connect()

    try:
        with pytest.raises(HTTPErrorWrapper, match="Connection error during async GET"):
            await offline_connector.request("GET", "/unreachable")
    finally:
        await offline_connector.close()