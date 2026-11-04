# --- Standard Library Imports ---
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

# --- Third-Party Imports ---
import aiohttp
import pytest

# --- Local Application Imports ---
from app.core.connectors.async_http_connector import AsyncHTTPConnector, HTTPErrorWrapper
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
        component_name="async_http_connector_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def valid_url_config():
    """Provides a valid configuration dictionary using a explicit 'url'."""
    return {
        "url": "https://api.cleanwatts.energy/v1",
        "verify_ssl": True,
        "auth": {
            "username": "api_user",
            "password": "api_password",
        },
        "headers": {
            "Authorization": "Bearer token_abc123"
        }
    }


@pytest.fixture
def valid_host_config():
    """Provides a valid configuration dictionary using 'schema', 'host', and 'port'."""
    return {
        "schema": "http",
        "host": "localhost",
        "port": 8080,
        "verify_ssl": False
    }


@pytest.fixture
def connector(valid_url_config, mock_logger):
    """Fixture returning an initialized AsyncHTTPConnector instance."""
    return AsyncHTTPConnector(server_configurations=valid_url_config, logger=mock_logger)


# ============================================================================
# 1. INITIALIZATION & URL RESOLUTION UNIT TESTS
# ============================================================================

def test_init_with_explicit_url_unit_test(valid_url_config, mock_logger):
    """Verifies that an explicit 'url' overrides schema/host/port and strips trailing slashes."""
    connector = AsyncHTTPConnector(server_configurations=valid_url_config, logger=mock_logger)
    assert connector._base_url == "https://api.cleanwatts.energy/v1"
    assert connector._verify_ssl is True


def test_init_with_host_and_port_unit_test(valid_host_config, mock_logger):
    """Verifies URL construction from schema, host, and port parameters."""
    connector = AsyncHTTPConnector(server_configurations=valid_host_config, logger=mock_logger)
    assert connector._base_url == "http://localhost:8080"
    assert connector._verify_ssl is False


def test_init_with_host_only_unit_test(mock_logger):
    """Verifies URL construction when port is omitted."""
    config = {"schema": "https", "host": "api.example.com"}
    connector = AsyncHTTPConnector(server_configurations=config, logger=mock_logger)
    assert connector._base_url == "https://api.example.com"


def test_init_raises_error_when_url_and_host_missing_unit_test(mock_logger):
    """Verifies that HTTPErrorWrapper is raised if both 'url' and 'host' are missing."""
    invalid_config = {"schema": "http", "port": 8080}
    with pytest.raises(HTTPErrorWrapper, match="Either 'url' or 'host' must be provided"):
        AsyncHTTPConnector(server_configurations=invalid_config, logger=mock_logger)


# ============================================================================
# 2. SESSION CONNECTION & LIFECYCLE UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
@patch("aiohttp.TCPConnector")
@patch("aiohttp.ClientSession")
async def test_connect_success_unit_test(mock_client_session, mock_tcp_connector, connector):
    """Verifies successful ClientSession initialization with SSL, Auth, and custom Headers."""
    mock_session_instance = MagicMock(closed=False)
    mock_client_session.return_value = mock_session_instance

    await connector.connect()

    # 1. Ensure TCPConnector built with ssl settings
    mock_tcp_connector.assert_called_once_with(ssl=True)

    # 2. Ensure ClientSession created with expected headers and basic auth
    mock_client_session.assert_called_once()
    call_kwargs = mock_client_session.call_args.kwargs

    assert call_kwargs["headers"]["Accept"] == "application/json"
    assert call_kwargs["headers"]["Authorization"] == "Bearer token_abc123"
    assert call_kwargs["auth"].login == "api_user"
    assert call_kwargs["auth"].password == "api_password"

    assert connector.is_connected() is True


@pytest.mark.asyncio
@patch("aiohttp.ClientSession", side_effect=Exception("Failed to allocate socket pool"))
async def test_connect_failure_raises_custom_error_unit_test(mock_client_session, connector):
    """Ensures connection failures raise HTTPErrorWrapper."""
    with pytest.raises(HTTPErrorWrapper, match="Error establishing HTTP persistent link context"):
        await connector.connect()

    assert connector.is_connected() is False


def test_is_connected_states_unit_test(connector):
    """Tests all potential ClientSession states checked by is_connected()."""
    # 1. Unallocated session
    assert connector.is_connected() is False

    # 2. Active open session
    mock_session = MagicMock(closed=False)
    connector._session = mock_session
    assert connector.is_connected() is True

    # 3. Closed session
    mock_session.closed = True
    assert connector.is_connected() is False


# ============================================================================
# 3. REQUEST EXECUTION UNIT TESTS (request & get)
# ============================================================================

@pytest.mark.asyncio
async def test_request_when_offline_raises_error_unit_test(connector):
    """Ensures requests fail immediately if the session is not established."""
    assert connector.is_connected() is False
    with pytest.raises(HTTPErrorWrapper, match="ClientSession is closed or unallocated"):
        await connector.request("GET", "/test")


@pytest.mark.asyncio
async def test_request_success_json_parsing_unit_test(connector):
    """Verifies successful HTTP request with JSON response parsing and URL formatting."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.return_value = {"status": "success", "data": [1, 2, 3]}

    # AsyncContextManager mock for session.request()
    mock_request_ctx = AsyncMock()
    mock_request_ctx.__aenter__.return_value = mock_response

    mock_session = MagicMock(closed=False)
    mock_session.request.return_value = mock_request_ctx
    connector._session = mock_session

    result = await connector.request("post", "telemetry/data", json={"key": "val"}, timeout=5.0)

    # Validate URL sanitization and UPPERCASE method
    expected_url = "https://api.cleanwatts.energy/v1/telemetry/data"
    mock_session.request.assert_called_once()
    call_kwargs = mock_session.request.call_args.kwargs

    assert call_kwargs["method"] == "POST"
    assert call_kwargs["url"] == expected_url
    assert isinstance(call_kwargs["timeout"], aiohttp.ClientTimeout)
    assert call_kwargs["timeout"].total == 5.0
    assert result == {"status": "success", "data": [1, 2, 3]}


@pytest.mark.asyncio
async def test_request_success_fallback_to_raw_text_unit_test(connector):
    """Verifies fallback to raw text response when JSON parsing fails."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.side_effect = Exception("Not a JSON response")
    mock_response.text.return_value = "<html>OK</html>"

    mock_request_ctx = AsyncMock()
    mock_request_ctx.__aenter__.return_value = mock_response

    mock_session = MagicMock(closed=False)
    mock_session.request.return_value = mock_request_ctx
    connector._session = mock_session

    result = await connector.request("GET", "/health")

    assert result == "<html>OK</html>"


@pytest.mark.asyncio
async def test_request_http_error_status_300_to_599_raises_custom_error_unit_test(connector):
    """Verifies HTTPErrorWrapper is raised when status code is within 300-599."""
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text.return_value = "Internal Server Error details..."

    mock_request_ctx = AsyncMock()
    mock_request_ctx.__aenter__.return_value = mock_response

    mock_session = MagicMock(closed=False)
    mock_session.request.return_value = mock_request_ctx
    connector._session = mock_session

    with pytest.raises(HTTPErrorWrapper, match="HTTP error during async GET.*status code 500"):
        await connector.request("GET", "/error_route")


@pytest.mark.asyncio
async def test_request_client_connector_error_raises_custom_error_unit_test(connector):
    """Ensures aiohttp.ClientConnectorError maps to HTTPErrorWrapper."""
    mock_session = MagicMock(closed=False)
    mock_session.request.side_effect = aiohttp.ClientConnectorError(
        connection_key=MagicMock(), os_error=OSError("Cannot connect")
    )
    connector._session = mock_session

    with pytest.raises(HTTPErrorWrapper, match="Connection error during async GET"):
        await connector.request("GET", "/endpoint")


@pytest.mark.asyncio
async def test_request_timeout_error_raises_custom_error_unit_test(connector):
    """Ensures asyncio.TimeoutError maps to HTTPErrorWrapper."""
    mock_session = MagicMock(closed=False)
    mock_session.request.side_effect = asyncio.TimeoutError("Request timed out")
    connector._session = mock_session

    with pytest.raises(HTTPErrorWrapper, match="Timeout during async GET"):
        await connector.request("GET", "/endpoint")


# ============================================================================
# 4. DYNAMIC HEADER UPDATES & TEARDOWN UNIT TESTS
# ============================================================================

def test_update_headers_success_unit_test(connector):
    """Verifies dynamic updates to global session headers when connected."""
    mock_session = MagicMock(closed=False)
    mock_headers = MagicMock()
    mock_session.headers = mock_headers
    connector._session = mock_session

    new_headers = {"X-API-Key": "secret_key_123"}
    connector.update_headers(new_headers)

    mock_headers.update.assert_called_once_with(new_headers)


def test_update_headers_when_offline_raises_error_unit_test(connector):
    """Ensures updating headers fails fast if offline."""
    assert connector.is_connected() is False
    with pytest.raises(HTTPErrorWrapper, match="Session is not established"):
        connector.update_headers({"Header": "Val"})


@pytest.mark.asyncio
async def test_close_clean_shutdown_unit_test(connector):
    """Verifies graceful session teardown and state cleanup."""
    mock_session = AsyncMock(closed=False)
    connector._session = mock_session

    await connector.close()

    mock_session.close.assert_called_once()
    assert connector._session is None
    assert connector.is_connected() is False


@pytest.mark.asyncio
async def test_close_failure_raises_custom_error_unit_test(connector):
    """Ensures session closure errors raise HTTPErrorWrapper and reset session state."""
    mock_session = AsyncMock(closed=False)
    mock_session.close.side_effect = Exception("Socket flush error")
    connector._session = mock_session

    with pytest.raises(HTTPErrorWrapper, match="Failed to close HTTP session"):
        await connector.close()

    # Verify state cleanup guarantee in finally block
    assert connector._session is None