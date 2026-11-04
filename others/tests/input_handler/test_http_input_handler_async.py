# --- Standard Library Imports ---
from unittest.mock import AsyncMock, MagicMock

# --- Third-Party Imports ---
import pytest

# --- Internal Framework Imports ---
from app.core.input_handlers.async_http_input_handler import HTTPInputHandler
from app.core.connectors.async_http_connector import AsyncHTTPConnector
from app.core.utils.sync_periodic_scheduler import SyncPeriodicScheduler
from app.core.utils.logger import LoggingUtils


@pytest.fixture
def mock_logger():
    """Provides a REAL configured instance of LoggingUtils for verbose testing."""
    config = {
        "log_files": {
            "max_size": 1024 * 1024  # 1MB
        }
    }
    # Create a real logger instance for the testing environment
    return LoggingUtils(
        component_name="http_handler_test",
        configurations=config,
        environment="test"
    )

@pytest.fixture
def mock_connector():
    """Provides a mocked low-level HTTPConnector."""
    return MagicMock(spec=AsyncHTTPConnector)


@pytest.fixture
def mock_scheduler():
    """Provides a mocked infrastructure scheduler."""
    return MagicMock(spec=SyncPeriodicScheduler)


@pytest.fixture
def mock_callback():
    """Provides a mocked domain processing callback to assert data delivery."""
    return MagicMock()


@pytest.fixture
def sample_topology():
    """Returns a realistic HTTP input topology with relative resource paths."""
    return {
        "poll_interval_seconds": 10,
        "endpoints": [
            {
                "path": "/v1/telemetry/meter_01",
                "method": "GET",
                "timeout_ms": 2000,
                "headers": {"Accept": "application/json"}
            },
            {
                "path": "/v1/telemetry/meter_02",
                "method": "GET",
                "timeout_ms": 2000,
                "headers": {"Accept": "application/json"}
            }
        ]
    }


@pytest.fixture
def http_handler(sample_topology, mock_callback, mock_connector, mock_scheduler, mock_logger):
    """Instantiates the HTTPInputHandler bound to mocked dependencies."""
    return HTTPInputHandler(
        topology_setup=sample_topology,
        on_message_callback=mock_callback,
        connector=mock_connector,
        scheduler=mock_scheduler,
        logger=mock_logger
    )


@pytest.mark.asyncio
async def test_poll_all_endpoints_success(http_handler, mock_connector, mock_callback):
    """
    Asserts that when all endpoints return successful responses, the data is concurrently
    fetched, parsed, and successfully routed to the injected system callback.
    """
    # 1. Prepare Mock Responses representing remote API nodes
    mock_resp_1 = AsyncMock()
    mock_resp_1.status = 200
    mock_resp_1.headers = {"Content-Type": "application/json"}
    mock_resp_1.json.return_value = {"active_power": 12.5}

    mock_resp_2 = AsyncMock()
    mock_resp_2.status = 200
    mock_resp_2.headers = {"Content-Type": "application/json"}
    mock_resp_2.json.return_value = {"active_power": 15.0}

    # Set up the low-level client connector to return these responses sequentially on async calls
    mock_connector.request.side_effect = [mock_resp_1, mock_resp_2]

    # 2. Trigger the asynchronous concurrent fetch strategy
    await http_handler._poll_all_endpoints()

    # 3. Structural Assertions
    # Ensure the physical request was dispatched twice with correct parameters
    assert mock_connector.request.call_count == 2

    # Assert the domain processing callback was invoked twice with mapped structures
    assert mock_callback.call_count == 2

    # Verify the structure of the data delivered to the domain callback
    calls = mock_callback.call_args_list

    # First payload verification
    assert calls[0][0][0]["source"] == "/v1/telemetry/meter_01"
    assert calls[0][0][0]["status_code"] == 200
    assert calls[0][0][0]["payload"] == {"active_power": 12.5}

    # Second payload verification
    assert calls[1][0][0]["source"] == "/v1/telemetry/meter_02"
    assert calls[1][0][0]["status_code"] == 200
    assert calls[1][0][0]["payload"] == {"active_power": 15.0}


@pytest.mark.asyncio
async def test_fetch_endpoint_unsupported_method_raises_value_error(mock_connector, http_handler, caplog):
    """
    Asserts that attempting to fetch an endpoint configured with an unsupported HTTP method
    (e.g., POST) immediately raises a ValueError, logs the critical boundary violation using
    the real logger, and prevents any egress network request from being dispatched.
    """
    # --- Setup caplog for the real logger ---
    # Since propagate is False on LoggingUtils setup, manually bind pytest's
    # caplog handler to our handler's active logger engine
    http_handler._logger._logger.addHandler(caplog.handler)

    # 1. Prepare an endpoint payload block with an illegal HTTP verb
    invalid_endpoint = {
        "path": "/v1/control/actuate",
        "method": "POST",  # Only 'GET' is registered in self._ALLOWED_METHODS
        "timeout_ms": 3000,
        "headers": {}
    }

    # 2. Trigger execution inside the exception assertion context
    with pytest.raises(ValueError) as exc_info:
        await http_handler._fetch_endpoint(invalid_endpoint)

    # 3. Validation: Verify the exception boundary and the correct error message
    expected_error_msg = (
        "HTTP Input Handler | Unsupported HTTP method 'POST' requested for path '/v1/control/actuate'. "
        "Allowed methods: {'GET'}"
    )
    assert expected_error_msg in str(exc_info.value)

    # 4. Telemetry Validation: Audit the real logger output via caplog
    assert any(expected_error_msg in record.message for record in caplog.records)

    # 5. Isolation Validation: Ensure no physical connection was attempted
    mock_connector.request.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_endpoint_timeout_returns_none_and_logs_error(mock_connector, http_handler, caplog):
    """
    Asserts that if the low-level HTTPConnector raises an exception during a request,
    the handler catches the exception, logs a structured error, and returns None.
    """
    # --- Setup caplog for the real logger ---
    http_handler._logger._logger.addHandler(caplog.handler)

    # 1. Setup the mock connector to raise an Exception simulating a connection timeout when requested
    mock_connector.request.side_effect = Exception("Connection timed out")

    # 2. Prepare a single endpoint configuration
    single_endpoint = {
        "path": "/v1/telemetry/meter_01",
        "method": "GET",
        "timeout_ms": 2000,
        "headers": {}
    }

    # 3. Direct execution of the internal coroutine
    result = await http_handler._fetch_endpoint(single_endpoint)

    # 4. Core Assertions
    # Ensure that the method handled the error internally and returned None
    assert result is None

    # Verify that the physical request was actually attempted with the correct timeout
    mock_connector.request.assert_called_once_with(
        method="GET",
        endpoint="/v1/telemetry/meter_01",
        headers={},
        timeout=2.0  # 2000 ms / 1000
    )

    # 5. Telemetry Validation: Check that the error was caught and registered by the logger
    expected_error_msg = "HTTP Input Handler | Egress fetch failed for path [/v1/telemetry/meter_01]: Connection timed out"
    assert any(expected_error_msg in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_poll_all_endpoints_with_failures(http_handler, mock_connector, mock_callback, caplog):
    """
    Asserts the resilience of the concurrent gather strategy. If one endpoint raises
    a connection exception, the other should still execute successfully and deliver its payload.
    """
    # --- Fix for non-propagating loggers ---
    # We manually attach pytest's caplog handler directly to our logger instance
    # since propagation is disabled (propagate=False) in LoggingUtils.
    http_handler._logger._logger.addHandler(caplog.handler)

    # 1. Setup one successful response and one network crash exception
    mock_resp_success = AsyncMock()
    mock_resp_success.status = 200
    mock_resp_success.headers = {"Content-Type": "application/json"}
    mock_resp_success.json.return_value = {"active_power": 99.9}

    # The connector raises a timeout exception on the first call, and succeeds on the second
    mock_connector.request.side_effect = [
        Exception("Connection timed out"),
        mock_resp_success
    ]

    # 2. Trigger the ingestion cycle
    await http_handler._poll_all_endpoints()

    # 3. Assertions
    # The callback should only be called ONCE (for the successful endpoint)
    assert mock_callback.call_count == 1

    # Verify the delivered payload
    delivered_payload = mock_callback.call_args[0][0]
    assert delivered_payload["source"] == "/v1/telemetry/meter_02"
    assert delivered_payload["payload"] == {"active_power": 99.9}

    # 4. Verify that the logger captured the warning
    expected_log_message = "HTTP Input Handler | Egress fetch failed for path [/v1/telemetry/meter_01]: Connection timed out"
    assert any(expected_log_message in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_poll_all_endpoints_honors_stop_event(http_handler, mock_connector, mock_callback):
    """
    Asserts that if the handler receives a shutdown signal (stop_event) mid-flight
    during parallel network executions, it immediately halts and discards any received payloads.
    """
    # 1. Setup successful mock responses
    mock_resp_1 = AsyncMock()
    mock_resp_1.status = 200
    mock_resp_1.headers = {"Content-Type": "application/json"}
    mock_resp_1.json.return_value = {"active_power": 10.0}

    mock_connector.request.side_effect = [mock_resp_1, mock_resp_1]

    # 2. Simulate a shutdown trigger AFTER requests are sent but BEFORE the loop finishes.
    # To mimic this mid-flight timing, we trigger the stop event in a way that it is active
    # when _poll_all_endpoints parses the results.
    http_handler.stop()  # Sets self._stop_event to True

    # 3. Execute
    await http_handler._poll_all_endpoints()

    # 4. Assertions
    # Since the stop event was set, the callback MUST NEVER be called with any payload.
    mock_callback.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_endpoint_non_json_fallback(http_handler, mock_connector):
    """
    Asserts that if a remote server returns a non-JSON Content-Type (e.g., text/plain),
    the method gracefully falls back to raw text extraction and packages it safely.
    """
    # 1. Setup a mockup response simulating a non-JSON payload (like a plain text log or message)
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.headers = {"Content-Type": "text/plain; charset=utf-8"}
    mock_response.text.return_value = "raw_unstructured_string_data"

    mock_connector.request.return_value = mock_response

    # 2. Build a single sample endpoint configuration block
    single_endpoint = {
        "path": "/v1/status",
        "method": "GET",
        "timeout_ms": 1000,
        "headers": {}
    }

    # 3. Direct execution of the internal coroutine
    result = await http_handler._fetch_endpoint(single_endpoint)

    # 4. In-depth Assertions
    # Verify the lower level connector was hit with the precise configuration mapping
    mock_connector.request.assert_called_once_with(
        method="GET",
        endpoint="/v1/status",
        headers={},
        timeout=1.0  # ms divided by 1000
    )

    # Verify that .text() was awaited instead of .json()
    mock_response.text.assert_awaited_once()
    mock_response.json.assert_not_called()

    # Validate the structural schema adaptation of the returned boundary dictionary
    assert result is not None
    assert result["source"] == "/v1/status"
    assert result["status_code"] == 200
    assert result["payload"] == {"raw_payload": "raw_unstructured_string_data"}


def test_start_schedules_and_runs(http_handler, mock_scheduler):
    """
    Asserts that starting the handler successfully registers the cron job into the
    PeriodicScheduler engine and enters the blocking thread execution loop.
    """
    # Execute startup
    http_handler.start()

    # Verify the scheduler registered our callback and entered the thread loop
    mock_scheduler.create_job.assert_called_once_with(
        time_interval=10,
        trigger_callback=http_handler._execute_polling_cycle,
        job_id="http_polling_ingest"
    )
    mock_scheduler.start.assert_called_once()


def test_stop_triggers_shutdown(http_handler, mock_scheduler):
    """
    Asserts that calling stop gracefully shuts down the scheduler engine
    and sets the thread-safe synchronization stop flag.
    """
    assert not http_handler._stop_event.is_set()

    # Execute shutdown hook
    http_handler.stop()

    # Verify the scheduler stopped and the thread-safe event was signaled
    mock_scheduler.stop.assert_called_once()
    assert http_handler._stop_event.is_set()