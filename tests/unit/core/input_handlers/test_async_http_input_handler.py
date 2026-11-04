# --- Standard Library Imports ---
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.input_handlers.async_http_input_handler import HTTPInputHandler
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
        component_name="http_input_handler_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def valid_topology_setup():
    """Provides a valid topology setup dictionary with two polling endpoints."""
    return {
        "endpoints": [
            {
                "path": "/v1/telemetry/meter_01",
                "method": "GET",
                "timeout_ms": 4000,
                "headers": {"Authorization": "Bearer token_abc"}
            },
            {
                "path": "/v1/telemetry/meter_02",
                "method": "GET",
                "timeout_ms": 4000,
                "headers": {"Authorization": "Bearer token_abc"}
            }
        ]
    }


@pytest.fixture
def mock_connector():
    """Provides an AsyncMock instance for the AsyncHTTPConnector dependency."""
    return AsyncMock()


@pytest.fixture
def mock_scheduler():
    """Provides a MagicMock instance for the AsyncPeriodicScheduler dependency."""
    return MagicMock()


@pytest.fixture
def mock_callback():
    """Provides an AsyncMock instance for the processing callback."""
    return AsyncMock()


@pytest.fixture
def handler(valid_topology_setup, mock_callback, mock_connector, mock_scheduler, mock_logger):
    """Fixture returning an initialized HTTPInputHandler instance."""
    return HTTPInputHandler(
        topology_setup=valid_topology_setup,
        on_message_callback=mock_callback,
        connector=mock_connector,
        scheduler=mock_scheduler,
        time_interval_seconds=15,
        logger=mock_logger
    )


# ============================================================================
# 1. INITIALIZATION & GUARD CLAUSES UNIT TESTS
# ============================================================================

def test_init_success_unit_test(handler):
    """Verifies successful initialization and attribute extraction."""
    assert handler._time_interval_seconds == 15
    assert len(handler._endpoints) == 2
    assert handler._endpoints[0]["path"] == "/v1/telemetry/meter_01"


def test_init_raises_key_error_when_topology_missing_unit_test(
    mock_callback, mock_connector, mock_scheduler, mock_logger
):
    """Verifies that KeyError is raised when topology_setup is missing or empty."""
    with pytest.raises(KeyError, match="topology configuration footprint is required"):
        HTTPInputHandler(
            topology_setup={},
            on_message_callback=mock_callback,
            connector=mock_connector,
            scheduler=mock_scheduler,
            time_interval_seconds=15,
            logger=mock_logger
        )


def test_init_raises_value_error_when_time_interval_invalid_unit_test(
    valid_topology_setup, mock_callback, mock_connector, mock_scheduler, mock_logger
):
    """Verifies that ValueError is raised when time_interval_seconds is negative or zero."""
    with pytest.raises(ValueError, match="'time_interval_seconds' must be a positive number"):
        HTTPInputHandler(
            topology_setup=valid_topology_setup,
            on_message_callback=mock_callback,
            connector=mock_connector,
            scheduler=mock_scheduler,
            time_interval_seconds=0,
            logger=mock_logger
        )


def test_init_raises_value_error_when_callback_missing_or_not_callable_unit_test(
    valid_topology_setup, mock_connector, mock_scheduler, mock_logger
):
    """Verifies that ValueError is raised when on_message_callback is invalid or None."""
    with pytest.raises(ValueError, match="'on_message_callback' is missing or not callable"):
        HTTPInputHandler(
            topology_setup=valid_topology_setup,
            on_message_callback=None,  # type: ignore
            connector=mock_connector,
            scheduler=mock_scheduler,
            time_interval_seconds=15,
            logger=mock_logger
        )


def test_init_raises_value_error_when_connector_missing_unit_test(
    valid_topology_setup, mock_callback, mock_scheduler, mock_logger
):
    """Verifies that ValueError is raised if connector is missing or None."""
    with pytest.raises(ValueError, match="'connector' dependency is missing"):
        HTTPInputHandler(
            topology_setup=valid_topology_setup,
            on_message_callback=mock_callback,
            connector=None,  # type: ignore
            scheduler=mock_scheduler,
            time_interval_seconds=15,
            logger=mock_logger
        )


def test_init_raises_value_error_when_scheduler_missing_unit_test(
    valid_topology_setup, mock_callback, mock_connector, mock_logger
):
    """Verifies that ValueError is raised if scheduler is missing or None."""
    with pytest.raises(ValueError, match="'scheduler' dependency is missing"):
        HTTPInputHandler(
            topology_setup=valid_topology_setup,
            on_message_callback=mock_callback,
            connector=mock_connector,
            scheduler=None,  # type: ignore
            time_interval_seconds=15,
            logger=mock_logger
        )


def test_init_raises_value_error_when_endpoints_empty_unit_test(
    mock_callback, mock_connector, mock_scheduler, mock_logger
):
    """Verifies that ValueError is raised if the 'endpoints' list is missing or empty."""
    invalid_topology = {"endpoints": []}
    with pytest.raises(ValueError, match="'endpoints' must be a non-empty list."):
        HTTPInputHandler(
            topology_setup=invalid_topology,
            on_message_callback=mock_callback,
            connector=mock_connector,
            scheduler=mock_scheduler,
            time_interval_seconds=15,
            logger=mock_logger
        )


# ============================================================================
# 2. START & STOP LIFECYCLE UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_start_registers_and_starts_scheduler_unit_test(handler, mock_scheduler):
    """Verifies that start() creates a job on the scheduler using time_interval_seconds and starts."""
    await handler.start()

    mock_scheduler.create_job.assert_called_once_with(
        time_interval=15,
        trigger_callback=handler._poll_all_endpoints,
        job_id="http_polling_ingest"
    )
    mock_scheduler.start.assert_called_once()


@pytest.mark.asyncio
async def test_start_aborts_if_stopped_unit_test(handler, mock_scheduler):
    """Verifies that start() aborts if the stop event has already been set."""
    handler.stop()
    await handler.start()

    mock_scheduler.create_job.assert_not_called()
    mock_scheduler.start.assert_not_called()


def test_stop_pipeline_unit_test(handler, mock_scheduler):
    """Verifies that stop() halts the scheduler engine and sets the stop_event."""
    assert not handler._stop_event.is_set()

    handler.stop()

    mock_scheduler.stop.assert_called_once()
    assert handler._stop_event.is_set()


# ============================================================================
# 3. ENDPOINT FETCHING & VALIDATION UNIT TESTS (_fetch_endpoint)
# ============================================================================

@pytest.mark.asyncio
async def test_fetch_endpoint_raises_value_error_if_path_missing_unit_test(handler):
    """Verifies that _fetch_endpoint raises ValueError if path is missing or empty."""
    invalid_endpoint_block = {"method": "GET"}
    with pytest.raises(ValueError, match="Missing mandatory 'path' key"):
        await handler._fetch_endpoint(invalid_endpoint_block)


@pytest.mark.asyncio
async def test_fetch_endpoint_raises_value_error_if_method_unsupported_unit_test(handler):
    """Verifies that _fetch_endpoint raises ValueError if method is not in _ALLOWED_METHODS."""
    unsupported_block = {"path": "/v1/data", "method": "POST"}
    with pytest.raises(ValueError, match="Unsupported HTTP method 'POST'"):
        await handler._fetch_endpoint(unsupported_block)


@pytest.mark.asyncio
@patch("app.core.endpoints_builders.payload_normalizer.PayloadNormalizer.from_http", new_callable=AsyncMock)
async def test_fetch_endpoint_success_unit_test(mock_normalizer_from_http, handler, mock_connector):
    """Verifies successful network execution and delegation to PayloadNormalizer."""
    mock_connector.request.return_value = {"sensor_id": "meter_01", "voltage": 230}
    mock_normalizer_from_http.return_value = {
        "protocol": "HTTP",
        "source": "/v1/telemetry/meter_01",
        "status": 200,
        "content_type": "application/json",
        "payload": b'{"sensor_id": "meter_01", "voltage": 230}'
    }

    endpoint_block = {
        "path": "/v1/telemetry/meter_01",
        "method": "GET",
        "timeout_ms": 3000,
        "headers": {"HeaderKey": "HeaderVal"}
    }

    result = await handler._fetch_endpoint(endpoint_block)

    mock_connector.request.assert_called_once_with(
        method="GET",
        endpoint="/v1/telemetry/meter_01",
        timeout=3.0
    )
    mock_normalizer_from_http.assert_called_once_with(
        response={"sensor_id": "meter_01", "voltage": 230},
        endpoint="/v1/telemetry/meter_01",
        metadata={}
    )
    assert result["protocol"] == "HTTP"


@pytest.mark.asyncio
@patch("app.core.endpoints_builders.payload_normalizer.PayloadNormalizer.from_http", new_callable=AsyncMock)
async def test_fetch_endpoint_handles_network_exception_unit_test(
    mock_normalizer_from_http, handler, mock_connector
):
    """Verifies that connector network exceptions are caught and passed to PayloadNormalizer."""
    network_err = TimeoutError("Connection timed out")
    mock_connector.request.side_effect = network_err

    mock_normalizer_from_http.return_value = {
        "protocol": "HTTP",
        "source": "/v1/telemetry/meter_01",
        "status": 503,
        "content_type": "text/plain",
        "payload": "Network Error: Connection timed out"
    }

    endpoint_block = {"path": "/v1/telemetry/meter_01", "method": "GET"}

    result = await handler._fetch_endpoint(endpoint_block)

    mock_normalizer_from_http.assert_called_once_with(
        endpoint="/v1/telemetry/meter_01",
        exception=network_err,
        metadata={}
    )
    assert result["status"] == 503


@pytest.mark.asyncio
async def test_fetch_endpoint_returns_none_when_stopped_unit_test(handler):
    """Verifies that _fetch_endpoint aborts and returns None if stop_event is set."""
    handler.stop()
    endpoint_block = {"path": "/v1/telemetry/meter_01", "method": "GET"}

    result = await handler._fetch_endpoint(endpoint_block)

    assert result is None


# ============================================================================
# 4. GROUPED POLLING & ASYNC CALLBACK EXECUTION UNIT TESTS (_poll_all_endpoints)
# ============================================================================

@pytest.mark.asyncio
async def test_poll_all_endpoints_with_flat_endpoints_dispatches_batch_callback_unit_test(
    handler, mock_callback
):
    """Verifies that flat endpoint lists are grouped under 'default' and delivered as a batch list."""
    fake_envelope_1 = {"protocol": "HTTP", "source": "meter_01", "payload": b"data1"}
    fake_envelope_2 = {"protocol": "HTTP", "source": "meter_02", "payload": b"data2"}

    with patch.object(handler, "_fetch_endpoint", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.side_effect = [fake_envelope_1, fake_envelope_2]

        await handler._poll_all_endpoints()
        # Yield execution to allow background create_task coroutines to complete
        await asyncio.sleep(0.01)

        assert mock_fetch.call_count == 2
        # Flat endpoints are gathered into a single batch list
        mock_callback.assert_called_once_with([fake_envelope_1, fake_envelope_2])


@pytest.mark.asyncio
async def test_poll_all_endpoints_with_grouped_builder_dispatches_per_group_unit_test(
    valid_topology_setup, mock_callback, mock_connector, mock_scheduler, mock_logger
):
    """Verifies that grouped endpoints from EndpointBuilder trigger separate callback tasks per group."""
    mock_builder = AsyncMock()
    mock_builder.build_endpoints.return_value = {
        "PV01": [
            {"path": "/tags/33711", "method": "GET"}
        ],
        "B01": [
            {"path": "/tags/35927", "method": "GET"},
            {"path": "/tags/33690", "method": "GET"}
        ]
    }

    handler_grouped = HTTPInputHandler(
        topology_setup=valid_topology_setup,
        on_message_callback=mock_callback,
        connector=mock_connector,
        scheduler=mock_scheduler,
        time_interval_seconds=15,
        logger=mock_logger,
        endpoint_builder=mock_builder
    )

    env_pv = {"source": "tags/33711", "payload": "pv_data"}
    env_b1 = {"source": "tags/35927", "payload": "b_in"}
    env_b2 = {"source": "tags/33690", "payload": "b_soc"}

    with patch.object(handler_grouped, "_fetch_endpoint", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.side_effect = [env_pv, env_b1, env_b2]

        await handler_grouped._poll_all_endpoints()
        await asyncio.sleep(0.01)

        assert mock_fetch.call_count == 3
        # Should invoke callback independently for each entity group
        assert mock_callback.call_count == 2
        mock_callback.assert_any_call([env_pv])
        mock_callback.assert_any_call([env_b1, env_b2])


@pytest.mark.asyncio
async def test_poll_all_endpoints_updates_dynamic_headers_on_connector_unit_test(
    valid_topology_setup, mock_callback, mock_connector, mock_scheduler, mock_logger
):
    """Verifies that dynamic headers from endpoints update the connector session."""
    mock_connector.update_headers = MagicMock()
    mock_builder = AsyncMock()
    mock_builder.build_endpoints.return_value = {
        "PV01": [
            {
                "path": "/tags/33711",
                "method": "GET",
                "headers": {"Authorization": "CW jwt_token_123"}
            }
        ]
    }

    handler_dynamic_auth = HTTPInputHandler(
        topology_setup=valid_topology_setup,
        on_message_callback=mock_callback,
        connector=mock_connector,
        scheduler=mock_scheduler,
        time_interval_seconds=15,
        logger=mock_logger,
        endpoint_builder=mock_builder
    )

    with patch.object(handler_dynamic_auth, "_fetch_endpoint", new_callable=AsyncMock):
        await handler_dynamic_auth._poll_all_endpoints()
        await asyncio.sleep(0.01)

        mock_connector.update_headers.assert_called_once_with(
            {"Authorization": "CW jwt_token_123"}
        )


@pytest.mark.asyncio
async def test_poll_all_endpoints_aborts_when_stop_event_is_set_unit_test(handler):
    """Verifies that _poll_all_endpoints aborts immediately if handler is stopped."""
    handler.stop()

    with patch.object(handler, "_fetch_endpoint", new_callable=AsyncMock) as mock_fetch:
        await handler._poll_all_endpoints()
        await asyncio.sleep(0.01)

        mock_fetch.assert_not_called()


@pytest.mark.asyncio
async def test_poll_all_endpoints_triggers_sync_callback_via_to_thread_unit_test(
    valid_topology_setup, mock_connector, mock_scheduler, mock_logger
):
    """Verifies that synchronous callbacks are offloaded safely using asyncio.to_thread."""
    sync_callback = MagicMock()
    fake_envelope = {"protocol": "HTTP", "source": "meter_01"}

    handler_sync = HTTPInputHandler(
        topology_setup=valid_topology_setup,
        on_message_callback=sync_callback,
        connector=mock_connector,
        scheduler=mock_scheduler,
        time_interval_seconds=15,
        logger=mock_logger
    )

    with patch.object(handler_sync, "_fetch_endpoint", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = fake_envelope

        await handler_sync._poll_all_endpoints()
        await asyncio.sleep(0.01)

        sync_callback.assert_called_once_with([fake_envelope, fake_envelope])


@pytest.mark.asyncio
async def test_poll_all_endpoints_discards_data_when_stopped_mid_flight_unit_test(
    handler, mock_callback
):
    """Verifies that fetched payloads are discarded if handler is stopped during transit."""
    fake_envelope = {"protocol": "HTTP", "source": "meter_01"}

    async def mock_fetch_and_stop(endpoint):
        handler.stop()  # Signal stop mid-flight
        return fake_envelope

    with patch.object(handler, "_fetch_endpoint", side_effect=mock_fetch_and_stop):
        await handler._poll_all_endpoints()
        await asyncio.sleep(0.01)

        mock_callback.assert_not_called()


@pytest.mark.asyncio
async def test_poll_all_endpoints_handles_callback_exception_gracefully_unit_test(
    handler, mock_callback
):
    """Verifies that an exception inside the processing callback is caught without breaking the loop."""
    fake_envelope = {"protocol": "HTTP", "source": "meter_01"}
    mock_callback.side_effect = Exception("Domain processing crash")

    with patch.object(handler, "_fetch_endpoint", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = fake_envelope

        await handler._poll_all_endpoints()
        await asyncio.sleep(0.01)

        mock_callback.assert_called_once_with([fake_envelope, fake_envelope])