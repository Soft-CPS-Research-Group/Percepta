# --- Standard Library Imports ---
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.output_handlers.async_rabbitmq_output_handler import (
    MqPublishMode,
    AsyncRabbitMQOutputHandler,
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
        component_name="rabbitmq_output_handler_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def valid_topology_setup():
    """Provides a valid topology setup dictionary with static exchanges and RPC configuration."""
    return {
        "exchanges": [
            {
                "exchange_name": "exchange_telemetry",
                "routing_key": "telemetry.v1.*",
                "exchange_properties": {
                    "exchange_type": "topic",
                    "durable": True,
                    "auto_delete": False
                },
                "message_properties": {
                    "delivery_mode": 2,
                    "content_type": "application/json"
                },
                "rpc_properties": {
                    "enabled": True,
                    "timeout_ms": 2000,
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
def mock_connector():
    """Provides an AsyncMock instance for the AsyncRabbitMQConnector dependency."""
    connector = AsyncMock()
    connector.is_connected.return_value = True
    connector.declare_exchange = AsyncMock()
    connector.publish = AsyncMock()
    return connector


@pytest.fixture
def handler(valid_topology_setup, mock_connector, mock_logger):
    """Fixture returning an initialized AsyncRabbitMQOutputHandler instance."""
    return AsyncRabbitMQOutputHandler(
        topology_setup=valid_topology_setup,
        connector=mock_connector,
        logger=mock_logger
    )


# ============================================================================
# 1. INITIALIZATION & GUARD CLAUSES UNIT TESTS
# ============================================================================

def test_init_success_unit_test(valid_topology_setup, mock_connector, mock_logger):
    """Verifies successful initialization and guard clause execution."""
    handler_instance = AsyncRabbitMQOutputHandler(
        topology_setup=valid_topology_setup,
        connector=mock_connector,
        logger=mock_logger
    )

    assert handler_instance._topology_setup == valid_topology_setup
    assert handler_instance._publisher_connector == mock_connector
    assert not handler_instance._stop_event.is_set()


def test_init_raises_value_error_when_connector_missing_unit_test(valid_topology_setup, mock_logger):
    """Verifies that ValueError is raised with correct message when connector dependency is missing."""
    expected_error = "Initialization error: 'connector' dependency is missing."

    with pytest.raises(ValueError, match=expected_error):
        AsyncRabbitMQOutputHandler(
            topology_setup=valid_topology_setup,
            connector=None,  # type: ignore
            logger=mock_logger
        )


# ============================================================================
# 2. SCHEMA VALIDATION UNIT TESTS (_validate_topology_schema)
# ============================================================================

def test_validate_schema_raises_value_error_for_empty_exchanges_unit_test():
    """Verifies ValueError when exchanges key is empty or not a list."""
    invalid_topology = {"exchanges": []}
    expected_error = "Topology validation error: 'exchanges' must be a non-empty list."

    with pytest.raises(ValueError, match=expected_error):
        AsyncRabbitMQOutputHandler._validate_topology_schema(invalid_topology)


def test_validate_schema_raises_value_error_for_missing_exchange_name_unit_test():
    """Verifies ValueError when exchange_name is missing or whitespace."""
    invalid_topology = {"exchanges": [{"routing_key": "key.1"}]}
    expected_error = "Topology validation error: Mandatory string 'exchange_name' is missing or empty at index 0."

    with pytest.raises(ValueError, match=expected_error):
        AsyncRabbitMQOutputHandler._validate_topology_schema(invalid_topology)


def test_validate_schema_raises_value_error_for_invalid_rpc_reply_hierarchy_unit_test():
    """Verifies ValueError when RPC is enabled but reply_to structure is invalid."""
    invalid_rpc_topology = {
        "exchanges": [
            {
                "exchange_name": "ex_test",
                "rpc_properties": {
                    "enabled": True,
                    "reply_to": {}  # Missing 'exchanges' list
                }
            }
        ]
    }
    expected_error = "Topology validation error: 'exchanges' inside 'reply_to' for exchange 'ex_test' must be a non-empty list."

    with pytest.raises(ValueError, match=expected_error):
        AsyncRabbitMQOutputHandler._validate_topology_schema(invalid_rpc_topology)


# ============================================================================
# 3. SETUP & EXCHANGE DECLARATION UNIT TESTS (_setup_publisher_service)
# ============================================================================

@pytest.mark.asyncio
async def test_setup_publisher_service_connects_and_declares_exchanges_unit_test(handler, mock_connector):
    """Verifies that _setup_publisher_service checks connection and registers non-cached exchanges."""
    mock_connector.is_connected = MagicMock(return_value=False)
    exchanges_list = [
        {
            "exchange_name": "ex_new",
            "exchange_properties": {"durable": True}
        }
    ]

    await handler._setup_publisher_service(exchanges_list)

    mock_connector.connect.assert_called_once()
    mock_connector.declare_exchange.assert_called_once_with(
        exchange_name="ex_new",
        durable=True
    )
    assert "ex_new" in handler._declared_exchanges


@pytest.mark.asyncio
async def test_setup_publisher_service_skips_already_declared_exchanges_unit_test(handler, mock_connector):
    """Verifies that previously declared exchanges stored in the cache are not redeclared."""
    handler._declared_exchanges.add("ex_cached")
    exchanges_list = [{"exchange_name": "ex_cached"}]

    await handler._setup_publisher_service(exchanges_list)

    mock_connector.declare_exchange.assert_not_called()


# ============================================================================
# 4. PUBLISHING & MODES UNIT TESTS (send)
# ============================================================================

@pytest.mark.asyncio
async def test_send_rejects_when_stopped_unit_test(handler, mock_connector):
    """Verifies that send() returns None immediately if the handler is stopped."""
    handler.stop()

    result = await handler.send(message={"payload": "data"})

    assert result is None
    mock_connector.publish.assert_not_called()


@pytest.mark.asyncio
async def test_send_static_mode_publishes_to_topology_exchanges_unit_test(handler, mock_connector):
    """Verifies STATIC mode publishes payloads and constructs correct RPC wait entries with source_identifier."""
    message = {"telemetry": 12.34}

    with patch.object(handler, "_wait_for_responses", new_callable=AsyncMock) as mock_wait:
        mock_wait.return_value = {"corr_123": {"protocol": "AMQP", "status": 200, "payload": "ack"}}

        result = await handler.send(message=message, mode=MqPublishMode.STATIC)

        assert result == {"corr_123": {"protocol": "AMQP", "status": 200, "payload": "ack"}}
        mock_connector.publish.assert_called_once()

        # Verify publish parameters
        _, kwargs = mock_connector.publish.call_args
        assert kwargs["exchange_name"] == "exchange_telemetry"
        assert kwargs["routing_key"] == "telemetry.v1.*"
        assert kwargs["message"] == message
        assert "reply_to" in kwargs["properties"]
        assert "correlation_id" in kwargs["properties"]

        # Verify that rpc_wait_list was passed with constructed source_identifier
        wait_list_passed = mock_wait.call_args[0][0]
        assert len(wait_list_passed) == 1
        assert wait_list_passed[0]["source_identifier"] == "rpc_reply_exchange/rpc_reply_queue"


@pytest.mark.asyncio
async def test_send_override_mode_falls_back_to_static_when_dynamic_empty_unit_test(
        handler, mock_connector
):
    """Verifies OVERRIDE mode falls back to static topology if dynamic exchanges are empty."""
    message = {"data": "test"}
    with patch.object(handler._logger, "warning") as spy_warning, \
         patch.object(handler, "_wait_for_responses", new_callable=AsyncMock) as mock_wait:

        mock_wait.return_value = None

        await handler.send(message=message, dynamic_exchanges=[], mode=MqPublishMode.OVERRIDE)

        spy_warning.assert_called_once()
        actual_msg = spy_warning.call_args[0][0]
        assert "Override mode requested but no dynamic exchanges were provided. Falling back to static." in actual_msg

@pytest.mark.asyncio
async def test_send_additive_mode_combines_static_and_dynamic_exchanges_unit_test(handler, mock_connector):
    """Verifies ADDITIVE mode publishes to both static topology and dynamic exchanges."""
    message = {"data": "test"}
    dynamic_exchanges = [
        {
            "exchange_name": "ex_dynamic",
            "routing_key": "dynamic.route"
        }
    ]

    with patch.object(handler, "_wait_for_responses", new_callable=AsyncMock):
        await handler.send(message=message, dynamic_exchanges=dynamic_exchanges, mode=MqPublishMode.ADDITIVE)

        assert mock_connector.publish.call_count == 2


# ============================================================================
# 5. RPC RESPONSE HANDLING UNIT TESTS (_wait_for_responses & on_raw_rpc_response)
# ============================================================================

@pytest.mark.asyncio
async def test_wait_for_responses_success_unit_test(handler):
    """Verifies successful collection of RPC response payload dictionaries before timeout expires."""
    corr_id = "corr_abc_123"
    event = asyncio.Event()

    expected_payload = {
        "protocol": "AMQP",
        "source": "rpc_reply_exchange/rpc_reply_queue",
        "status": None,
        "content_type": "application/json",
        "payload": {"status": "ok"},
        "properties": {"correlation_id": corr_id}
    }

    async with handler._requests_lock:
        handler._pending_requests[corr_id] = {"event": event, "payload": None}

    async def simulate_inbound_rpc():
        await asyncio.sleep(0.01)
        async with handler._requests_lock:
            handler._pending_requests[corr_id]["payload"] = expected_payload
            event.set()

    asyncio.create_task(simulate_inbound_rpc())

    rpc_wait_list = [{"correlation_id": corr_id, "timeout_ms": 1000}]
    results = await handler._wait_for_responses(rpc_wait_list)

    assert results == {corr_id: expected_payload}
    assert corr_id not in handler._pending_requests


@pytest.mark.asyncio
async def test_wait_for_responses_timeout_unit_test(handler, caplog):
    """Verifies timeout handling returns normalized 408 envelope when RPC response is not received."""
    corr_id = "corr_timeout_999"
    event = asyncio.Event()

    async with handler._requests_lock:
        handler._pending_requests[corr_id] = {"event": event, "payload": None}

    rpc_wait_list = [{
        "source_identifier": "rpc_reply_exchange/rpc_reply_queue",
        "correlation_id": corr_id,
        "timeout_ms": 10
    }]

    expected_timeout_envelope = {
        "protocol": "AMQP",
        "source": "rpc_reply_exchange/rpc_reply_queue",
        "status": 408,
        "content_type": "text/plain",
        "payload": "timeout"
    }

    with patch.object(handler._logger, "warning") as spy_warning:
        results = await handler._wait_for_responses(rpc_wait_list)
        assert results == {corr_id: expected_timeout_envelope}
        actual_msg = spy_warning.call_args[0][0]
        assert f"RPC Response timeout expired for ID: {corr_id}" in actual_msg
        assert corr_id not in handler._pending_requests


@pytest.mark.asyncio
async def test_on_raw_rpc_response_matches_pending_request_unit_test(handler):
    """Verifies processing dictionary response frame, matching correlation_id in properties and setting event."""
    corr_id = "corr_match_555"
    event = asyncio.Event()

    async with handler._requests_lock:
        handler._pending_requests[corr_id] = {"event": event, "payload": None}

    mock_response_envelope = {
        "protocol": "AMQP",
        "source": "rpc_reply_exchange/rpc_reply_queue",
        "status": None,
        "content_type": "application/json",
        "payload": b'{"result": "done"}',
        "properties": {"correlation_id": corr_id}
    }

    success = await handler.on_rpc_response(mock_response_envelope)

    assert success is True
    assert event.is_set()
    assert handler._pending_requests[corr_id]["payload"] == mock_response_envelope


@pytest.mark.asyncio
async def test_on_raw_rpc_response_returns_false_for_invalid_structure_unit_test(handler, caplog):
    """Verifies handling of invalid response dictionaries without properties context in raw RPC response hook."""
    bad_response = {"invalid_key": "no_properties_dict"}


    with patch.object(handler._logger, "warning") as spy_warning:
        success = await handler.on_rpc_response(bad_response)
        assert success is False

        actual_msg = spy_warning.call_args[0][0]
        assert f"Received unmatched, corrupted or expired" in actual_msg


# ============================================================================
# 6. EDGE CASES & ADVANCED CONCURRENCY UNIT TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_setup_publisher_service_propagates_connection_exception_unit_test(handler, mock_connector):
    """Verifies that network connection failures during exchange setup are properly raised."""
    mock_connector.is_connected = MagicMock(return_value=False)
    mock_connector.connect.side_effect = ConnectionError("AMQP Connection dropped")

    exchanges = [{"exchange_name": "ex_fail"}]

    with pytest.raises(ConnectionError, match="AMQP Connection dropped"):
        await handler._setup_publisher_service(exchanges)


@pytest.mark.asyncio
async def test_send_skips_exchange_blocks_missing_exchange_name_unit_test(handler, mock_connector):
    """Verifies that send() safely skips configuration blocks missing 'exchange_name'."""
    message = {"data": "test"}
    dynamic_exchanges = [
        {"routing_key": "orphaned.route"},  # Missing exchange_name
        {"exchange_name": "valid_dynamic", "routing_key": "valid.route"}
    ]

    with patch.object(handler, "_wait_for_responses", new_callable=AsyncMock):
        await handler.send(message=message, dynamic_exchanges=dynamic_exchanges, mode=MqPublishMode.OVERRIDE)

        # Should only publish to the valid dynamic exchange
        assert mock_connector.publish.call_count == 1
        _, kwargs = mock_connector.publish.call_args
        assert kwargs["exchange_name"] == "valid_dynamic"


@pytest.mark.asyncio
async def test_send_additive_mode_with_none_dynamic_exchanges_unit_test(handler, mock_connector):
    """Verifies ADDITIVE mode degrades gracefully to static topology when dynamic_exchanges is None."""
    message = {"data": "test"}

    with patch.object(handler, "_wait_for_responses", new_callable=AsyncMock):
        await handler.send(message=message, dynamic_exchanges=None, mode=MqPublishMode.ADDITIVE)

        # Should publish only to the static topology exchange
        mock_connector.publish.assert_called_once()
        _, kwargs = mock_connector.publish.call_args
        assert kwargs["exchange_name"] == "exchange_telemetry"


@pytest.mark.asyncio
async def test_on_rpc_response_unmatched_correlation_id_returns_false_unit_test(handler):
    """Verifies that an incoming response with an unknown correlation_id returns False and logs a warning."""
    unmatched_envelope = {
        "protocol": "AMQP",
        "source": "rpc_reply_exchange/rpc_reply_queue",
        "properties": {"correlation_id": "unknown_corr_999"}
    }

    with patch.object(handler._logger, "warning") as spy_warning:
        success = await handler.on_rpc_response(unmatched_envelope)

        assert success is False
        spy_warning.assert_called_once()
        actual_msg = spy_warning.call_args[0][0]
        assert "Received unmatched, corrupted or expired RPC response" in actual_msg


@pytest.mark.asyncio
async def test_on_rpc_response_missing_correlation_id_key_returns_false_unit_test(handler):
    """Verifies that an envelope with properties missing correlation_id returns False."""
    malformed_envelope = {
        "protocol": "AMQP",
        "properties": {"delivery_tag": 1}  # Missing correlation_id
    }

    with patch.object(handler._logger, "warning") as spy_warning:
        success = await handler.on_rpc_response(malformed_envelope)

        assert success is False
        spy_warning.assert_called_once()


@pytest.mark.asyncio
async def test_wait_for_responses_cleans_up_heap_allocations_on_data_loss_unit_test(handler):
    """Verifies _wait_for_responses returns error payload if pending request mapping is lost."""
    corr_id = "corr_lost_123"
    rpc_wait_list = [{
        "source_identifier": "rpc_reply_exchange/rpc_reply_queue",
        "correlation_id": corr_id,
        "timeout_ms": 1000
    }]

    # We DO NOT register corr_id into handler._pending_requests

    results = await handler._wait_for_responses(rpc_wait_list)

    assert results == {
        corr_id: {"error": "Internal synchronization mapping data loss"}
    }


@pytest.mark.asyncio
async def test_wait_for_responses_parallel_execution_timing_unit_test(handler):
    """Verifies that multiple RPC waits run concurrently in parallel rather than sequentially."""
    corr_id_1 = "corr_1"
    corr_id_2 = "corr_2"

    async with handler._requests_lock:
        handler._pending_requests[corr_id_1] = {"event": asyncio.Event(), "payload": None}
        handler._pending_requests[corr_id_2] = {"event": asyncio.Event(), "payload": None}

    # Simulate both responses arriving almost simultaneously after 50ms
    async def simulate_responses():
        await asyncio.sleep(0.05)
        async with handler._requests_lock:
            handler._pending_requests[corr_id_1]["payload"] = {"res": 1}
            handler._pending_requests[corr_id_1]["event"].set()
            handler._pending_requests[corr_id_2]["payload"] = {"res": 2}
            handler._pending_requests[corr_id_2]["event"].set()

    asyncio.create_task(simulate_responses())

    rpc_wait_list = [
        {"source_identifier": "ex/q1", "correlation_id": corr_id_1, "timeout_ms": 2000},
        {"source_identifier": "ex/q2", "correlation_id": corr_id_2, "timeout_ms": 2000}
    ]

    start_time = asyncio.get_event_loop().time()
    results = await handler._wait_for_responses(rpc_wait_list)
    elapsed = asyncio.get_event_loop().time() - start_time

    # Parallel wait should finish in ~50ms, well below the sum of timeouts (4000ms)
    assert elapsed < 0.5
    assert len(results) == 2
    assert results[corr_id_1] == {"res": 1}
    assert results[corr_id_2] == {"res": 2}