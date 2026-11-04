# --- Standard Library Imports ---
import asyncio
import time
import pytest
from unittest.mock import MagicMock, patch

# --- Application Core Imports ---
from app.core.connectors.async_local_connector import AsyncLocalConnector
from app.core.output_handlers.async_local_output_handler import AsyncLocalOutputHandler
from app.core.utils.logger import LoggingUtils


# ============================================================================
# FIXTURES
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
        component_name="async_local_connector_unit_test",
        configurations=config,
        environment="test"
    )



@pytest.fixture
def sample_payload():
    """Sample standardized payload message list produced by a translator."""
    return [
        {
            "id": "AC000001_1",
            "value": {"power": [{"timestamp": "2026-07-31 18:00:00 +0000", "value": 11.5}]},
            "timestamp": "2026-07-31 18:00:00 +0000"
        }
    ]


@pytest.fixture
def rpc_enabled_topology():
    """Topology setup specifying RPC Enabled mode."""
    return {
        "rpc_properties": {
            "enabled": True
        }
    }


@pytest.fixture
def fire_and_forget_topology():
    """Topology setup specifying Fire-and-Forget mode (RPC Disabled)."""
    return {
        "rpc_properties": {
            "enabled": False
        }
    }


# ============================================================================
# UNIT TESTS: AsyncLocalConnector
# ============================================================================

@pytest.mark.asyncio
async def test_local_connector_initialization_validation(mock_logger):
    """
    Ensures AsyncLocalConnector raises ValueError if target_callable is missing or invalid.
    """
    # 1. Missing target_callable key
    with pytest.raises(ValueError, match="must be a valid executable function or coroutine"):
        AsyncLocalConnector(server_configurations={}, logger=mock_logger)

    # 2. Non-callable target
    with pytest.raises(ValueError, match="must be a valid executable function or coroutine"):
        AsyncLocalConnector(server_configurations={"target_callable": "not_a_function"}, logger=mock_logger)


@pytest.mark.asyncio
async def test_local_connector_async_target_dispatch(mock_logger, sample_payload):
    """
    Verifies AsyncLocalConnector natively awaits an asynchronous target function (async def).
    """
    async def mock_async_receiver(payload):
        await asyncio.sleep(0.01)
        return {"processed": True, "count": len(payload)}

    connector = AsyncLocalConnector(
        server_configurations={"target_callable": mock_async_receiver},
        logger=mock_logger
    )

    result = await connector.dispatch(sample_payload)

    assert result == {"processed": True, "count": 1}


@pytest.mark.asyncio
async def test_local_connector_sync_blocking_target_dispatch(mock_logger, sample_payload):
    """
    Verifies LocalConnector offloads a synchronous blocking function (def)
    to a background thread via asyncio.to_thread without blocking the main event loop.
    """
    def mock_sync_blocking_receiver(payload):
        # Simulates a synchronous blocking I/O / DB write
        time.sleep(0.05)
        return "SYNC_SUCCESS"

    connector = AsyncLocalConnector(
        server_configurations={"target_callable": mock_sync_blocking_receiver},
        logger=mock_logger
    )
    start_time = time.monotonic()
    result = await connector.dispatch(sample_payload)
    elapsed = time.monotonic() - start_time

    assert result == "SYNC_SUCCESS"
    assert elapsed >= 0.05


# ============================================================================
# UNIT TESTS: AsyncLocalOutputHandler (RPC ENABLED vs FIRE-AND-FORGET)
# ============================================================================

@pytest.mark.asyncio
async def test_async_local_output_handler_initialization_fails_without_connector(mock_logger):
    """
    Ensures AsyncLocalOutputHandler raises ValueError if initialized with None connector.
    """
    with pytest.raises(ValueError, match="'connector' dependency is missing"):
        AsyncLocalOutputHandler(topology_setup={}, connector=None, logger=mock_logger)


@pytest.mark.asyncio
async def test_async_local_output_handler_rpc_enabled_mode(
        mock_logger, sample_payload, rpc_enabled_topology
):
    """
    UNIT TEST: RPC Enabled Mode (rpc_properties.enabled = True)
    - Verifies the handler performs an awaited synchronous dispatch.
    - Verifies it returns the actual payload execution result back to the caller.
    """
    async def mock_receiver(payload):
        return {"status": "ACK", "received_id": payload[0]["id"]}

    connector = AsyncLocalConnector(
        server_configurations={"target_callable": mock_receiver},
        logger=mock_logger
    )
    handler = AsyncLocalOutputHandler(
        topology_setup=rpc_enabled_topology,
        connector=connector,
        logger=mock_logger
    )

    # Execute send() -> Expected to await target and collect return dict
    response = await handler.send(sample_payload)

    assert response == {"status": "ACK", "received_id": "AC000001_1"}


@pytest.mark.asyncio
async def test_async_local_output_handler_fire_and_forget_mode(
        mock_logger, sample_payload, fire_and_forget_topology
):
    """
    UNIT TEST: Fire-and-Forget Mode (rpc_properties.enabled = False)
    - Verifies the handler spawns a background asyncio.create_task.
    - Verifies send() returns None INSTANTLY without waiting for receiver completion.
    """
    receiver_executed = asyncio.Event()

    async def slow_receiver(payload):
        await asyncio.sleep(0.05)
        receiver_executed.set()
        return "SHOULD_BE_IGNORED"

    connector = AsyncLocalConnector(
        server_configurations={"target_callable": slow_receiver},
        logger=mock_logger
    )

    handler = AsyncLocalOutputHandler(
        topology_setup=fire_and_forget_topology,
        connector=connector,
        logger=mock_logger
    )

    start_time = time.monotonic()
    # Execute send() in Fire-and-Forget mode
    response = await handler.send(sample_payload)
    elapsed = time.monotonic() - start_time

    # 1. Must return None immediately
    assert response is None
    # 2. Execution time of send() must be near instantaneous (< 10ms)
    assert elapsed < 0.02
    # 3. Target receiver hasn't finished yet at the moment of return
    assert not receiver_executed.is_set()

    # Wait for the background task to complete to settle test environment
    await asyncio.sleep(0.06)
    assert receiver_executed.is_set()


@pytest.mark.asyncio
async def test_async_local_output_handler_fire_and_forget_exception_isolation(
        mock_logger, sample_payload, fire_and_forget_topology
):
    """
    UNIT TEST: Background Exception Handling in Fire-and-Forget
    - Ensures that an uncaught exception in the target function inside _safe_background_dispatch
      does NOT crash the main event loop or raise an unhandled task exception.
    """
    def failing_receiver(payload):
        raise RuntimeError("Database connection lost inside target function!")

    connector = AsyncLocalConnector(
        server_configurations={"target_callable": failing_receiver},
        logger=mock_logger
    )
    handler = AsyncLocalOutputHandler(
        topology_setup=fire_and_forget_topology,
        connector=connector,
        logger=mock_logger
    )

    # Spy on logger.error to verify background exception logging
    with patch.object(mock_logger, "error") as mock_log_error:
        # Must return None immediately without raising exception
        response = await handler.send(sample_payload)
        assert response is None

        # Give small yield to let background task process and fail safely
        await asyncio.sleep(0.02)

        # Confirm the exception was caught safely and logged with stack trace
        assert mock_log_error.call_count >= 1
        error_msg = mock_log_error.call_args_list[0][0][0]
        assert "Background local dispatch failed" in error_msg


@pytest.mark.asyncio
async def test_async_local_output_handler_stops_on_shutdown(
        mock_logger, sample_payload, rpc_enabled_topology
):
    """
    Verifies that calling stop() triggers rejection constraints and prevents sending new payloads.
    """
    async def mock_receiver(payload):
        return "OK"

    connector = AsyncLocalConnector(
        server_configurations={"target_callable": mock_receiver},
        logger=mock_logger
    )

    handler = AsyncLocalOutputHandler(
        topology_setup=rpc_enabled_topology,
        connector=connector,
        logger=mock_logger
    )

    # Trigger handler shutdown
    handler.stop()

    # Sending payload should be rejected immediately
    result = await handler.send(sample_payload)
    assert result is None