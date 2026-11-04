# --- Standard Library Imports ---
import asyncio
from unittest.mock import AsyncMock, MagicMock

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.connectors.async_local_connector import AsyncLocalConnector
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
        component_name="async_local_connector_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def sync_target_callable():
    """Provides a mock synchronous callable."""
    return MagicMock(return_value={"status": "sync_success"})


@pytest.fixture
def async_target_callable():
    """Provides a mock asynchronous coroutine callable."""
    return AsyncMock(return_value={"status": "async_success"})


# ============================================================================
# 1. INITIALIZATION & VALIDATION UNIT TESTS
# ============================================================================

def test_init_with_valid_sync_callable_unit_test(mock_logger, sync_target_callable):
    """Verifies successful initialization when a valid synchronous callable is provided."""
    config = {"target_callable": sync_target_callable}
    connector = AsyncLocalConnector(server_configurations=config, logger=mock_logger)

    assert connector._target_callable is sync_target_callable
    assert connector._logger is mock_logger


def test_init_with_valid_async_callable_unit_test(mock_logger, async_target_callable):
    """Verifies successful initialization when a valid asynchronous coroutine is provided."""
    config = {"target_callable": async_target_callable}
    connector = AsyncLocalConnector(server_configurations=config, logger=mock_logger)

    assert connector._target_callable is async_target_callable
    assert connector._logger is mock_logger


def test_init_raises_error_when_target_callable_missing_unit_test(mock_logger):
    """Verifies that ValueError is raised if 'target_callable' is missing in configurations."""
    invalid_config = {}
    with pytest.raises(ValueError, match="'target_callable' must be a valid executable function or coroutine"):
        AsyncLocalConnector(server_configurations=invalid_config, logger=mock_logger)


def test_init_raises_error_when_configurations_is_none_or_empty_unit_test(mock_logger):
    """Verifies that ValueError is raised if server_configurations is empty or None."""
    with pytest.raises(ValueError, match="'target_callable' must be a valid executable function or coroutine"):
        AsyncLocalConnector(server_configurations=None, logger=mock_logger)


def test_init_raises_error_when_target_is_not_callable_unit_test(mock_logger):
    """Verifies that ValueError is raised if 'target_callable' is not an executable function."""
    invalid_config = {"target_callable": "not_a_callable_string"}
    with pytest.raises(ValueError, match="'target_callable' must be a valid executable function or coroutine"):
        AsyncLocalConnector(server_configurations=invalid_config, logger=mock_logger)


# ============================================================================
# 2. DISPATCH EXECUTION UNIT TESTS (ASYNC & SYNC CALLABLES)
# ============================================================================

@pytest.mark.asyncio
async def test_dispatch_to_async_coroutine_function_unit_test(mock_logger, async_target_callable):
    """Verifies direct native await execution when target_callable is an async coroutine."""
    config = {"target_callable": async_target_callable}
    connector = AsyncLocalConnector(server_configurations=config, logger=mock_logger)

    payload = {"telemetry_id": "meter_01", "active_power": 120.5}
    result = await connector.dispatch(payload)

    async_target_callable.assert_awaited_once_with(payload)
    assert result == {"status": "async_success"}


@pytest.mark.asyncio
async def test_dispatch_to_sync_blocking_function_unit_test(mock_logger, sync_target_callable):
    """Verifies thread-pool delegation via asyncio.to_thread when target_callable is synchronous."""
    config = {"target_callable": sync_target_callable}
    connector = AsyncLocalConnector(server_configurations=config, logger=mock_logger)

    payload = {"command": "SET_RELAY", "value": 1}
    result = await connector.dispatch(payload)

    sync_target_callable.assert_called_once_with(payload)
    assert result == {"status": "sync_success"}


@pytest.mark.asyncio
async def test_dispatch_propagates_exception_from_async_callable_unit_test(mock_logger):
    """Ensures exceptions raised inside an asynchronous target callable are cleanly propagated."""
    async def failing_async_target(payload):
        raise RuntimeError("Async downstream receiver crashed")

    config = {"target_callable": failing_async_target}
    connector = AsyncLocalConnector(server_configurations=config, logger=mock_logger)

    with pytest.raises(RuntimeError, match="Async downstream receiver crashed"):
        await connector.dispatch({"data": 123})


@pytest.mark.asyncio
async def test_dispatch_propagates_exception_from_sync_callable_unit_test(mock_logger):
    """Ensures exceptions raised inside a synchronous target callable offloaded to thread are cleanly propagated."""
    def failing_sync_target(payload):
        raise ValueError("Sync downstream receiver crashed")

    config = {"target_callable": failing_sync_target}
    connector = AsyncLocalConnector(server_configurations=config, logger=mock_logger)

    with pytest.raises(ValueError, match="Sync downstream receiver crashed"):
        await connector.dispatch({"data": 123})