# --- Standard Library Imports ---
from unittest.mock import MagicMock, patch, AsyncMock
from zoneinfo import ZoneInfo
import pytest

# --- Internal Framework Imports ---
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher, _INPUT_BUILDERS, \
    register_input_handler
from app.core.utils.logger import LoggingUtils
from app.core.input_handlers.async_http_input_handler import HTTPInputHandler


# ==============================================================================
# --- Test Fixtures ---
# ==============================================================================

@pytest.fixture
def mock_logger():
    """Provides a mocked instance of the framework's central logger."""
    return MagicMock(spec=LoggingUtils)


@pytest.fixture
def launcher(mock_logger):
    """Instantiates the InputHandlersLauncher bound to the mocked logger."""
    return InputHandlersLauncher(logger=mock_logger)


# ==============================================================================
# --- Launcher Core & Factory Registry Tests ---
# ==============================================================================

def test_build_input_handler_success(launcher):
    """
    Asserts that the public factory method successfully resolves a registered text identifier,
    delegates parameters to the unbound hook, and returns the materialized handler.
    """

    # 1. Register a temporary dummy builder to isolate global state testing
    @register_input_handler("MockProtocol")
    def _dummy_builder(launcher_instance, topology, callback, server_config):
        return "mocked_handler_instance"

    # 2. Mock execution parameters
    sample_topology = {"interval": 5}
    sample_callback = MagicMock()
    sample_config = {"host": "localhost"}

    # 3. Request resolution from launcher core
    handler = launcher.build_input_handler(
        handler_type="MockProtocol",
        topology_setup=sample_topology,
        callback=sample_callback,
        server_configurations=sample_config
    )

    # 4. Assertions
    assert handler == "mocked_handler_instance"

    # Clean up the global registry mutation to keep other tests deterministic
    _INPUT_BUILDERS.pop("MockProtocol", None)


def test_build_input_handler_unregistered_raises_value_error(launcher, mock_logger):
    """
    Asserts that requesting a protocol type string missing from the global registry
    safely triggers a ValueError guard clause and logs the event boundary.
    """
    with pytest.raises(ValueError) as exc_info:
        launcher.build_input_handler(
            handler_type="NonExistentProtocol",
            topology_setup={},
            callback=lambda x: None,
            server_configurations={}
        )

    # Structural Verifications
    assert "is not registered in the global registry" in str(exc_info.value)
    mock_logger.error.assert_called_once()


# ==============================================================================
# --- Protocol-Specific Builders & Teardown Registry Tests ---
# ==============================================================================

@patch("app.endpoints_builders.input_handlers_launcher.RabbitMQConnector")
def test_rabbitmq_builder_registers_teardown(mock_connector_class, launcher):
    """
    Asserts that executing the specific RabbitMQ builder factory creates a network link
    and successfully escrows an infrastructure teardown closure inside the local stack.
    """
    # 1. Mock the physical connection layers to prevent background IO socket tasks
    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.is_connected.return_value = True

    # Ensure the stateful teardown hooks registry starts completely empty
    assert len(launcher._teardown_hooks) == 0

    # 2. Trigger internal factory provider execution context
    handler = launcher._build_async_rabbitmq_input_handler(
        topology_setup={"endpoints": []},
        callback=lambda x: None,
        server_configurations={"host": "127.0.0.1"}
    )

    # 3. Structural assertions for initialization boundary
    assert isinstance(handler, RabbitMQInputHandler)
    mock_connector_instance.connect.assert_called_once()
    assert len(launcher._teardown_hooks) == 1

    # 4. Validate the operational behavior of the registered Teardown Hook
    # Extract and execute the anonymous closure stored inside the registry array stack
    rabbitmq_teardown_hook = launcher._teardown_hooks[0]
    rabbitmq_teardown_hook()

    # 5. Core teardown lifecycle assertions
    # Verify that the hook queried the connection state and closed the physical link safely
    mock_connector_instance.is_connected.assert_called_once()
    mock_connector_instance.close.assert_called_once()

@patch("app.endpoints_builders.input_handlers_launcher.HTTPConnector")  # Top patch -> Injected as 2nd mock argument
@patch("app.endpoints_builders.input_handlers_launcher.PeriodicScheduler")  # Bottom patch -> Injected as 1st mock argument
def test_http_builder_registers_teardown(mock_scheduler_class, mock_connector_class, launcher):
    """
    Asserts that executing the specific HTTP builder factory initializes the infrastructure
    components and successfully escrows an asynchronous teardown closure inside the stack.
    """
    # 1. Setup mock instances matching the positional parameters injected by the patch stack
    mock_scheduler_instance = MagicMock()
    mock_scheduler_class.return_value = mock_scheduler_instance

    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance

    # Since HTTPConnector.close is an asynchronous coroutine, we redefine it as an AsyncMock
    mock_connector_instance.close = AsyncMock()

    # Ensure the stateful teardown hooks registry starts completely empty
    assert len(launcher._teardown_hooks) == 0

    # 2. Trigger the internal HTTP factory provider execution context
    handler = launcher._build_http_input_handler(
        topology_setup={"endpoints": [], "poll_interval_seconds": 10},
        callback=lambda x: None,
        server_configurations={"timezone": "Europe/Lisbon"}
    )

    # 3. Structural assertions for initialization boundary
    assert isinstance(handler, HTTPInputHandler)
    assert len(launcher._teardown_hooks) == 1

    # 4. Validate the operational behavior of the registered Teardown Hook
    handler.stop = MagicMock()

    # Extract and execute the anonymous closure stored inside the registry array stack
    http_teardown_hook = launcher._teardown_hooks[0]
    http_teardown_hook()

    # 5. Core teardown lifecycle assertions
    handler.stop.assert_called_once()
    mock_connector_instance.close.assert_called_once()


@patch("app.endpoints_builders.input_handlers_launcher.HTTPConnector")  # Top patch -> Injected as 2nd mock argument
@patch("app.endpoints_builders.input_handlers_launcher.PeriodicScheduler")  # Bottom patch -> Injected as 1st mock argument
def test_http_builder_timezone_fallback_on_invalid_zone(mock_scheduler_class, mock_connector_class, launcher):
    """
    Asserts that the HTTP factory builder is resilient to invalid timezone strings
    provided in the server configurations, falling back gracefully to UTC.
    """
    # 1. Setup server configuration with an illegal/non-existent timezone name
    faulty_server_config = {"timezone": "Invalid/Timezone_Name"}

    # 2. Trigger the HTTP factory initialization process (scheduler/connector mocked via decorators)
    launcher._build_http_input_handler(
        topology_setup={"endpoints": [], "poll_interval_seconds": 30},
        callback=lambda x: None,
        server_configurations=faulty_server_config
    )

    # 3. Assertions
    mock_scheduler_class.assert_called_once()
    called_args, called_kwargs = mock_scheduler_class.call_args

    # Check if the timezone fell back to UTC instead of raising a ZoneInfoNotFoundError
    assert "timezone" in called_kwargs
    assert called_kwargs["timezone"] == ZoneInfo("UTC")

    # 4. Telemetry Assertions (Validating the Log output)
    # We query the mock_logger that was injected into the launcher during fixture setup
    expected_warning_msg = "Launcher | Invalid timezone 'Invalid/Timezone_Name' provided. Falling back to 'UTC'."
    launcher._logger.warning.assert_called_once_with(expected_warning_msg)

# ==============================================================================
# --- Infrastructure Teardown & Lifecycle Resiliency Tests ---
# ==============================================================================

def test_shutdown_infrastructure_clears_hooks(launcher, mock_logger):
    """
    Asserts that the infrastructure shutdown sequence sequentially fires all registered
    anonymous technology-specific hooks and flushes the collection array pointers cleanly.
    """
    # 1. Create independent spying closures simulating low-level link teardowns
    mock_hook_1 = MagicMock()
    mock_hook_2 = MagicMock()

    launcher._teardown_hooks.append(mock_hook_1)
    launcher._teardown_hooks.append(mock_hook_2)

    # 2. Command infrastructure demolition
    launcher.shutdown_infrastructure()

    # 3. Core Assertions
    mock_hook_1.assert_called_once()
    mock_hook_2.assert_called_once()

    # Crucial memory footprint check: Stack buffer must be blanked out for GC
    assert len(launcher._teardown_hooks) == 0


def test_shutdown_infrastructure_resilient_to_individual_failures(launcher, mock_logger):
    """
    Asserts that a critical failure or unhandled exception within an individual teardown hook
    does not break the destruction loop, allowing the remaining infrastructure contexts to dismantle.
    """
    mock_hook_faulty = MagicMock(side_effect=RuntimeException("Network Link Refused To Terminate"))
    mock_hook_stable = MagicMock()

    launcher._teardown_hooks.append(mock_hook_faulty)
    launcher._teardown_hooks.append(mock_hook_stable)

    # Execute teardown loop
    launcher.shutdown_infrastructure()

    # Verify both were processed despite the mid-flight runtime crash
    mock_hook_faulty.assert_called_once()
    mock_hook_stable.assert_called_once()

    # Ensure the error boundary logged the structural failure
    mock_logger.error.assert_called_once()
    assert len(launcher._teardown_hooks) == 0


@patch("app.endpoints_builders.input_handlers_launcher.RabbitMQConnector")
def test_rabbitmq_builder_failure_does_not_leak_teardown_hooks(mock_connector_class, launcher):
    """
    Asserts that if the physical connector fails to connect during the initialization
    phase, the factory immediately propagates the exception and ensures no partial or
    broken teardown hook is registered in the stateful allocation stack.
    """
    # 1. Setup a connector mock that raises a connection exception on startup
    mock_connector_instance = MagicMock()
    mock_connector_class.return_value = mock_connector_instance
    mock_connector_instance.connect.side_effect = ConnectionRefusedError("RabbitMQ broker is unreachable")

    # The registry must start empty
    assert len(launcher._teardown_hooks) == 0

    # 2. Trigger the context execution expecting a failure
    with pytest.raises(ConnectionRefusedError) as exc_info:
        launcher._build_async_rabbitmq_input_handler(
            topology_setup={"endpoints": []},
            callback=lambda x: None,
            server_configurations={"host": "127.0.0.1"}
        )

    # 3. Structural Security Assertions
    assert "RabbitMQ broker is unreachable" in str(exc_info.value)

    # Ensure no dead teardown hook was appended since the connection failed
    assert len(launcher._teardown_hooks) == 0


# ==============================================================================
# --- Custom Exceptions for Testing ---
# ==============================================================================

class RuntimeException(Exception):
    """Custom exception used to simulate unexpected failures during teardown tests."""
    pass