# --- Standard Library Imports ---
from typing import Any

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.output_handlers import OutputHandlerBase
from app.core.utils.logger import LoggingUtils


# ============================================================================
# DUMMY CONCRETE IMPLEMENTATION FOR TESTING
# ============================================================================

class DummyOutputHandler(OutputHandlerBase):
    """Concrete implementation of OutputHandlerBase used exclusively for unit testing."""

    def send(self, message: dict) -> Any:
        return {"status": "sent", "payload": message}

    def stop(self) -> None:
        pass


class IncompleteOutputHandler(OutputHandlerBase):
    """Incomplete implementation that forgets to implement abstract methods."""
    pass


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


# ============================================================================
# 1. ABSTRACT CONTRACT UNIT TESTS
# ============================================================================

def test_instantiating_base_class_directly_raises_type_error_unit_test(mock_logger):
    """Verifies that OutputHandlerBase cannot be instantiated directly due to ABC constraints."""
    with pytest.raises(TypeError) as exc_info:
        OutputHandlerBase(logger=mock_logger)  # type: ignore

    assert "Can't instantiate abstract class OutputHandlerBase" in str(exc_info.value)


def test_instantiating_incomplete_subclass_raises_type_error_unit_test(mock_logger):
    """Verifies that a subclass missing required abstract methods cannot be instantiated."""
    with pytest.raises(TypeError) as exc_info:
        IncompleteOutputHandler(logger=mock_logger)  # type: ignore

    error_msg = str(exc_info.value)
    assert "send" in error_msg
    assert "stop" in error_msg


# ============================================================================
# 2. CONCRETE IMPLEMENTATION & INITIALIZATION UNIT TESTS
# ============================================================================

def test_concrete_handler_initialization_success_unit_test(mock_logger):
    """Verifies that a valid concrete subclass initializes and stores the logger reference."""
    handler = DummyOutputHandler(logger=mock_logger)

    assert handler._logger == mock_logger


def test_concrete_handler_executes_abstract_methods_successfully_unit_test(mock_logger):
    """Verifies that concrete subclasses properly execute send and stop methods."""
    handler = DummyOutputHandler(logger=mock_logger)
    test_message = {"data": "test_payload"}

    result = handler.send(test_message)

    assert result == {"status": "sent", "payload": test_message}
    # Calling stop should execute cleanly without raising exceptions
    handler.stop()


def test_base_class_default_not_implemented_error_unit_test(mock_logger):
    """Verifies direct invocation of base class super() methods raises NotImplementedError if called."""
    class SuperCallingOutputHandler(OutputHandlerBase):
        def send(self, message: dict) -> Any:
            return super().send(message)

        def stop(self) -> None:
            super().stop()

    handler = SuperCallingOutputHandler(logger=mock_logger)

    with pytest.raises(NotImplementedError):
        handler.send({"test": "data"})

    with pytest.raises(NotImplementedError):
        handler.stop()