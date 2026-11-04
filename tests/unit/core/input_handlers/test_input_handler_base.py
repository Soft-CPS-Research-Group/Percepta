# --- Standard Library Imports ---
from unittest.mock import MagicMock
import pytest

# --- Internal Framework Imports ---
from app.core.input_handlers.input_handler_base import InputHandlerBase
from app.core.utils.logger import LoggingUtils


def test_cannot_instantiate_base_class_directly():
    """
    Asserts that InputHandlerBase acts as a strict abstract contract and
    cannot be instantiated directly by the runtime engine.
    """
    mock_logger = MagicMock(spec=LoggingUtils)

    # Python must raise a TypeError because abstract methods are not implemented
    with pytest.raises(TypeError) as exc_info:
        InputHandlerBase(logger=mock_logger)

    assert "Can't instantiate abstract class InputHandlerBase" in str(exc_info.value)


def test_concrete_subclass_must_implement_abstract_methods():
    """
    Asserts that any concrete class inheriting from InputHandlerBase is
    architecturally forced to override all abstract methods.
    """
    mock_logger = MagicMock(spec=LoggingUtils)

    # 1. Create an illegal subclass that forgets to implement start() and stop()
    class BrokenHandler(InputHandlerBase):
        pass

    # 2. Verify that trying to instantiate this broken class triggers a TypeError
    with pytest.raises(TypeError) as exc_info:
        BrokenHandler(logger=mock_logger)

    assert "Can't instantiate abstract class BrokenHandler" in str(exc_info.value)
    assert "start" in str(exc_info.value)
    assert "stop" in str(exc_info.value)


def test_valid_concrete_subclass_instantiation():
    """
    Asserts that a correctly built subclass that honors the abstract contract
    can be instantiated and retains the injected core infrastructure variables.
    """
    mock_logger = MagicMock(spec=LoggingUtils)

    # 1. Create a well-behaved subclass fulfilling the contract
    class ValidHandler(InputHandlerBase):
        def start(self):
            return "started"

        def stop(self) -> None:
            return "stopped"

    # 2. Execute instantiation
    handler = ValidHandler(logger=mock_logger)

    # 3. Assertions
    assert handler._logger == mock_logger
    assert handler.start() == "started"
    assert handler.stop() == "stopped"