import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.integrated_logic.decision_validation_correction_logic_base import DecisionValidationCorrectionLogicBase
from app.core.integrated_logic.model_execution_error_handling_logic_base import ModelExecutionErrorHandlingLogicBase
from app.extension.ml_models.energaize.energaize_ml_model_adapter import EnergAIzeMLModelAdapter
from app.extension.ml_models.energaize.energaize_ml_model_launcher import EnergAIzeMLModelLauncher
from app.extension.ml_models.energaize.energaize_model_execution_error_handling_logic import (
    EnergAIzeModelExecutionErrorHandlingLogic,
)

ENDPOINT = {"path": "/inference", "method": "POST", "timeout_ms": 5000,
            "headers": {"Content-Type": "application/json"}, "rpc_properties": {"enabled": True}}


def _configuration(**changes):
    configuration = {"protocol_id": "AsyncHTTP", "connection_info": {
        "connection": {"url": "http://localhost:8000"}, "topology": {"endpoints": [dict(ENDPOINT)]}}}
    configuration.update(changes)
    return configuration


class FakeValidation(DecisionValidationCorrectionLogicBase):
    def validate_and_correct(self, decisions):
        return decisions


class FakeErrorHandling(ModelExecutionErrorHandlingLogicBase):
    async def should_retry(self, error, period_id):
        return False


class NotALogic:
    pass


_MODULE = __name__


def _launch(configuration, **logics):
    output_launcher = MagicMock()
    output_handler = MagicMock()
    output_launcher.build_output_handler = AsyncMock(return_value=(output_handler, None))
    launcher = EnergAIzeMLModelLauncher(output_launcher, MagicMock())
    components = asyncio.run(launcher.launch(
        "m1", {"connection_configuration": configuration, **logics}, "R-H-01", {"entities": {}}, MagicMock()
    ))
    return components, output_launcher, output_handler


def test_components_are_built():
    components, output_launcher, output_handler = _launch(_configuration())
    assert isinstance(components.adapter, EnergAIzeMLModelAdapter)
    assert isinstance(components.error_handling_logic, EnergAIzeModelExecutionErrorHandlingLogic)
    assert components.output_handler is output_handler
    assert components.validation_logic is None
    kwargs = output_launcher.build_output_handler.call_args.kwargs
    assert kwargs["handler_type"] == "AsyncHTTP"
    assert kwargs["server_configurations"] == {"url": "http://localhost:8000"}
    assert kwargs["topology_setup"] == {"endpoints": [ENDPOINT]}


@pytest.mark.parametrize("configuration", [
    _configuration(protocol_id="AsyncRabbitMQ"),
    _configuration(connection_info=None),
    _configuration(connection_info={"topology": {"endpoints": [ENDPOINT]}}),
    _configuration(connection_info={"connection": {}, "topology": {"endpoints": []}}),
    _configuration(connection_info={"connection": {}, "topology": {"endpoints": {"a": ENDPOINT}}}),
    _configuration(connection_info={"connection": {}, "topology": {"endpoints": [{"path": " "}]}}),
    _configuration(connection_info={"connection": {}, "topology": {"endpoints": [
        {"path": "/inference", "rpc_properties": {"enabled": False}}]}}),
    _configuration(connection_info={"connection": {}, "topology": {"endpoints": [{"path": "/inference"}]}}),
])
def test_invalid_configuration(configuration):
    with pytest.raises(ValueError):
        _launch(configuration)


def test_launcher_needs_its_dependencies():
    with pytest.raises(ValueError):
        EnergAIzeMLModelLauncher(None, MagicMock())
    with pytest.raises(ValueError):
        EnergAIzeMLModelLauncher(MagicMock(), None)


def test_logics_come_from_the_entry_of_the_model():
    components, _, _ = _launch(
        _configuration(),
        model_execution_error_handling_logic={"path": f"{_MODULE}.FakeErrorHandling"},
        decision_validation_correction_logic={"path": f"{_MODULE}.FakeValidation"}
    )
    assert isinstance(components.error_handling_logic, FakeErrorHandling)
    assert isinstance(components.validation_logic, FakeValidation)


def test_without_logics_in_the_entry_uses_the_default_error_handling_and_no_validation():
    components, _, _ = _launch(_configuration())
    assert isinstance(components.error_handling_logic, EnergAIzeModelExecutionErrorHandlingLogic)
    assert components.validation_logic is None


@pytest.mark.parametrize("logic", [
    {"path": f"{_MODULE}.NotALogic"},
    {"path": f"{_MODULE}.DoesNotExist"},
    {"path": "no.such.module.Class"},
    {"path": " "},
    "not a dictionary",
])
def test_invalid_logic_in_the_entry(logic):
    with pytest.raises(ValueError):
        _launch(_configuration(), decision_validation_correction_logic=logic)
