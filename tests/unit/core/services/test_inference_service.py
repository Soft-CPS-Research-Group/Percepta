import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.adapters.ml_model_adapter_base import MLModelAdapterBase
from app.core.integrated_logic.decision_validation_correction_logic_base import DecisionValidationCorrectionLogicBase
from app.core.integrated_logic.model_execution_error_handling_logic_base import ModelExecutionErrorHandlingLogicBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.services.inference_service import InferenceService
from app.core.storages.runtime_data_storage.runtime_data_storage_base import RuntimeDataStorageBase

MESSAGE = {"timestamp": "2026-08-19 11:15:00 +0000", "entities": []}
SUCCESS = {"/inference": {"status": "success", "code": 200, "data": {"B01": 3}}}
NORMALISED = [{"id": "B01"}]


def _mock(base, **attributes):
    mock = MagicMock(spec=base)
    for name, value in attributes.items():
        setattr(mock, name, value)
    return mock


@pytest.fixture
def parts():
    adapter = _mock(MLModelAdapterBase, adapt_model_input=MagicMock(return_value="input"),
                    adapt_model_decisions=MagicMock(return_value=NORMALISED))
    model = _mock(OutputHandlerBase, send=AsyncMock(return_value=SUCCESS))
    errors = _mock(ModelExecutionErrorHandlingLogicBase, should_retry=AsyncMock(return_value=False))
    validation = _mock(DecisionValidationCorrectionLogicBase,
                       validate_and_correct=MagicMock(return_value={"B01": 2}))
    out = _mock(OutputHandlerBase, send=AsyncMock())
    return {"adapter": adapter, "model_output_handler": model, "error_handling_logic": errors,
            "validation_logic": None, "runtime_data_storage": None, "output_handler": out, "logger": MagicMock()}


def _service(parts):
    return InferenceService(**parts)


def _receive(service, payload=MESSAGE):
    asyncio.run(service.receive_message({"payload": payload}))


def test_decisions_are_sent_in_the_normalised_format(parts):
    _receive(_service(parts))
    parts["adapter"].adapt_model_input.assert_called_once_with(MESSAGE)
    parts["model_output_handler"].send.assert_awaited_once_with("input")
    parts["adapter"].adapt_model_decisions.assert_called_once_with({"B01": 3})
    parts["output_handler"].send.assert_awaited_once_with(NORMALISED)


def test_validation_logic_replaces_the_decisions(parts):
    parts["validation_logic"] = _mock(DecisionValidationCorrectionLogicBase,
                                      validate_and_correct=MagicMock(return_value={"B01": 2}))
    _receive(_service(parts))
    parts["validation_logic"].validate_and_correct.assert_called_once_with({"B01": 3})
    parts["adapter"].adapt_model_decisions.assert_called_once_with({"B01": 2})


def test_message_as_json_text(parts):
    _receive(_service(parts), json.dumps(MESSAGE))
    parts["adapter"].adapt_model_input.assert_called_once_with(MESSAGE)


def test_error_is_retried_while_the_logic_says_so(parts):
    parts["model_output_handler"].send = AsyncMock(side_effect=[ConnectionError("x"), SUCCESS])
    parts["error_handling_logic"].should_retry = AsyncMock(return_value=True)
    _receive(_service(parts))
    assert parts["model_output_handler"].send.await_count == 2
    parts["error_handling_logic"].should_retry.assert_awaited_once()
    assert parts["error_handling_logic"].should_retry.await_args.args[1] == MESSAGE["timestamp"]
    parts["output_handler"].send.assert_awaited_once()


@pytest.mark.parametrize("response", [
    None, {}, {"/inference": {"status": "error", "error": "refused"}},
    {"/inference": {"status": "success", "code": 500, "data": "boom"}},
])
def test_bad_responses_end_the_process_when_the_logic_says_so(parts, response):
    parts["model_output_handler"].send = AsyncMock(return_value=response)
    _receive(_service(parts))
    parts["error_handling_logic"].should_retry.assert_awaited_once()
    parts["adapter"].adapt_model_decisions.assert_not_called()
    parts["output_handler"].send.assert_not_awaited()


def test_nothing_is_sent_without_decisions(parts):
    parts["adapter"].adapt_model_decisions = MagicMock(return_value=[])
    _receive(_service(parts))
    parts["output_handler"].send.assert_not_awaited()


def test_errors_in_a_period_do_not_stop_the_service(parts):
    parts["adapter"].adapt_model_input = MagicMock(side_effect=[ValueError("bad"), "input"])
    service = _service(parts)
    _receive(service)
    _receive(service)
    parts["output_handler"].send.assert_awaited_once()


def test_failure_to_send_is_logged(parts):
    parts["output_handler"].send = AsyncMock(side_effect=ConnectionError("down"))
    _receive(_service(parts))
    parts["logger"].error.assert_called()


@pytest.mark.parametrize("payload", [None, "not json", [1, 2], 5])
def test_unreadable_messages_are_skipped(parts, payload):
    _receive(_service(parts), payload)
    parts["adapter"].adapt_model_input.assert_not_called()


def test_message_without_timestamp_gets_an_id(parts):
    parts["model_output_handler"].send = AsyncMock(side_effect=ConnectionError("x"))
    service = _service(parts)
    _receive(service, {"entities": []})
    _receive(service, {"entities": []})
    ids = [call.args[1] for call in parts["error_handling_logic"].should_retry.await_args_list]
    assert ids == ["message-1", "message-2"]


@pytest.mark.parametrize("name", ["adapter", "model_output_handler", "error_handling_logic", "output_handler"])
def test_wrong_types(parts, name):
    parts[name] = object()
    with pytest.raises(TypeError):
        _service(parts)


def test_wrong_validation_logic_type(parts):
    parts["validation_logic"] = object()
    with pytest.raises(TypeError):
        _service(parts)


def _storage(**attributes):
    return _mock(RuntimeDataStorageBase, store=AsyncMock(), **attributes)


def test_data_is_stored_without_validation_logic(parts):
    parts["runtime_data_storage"] = _storage()
    _receive(_service(parts))
    parts["runtime_data_storage"].store.assert_awaited_once_with({
        "model_inputs": "input", "decisions": {"B01": 3}, "corrected_decisions": None,
        "timestamp": MESSAGE["timestamp"]})


def test_both_decisions_are_stored_with_validation_logic(parts):
    parts["runtime_data_storage"] = _storage()
    parts["validation_logic"] = _mock(DecisionValidationCorrectionLogicBase,
                                      validate_and_correct=MagicMock(return_value={"B01": 2}))
    _receive(_service(parts))
    stored = parts["runtime_data_storage"].store.await_args.args[0]
    assert stored["decisions"] == {"B01": 3}
    assert stored["corrected_decisions"] == {"B01": 2}


def test_validation_logic_changing_its_input_does_not_change_the_stored_decisions(parts):
    parts["runtime_data_storage"] = _storage()

    def correct(decisions):
        decisions["B01"] = 0
        return decisions

    parts["validation_logic"] = _mock(DecisionValidationCorrectionLogicBase,
                                      validate_and_correct=MagicMock(side_effect=correct))
    _receive(_service(parts))
    stored = parts["runtime_data_storage"].store.await_args.args[0]
    assert stored["decisions"] == {"B01": 3}
    assert stored["corrected_decisions"] == {"B01": 0}


def test_failure_of_the_storage_does_not_stop_the_decisions(parts):
    parts["runtime_data_storage"] = _storage()
    parts["runtime_data_storage"].store = AsyncMock(side_effect=ConnectionError("down"))
    _receive(_service(parts))
    parts["logger"].error.assert_called_once()
    parts["output_handler"].send.assert_awaited_once_with(NORMALISED)


def test_record_without_a_valid_timestamp_gets_the_current_time(parts):
    parts["runtime_data_storage"] = _storage()
    _receive(_service(parts), {"timestamp": "not a date", "entities": []})
    timestamp = parts["runtime_data_storage"].store.await_args.args[0]["timestamp"]
    assert timestamp.endswith("+0000") and timestamp != "not a date"


def test_wrong_storage_type(parts):
    parts["runtime_data_storage"] = object()
    with pytest.raises(TypeError):
        _service(parts)
