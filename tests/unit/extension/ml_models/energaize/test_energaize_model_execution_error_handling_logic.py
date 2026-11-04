import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from app.extension.ml_models.energaize.energaize_model_execution_error_handling_logic import (
    EnergAIzeModelExecutionErrorHandlingLogic,
)

_SLEEP = "app.extension.ml_models.energaize.energaize_model_execution_error_handling_logic.asyncio.sleep"


def _logic():
    return EnergAIzeModelExecutionErrorHandlingLogic({"entities": {}}, MagicMock())


def _fail(logic, period_id="p1"):
    return asyncio.run(logic.should_retry(ConnectionError("x"), period_id))


def test_retries_after_the_first_two_failures_waiting_one_second():
    logic = _logic()
    with patch(_SLEEP, new=AsyncMock()) as sleep:
        assert _fail(logic) is True
        assert _fail(logic) is True
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 1.0]


def test_stops_after_the_third_failure_without_waiting():
    logic = _logic()
    with patch(_SLEEP, new=AsyncMock()) as sleep:
        _fail(logic)
        _fail(logic)
        sleep.reset_mock()
        assert _fail(logic) is False
    sleep.assert_not_called()


def test_count_restarts_in_a_new_period():
    logic = _logic()
    with patch(_SLEEP, new=AsyncMock()):
        # Period p1 fails once and recovers, so p2 still gets its three attempts
        assert _fail(logic, "p1") is True
        results = [_fail(logic, "p2") for _ in range(3)]
    assert results == [True, True, False]
