from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from app.extension.ml_models.energaize.energaize_ml_model_adapter import EnergAIzeMLModelAdapter

ENVIRONMENT = {"entities": {
    "AC000001_1": {"entity_type_id": "ev_charger", "actuators": {
        "on_off": {"actuator_type_id": "on_off"},
        "power_1": {"actuator_type_id": "power"},
        "power_2": {"actuator_type_id": "power"}}},
    "B01": {"entity_type_id": "battery", "actuators": {"power": {"actuator_type_id": "power"}}},
    "GR01": {"entity_type_id": "grid_meter"},
    "EV1": {"entity_type_id": "ev_charger", "actuators": {"on_off": {"actuator_type_id": "on_off"}}},
}}


@pytest.fixture
def logger():
    return MagicMock()


@pytest.fixture
def adapter(logger):
    return EnergAIzeMLModelAdapter("env", ENVIRONMENT, logger)


def test_input_is_not_changed(adapter, logger):
    message = {"timestamp": "x", "entities": []}
    assert adapter.adapt_model_input(message) is message
    logger.debug.assert_called_once()


def test_decision_in_the_normalised_format(adapter):
    now = datetime(2026, 8, 19, 11, 15, 0, tzinfo=timezone.utc)
    with patch("app.extension.ml_models.energaize.energaize_ml_model_adapter.datetime") as mocked:
        mocked.now.return_value = now
        result = adapter.adapt_model_decisions({"B01": 3.4})
    mocked.now.assert_called_once_with(timezone.utc)
    assert result == [{
        "id": "B01", "entity_type_id": "battery",
        "actuators": {"power": {"actuator_type_id": "power", "value": 3.4}},
        "timestamp": "2026-08-19 11:15:00 +0000"}]


def test_first_power_actuator_is_used(adapter):
    decision = adapter.adapt_model_decisions({"AC000001_1": 3})[0]
    assert list(decision["actuators"]) == ["power_1"]


def test_several_entities(adapter):
    result = adapter.adapt_model_decisions({"AC000001_1": 3, "B01": -1.0})
    assert [d["id"] for d in result] == ["AC000001_1", "B01"]


@pytest.mark.parametrize("decisions", [{"UNKNOWN": 1}, {"GR01": 1}, {"EV1": 1}, {"B01": "high"}, {"B01": True},
                                       {"B01": None}])
def test_invalid_decisions_are_ignored(adapter, logger, decisions):
    assert adapter.adapt_model_decisions(decisions) == []
    logger.warning.assert_called_once()


def test_other_decisions_are_kept(adapter):
    result = adapter.adapt_model_decisions({"UNKNOWN": 1, "B01": 2})
    assert [d["id"] for d in result] == ["B01"]


def test_empty_and_wrong_type(adapter, logger):
    assert adapter.adapt_model_decisions({}) == []
    assert adapter.adapt_model_decisions([1, 2]) == []
    logger.warning.assert_called_once()
