from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from app.extension.integrated_logic.combination_logic.energaize_combination_logic import EnergAIzeCombinationLogic

PERIOD_END = datetime(2026, 10, 13, 16, 35, 45, tzinfo=timezone.utc)

ENVIRONMENT = {"entities": {"PV01": {"entity_type_id": "pv_panel", "parameters": {"33711": {
    "parameter_type_id": "energy", "measurement_unit": "kWh",
    "temporal_behavior": {"cumulative": False, "periodicity": {"value": 5, "unit": "s"},
                          "aggregation_operation": "sum"}}}}}}


def _data(values):
    return [{"id": "PV01", "entity_type_id": "pv_panel",
             "parameters": {"33711": {"parameter_type_id": "energy", "values": values}}}]


@pytest.fixture
def logger():
    return MagicMock()


@pytest.fixture
def logic(logger):
    return EnergAIzeCombinationLogic(ENVIRONMENT, {"frequency_seconds": 15}, logger)


def test_sum_of_the_readings(logic):
    data = _data([{"timestamp": "2026-10-13 16:35:40 +0000", "value": 0.07},
                  {"timestamp": "2026-10-13 16:35:35 +0000", "value": 0.05}])
    parameter = logic.combine(data, PERIOD_END)["entities"][0]["parameters"]["33711"]
    assert parameter["values"] == [pytest.approx(0.12)]
    assert parameter["aggregation_method"] == "sum"
    assert parameter["measurement_unit"] == "kWh"


@pytest.mark.parametrize("bad_reading", [
    {"timestamp": "not a date", "value": 9.0},
    {"value": 9.0},
    {"timestamp": 123, "value": 9.0},
])
def test_reading_without_valid_timestamp_is_ignored(logic, logger, bad_reading):
    data = _data([{"timestamp": "2026-10-13 16:35:35 +0000", "value": 0.05}, bad_reading])
    parameter = logic.combine(data, PERIOD_END)["entities"][0]["parameters"]["33711"]
    assert parameter["values"] == [0.05]
    logger.warning.assert_called_once()
