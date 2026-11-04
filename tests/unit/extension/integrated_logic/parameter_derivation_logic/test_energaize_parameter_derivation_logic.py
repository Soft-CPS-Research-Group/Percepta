from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from app.extension.integrated_logic.parameter_derivation_logic.energaize_parameter_derivation_logic import (
    EnergAIzeParameterDerivationLogic,
)

PERIOD_END = datetime(2026, 10, 13, 16, 35, 45, tzinfo=timezone.utc)


def _param(parameter_type_id, value, generated=False):
    return {"parameter_type_id": parameter_type_id, "values": [value], "generated": generated}


def _message(grid_in=0.30, grid_out=0.05, panels=(0.12,), batteries=(), generated=False):
    entities = [{"id": "GR01", "entity_type_id": "grid_meter", "parameters": {
        "energy_in_total": _param("energy_in_total", grid_in, generated),
        "energy_out_total": _param("energy_out_total", grid_out),
        "energy_in_l1": _param("energy_in_l1", 99.0)}}]
    for i, v in enumerate(panels):
        entities.append({"id": f"PV{i}", "entity_type_id": "pv_panel",
                         "parameters": {"p": _param("energy", v)}})
    for i, (e_in, e_out) in enumerate(batteries):
        entities.append({"id": f"B{i}", "entity_type_id": "battery", "parameters": {
            "in": _param("energy_in", e_in), "out": _param("energy_out", e_out)}})
    return {"timestamp": "2026-10-13 16:35:45 +0000", "entities": entities}


@pytest.fixture
def logic():
    return EnergAIzeParameterDerivationLogic({"entities": {}}, {"frequency_seconds": 15}, MagicMock())


def test_solar_and_non_shiftable_load(logic):
    result = logic.derive(_message(), PERIOD_END)
    derived = result["derived_parameters"]
    assert derived["solar_generation"]["values"] == [0.12]
    assert derived["non_shiftable_load"]["values"] == [pytest.approx(0.37)]
    assert derived["solar_generation"]["start_date"] == "2026-10-13 16:35:30 +0000"
    assert derived["solar_generation"]["frequency_seconds"] == 15
    assert derived["solar_generation"]["aggregation_method"] == "sum"
    assert derived["solar_generation"]["generated"] is False


def test_several_panels_are_summed(logic):
    derived = logic.derive(_message(panels=(0.1, 0.2)), PERIOD_END)["derived_parameters"]
    assert derived["solar_generation"]["values"] == [pytest.approx(0.3)]


def test_batteries_enter_the_balance(logic):
    derived = logic.derive(_message(batteries=((0.10, 0.04),)), PERIOD_END)["derived_parameters"]
    # 0.30 + 0.12 - 0.05 + 0.04 - 0.10
    assert derived["non_shiftable_load"]["values"] == [pytest.approx(0.31)]


def test_negative_balance_is_zero(logic):
    derived = logic.derive(_message(grid_in=0.0, grid_out=1.0, panels=()), PERIOD_END)["derived_parameters"]
    assert derived["non_shiftable_load"]["values"] == [0.0]
    assert derived["solar_generation"]["values"] == [0.0]


def test_generated_flag_propagates(logic):
    derived = logic.derive(_message(generated=True), PERIOD_END)["derived_parameters"]
    assert derived["non_shiftable_load"]["generated"] is True
    assert derived["solar_generation"]["generated"] is False


def test_message_is_kept_and_not_mutated(logic):
    message = _message()
    result = logic.derive(message, PERIOD_END)
    assert result["entities"] == message["entities"]
    assert "derived_parameters" not in message


def test_period_end_needs_timezone(logic):
    with pytest.raises(ValueError):
        logic.derive(_message(), datetime(2026, 10, 13, 16, 35, 45))


@pytest.mark.parametrize("environment,settings", [
    ({}, {"frequency_seconds": 15}),
    ({"entities": {}}, {}),
    ({"entities": {}}, {"frequency_seconds": 0}),
])
def test_invalid_construction(environment, settings):
    with pytest.raises(ValueError):
        EnergAIzeParameterDerivationLogic(environment, settings, MagicMock())
