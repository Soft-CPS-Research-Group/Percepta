from unittest.mock import MagicMock

import pytest

from app.core.services.target_system_resolver import TargetSystemResolver

ENVIRONMENT = {"target_systems": {
    "cleanwatts": {"actuator_mapping": {"act_power_1": "35445", "act_power_2": "39052"}},
    "softcps": {"actuator_mapping": {"power_actuation_grid": "ASSET_3"}},
}}


def _decision(entity_id, **actuators):
    return {"id": entity_id, "entity_type_id": "battery", "timestamp": "t",
            "actuators": {name: {"actuator_type_id": "power_actuation", "value": value}
                          for name, value in actuators.items()}}


def _resolver(environment=None):
    return TargetSystemResolver("R-H-01", ENVIRONMENT if environment is None else environment, MagicMock())


def test_actuator_goes_to_its_target_system_with_its_destination():
    result = _resolver().resolve([_decision("B01", act_power_1=-2.5)])
    assert result == {"cleanwatts": [{
        "id": "B01", "entity_type_id": "battery", "timestamp": "t",
        "actuators": {"act_power_1": {"actuator_type_id": "power_actuation", "value": -2.5,
                                              "destination": "35445"}}}]}


def test_decision_with_actuators_of_two_systems_is_split():
    result = _resolver().resolve([_decision("B01", act_power_1=1, power_actuation_grid=2)])
    assert list(result["cleanwatts"][0]["actuators"]) == ["act_power_1"]
    assert result["softcps"][0]["actuators"]["power_actuation_grid"]["destination"] == "ASSET_3"
    assert result["softcps"][0]["id"] == "B01"


def test_several_decisions_of_the_same_system_are_grouped():
    result = _resolver().resolve([_decision("B01", act_power_1=1), _decision("EV01", act_power_2=2)])
    assert [d["id"] for d in result["cleanwatts"]] == ["B01", "EV01"]


def test_unmapped_actuator_is_left_out_with_a_warning():
    logger = MagicMock()
    resolver = TargetSystemResolver("R-H-01", ENVIRONMENT, logger)
    result = resolver.resolve([_decision("B01", act_power_1=1, other=5)])
    assert list(result["cleanwatts"][0]["actuators"]) == ["act_power_1"]
    logger.warning.assert_called_once()


def test_decision_with_no_mapped_actuator_is_not_returned():
    assert _resolver().resolve([_decision("B01", other=5)]) == {}


@pytest.mark.parametrize("decision", [None, "text", {"id": "B01"}, {"id": "B01", "actuators": []}])
def test_invalid_decision_is_ignored(decision):
    assert _resolver().resolve([decision]) == {}


def test_input_is_not_changed():
    decisions = [_decision("B01", act_power_1=1)]
    _resolver().resolve(decisions)
    assert "destination" not in decisions[0]["actuators"]["act_power_1"]


def test_environment_without_target_systems_resolves_nothing():
    assert _resolver({}).resolve([_decision("B01", act_power_1=1)]) == {}


def test_actuator_mapped_by_two_systems_is_refused():
    environment = {"target_systems": {"a": {"actuator_mapping": {"x": "1"}}, "b": {"actuator_mapping": {"x": "2"}}}}
    with pytest.raises(ValueError, match="'a' and 'b'"):
        _resolver(environment)


def test_dependencies_are_validated():
    with pytest.raises(ValueError):
        TargetSystemResolver("R-H-01", {}, None)
    with pytest.raises(ValueError):
        TargetSystemResolver("R-H-01", [], MagicMock())
