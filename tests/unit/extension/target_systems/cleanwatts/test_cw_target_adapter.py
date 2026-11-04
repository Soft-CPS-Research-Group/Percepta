from unittest.mock import MagicMock

import pytest

from app.core.output_handlers.async_http_output_handler import HttpPublishMode
from app.extension.target_systems.cleanwatts.cw_target_adapter import CWTargetAdapter

ENDPOINT = {"path": "/actuations", "method": "POST", "headers": {"Content-Type": "application/json"}}


def _adapter(token="tok"):
    session = MagicMock()
    session.get_token.return_value = token
    return CWTargetAdapter("cleanwatts", "R-H-01", {}, MagicMock(), session, ENDPOINT), session


def _decision(entity_type, value, destination="35447", entity_id="E1", **device):
    return {"id": entity_id, "entity_type_id": entity_type, "timestamp": "t", "actuators": {
        "act_power_1": {"actuator_type_id": "power", "value": value, "destination": destination, **device}}}


def test_battery_sends_kw_and_negative_discharges():
    adapter, _ = _adapter()
    [mapped] = adapter.adapt_decisions([_decision("battery", -2.5)])
    assert mapped.message == {"Action": 2, "Value": "-2.5", "ValueFormat": 0, "TagIds": [35447]}


def test_ev_charger_is_sent_in_amperes():
    adapter, _ = _adapter()
    [mapped] = adapter.adapt_decisions([_decision("ev_charger", 4.6, "39052", measurement_unit="A", voltage=230)])
    assert float(mapped.message["Value"]) == pytest.approx(20.0)
    assert isinstance(mapped.message["Value"], str)
    assert mapped.message["TagIds"] == [39052]


def test_request_carries_the_current_token_in_a_copy_of_the_endpoint():
    adapter, session = _adapter("t1")
    [mapped] = adapter.adapt_decisions([_decision("battery", 1)])
    endpoint = mapped.send_arguments["dynamic_endpoints"][0]
    assert endpoint["headers"] == {"Content-Type": "application/json", "Authorization": "CW t1"}
    assert mapped.send_arguments["mode"] == HttpPublishMode.OVERRIDE
    assert "Authorization" not in ENDPOINT["headers"]
    session.get_token.return_value = "t2"
    [mapped] = adapter.adapt_decisions([_decision("battery", 1)])
    assert mapped.send_arguments["dynamic_endpoints"][0]["headers"]["Authorization"] == "CW t2"


def test_one_request_for_each_actuator():
    adapter, _ = _adapter()
    decisions = [_decision("battery", 1, entity_id="B1"), _decision("ev_charger", 2, "7", entity_id="EV1", measurement_unit="A", voltage=230)]
    assert len(adapter.adapt_decisions(decisions)) == 2


@pytest.mark.parametrize("decision", [
    _decision("battery", 1, measurement_unit="W"), _decision("battery", 1, measurement_unit="A"),
    _decision("battery", 1, measurement_unit="A", voltage=0), _decision("battery", "1"), _decision("battery", True),
    _decision("battery", 1, "not a number"), _decision("battery", 1, None)])
def test_invalid_actuator_is_left_out_with_a_warning(decision):
    adapter, _ = _adapter()
    assert adapter.adapt_decisions([decision, _decision("battery", 3)])[0].message["Value"] == "3.0"
    assert len(adapter.adapt_decisions([decision, _decision("battery", 3)])) == 1
    adapter._logger.warning.assert_called()


def test_without_token_nothing_is_sent():
    adapter, _ = _adapter(token=None)
    with pytest.raises(RuntimeError):
        adapter.adapt_decisions([_decision("battery", 1)])


def test_dependencies_are_validated():
    with pytest.raises(ValueError):
        CWTargetAdapter("c", "R", {}, MagicMock(), None, ENDPOINT)
    with pytest.raises(ValueError):
        CWTargetAdapter("c", "R", {}, MagicMock(), MagicMock(), {})


def test_kw_unit_is_sent_as_is():
    adapter, _ = _adapter()
    [mapped] = adapter.adapt_decisions([_decision("battery", 2, measurement_unit="kW")])
    assert mapped.message["Value"] == "2.0"


def test_check_device():
    CWTargetAdapter.check_device(None, None)
    CWTargetAdapter.check_device("A", 230)
    for unit, voltage in [("W", None), ("A", None), ("A", -1), ("A", True)]:
        with pytest.raises(ValueError):
            CWTargetAdapter.check_device(unit, voltage)
