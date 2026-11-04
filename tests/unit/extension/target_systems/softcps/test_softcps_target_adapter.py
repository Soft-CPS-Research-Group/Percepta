from unittest.mock import MagicMock

import pytest

from app.core.output_handlers.async_http_output_handler import HttpPublishMode
from app.extension.target_systems.softcps.softcps_target_adapter import SoftCPSTargetAdapter

ENDPOINT = {"path": "/assets/{asset_id}/actions", "method": "POST", "timeout_ms": 5000}


def _adapter():
    return SoftCPSTargetAdapter("softcps", "R-H-01", {}, MagicMock(), ENDPOINT)


def _decision(value, destination="ASSET_1", entity_type="battery"):
    return {"id": "B01", "entity_type_id": entity_type, "timestamp": "t", "actuators": {
        "act": {"actuator_type_id": "power_actuation", "value": value, "destination": destination}}}


def test_request_is_addressed_to_the_asset_of_the_actuator():
    [mapped] = _adapter().adapt_decisions([_decision(-2.5)])
    assert mapped.message == {"power_kw": -2.5}
    assert mapped.send_arguments["dynamic_endpoints"] == [{**ENDPOINT, "path": "/assets/ASSET_1/actions"}]
    assert mapped.send_arguments["mode"] == HttpPublishMode.OVERRIDE
    assert ENDPOINT["path"] == "/assets/{asset_id}/actions"


def test_each_actuator_goes_to_its_own_asset():
    mapped = _adapter().adapt_decisions([_decision(1, "A"), _decision(2, "B")])
    assert [m.send_arguments["dynamic_endpoints"][0]["path"] for m in mapped] == ["/assets/A/actions", "/assets/B/actions"]


@pytest.mark.parametrize("decision", [
    _decision(1, entity_type="ev_charger"), _decision("1"), _decision(True), _decision(1, ""), _decision(1, None)])
def test_invalid_actuator_is_left_out_with_a_warning(decision):
    adapter = _adapter()
    assert len(adapter.adapt_decisions([decision, _decision(3)])) == 1
    adapter._logger.warning.assert_called()


@pytest.mark.parametrize("endpoint", [{}, {"path": "/actions"}, None])
def test_endpoint_needs_the_asset_placeholder(endpoint):
    with pytest.raises(ValueError):
        SoftCPSTargetAdapter("softcps", "R", {}, MagicMock(), endpoint)
