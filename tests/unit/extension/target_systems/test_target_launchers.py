import copy
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.extension.target_systems.cleanwatts.cw_target_adapter import CWTargetAdapter
from app.extension.target_systems.cleanwatts.cw_target_launcher import CWTargetLauncher
from app.extension.target_systems.default_actuation_error_handling_logic import DefaultActuationErrorHandlingLogic
from app.extension.target_systems.softcps.softcps_target_adapter import SoftCPSTargetAdapter
from app.extension.target_systems.softcps.softcps_target_launcher import SoftCPSTargetLauncher

ACTUATION = {"path": "/actuations", "method": "POST", "rpc_properties": {"enabled": True}}
LOGIN = {"path": "/sessions", "method": "POST", "rpc_properties": {"enabled": True}}
CW = {"connection_configuration": {"protocol_id": "AsyncHTTP", "connection_info": {
    "timezone": "UTC", "refresh_interval_seconds": 3300,
    "credentials": {"username": "u", "password": "p"}, "connection": {"url": "https://x"},
    "authentication": {"topology": {"endpoints": [LOGIN]}}, "topology": {"endpoints": [ACTUATION]}}}}
SOFT = {"connection_configuration": {"protocol_id": "AsyncHTTP", "connection_info": {
    "connection": {"url": "http://y"},
    "topology": {"endpoints": [{"path": "/assets/{asset_id}/actions", "method": "POST",
                                "rpc_properties": {"enabled": True}}]}}}}


def _output_launcher():
    launcher = MagicMock()
    launcher.build_output_handler = AsyncMock(
        side_effect=lambda **kw: (MagicMock(spec=OutputHandlerBase, stop=AsyncMock()), None))
    return launcher


async def _refresh_job():
    return None


def _cw_launcher():
    output_launcher = _output_launcher()
    launcher = CWTargetLauncher(output_launcher, MagicMock())
    launcher._session = MagicMock(is_token_valid=MagicMock(return_value=False), login=AsyncMock(),
                                  initialize=MagicMock(), run_refresh_job=_refresh_job)
    return launcher, output_launcher


def _change(configuration, path, value):
    configuration = copy.deepcopy(configuration)
    target = configuration
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return configuration


@pytest.mark.asyncio
async def test_cleanwatts_components_and_login():
    launcher, output_launcher = _cw_launcher()
    components = await launcher.launch("cleanwatts", CW, "R-H-01", {}, MagicMock())
    assert isinstance(components.adapter, CWTargetAdapter)
    assert isinstance(components.error_handling_logic, DefaultActuationErrorHandlingLogic)
    launcher._session.initialize.assert_called_once()
    launcher._session.login.assert_awaited_once()
    auth_config = launcher._session.initialize.call_args.kwargs["auth_config"]
    assert auth_config["connection"]["credentials"] == {"username": "u", "password": "p"}
    assert auth_config["topology"] == {"endpoints": [LOGIN]}
    kwargs = output_launcher.build_output_handler.call_args.kwargs
    assert kwargs["topology_setup"] == {"endpoints": [ACTUATION]}
    await launcher.stop()


@pytest.mark.asyncio
async def test_cleanwatts_logs_in_only_once_for_several_environments():
    launcher, _ = _cw_launcher()
    launcher._session.is_token_valid.side_effect = [False, True]
    await launcher.launch("cleanwatts", CW, "R-H-01", {}, MagicMock())
    await launcher.launch("cleanwatts", CW, "R-H-02", {}, MagicMock())
    launcher._session.login.assert_awaited_once()
    await launcher.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("path,value", [
    (["connection_configuration", "protocol_id"], "AsyncRabbitMQ"),
    (["connection_configuration", "connection_info"], None),
    (["connection_configuration", "connection_info", "credentials"], None),
    (["connection_configuration", "connection_info", "connection"], None),
    (["connection_configuration", "connection_info", "authentication"], {}),
    (["connection_configuration", "connection_info", "topology"], {"endpoints": []}),
    (["connection_configuration", "connection_info", "topology"], {"endpoints": [{"path": "/a"}]}),
])
async def test_cleanwatts_invalid_configuration(path, value):
    launcher, _ = _cw_launcher()
    with pytest.raises(ValueError):
        await launcher.launch("cleanwatts", _change(CW, path, value), "R-H-01", {}, MagicMock())


@pytest.mark.asyncio
async def test_softcps_components():
    launcher = SoftCPSTargetLauncher(_output_launcher(), MagicMock())
    components = await launcher.launch("softcps", SOFT, "R-H-01", {}, MagicMock())
    assert isinstance(components.adapter, SoftCPSTargetAdapter)
    assert isinstance(components.error_handling_logic, DefaultActuationErrorHandlingLogic)


@pytest.mark.asyncio
@pytest.mark.parametrize("path,value", [
    (["connection_configuration", "protocol_id"], "AsyncRabbitMQ"),
    (["connection_configuration", "connection_info"], None),
    (["connection_configuration", "connection_info", "connection"], None),
    (["connection_configuration", "connection_info", "topology"], {"endpoints": []}),
    (["connection_configuration", "connection_info", "topology"],
     {"endpoints": [{"path": "/assets/{asset_id}/actions"}]}),
])
async def test_softcps_invalid_configuration(path, value):
    launcher = SoftCPSTargetLauncher(_output_launcher(), MagicMock())
    with pytest.raises(ValueError):
        await launcher.launch("softcps", _change(SOFT, path, value), "R-H-01", {}, MagicMock())


@pytest.mark.asyncio
async def test_logic_named_in_the_entry_replaces_the_default():
    entry = {**SOFT, "actuation_error_handling_logic": {
        "path": "app.extension.target_systems.default_actuation_error_handling_logic.DefaultActuationErrorHandlingLogic"}}
    launcher = SoftCPSTargetLauncher(_output_launcher(), MagicMock())
    components = await launcher.launch("softcps", entry, "R-H-01", {}, MagicMock())
    assert isinstance(components.error_handling_logic, DefaultActuationErrorHandlingLogic)
    with pytest.raises(ValueError):
        await launcher.launch("softcps", {**SOFT, "actuation_error_handling_logic": {"path": "os.path.join"}},
                              "R-H-01", {}, MagicMock())
