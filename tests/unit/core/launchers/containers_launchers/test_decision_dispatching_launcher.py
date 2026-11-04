from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.adapters.target_system_adapter_base import TargetSystemAdapterBase
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.integrated_logic.actuation_error_handling_logic_base import ActuationErrorHandlingLogicBase
from app.core.launchers.containers_launchers.decision_dispatching_launcher import DecisionDispatchingLauncher
from app.core.launchers.external_systems_launchers.target_system_launcher_base import (
    TargetSystemComponents,
    TargetSystemLauncherBase
)
from app.core.output_handlers.output_handler_base import OutputHandlerBase

_MODULE = "tests.unit.core.launchers.containers_launchers.test_decision_dispatching_launcher"


def _output():
    return MagicMock(spec=OutputHandlerBase, stop=AsyncMock())


class FakeTargetLauncher(TargetSystemLauncherBase):
    async def launch(self, target_system_id, target_system_configuration, environment_name, environment, logger):
        return TargetSystemComponents(
            adapter=MagicMock(spec=TargetSystemAdapterBase), output_handler=_output(),
            error_handling_logic=MagicMock(spec=ActuationErrorHandlingLogicBase)
        )


class NotALauncher:
    pass


LOCAL = {"protocol_id": "AsyncLocal", "connection_info": {}}
ENVIRONMENT = {"entities": {}, "target_systems": {
    "a": {"actuator_mapping": {"act_a": "A1"}}, "b": {"actuator_mapping": {"act_b": "B1"}}}}


def _config(target_systems=None, input_link=LOCAL):
    config = MagicMock()
    config.target_systems = target_systems if target_systems is not None else {
        "a": {"path": f"{_MODULE}.FakeTargetLauncher", "connection_configuration": {}},
        "b": {"path": f"{_MODULE}.FakeTargetLauncher", "connection_configuration": {}}}
    config.inference_to_decision_dispatching = input_link
    return config


def _local_handler(kw):
    handler = MagicMock(spec=AsyncLocalInputHandler)
    handler.start = AsyncMock()
    handler.stop = AsyncMock()
    return handler


def _launcher(config=None):
    input_launcher = MagicMock()
    input_launcher.build_input_handler = AsyncMock(side_effect=lambda **kw: _local_handler(kw))
    launcher = DecisionDispatchingLauncher(config or _config(), input_launcher, MagicMock(), MagicMock())
    return launcher, input_launcher


def test_init_requires_dependencies():
    with pytest.raises(ValueError):
        DecisionDispatchingLauncher(None, MagicMock(), MagicMock(), MagicMock())


def test_init_requires_target_systems_and_link():
    with pytest.raises(ValueError, match="no Target System"):
        _launcher(_config(target_systems={}))
    with pytest.raises(ValueError, match="Inference"):
        _launcher(_config(input_link=None))


def test_init_rejects_wrong_launcher_class():
    systems = {"a": {"path": f"{_MODULE}.NotALauncher", "connection_configuration": {}}}
    with pytest.raises(ValueError, match="must extend"):
        _launcher(_config(target_systems=systems))


@pytest.mark.asyncio
async def test_launch_gives_each_system_its_whole_entry_and_starts_the_handler():
    config = _config()
    launcher, _ = _launcher(config)
    for target_launcher in launcher._target_systems.values():
        target_launcher.launch = AsyncMock(wraps=target_launcher.launch)
    handler, receiver = await launcher.launch_environment("R-H-01", ENVIRONMENT)
    handler.start.assert_awaited_once()
    assert receiver is not None
    call = launcher._target_systems["a"].launch.call_args.args
    assert call[:3] == ("a", config.target_systems["a"], "R-H-01")


@pytest.mark.asyncio
async def test_only_the_target_systems_of_the_environment_are_launched():
    launcher, _ = _launcher()
    for target_launcher in launcher._target_systems.values():
        target_launcher.launch = AsyncMock(wraps=target_launcher.launch)
    environment = {"entities": {}, "target_systems": {"a": {"actuator_mapping": {}}}}
    await launcher.launch_environment("R-H-01", environment)
    launcher._target_systems["a"].launch.assert_awaited_once()
    launcher._target_systems["b"].launch.assert_not_awaited()


@pytest.mark.asyncio
async def test_receiver_gives_decisions_to_the_input_handler():
    launcher, _ = _launcher()
    handler, receiver = await launcher.launch_environment("R-H-01", ENVIRONMENT)
    await receiver([{"id": "B01"}])
    handler.on_message.assert_called_once_with([{"id": "B01"}])


@pytest.mark.asyncio
@pytest.mark.parametrize("environment", [
    {"entities": {}}, {"entities": {}, "target_systems": {}}, {"target_systems": {"a": {}}},
    {"entities": {}, "target_systems": {"unknown": {"actuator_mapping": {}}}}])
async def test_invalid_environment(environment):
    launcher, _ = _launcher()
    with pytest.raises(ValueError):
        await launcher.launch_environment("R-H-01", environment)


@pytest.mark.asyncio
async def test_invalid_name():
    launcher, _ = _launcher()
    with pytest.raises(ValueError):
        await launcher.launch_environment(" ", ENVIRONMENT)


@pytest.mark.asyncio
async def test_launch_failure_stops_what_was_started():
    launcher, input_launcher = _launcher()
    handler = _local_handler({})
    handler.start = AsyncMock(side_effect=RuntimeError("boom"))
    input_launcher.build_input_handler = AsyncMock(return_value=handler)
    with pytest.raises(RuntimeError):
        await launcher.launch_environment("R-H-01", ENVIRONMENT)
    handler.stop.assert_awaited()
    assert launcher._input_handlers == [] and launcher._output_handlers == []


@pytest.mark.asyncio
async def test_remote_link_uses_topology_with_environment():
    link = {"protocol_id": "AsyncRabbitMQ", "connection_info": {
        "connection": {"host": "h"},
        "topology": {"exchanges": [{"exchange_name": "x.{environment}", "queues": [{"queue_name": "q.{environment}"}]}]}}}
    launcher, input_launcher = _launcher(_config(input_link=link))
    _, receiver = await launcher.launch_environment("R-H-01", ENVIRONMENT)
    assert receiver is None
    topology = input_launcher.build_input_handler.call_args.kwargs["topology_setup"]
    assert topology["exchanges"][0]["exchange_name"] == "x.R-H-01"
    assert topology["exchanges"][0]["queues"][0]["queue_name"] == "q.R-H-01"
    assert link["connection_info"]["topology"]["exchanges"][0]["exchange_name"] == "x.{environment}"


@pytest.mark.asyncio
async def test_stop_stops_everything_including_the_target_system_launchers():
    launcher, _ = _launcher()
    handler, _ = await launcher.launch_environment("R-H-01", ENVIRONMENT)
    handler.stop = AsyncMock(side_effect=RuntimeError("x"))
    for target_launcher in launcher._target_systems.values():
        target_launcher.stop = AsyncMock()
    await launcher.stop()
    assert launcher._input_handlers == [] and launcher._output_handlers == []
    for target_launcher in launcher._target_systems.values():
        target_launcher.stop.assert_awaited_once()
