import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.adapters.ml_model_adapter_base import MLModelAdapterBase
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.integrated_logic.model_execution_error_handling_logic_base import ModelExecutionErrorHandlingLogicBase
from app.core.launchers.containers_launchers.inference_launcher import InferenceLauncher
from app.core.launchers.external_systems_launchers.ml_model_launcher_base import (
    MLModelComponents,
    MLModelLauncherBase
)
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.storages.runtime_data_storage.runtime_data_storage_base import RuntimeDataStorageBase

_MODULE = "tests.unit.core.launchers.containers_launchers.test_inference_launcher"


def _output():
    return MagicMock(spec=OutputHandlerBase, stop=AsyncMock())


class FakeMLLauncher(MLModelLauncherBase):
    async def launch(self, ml_model_id, ml_model_configuration, environment_name, environment, logger):
        return MLModelComponents(
            adapter=MagicMock(spec=MLModelAdapterBase), output_handler=_output(),
            error_handling_logic=MagicMock(spec=ModelExecutionErrorHandlingLogicBase), validation_logic=None
        )


LOCAL = {"protocol_id": "AsyncLocal", "connection_info": {}}
ENVIRONMENT = {"group": "g", "entities": {}, "ml_model_id": "m1"}


def _config(ml_models=None, input_link=LOCAL, output_link=LOCAL, storage=None):
    config = MagicMock()
    config.ml_models = ml_models if ml_models is not None else {
        "m1": {"path": f"{_MODULE}.FakeMLLauncher", "connection_configuration": {}}}
    config.data_processing_to_inference = input_link
    config.inference_to_decision_dispatching = output_link
    config.runtime_data_storage = storage
    return config


def _launcher(config=None):
    input_launcher = MagicMock()
    input_launcher.build_input_handler = AsyncMock(side_effect=lambda **kw: _local_handler(kw))
    output_launcher = MagicMock()
    output_launcher.build_output_handler = AsyncMock(return_value=(_output(), None))
    storage_launcher = MagicMock()
    storage = MagicMock(spec=RuntimeDataStorageBase, stop=AsyncMock())
    storage_launcher.build_runtime_data_storage = MagicMock(return_value=storage)
    launcher = InferenceLauncher(config or _config(), input_launcher, output_launcher, storage_launcher, MagicMock())
    return launcher, input_launcher, output_launcher, storage_launcher


def _local_handler(kw):
    handler = MagicMock(spec=AsyncLocalInputHandler)
    handler.start = AsyncMock()
    handler.stop = AsyncMock()
    handler.callback = kw["callback"]
    return handler


def test_init_requires_dependencies():
    with pytest.raises(ValueError):
        InferenceLauncher(None, MagicMock(), MagicMock(), MagicMock(), MagicMock())


def test_init_requires_ml_models():
    with pytest.raises(ValueError, match="no ML Model"):
        _launcher(_config(ml_models={}))


def test_init_requires_links():
    with pytest.raises(ValueError, match="Data Processing"):
        _launcher(_config(input_link=None))
    with pytest.raises(ValueError, match="Decision Dispatching"):
        _launcher(_config(output_link=None))


@pytest.mark.asyncio
async def test_launch_without_validation_logic_and_storage():
    launcher, input_launcher, _, storage_launcher = _launcher()
    handler, receiver = await launcher.launch_environment("R-H-01", ENVIRONMENT, next_container_receiver=AsyncMock())
    assert receiver is not None
    handler.start.assert_awaited_once()
    storage_launcher.build_runtime_data_storage.assert_not_called()


@pytest.mark.asyncio
async def test_launch_gives_the_whole_model_entry_to_the_model_launcher():
    entry = {"path": f"{_MODULE}.FakeMLLauncher", "connection_configuration": {"x": 1},
             "decision_validation_correction_logic": {"path": "a.B"}}
    launcher, *_ = _launcher(_config(ml_models={"m1": entry}))
    model_launcher = launcher._ml_models["m1"]
    model_launcher.launch = AsyncMock(wraps=model_launcher.launch)
    await launcher.launch_environment("R-H-01", ENVIRONMENT, next_container_receiver=AsyncMock())
    assert model_launcher.launch.call_args.args[:3] == ("m1", entry, "R-H-01")


@pytest.mark.asyncio
async def test_launch_with_storage_builds_it_with_group():
    storage = {"runtime_data_storage_type_id": "mongodb", "connection_info": {"host": "h"}}
    launcher, _, _, storage_launcher = _launcher(_config(storage=storage))
    await launcher.launch_environment("R-H-01", ENVIRONMENT, next_container_receiver=AsyncMock())
    args = storage_launcher.build_runtime_data_storage.call_args.args
    assert args[:4] == ("mongodb", {"host": "h"}, "g", "R-H-01")


@pytest.mark.asyncio
async def test_launch_storage_requires_group():
    storage = {"runtime_data_storage_type_id": "mongodb", "connection_info": {}}
    launcher, *_ = _launcher(_config(storage=storage))
    with pytest.raises(ValueError, match="group"):
        await launcher.launch_environment("R-H-01", {"entities": {}, "ml_model_id": "m1"}, AsyncMock())


@pytest.mark.asyncio
async def test_launch_unknown_model():
    launcher, *_ = _launcher()
    with pytest.raises(ValueError, match="ml_model_id"):
        await launcher.launch_environment("R-H-01", {**ENVIRONMENT, "ml_model_id": "x"}, AsyncMock())


@pytest.mark.asyncio
async def test_launch_local_output_requires_receiver():
    launcher, *_ = _launcher()
    with pytest.raises(ValueError, match="next_container_receiver"):
        await launcher.launch_environment("R-H-01", ENVIRONMENT)


@pytest.mark.asyncio
async def test_launch_invalid_environment():
    launcher, *_ = _launcher()
    with pytest.raises(ValueError):
        await launcher.launch_environment("", ENVIRONMENT, AsyncMock())
    with pytest.raises(ValueError):
        await launcher.launch_environment("R-H-01", {"ml_model_id": "m1"}, AsyncMock())


@pytest.mark.asyncio
async def test_receiver_gives_message_to_input_handler():
    launcher, *_ = _launcher()
    handler, receiver = await launcher.launch_environment("R-H-01", ENVIRONMENT, AsyncMock())
    await receiver({"a": 1})
    handler.on_message.assert_called_once_with({"a": 1})


@pytest.mark.asyncio
async def test_launch_failure_stops_what_was_started():
    launcher, input_launcher, output_launcher, _ = _launcher()
    handler = _local_handler({"callback": None})
    handler.start = AsyncMock(side_effect=RuntimeError("boom"))
    input_launcher.build_input_handler = AsyncMock(return_value=handler)
    with pytest.raises(RuntimeError):
        await launcher.launch_environment("R-H-01", ENVIRONMENT, AsyncMock())
    handler.stop.assert_awaited()
    assert launcher._input_handlers == [] and launcher._output_handlers == []


@pytest.mark.asyncio
async def test_remote_links_use_topology_with_environment():
    link = {"protocol_id": "AsyncRabbitMQ", "connection_info": {
        "connection": {"host": "h"},
        "topology": {"exchanges": [{"exchange_name": "x.{environment}", "queues": [{"queue_name": "q.{environment}"}]}]}}}
    launcher, input_launcher, output_launcher, _ = _launcher(_config(input_link=link, output_link=link))
    handler, receiver = await launcher.launch_environment("R-H-01", ENVIRONMENT)
    assert receiver is None
    kwargs = input_launcher.build_input_handler.call_args.kwargs
    assert kwargs["topology_setup"]["exchanges"][0]["exchange_name"] == "x.R-H-01"
    assert kwargs["topology_setup"]["exchanges"][0]["queues"][0]["queue_name"] == "q.R-H-01"
    # The configuration is not changed
    assert link["connection_info"]["topology"]["exchanges"][0]["exchange_name"] == "x.{environment}"


@pytest.mark.asyncio
async def test_remote_output_refuses_rpc():
    link = {"protocol_id": "AsyncRabbitMQ", "connection_info": {
        "connection": {}, "topology": {"exchanges": [{"exchange_name": "x", "rpc_properties": {"enabled": True}}]}}}
    launcher, *_ = _launcher(_config(input_link=LOCAL, output_link=link))
    with pytest.raises(ValueError, match="RPC"):
        await launcher.launch_environment("R-H-01", ENVIRONMENT)


@pytest.mark.asyncio
async def test_stop_stops_everything_and_tolerates_failures():
    launcher, *_ = _launcher()
    handler, _ = await launcher.launch_environment("R-H-01", ENVIRONMENT, AsyncMock())
    handler.stop = AsyncMock(side_effect=RuntimeError("x"))
    await launcher.stop()
    assert launcher._input_handlers == [] and launcher._output_handlers == []
