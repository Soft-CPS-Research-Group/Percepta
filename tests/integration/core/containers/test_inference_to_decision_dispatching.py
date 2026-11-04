"""End-to-end test of the Inference and Decision Dispatching containers.

A fake EnergAIze model and a fake SoftCPS run as real HTTP servers on local ports. The two containers are launched with
the real launchers, the Percepta Instance Configuration of the project (only the addresses of the two systems are
changed to the local ports) and a message is given to Inference as Data Processing would give it. The test checks the
request that SoftCPS receives.
"""
import asyncio
import copy

import pytest
from aiohttp import web

from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.launchers.application_launchers.single_process_application_launcher import (
    SingleProcessApplicationLauncher
)
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher

CONFIGURATION_PATH = "configs/percepta_instance_configuration.json"

ENVIRONMENT = {
    "group": "living lab",
    "ml_model_id": "energaize_r_h_01",
    "entities": {
        "B01": {"entity_type_id": "battery", "actuators": {"act_power_1": {"actuator_type_id": "power"}}},
    },
    "target_systems": {"softcps": {"actuator_mapping": {"act_power_1": "ASSET_1"}}},
}
MESSAGE = {"timestamp": "2026-08-19 11:15:00 +0000", "entities": []}


async def _start(routes):
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"


async def _launch(model_url, softcps_url):
    launcher = SingleProcessApplicationLauncher(CONFIGURATION_PATH)

    # Only the addresses change, and no Runtime Data Storage is used (there is no database in the test)
    model = launcher._inference_launcher._ml_model_configurations["energaize_r_h_01"]
    launcher._inference_launcher._ml_model_configurations["energaize_r_h_01"] = copy.deepcopy(model)
    launcher._inference_launcher._ml_model_configurations["energaize_r_h_01"][
        "connection_configuration"]["connection_info"]["connection"]["url"] = model_url
    launcher._inference_launcher._runtime_data_storage_config = None

    softcps = copy.deepcopy(launcher._decision_dispatching_launcher._target_system_configurations["softcps"])
    softcps["connection_configuration"]["connection_info"]["connection"]["url"] = softcps_url
    launcher._decision_dispatching_launcher._target_system_configurations["softcps"] = softcps

    _, dispatching_receiver = await launcher._decision_dispatching_launcher.launch_environment("R-H-01", ENVIRONMENT)
    _, inference_receiver = await launcher._inference_launcher.launch_environment(
        "R-H-01", ENVIRONMENT, dispatching_receiver
    )
    return launcher, inference_receiver


async def _wait_for(condition, seconds=5.0):
    for _ in range(int(seconds / 0.05)):
        if condition():
            return True
        await asyncio.sleep(0.05)
    return False


@pytest.mark.asyncio
async def test_message_becomes_an_actuation_in_the_target_system():
    model_calls, softcps_calls = [], []

    async def model(request):
        model_calls.append(await request.json())
        return web.json_response({"B01": -2.5})

    async def softcps(request):
        softcps_calls.append((request.match_info["asset_id"], await request.json()))
        return web.json_response({"ok": True})

    model_runner, model_url = await _start([web.post("/inference", model)])
    softcps_runner, softcps_url = await _start([web.post("/assets/{asset_id}/actions", softcps)])
    launcher = None
    try:
        launcher, inference_receiver = await _launch(model_url, softcps_url)
        await inference_receiver(MESSAGE)

        assert await _wait_for(lambda: softcps_calls), "SoftCPS did not receive the actuation"
        assert model_calls
        assert softcps_calls == [("ASSET_1", {"power_kw": -2.5})]
    finally:
        if launcher:
            await launcher._inference_launcher.stop()
            await launcher._decision_dispatching_launcher.stop()
        await model_runner.cleanup()
        await softcps_runner.cleanup()


@pytest.mark.asyncio
async def test_target_system_failure_is_retried_and_then_given_up():
    softcps_calls = []

    async def model(request):
        return web.json_response({"B01": 1.0})

    async def softcps(request):
        softcps_calls.append(await request.json())
        return web.Response(status=500, text="down")

    model_runner, model_url = await _start([web.post("/inference", model)])
    softcps_runner, softcps_url = await _start([web.post("/assets/{asset_id}/actions", softcps)])
    launcher = None
    try:
        launcher, inference_receiver = await _launch(model_url, softcps_url)
        await inference_receiver(MESSAGE)

        # Three attempts, one second apart (the default Actuation Error Handling Logic)
        assert await _wait_for(lambda: len(softcps_calls) >= 3, seconds=10.0)
        await asyncio.sleep(1.5)
        assert len(softcps_calls) == 3
    finally:
        if launcher:
            await launcher._inference_launcher.stop()
            await launcher._decision_dispatching_launcher.stop()
        await model_runner.cleanup()
        await softcps_runner.cleanup()
