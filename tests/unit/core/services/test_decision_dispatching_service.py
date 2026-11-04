from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.adapters.target_system_adapter_base import MappedDecision, TargetSystemAdapterBase
from app.core.integrated_logic.actuation_error_handling_logic_base import ActuationErrorHandlingLogicBase
from app.core.launchers.external_systems_launchers.target_system_launcher_base import TargetSystemComponents
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.services.decision_dispatching_service import DecisionDispatchingService
from app.core.services.target_system_resolver import TargetSystemResolver

ENVIRONMENT = {"target_systems": {
    "a": {"actuator_mapping": {"act_a": "A1"}},
    "b": {"actuator_mapping": {"act_b": "B1"}},
}}
DECISION = {"id": "E1", "entity_type_id": "battery", "timestamp": "t1", "actuators": {
    "act_a": {"actuator_type_id": "p", "value": 1}, "act_b": {"actuator_type_id": "p", "value": 2}}}
OK = {"/x": {"status": "success", "code": 200}}
ERROR = {"/x": {"status": "error", "error": "boom"}}


def _components(send=None, retry=False, mapped=None):
    adapter = MagicMock(spec=TargetSystemAdapterBase)
    adapter.adapt_decisions = MagicMock(return_value=mapped if mapped is not None else [MappedDecision("m")])
    handler = MagicMock(spec=OutputHandlerBase)
    handler.send = send or AsyncMock(return_value=OK)
    logic = MagicMock(spec=ActuationErrorHandlingLogicBase)
    logic.should_retry = AsyncMock(side_effect=retry) if isinstance(retry, list) else AsyncMock(return_value=retry)
    return TargetSystemComponents(adapter=adapter, output_handler=handler, error_handling_logic=logic)


def _service(a=None, b=None):
    logger = MagicMock()
    systems = {"a": a or _components(), "b": b or _components()}
    return DecisionDispatchingService(TargetSystemResolver("R", ENVIRONMENT, logger), systems, logger), systems, logger


def _envelope(payload=None):
    return {"payload": [DECISION] if payload is None else payload}


@pytest.mark.asyncio
async def test_each_system_gets_only_its_actuators():
    service, systems, _ = _service()
    await service.receive_message(_envelope())
    for system_id, actuator in (("a", "act_a"), ("b", "act_b")):
        decisions = systems[system_id].adapter.adapt_decisions.call_args.args[0]
        assert list(decisions[0]["actuators"]) == [actuator]
        systems[system_id].output_handler.send.assert_awaited_once_with("m")


@pytest.mark.asyncio
async def test_send_arguments_are_given_to_the_output_handler():
    a = _components(mapped=[MappedDecision("m", {"mode": "override"})])
    service, _, _ = _service(a=a)
    await service.receive_message(_envelope())
    a.output_handler.send.assert_awaited_once_with("m", mode="override")


@pytest.mark.asyncio
async def test_json_payload_is_accepted():
    service, systems, _ = _service()
    await service.receive_message({"payload": '[{"id": "E1", "actuators": {"act_a": {"value": 1}}}]'})
    systems["a"].output_handler.send.assert_awaited_once()
    systems["b"].output_handler.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("envelope", [None, {}, {"payload": "not json"}, {"payload": {"a": 1}}, {"payload": 3}])
async def test_unreadable_message_is_skipped(envelope):
    service, systems, logger = _service()
    await service.receive_message(envelope)
    logger.warning.assert_called()
    systems["a"].output_handler.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_error_response_is_retried_while_the_logic_says_so():
    send = AsyncMock(side_effect=[ERROR, ERROR, OK])
    a = _components(send=send, retry=[True, True])
    service, _, _ = _service(a=a)
    await service.receive_message(_envelope())
    assert send.await_count == 3
    assert a.error_handling_logic.should_retry.await_args_list[0].args == ("boom", "t1 | a | 0")


@pytest.mark.asyncio
async def test_exception_gives_up_when_the_logic_says_so_and_does_not_affect_the_other_system():
    a = _components(send=AsyncMock(side_effect=RuntimeError("down")), retry=False)
    service, systems, logger = _service(a=a)
    await service.receive_message(_envelope())
    a.error_handling_logic.should_retry.assert_awaited_once()
    systems["b"].output_handler.send.assert_awaited_once()
    logger.error.assert_called()


@pytest.mark.asyncio
async def test_failure_in_one_message_does_not_stop_the_others_of_the_system():
    send = AsyncMock(side_effect=[ERROR, OK])
    a = _components(send=send, mapped=[MappedDecision("m1"), MappedDecision("m2")], retry=False)
    service, _, _ = _service(a=a)
    await service.receive_message(_envelope())
    assert send.await_count == 2


@pytest.mark.asyncio
async def test_adapter_failure_is_logged_and_the_other_system_still_sends():
    a = _components()
    a.adapter.adapt_decisions = MagicMock(side_effect=ValueError("bad"))
    service, systems, logger = _service(a=a)
    await service.receive_message(_envelope())
    systems["b"].output_handler.send.assert_awaited_once()
    logger.error.assert_called()


@pytest.mark.asyncio
async def test_none_response_is_a_success():
    a = _components(send=AsyncMock(return_value=None))
    service, _, _ = _service(a=a)
    await service.receive_message(_envelope())
    a.error_handling_logic.should_retry.assert_not_awaited()


def test_dependencies_are_validated():
    logger = MagicMock()
    resolver = TargetSystemResolver("R", {}, logger)
    with pytest.raises(ValueError):
        DecisionDispatchingService(resolver, {}, None)
    with pytest.raises(TypeError):
        DecisionDispatchingService(None, {}, logger)
    with pytest.raises(TypeError):
        DecisionDispatchingService(resolver, {"a": object()}, logger)
