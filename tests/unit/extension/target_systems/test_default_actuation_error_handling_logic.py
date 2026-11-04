import asyncio
from unittest.mock import MagicMock

import pytest

from app.extension.target_systems.default_actuation_error_handling_logic import DefaultActuationErrorHandlingLogic


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    async def fast(_):
        return None
    monkeypatch.setattr(asyncio, "sleep", fast)


def _logic():
    return DefaultActuationErrorHandlingLogic({}, MagicMock())


@pytest.mark.asyncio
async def test_retries_twice_then_gives_up():
    logic = _logic()
    assert await logic.should_retry("e", "m") is True
    assert await logic.should_retry("e", "m") is True
    assert await logic.should_retry("e", "m") is False


@pytest.mark.asyncio
async def test_count_restarts_after_giving_up():
    logic = _logic()
    for _ in range(3):
        await logic.should_retry("e", "m")
    assert await logic.should_retry("e", "m") is True


@pytest.mark.asyncio
async def test_messages_are_counted_separately():
    logic = _logic()
    await logic.should_retry("e", "m1")
    await logic.should_retry("e", "m1")
    assert await logic.should_retry("e", "m2") is True
    assert await logic.should_retry("e", "m1") is False


@pytest.mark.asyncio
async def test_old_counts_are_discarded():
    logic = _logic()
    for index in range(logic._MAX_TRACKED_MESSAGES + 5):
        await logic.should_retry("e", f"m{index}")
    assert len(logic._failed_attempts) == logic._MAX_TRACKED_MESSAGES


def test_needs_its_dependencies():
    with pytest.raises(ValueError):
        DefaultActuationErrorHandlingLogic({}, None)
    with pytest.raises(ValueError):
        DefaultActuationErrorHandlingLogic([], MagicMock())
