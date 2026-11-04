from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.output_handlers.async_http_output_handler import AsyncHTTPOutputHandler
from app.extension.telemetry_systems.cleanwatts.cw_session import CWSession

LOGIN = {"path": "/sessions", "method": "POST", "rpc_properties": {"enabled": True}}


def _initialized(token, refresh_token="r"):
    handler = MagicMock(spec=AsyncHTTPOutputHandler)
    handler.send = AsyncMock(return_value={"/sessions": {"data": {"Token": token, "RefreshToken": refresh_token}}})
    session = CWSession()
    session.initialize(
        output_handler=handler,
        auth_config={"connection": {"credentials": {"username": "u", "password": "p"}}, "topology": {"endpoints": [LOGIN]}},
        logger=MagicMock()
    )
    return session, handler


def test_new_session_is_empty():
    session = CWSession()
    assert session.get_token() is None
    assert session.is_token_valid() is False


@pytest.mark.asyncio
async def test_login_stores_the_token():
    session, handler = _initialized("t1")
    await session.login()
    assert session.get_token() == "t1"
    assert session.is_token_valid() is True
    assert handler.send.call_args.kwargs["message"] == {"Login": "u", "Password": "p"}


@pytest.mark.asyncio
async def test_sessions_are_independent():
    first, _ = _initialized("t1")
    second, _ = _initialized("t2")
    await first.login()
    assert second.get_token() is None and second.is_token_valid() is False
    await second.login()
    assert first.get_token() == "t1" and second.get_token() == "t2"


@pytest.mark.asyncio
async def test_refresh_without_initialization_does_nothing():
    session = CWSession()
    await session.refresh_tokens()
    assert session.get_token() is None


@pytest.mark.asyncio
async def test_login_failure_raises():
    session, handler = _initialized("t1")
    handler.send = AsyncMock(return_value={"/sessions": {"data": {}}})
    with pytest.raises(ValueError):
        await session.login()
    assert session.is_token_valid() is False
