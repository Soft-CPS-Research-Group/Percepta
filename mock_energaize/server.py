"""Mini service that simulates EnergAIze, to test Percepta without the real model.

It receives the request that Percepta sends to the ML Model (POST /inference), prints it, and answers with the
decisions of the environment variable DECISIONS (a JSON dictionary, e.g. '{"B02": -2.5}'). Without it, the answer is an
empty dictionary, which means "no decisions": nothing is sent to the Target Systems.

Expected request (what Percepta sends): the unified message of a period, as JSON.
Response: the DECISIONS dictionary, or {}  (status 200)
"""
import json
import os
from datetime import datetime

from aiohttp import web


async def inference(request: web.Request) -> web.Response:
    """Prints the request and answers with no decisions."""
    try:
        body = await request.json()
        text = json.dumps(body, indent=2, ensure_ascii=False)
    except Exception:
        text = (await request.text()) or "<empty body>"

    print(f"\n[{datetime.now().isoformat(timespec='seconds')}] POST {request.path} ({request.content_length} bytes)")
    print(text, flush=True)

    return web.json_response(_decisions())


def _decisions() -> dict:
    """Decisions to answer with, from the environment variable DECISIONS (a JSON dictionary). Default: no decisions.

    Example: DECISIONS='{"B02": -2.5}' (entity id -> power in kW; negative discharges a battery).
    """
    try:
        decisions = json.loads(os.environ.get("DECISIONS", "{}"))
        return decisions if isinstance(decisions, dict) else {}
    except json.JSONDecodeError:
        print("DECISIONS is not valid JSON, so no decisions are returned.", flush=True)
        return {}


async def health(_: web.Request) -> web.Response:
    """Tells that the service is up."""
    return web.json_response({"status": "ok"})


def create_app() -> web.Application:
    """Creates the web application with its routes."""
    app = web.Application()
    app.add_routes([web.post("/inference", inference), web.get("/health", health)])
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
