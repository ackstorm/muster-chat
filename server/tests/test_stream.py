import asyncio
import json
import socket

import httpx
import pytest
import uvicorn
from muster_api import app as app_mod, config, store
from muster_api.identity import Address

STATIC = ('{"k-jc": {"user_id": "jc", "groups": ["ackstorm"]},'
          ' "k-ana": {"user_id": "ana", "groups": ["ackstorm"]}}')
JC = Address("jc", "laptop", "claude", "muster-chat", "a1")
ANA = Address("ana", "devbox", "opencode", "muster-chat", "b1")


def hdrs(key, addr):
    return {"x-muster-api-key": key,
            "x-muster-agent": f"{addr.host}/{addr.runtime}/{addr.project}/{addr.session}",
            "x-muster-meta": '{"branch": "main"}'}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def client(r):
    # ponytail: httpx.ASGITransport fully buffers the ASGI app call before returning a
    # response — it cannot serve a `while True` SSE generator at all (confirmed: the
    # `async with client.stream(...)` line deadlocks forever, not just disconnect cleanup).
    # A real socket-backed uvicorn server is the smallest fix that keeps stream.py's
    # `while True` design untouched; upgrade path: revisit if httpx ever supports it.
    cfg = config.load(env={"MUSTER_VALKEY_URL": "redis://localhost:6379/3",
                           "MUSTER_STATIC_KEYS": STATIC, "MUSTER_PING_INTERVAL": "1"})
    application = app_mod.create_app(cfg)
    application.state.redis = r
    port = _free_port()
    uv_config = uvicorn.Config(application, host="127.0.0.1", port=port,
                               log_level="warning", lifespan="off")
    server = uvicorn.Server(uv_config)
    task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.02)
    else:
        raise RuntimeError("uvicorn test server did not start")
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=10) as c:
        yield c
    server.should_exit = True
    await asyncio.wait_for(task, timeout=5)


async def read_events(response, n, timeout=5):
    """Collect n SSE deliver events (skip pings) from a streaming response."""
    events, buf = [], ""
    async with asyncio.timeout(timeout):
        async for chunk in response.aiter_text():
            buf += chunk
            while "\n\n" in buf:
                block, buf = buf.split("\n\n", 1)
                if block.startswith("event: deliver"):
                    events.append(json.loads(block.split("data: ", 1)[1]))
                if len(events) >= n:
                    return events
    return events


async def test_connect_registers_and_delivers_live_chat(client, r):
    async with client.stream("GET", "/v1/stream", headers=hdrs("k-ana", ANA)) as resp:
        assert resp.status_code == 200
        await asyncio.sleep(0.2)  # let registration land
        agents = await store.list_agents(r)
        assert agents and agents[0]["status"] == "online" and agents[0]["meta"] == {"branch": "main"}
        # jc chats ana while her stream is open → envelope arrives as deliver event
        await client.post("/v1/rpc", json={"op": "chat", "args": {"to": "b1", "body": "live one"}},
                          headers=hdrs("k-jc", JC))
        events = await read_events(resp, 1)
    assert events[0]["kind"] == "chat" and events[0]["from"] == str(JC)
    assert "live one" in events[0]["envelope"]
    # stream closed → presence cleaned up
    await asyncio.sleep(0.2)
    assert (await store.list_agents(r))[0]["status"] == "offline"


async def test_reconnect_gets_coalesced_unread_nudge(client, r, ana_registered):
    # two messages land while ana is offline
    for body in ("one", "two"):
        await client.post("/v1/rpc", json={"op": "chat", "args": {"to": "b1", "body": body}},
                          headers=hdrs("k-jc", JC))
    # ana must exist for jc to chat her: register her directly first
    # (order note: register, then send, then connect)
    async with client.stream("GET", "/v1/stream", headers=hdrs("k-ana", ANA)) as resp:
        events = await read_events(resp, 1)
    assert events[0]["kind"] == "unread" and events[0]["count"] == 2


@pytest.fixture
async def ana_registered(r):
    await store.register_agent(r, ANA, ["ackstorm"], {}, retention=3600)
