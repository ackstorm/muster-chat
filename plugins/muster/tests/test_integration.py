"""MusterClient ↔ local muster-api roundtrip. Needs `docker compose up -d` (dev-key auth).
Skipped when muster-api isn't listening — pure/unit tests stay green without services."""
import anyio
import httpx
import pytest

from plugins.muster.mcp.httpbus import MusterClient, BusError

URL = "http://localhost:8765"


async def _register(c):
    """Opening the SSE stream is what registers the agent (server stream.py calls
    register_agent on connect). Wait for the first event or 2s, then close."""
    gen = c.stream()
    with anyio.move_on_after(2):
        try:
            await gen.__anext__()
        except StopAsyncIteration:
            pass
    await gen.aclose()


def _up():
    try:
        return httpx.get(f"{URL}/healthz", timeout=2).status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _up(), reason="muster-api not running (docker compose up -d)")


@pytest.fixture
def pair():
    a = MusterClient(URL, "dev-key", "testhost/claude/itest/1")
    b = MusterClient(URL, "dev-key", "testhost/claude/itest/2")
    return a, b


@pytest.mark.anyio
async def test_chat_fetch_roundtrip(pair):
    a, b = pair
    # register both: stream connect registers; rpc alone doesn't create the peer
    await _register(a)
    await _register(b)
    res = await a.rpc("chat", {"to": "itest/2", "body": "ping from itest", "subject": "it"})
    assert res["ok"] and res["to"].endswith("testhost/claude/itest/2")
    got = await b.rpc("fetch", {})
    bodies = [m["body"] for m in got["messages"]]
    assert "ping from itest" in bodies
    again = await b.rpc("fetch", {})
    assert "ping from itest" not in [m["body"] for m in again["messages"]]  # cursor advanced


@pytest.mark.anyio
async def test_self_send_refused(pair):
    a, _ = pair
    with pytest.raises(BusError) as ei:
        await a.rpc("chat", {"to": "testhost/claude/itest/1", "body": "hi me"})
    assert ei.value.payload["code"] == "self_send"
