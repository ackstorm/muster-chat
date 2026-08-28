"""_call_tool + _render_deliver against a stubbed MusterClient.rpc."""
import time

import pytest
from plugins.muster.mcp import muster_channel as mc
from plugins.muster.mcp.httpbus import BusError


def _stub(monkeypatch, result=None, error=None):
    async def rpc(op, args=None):
        if error:
            raise error
        return result
    monkeypatch.setattr(mc.client, "rpc", rpc)


@pytest.mark.anyio
async def test_roster_renders_and_hides_self(monkeypatch):
    self_addr = "dev/" + mc.AGENT
    _stub(monkeypatch, {"ok": True, "hidden": {}, "agents": [
        {"addr": self_addr, "status": "online", "project": "proj", "meta": {}},
        {"addr": "dev/laptop/claude/proj/1", "status": "online", "project": "proj",
         "meta": {"branch": "main"}},
    ]})
    out = (await mc._call_tool("roster", {}))[0].text
    assert "proj:" in out  # grouped by project — no second query to learn where an agent lives
    assert "dev/laptop/claude/proj/1" in out and "@main" in out
    assert out.count("dev/") == 1  # self filtered


@pytest.mark.anyio
async def test_roster_summarises_hidden_offline_agents(monkeypatch):
    """Offline peers are still mailable, so the default view must show that they exist."""
    _stub(monkeypatch, {"ok": True, "agents": [], "hidden": {"ach-memory": 8, "muster-chat": 2}})
    out = (await mc._call_tool("roster", {}))[0].text
    assert "No online agents visible." in out
    assert "Offline: ach-memory ×8 · muster-chat ×2" in out
    assert '"status":"all"' in out  # and how to list them


@pytest.mark.anyio
async def test_roster_offline_rows_carry_age(monkeypatch):
    _stub(monkeypatch, {"ok": True, "hidden": {}, "agents": [
        {"addr": "dev/laptop/claude/proj/1", "status": "offline", "project": "proj",
         "last_connect": int(time.time()) - 2 * 86400, "meta": {}},
    ]})
    out = (await mc._call_tool("roster", {"status": "offline"}))[0].text
    assert "(last connect 2d ago)" in out


@pytest.mark.anyio
async def test_bus_error_renders_machine_fields(monkeypatch):
    _stub(monkeypatch, error=BusError(429, {"code": "message_rate_exceeded", "retry_after": 23,
                                            "message": "slow down"}))
    out = (await mc._call_tool("chat", {"to": "x", "body": "y"}))[0].text
    assert "slow down" in out and "23" in out


@pytest.mark.anyio
async def test_offline_notice(monkeypatch):
    _stub(monkeypatch, error=ConnectionError("boom"))
    out = (await mc._call_tool("fetch", {}))[0].text
    assert "offline" in out.lower()


def test_render_deliver_kinds():
    # server's envelope already carries the ❗ mark when important — _render_deliver must not add a second one
    assert mc._render_deliver({"kind": "chat", "envelope": "❗ ✉ hi", "important": True}) == "❗ ✉ hi"
    assert "📢" in mc._render_deliver({"kind": "announce", "from": "u/h/r/p/s", "body": "release in 5"})
    assert "3 unread" in mc._render_deliver({"kind": "unread", "count": 3})
    assert mc._render_deliver({"kind": "??"}) is None
