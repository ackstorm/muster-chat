# plugins/muster/tests/test_busops.py
import anyio
import pytest
import redis.asyncio as redis
from plugins.muster.mcp import busops

URL = "redis://127.0.0.1:6379/1"

async def _clean(r, group):
    async for k in r.scan_iter(f"muster:*:{group}:*"):
        await r.delete(k)

def run(coro): return anyio.run(lambda: coro)

@pytest.fixture
def r():
    client = redis.from_url(URL, decode_responses=True)
    yield client

def test_roster_lists_live_presence(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "ach", {"pane_id": "w1:p1", "status": "idle",
            "branch": "main", "cwd": "/x/ach", "last_seen": "1"})
        await busops.write_presence(r, "busT", "ach-agent", {"pane_id": "w1:p2", "status": "idle",
            "branch": "dev", "cwd": "/x/ach-agent", "last_seen": "2"})
        rows = await busops.list_roster(r, "busT")
        assert {a["name"] for a in rows} == {"ach", "ach-agent"}
        # guard the write/read field pairing: write_presence stores group, _row must read it
        assert all(a["group"] == "busT" for a in rows)
        assert "ach" not in {a["name"] for a in await busops.list_roster(r, "busT", exclude="ach")}
    run(go())

def test_send_to_known_agent_lands_in_inbox(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "ach-agent", {"pane_id": "w1:p2"})
        res = await busops.send_message(r, "busT", "ach-agent", "ach", "please regen schema")
        assert res["ok"] is True
        got = await busops.fetch_inbox(r, "busT", "ach-agent")
        assert got[-1]["from"] == "ach" and got[-1]["body"] == "please regen schema"
    run(go())

def test_envelope_subject_and_fetch_nudge(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "ach-agent", {"pane_id": "w1:p2"})
        # long body, no subject -> envelope nudges fetch, full body still stored
        await busops.send_message(r, "busT", "ach-agent", "ach",
            "operator side is done, no blockers, but the schema regen still needs the "
            "new spec.mcpServers block wired in before we ship")
        got = await busops.fetch_inbox(r, "busT", "ach-agent")
        assert "fetch for full" in got[-1]["summary"] and got[-1]["summary"].startswith("✉ Message from ach:")
        assert got[-1]["body"].endswith("before we ship")   # nothing lost
        # explicit subject: stored + shown on the envelope
        await busops.send_message(r, "busT", "ach-agent", "ach", "full detail here", subject="schema regen")
        got = await busops.fetch_inbox(r, "busT", "ach-agent")
        assert got[-1]["subject"] == "schema regen" and "schema regen" in got[-1]["summary"]
        # short body, no subject -> whole message on the line, no fetch nudge
        await busops.send_message(r, "busT", "ach-agent", "ach", "ping")
        got = await busops.fetch_inbox(r, "busT", "ach-agent")
        assert "fetch for full" not in got[-1]["summary"] and "ping" in got[-1]["summary"]
    run(go())

def test_send_gated_by_herdr_status(r):
    async def go():
        await _clean(r, "busT")
        # working/blocked (herdr) -> refused, nothing delivered
        for st in ("working", "blocked"):
            await busops.write_presence(r, "busT", "busy", {"pane_id": "w1:p1", "status": st})
            res = await busops.send_message(r, "busT", "busy", "ach", "hi")
            assert res["ok"] is False and res.get("status") == st
            assert await busops.fetch_inbox(r, "busT", "busy") == []
        # idle (herdr) and online (no herdr) and unknown -> delivered
        for name, fields in (("free", {"status": "idle"}), ("plain", {"status": "online"}), ("unk", {})):
            await busops.write_presence(r, "busT", name, {"pane_id": "w1:px", **fields})
            res = await busops.send_message(r, "busT", name, "ach", "hi")
            assert res["ok"] is True
            got = await busops.fetch_inbox(r, "busT", name)
            assert got[-1]["body"] == "hi"
    run(go())

def test_important_overrides_status_gate(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "busy", {"pane_id": "w1:p1", "status": "working"})
        # normal send -> refused
        assert (await busops.send_message(r, "busT", "busy", "ach", "later"))["ok"] is False
        # important -> delivered even though working, and marked ❗
        res = await busops.send_message(r, "busT", "busy", "ach", "urgent now", important=True)
        assert res["ok"] is True
        got = await busops.fetch_inbox(r, "busT", "busy")
        assert got[-1]["body"] == "urgent now" and got[-1]["summary"].startswith("❗")
    run(go())

def test_announce_join_lands_in_peer_inbox(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "ach", {"pane_id": "w1:p1"})
        await busops.announce_join(r, "busT", "ach", "ach-agent")
        got = await busops.fetch_inbox(r, "busT", "ach")
        assert got[-1]["from"] == "ach-agent" and got[-1]["kind"] == "join"
        # wording contract: tagged [presence], names the peer, says no action is needed
        assert got[-1]["summary"] == '[presence] + "ach-agent" online (no action needed)' 
    run(go())

def test_announce_leave_lands_in_peer_inbox(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "ach", {"pane_id": "w1:p1"})
        await busops.announce_leave(r, "busT", "ach", "ach-agent")
        got = await busops.fetch_inbox(r, "busT", "ach")
        assert got[-1]["from"] == "ach-agent" and got[-1]["kind"] == "leave"
        assert got[-1]["summary"] == '[presence] − "ach-agent" offline (no action needed)' 
    run(go())

def test_send_to_unknown_agent_errors_with_roster(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "ach", {"pane_id": "w1:p1"})
        res = await busops.send_message(r, "busT", "ghost", "ach", "hi")
        assert res["ok"] is False and "ach" in res["roster"]
    run(go())

def test_build_orientation_identity_peers_pending(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "me", {"pane_id": "w1:pme", "status": "idle"})
        await busops.write_presence(r, "busT", "mate", {"pane_id": "w1:pmate", "status": "working"})
        await busops.send_message(r, "busT", "me", "mate", "ping")   # 1 pending for 'me'
        line = await busops.build_orientation(r, "busT", "me")
        assert line.startswith('you are "me" in group "busT".')
        assert "1 item(s) waiting" in line          # pending counted
        assert "mate (working)" in line             # peer + status shown
        assert "me (" not in line                   # self excluded from the peer list
        # no peers, no pending -> the empty-group phrasing
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "solo", {"pane_id": "w1:ps", "status": "idle"})
        solo = await busops.build_orientation(r, "busT", "solo")
        assert solo == 'you are "solo" in group "busT". No other agents live yet.'
    run(go())


def test_tail_resumes_from_cursor(r):
    async def go():
        await _clean(r, "busT")
        await busops.write_presence(r, "busT", "ach", {"pane_id": "w1:p1"})
        await busops.send_message(r, "busT", "ach", "ach-agent", "m1")
        entries, last = await busops.tail_inbox(r, "busT", "ach", "0-0")
        assert [f["body"] for _id, f in entries] == ["m1"]
        # nothing new from the advanced cursor (non-blocking check via a second sender)
        await busops.send_message(r, "busT", "ach", "ach-agent", "m2")
        entries2, last2 = await busops.tail_inbox(r, "busT", "ach", last)
        assert [f["body"] for _id, f in entries2] == ["m2"] and last2 != last
    run(go())
