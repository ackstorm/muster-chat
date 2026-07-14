import anyio
import redis.asyncio as redis
from plugins.muster.mcp import muster_channel, busops, naming

URL = "redis://127.0.0.1:6379/1"


def test_tool_surface_is_roster_chat_fetch():
    # Guards the send->chat rename and that importing the module doesn't start the server
    # (the anyio.run(main) is behind an `if __name__ == "__main__"` guard).
    tools = anyio.run(muster_channel._list_tools)
    names = [tool.name for tool in tools]
    assert names == ["roster", "chat", "fetch"]
    assert "send" not in names   # clean break, no backcompat
    chat = next(tool for tool in tools if tool.name == "chat")
    assert "important" in chat.inputSchema["properties"]


def test_inbox_line_renders_notice_vs_message():
    # join/leave notices show their summary, NOT a dangling empty "Message from X:" that
    # buried real mail among presence noise (the confusion this fixes).
    join = {"ts": "1", "from": "c", "subject": "", "body": "",
            "summary": '[presence] + "c" online (no action needed)', "kind": "join"}
    msg = {"ts": "2", "from": "a", "subject": "regen", "body": "do it", "summary": "", "kind": ""}
    assert muster_channel._inbox_line(join) == '[1] [presence] + "c" online (no action needed)'
    assert "Message from c:" not in muster_channel._inbox_line(join)
    assert muster_channel._inbox_line(msg) == "[2] Message from a: (regen) do it"


# --- _push_entries: guards the relay-loop message-drop fix (no Valkey; fakes only) ---

class _FailingStream:
    """Records sent messages; raises on the send at index `fail_at`."""
    def __init__(self, fail_at):
        self.sent = []
        self.fail_at = fail_at

    async def send(self, message):
        if len(self.sent) == self.fail_at:
            raise RuntimeError("stream broke")
        self.sent.append(message)


class _FakeSession:
    def __init__(self, fail_at):
        self._write_stream = _FailingStream(fail_at)


class _FakeRedis:
    """Only .set is exercised by _push_entries."""
    def __init__(self):
        self.cursor = None

    async def set(self, key, value):
        self.cursor = value


def test_push_entries_stops_at_failure_without_skipping():
    # The bug: a later success in the same batch advanced CURSOR past an earlier FAILED push,
    # stranding it. Fix: stop at the first failure, never advance past an undelivered message.
    async def go():
        session = _FakeSession(fail_at=1)   # 2nd push fails
        r = _FakeRedis()
        entries = [("1-0", {"summary": "a"}), ("2-0", {"summary": "b"}), ("3-0", {"summary": "c"})]
        pushed = await muster_channel._push_entries(session, r, entries)
        assert pushed == "1-0"                    # resume id = last delivered, not batch tail
        assert r.cursor == "1-0"                  # cursor NOT advanced onto/past the failed 2nd
        assert len(session._write_stream.sent) == 1
    anyio.run(go)


def test_push_entries_all_succeed_advances_to_last():
    async def go():
        session = _FakeSession(fail_at=99)   # never fails
        r = _FakeRedis()
        entries = [("1-0", {"summary": "a"}), ("2-0", {"summary": "b"})]
        pushed = await muster_channel._push_entries(session, r, entries)
        assert pushed == "2-0" and r.cursor == "2-0"
        assert len(session._write_stream.sent) == 2
    anyio.run(go)


def test_push_entries_empty_batch_returns_none():
    async def go():
        session = _FakeSession(fail_at=99)
        r = _FakeRedis()
        assert await muster_channel._push_entries(session, r, []) is None
        assert r.cursor is None
    anyio.run(go)


# --- _call_tool handler paths (real Valkey; only touches its own zz-ct-* keys) ---

def _client():
    return redis.from_url(URL, decode_responses=True)


def test_call_tool_handlers():
    # One event loop for all handler paths: _call_tool caches its Valkey client in the module
    # global muster_channel._R, so a fresh anyio.run per test would reuse a client bound to a
    # closed loop. Exercise every path in a single loop. Only touches its own zz-ct-* keys.
    async def go():
        r = _client()
        g = muster_channel.GROUP
        p1, p2, busy = "zz-ct-peer-1", "zz-ct-peer-2", "zz-ct-busy"
        await r.delete(naming.pkey(g, p1), naming.pkey(g, p2),
                       naming.pkey(g, busy), naming.ikey(g, busy))
        await busops.write_presence(r, g, p1, {"pane_id": "w1:pa", "status": "idle"})
        await busops.write_presence(r, g, p2, {"pane_id": "w1:pb", "status": "working"})

        roster = (await muster_channel._call_tool("roster", {}))[0].text
        assert p1 in roster and p2 in roster

        await busops.write_presence(r, g, busy, {"pane_id": "w1:pc", "status": "working"})
        blocked = (await muster_channel._call_tool("chat", {"to": busy, "body": "hi"}))[0].text
        assert "working" in blocked                       # gate error surfaces the status
        forced = (await muster_channel._call_tool(
            "chat", {"to": busy, "body": "hi", "important": True}))[0].text
        assert "Delivered" in forced                      # important overrides the gate

        miss = (await muster_channel._call_tool(
            "chat", {"to": "zz-ct-nobody", "body": "hi"}))[0].text
        assert "no live agent" in miss and "Live now" in miss

        unknown = (await muster_channel._call_tool("bogus", {}))[0].text
        assert "unknown tool" in unknown

        await r.delete(naming.pkey(g, p1), naming.pkey(g, p2),
                       naming.pkey(g, busy), naming.ikey(g, busy))
    anyio.run(go)
