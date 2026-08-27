"""SSE parser (pure async)."""
import pytest
from plugins.muster.mcp import httpbus


async def _lines(items):
    for i in items:
        yield i


async def _collect(items):
    return [e async for e in httpbus.parse_sse(_lines(items))]


@pytest.mark.anyio
async def test_parses_events_and_yields_pings():
    # pings surface as a bare _ping marker (not dropped) so a relay can reset its
    # reconnect backoff on a healthy idle stream, not only on real events.
    evs = await _collect([
        "event: deliver", 'data: {"kind": "chat", "msg_id": "1-0", "envelope": "hi"}', "",
        ": ping", "",
        "event: deliver", 'data: {"kind": "unread", "count": 3}', "",
    ])
    assert [e["_event"] for e in evs] == ["deliver", "_ping", "deliver"]
    assert evs[0]["msg_id"] == "1-0" and evs[2]["count"] == 3


@pytest.mark.anyio
async def test_malformed_json_yields_empty_payload_not_crash():
    evs = await _collect(["event: deliver", "data: {broken", "", "event: deliver", 'data: {"kind":"chat"}', ""])
    assert len(evs) == 2 and evs[0] == {"_event": "deliver"} and evs[1]["kind"] == "chat"


@pytest.mark.anyio
async def test_event_without_data_dropped():
    assert await _collect(["event: deliver", ""]) == []
