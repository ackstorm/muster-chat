# plugins/muster/mcp/busops.py
"""Async Valkey operations for the Muster channel plugin. All keys via naming.* so the
schema matches the Phase 0 daemon. Every function is fail-safe at the caller; here we
just do the operation."""
import time

try:                       # package context (pytest: plugins.muster.mcp.busops)
    from . import naming
except ImportError:        # flat script context (runtime: python .../mcp/muster_channel.py)
    import naming


async def write_presence(r, group, name, fields, ttl=90):
    key = naming.pkey(group, name)
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(key, mapping={**fields, "name": name, "group": group})  # name/group are read back from the hash
        pipe.expire(key, ttl)
        await pipe.execute()


def _row(key, h):
    return {"name": h.get("name") or naming.name_from_pkey(key), "group": h.get("group", ""),
            "status": h.get("status", ""), "pane_id": h.get("pane_id", ""),
            "branch": h.get("branch", ""), "cwd": h.get("cwd", ""), "last_seen": h.get("last_seen", "")}


async def _scan_rows(r, match):
    keys = [key async for key in r.scan_iter(match)]
    if not keys:
        return []
    async with r.pipeline(transaction=False) as pipe:  # one round trip, not one HGETALL per key
        for key in keys:
            pipe.hgetall(key)
        hashes = await pipe.execute()
    return [_row(key, h) for key, h in zip(keys, hashes)]


async def list_roster(r, group, exclude=None):  # vecinos: one group
    return [a for a in await _scan_rows(r, naming.presence_scan_match(group)) if a["name"] != exclude]


async def build_orientation(r, group, name):
    """The dynamic orientation line — identity + live peers + pending inbox count. Shared by the
    startup welcome (channel push). Read-only — no presence write, no join announce (those belong
    to the running server; re-announcing would false-notify peers on every /clear)."""
    peers = await list_roster(r, group, exclude=name)
    who = (" Live peers: " + ", ".join(f"{p['name']} ({p['status'] or 'online'})" for p in peers)
           + ".") if peers else " No other agents live yet."
    cur = await r.get(naming.rkey(group, name))
    inbox = naming.ikey(group, name)
    pending = len(await r.xrange(inbox, min="(" + cur, max="+")) if cur else await r.xlen(inbox)
    tail = f" You have {pending} item(s) waiting — they arrive as further muster messages." if pending else ""
    return f'you are "{name}" in group "{group}".' + tail + who


def _envelope(frm, body, subject):
    """Short channel line: sender + subject, plus a 'fetch for full' nudge whenever the
    line does not already carry the whole body. This is what gets pushed to the recipient's
    session — an envelope, not a chopped body, so a long message reads as a summary to
    `fetch`, never as a message that got truncated mid-sentence."""
    subj = (subject or body).strip()
    shown = (subj.splitlines()[0] if subj else "")[:56]
    has_more = shown != body.strip()  # anything not on the line → nudge fetch
    return f"✉ Message from {frm}: {shown}" + (" · fetch for full" if has_more else "")


async def send_message(r, group, to, frm, body, subject=None, important=False):
    pres = await r.hgetall(naming.pkey(group, to))
    if not pres:
        roster = [a["name"] for a in await list_roster(r, group)]
        return {"ok": False, "error": f"no live agent named {to!r} in this group", "roster": roster}
    # herdr status gate: only idle agents accept mail. "online" = no herdr (permissive),
    # empty = unknown (permissive); any other herdr status (working/blocked/…) is refused —
    # unless important=True, which overrides the gate and marks the message.
    status = pres.get("status", "")
    if not important and status and status not in ("idle", "online"):
        return {"ok": False, "status": status,
                "error": f'Not delivered: {to!r} is status: "{status}" (only deliver when '
                         f'idle/online). If this is important, resend with important=true to force it.'}
    summary = _envelope(frm, body, subject)
    fields = {"from": frm, "body": body, "summary": "❗ " + summary if important else summary,
              "ts": str(int(time.time()))}
    if subject:
        fields["subject"] = subject
    if important:
        fields["important"] = "1"
    msg_id = await r.xadd(naming.ikey(group, to), fields)
    return {"ok": True, "msg_id": msg_id, "to": to}


async def announce_join(r, group, to, frm):
    """Drop a one-line presence notice into a live peer's inbox so it surfaces in their
    channel. Summary-only (no body) — it's a heads-up, not mail to fetch.

    Wording is load-bearing: an idle agent that reads "👋 X joined" treats it as an event to
    look into (roster, herdr, who-is-this). The `[presence]` tag + "(no action needed)" say
    it is a roster fact, not a request. Keep it flat — no emoji, no greeting, no name-calling.

    ponytail: fires once per process start, so a peer that restarts often re-greets; add a
    dedup marker only if that noise ever bites."""
    return await r.xadd(naming.ikey(group, to), {
        "from": frm, "kind": "join", "ts": str(int(time.time())),
        "summary": f'[presence] + "{frm}" online (no action needed)'})


async def announce_leave(r, group, to, frm):
    """Drop a one-line presence notice into a live peer's inbox on graceful shutdown
    (SIGTERM). Summary-only, same wording contract as announce_join."""
    return await r.xadd(naming.ikey(group, to), {
        "from": frm, "kind": "leave", "ts": str(int(time.time())),
        "summary": f'[presence] − "{frm}" offline (no action needed)'})


async def fetch_inbox(r, group, name, limit=10):
    entries = await r.xrevrange(naming.ikey(group, name), count=limit)
    out = [{"msg_id": mid, "from": f.get("from", ""), "subject": f.get("subject", ""),
            "body": f.get("body", ""), "summary": f.get("summary", ""), "ts": f.get("ts", ""),
            "kind": f.get("kind", "")}  # join/leave notices carry a kind; real mail does not
           for mid, f in entries]
    out.reverse()  # newest-last for readability
    return out


async def tail_inbox(r, group, name, last):
    resp = await r.xread({naming.ikey(group, name): last}, block=0, count=10)
    entries = []
    for _stream, items in resp or []:
        for mid, f in items:
            entries.append((mid, f))
            last = mid
    return entries, last
