"""All Valkey operations. Key namespace muster2: only (spec §10) — the v0 muster:
prefix is never touched. Addresses appear verbatim inside keys ('/' is fine in
Valkey key names)."""
import json
import time

from . import metrics
from .identity import Address

AGENTS = "muster2:agents"  # SET of full address strings — the directory


def agent_key(a: str) -> str:    return f"muster2:agent:{a}"
def presence_key(a: str) -> str: return f"muster2:presence:{a}"
def inbox_key(a: str) -> str:    return f"muster2:inbox:{a}"
def cursor_key(a: str) -> str:   return f"muster2:cursor:{a}"
def rate_key(kind: str, a: str) -> str: return f"muster2:rate:{kind}:{a}"
def notify_channel(a: str) -> str:      return f"muster2:notify:{a}"


async def register_agent(r, addr: Address, groups, meta, retention: int) -> None:
    a = str(addr)
    async with r.pipeline(transaction=True) as p:
        p.sadd(AGENTS, a)
        p.hset(agent_key(a), mapping={
            "user": addr.user, "host": addr.host, "runtime": addr.runtime,
            "project": addr.project, "session": addr.session,
            "groups": json.dumps(list(groups)), "meta": json.dumps(meta or {}),
            "last_connect": str(int(time.time()))})
        p.expire(agent_key(a), retention)  # identity retention (spec §7); reaper GCs the rest
        await p.execute()


async def touch_presence(r, a: str, connection_id: str, ttl: int) -> None:
    async with r.pipeline(transaction=True) as p:
        p.hset(presence_key(a), mapping={"connection_id": connection_id,
                                         "connected_at": str(int(time.time()))})
        p.expire(presence_key(a), ttl)  # safety net: pod death without cleanup goes stale <= ttl
        await p.execute()


_CLEAR_IF_OWNER = """
if redis.call('HGET', KEYS[1], 'connection_id') == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


async def clear_presence(r, a: str, connection_id: str) -> int:
    """Connection race protection (spec §7.1): delete presence IFF still the owner."""
    return await r.eval(_CLEAR_IF_OWNER, 1, presence_key(a), connection_id)


async def list_agents(r) -> list[dict]:
    addrs = sorted(await r.smembers(AGENTS))
    if not addrs:
        return []
    async with r.pipeline(transaction=False) as p:  # one round trip
        for a in addrs:
            p.hgetall(agent_key(a))
            p.exists(presence_key(a))
        res = await p.execute()
    out = []
    for i, a in enumerate(addrs):
        h, online = res[2 * i], res[2 * i + 1]
        if not h:
            continue  # identity expired; still in the SET until the reaper runs
        out.append({"addr": a, "user": h["user"], "host": h["host"], "runtime": h["runtime"],
                    "project": h["project"], "session": h["session"],
                    "groups": json.loads(h.get("groups", "[]")),
                    "meta": json.loads(h.get("meta", "{}")),
                    "status": "online" if online else "offline"})
    return out


def envelope(frm: str, body: str, subject: str | None, important: bool, subject_max: int = 56) -> str:
    """v0 envelope semantics: a summary line, never a mid-sentence truncation."""
    subj = (subject or body).strip()
    shown = (subj.splitlines()[0] if subj else "")[:subject_max]
    line = f"✉ {frm}: {shown}" + ("" if shown == body.strip() else " · fetch for full")
    return ("❗ " + line) if important else line


async def append_message(r, to: str, fields: dict, maxlen: int, message_ttl: int) -> str:
    now = int(time.time())
    fields = {**fields, "ts": str(now), "expires_at": str(now + message_ttl)}
    return await r.xadd(inbox_key(to), fields, maxlen=maxlen, approximate=True)


async def publish_deliver(r, to: str, event: dict) -> None:
    await r.publish(notify_channel(to), json.dumps(event))
    metrics.DELIVERED.labels(kind=event.get("kind", "")).inc()


async def _entries_past_cursor(r, a: str, limit: int):
    cur = await r.get(cursor_key(a))
    start = ("(" + cur) if cur else "-"
    return await r.xrange(inbox_key(a), min=start, max="+", count=limit)


async def fetch_unread(r, a: str, limit: int) -> list[dict]:
    """Fetch IS the ack (spec §11): advance the single cursor to the last returned
    entry. deliver events never touch the cursor."""
    entries = await _entries_past_cursor(r, a, limit)
    if not entries:
        return []
    await r.set(cursor_key(a), entries[-1][0])
    now = int(time.time())
    return [{"msg_id": mid, **f} for mid, f in entries
            if int(f.get("expires_at", "0") or 0) > now]


async def unread_count(r, a: str) -> tuple[int, str]:
    entries = await _entries_past_cursor(r, a, limit=1000)
    now = int(time.time())
    live = [(mid, f) for mid, f in entries if int(f.get("expires_at", "0") or 0) > now]
    return len(live), (live[0][1].get("ts", "") if live else "")


async def rate_check(r, kind: str, a: str, limit: int, window: int) -> tuple[bool, int]:
    k = rate_key(kind, a)
    async with r.pipeline(transaction=True) as p:
        p.incr(k)
        p.expire(k, window, nx=True)  # only sets TTL if the key is new — one round trip, no gap
        n, _ = await p.execute()
    if n > limit:
        ttl = await r.ttl(k)
        if ttl < 0:  # crash/race left the key without a TTL — re-arm it
            await r.expire(k, window)
            ttl = window
        return False, max(ttl, 1)
    return True, 0
