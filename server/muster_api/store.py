"""All Valkey operations. Key namespace muster2: only (spec §10) — the v0 muster:
prefix is never touched. Addresses appear verbatim inside keys ('/' is fine in
Valkey key names)."""
import json
import time

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
