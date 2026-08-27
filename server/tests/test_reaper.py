from muster_api import reaper, store
from muster_api.identity import Address

GONE = Address("jc", "old", "claude", "dead-proj", "z9")
LIVE = Address("jc", "laptop", "claude", "muster-chat", "a1")


async def test_reap_once_removes_expired_agents_only(r):
    for addr in (GONE, LIVE):
        await store.register_agent(r, addr, [], {}, retention=3600)
        await store.append_message(r, str(addr), {"from": "f", "kind": "chat", "body": "x"}, 1000, 3600)
    await r.delete(store.agent_key(str(GONE)))  # simulate retention TTL firing

    assert await reaper.reap_once(r) == 1
    assert await r.exists(store.inbox_key(str(GONE))) == 0
    assert await r.sismember(store.AGENTS, str(GONE)) == 0
    assert await r.exists(store.inbox_key(str(LIVE))) == 1  # live agent untouched
    assert await r.sismember(store.AGENTS, str(LIVE)) == 1
