from muster_api import store
from muster_api.identity import Address

JC = Address("jc", "laptop", "claude", "muster-chat", "a3f9")


async def test_register_and_list(r):
    await store.register_agent(r, JC, ["ackstorm"], {"branch": "main"}, retention=3600)
    agents = await store.list_agents(r)
    assert len(agents) == 1
    a = agents[0]
    assert a["addr"] == str(JC) and a["user"] == "jc" and a["project"] == "muster-chat"
    assert a["groups"] == ["ackstorm"] and a["meta"] == {"branch": "main"}
    assert a["status"] == "offline"  # no presence yet


async def test_presence_makes_online(r):
    await store.register_agent(r, JC, [], {}, retention=3600)
    await store.touch_presence(r, str(JC), "conn-1", ttl=60)
    assert (await store.list_agents(r))[0]["status"] == "online"


async def test_clear_presence_only_when_connection_matches(r):
    a = str(JC)
    await store.register_agent(r, JC, [], {}, retention=3600)
    await store.touch_presence(r, a, "conn-A", ttl=60)
    await store.touch_presence(r, a, "conn-B", ttl=60)   # successor connection takes over
    assert await store.clear_presence(r, a, "conn-A") == 0  # late close of A must not kill B
    assert (await store.list_agents(r))[0]["status"] == "online"
    assert await store.clear_presence(r, a, "conn-B") == 1
    assert (await store.list_agents(r))[0]["status"] == "offline"


async def test_expired_agent_hash_is_skipped(r):
    await store.register_agent(r, JC, [], {}, retention=3600)
    await r.delete(store.agent_key(str(JC)))  # simulate retention expiry (TTL fired)
    assert await store.list_agents(r) == []
