import asyncio
import json

import httpx
import pytest
from muster_api import app as app_mod, config, store
from muster_api.identity import Address

STATIC = ('{"k-jc": {"user_id": "jc", "groups": ["ackstorm"]},'
          ' "k-ana": {"user_id": "ana", "groups": ["ackstorm"]},'
          ' "k-bob": {"user_id": "bob", "groups": ["otherteam"]}}')

JC = Address("jc", "laptop", "claude", "muster-chat", "a1")
JC2 = Address("jc", "devbox", "claude", "muster-chat", "a2")
ANA = Address("ana", "devbox", "opencode", "muster-chat", "b1")
BOB = Address("bob", "other", "claude", "muster-chat", "c1")


def hdrs(key, addr):
    return {"x-muster-api-key": key,
            "x-muster-agent": f"{addr.host}/{addr.runtime}/{addr.project}/{addr.session}"}


@pytest.fixture
async def client(r):
    cfg = config.load(env={"MUSTER_VALKEY_URL": "redis://localhost:6379/3",
                           "MUSTER_STATIC_KEYS": STATIC, "MUSTER_ANNOUNCE_RATE": "2"})
    application = app_mod.create_app(cfg)
    application.state.redis = r
    for addr, groups in ((JC, ["ackstorm"]), (JC2, ["ackstorm"]), (ANA, ["ackstorm"]), (BOB, ["otherteam"])):
        await store.register_agent(r, addr, groups, {}, retention=3600)
        await store.touch_presence(r, str(addr), f"conn-{addr.session}", ttl=60)  # everyone online
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application),
                                 base_url="http://t") as c:
        yield c


async def collect_published(r, addr, coro):
    """Run coro while subscribed to addr's notify channel; return published events."""
    pubsub = r.pubsub()
    await pubsub.subscribe(store.notify_channel(str(addr)))
    await pubsub.get_message(timeout=1)  # consume subscribe confirmation
    await coro
    events = []
    while (m := await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)):
        events.append(json.loads(m["data"]))
    await pubsub.aclose()
    return events


async def announce(client, key, addr, args, expect=200):
    resp = await client.post("/v1/rpc", json={"op": "announce", "args": args}, headers=hdrs(key, addr))
    assert resp.status_code == expect, resp.text
    return resp.json()


async def test_group_scope_reaches_online_group_agents_not_sender(client, r):
    args = {"scope": "group:ackstorm", "project": "muster-chat",
            "body": "release in 5 minutes — push what you have", "subject": "release window"}
    events = await collect_published(r, ANA, announce(client, "k-jc", JC, args))
    assert len(events) == 1
    e = events[0]
    assert e["kind"] == "announce" and e["from"] == str(JC)
    assert e["body"].startswith("release in 5") and e["subject"] == "release window"
    out = await announce(client, "k-jc", JC, args)  # second call: count recipients
    assert out["recipients"] == 2  # jc2 + ana; sender excluded; bob not in group


async def test_no_inbox_write(client, r):
    await announce(client, "k-jc", JC, {"scope": "group:ackstorm", "project": "muster-chat", "body": "x"})
    assert await r.xlen(store.inbox_key(str(ANA))) == 0  # ephemeral: nothing stored


async def test_user_scope_must_be_self(client):
    out = await announce(client, "k-jc", JC,
                         {"scope": "user:ana", "project": "muster-chat", "body": "x"}, expect=403)
    assert out["code"] == "invalid_scope"


async def test_foreign_group_scope_refused(client):
    out = await announce(client, "k-jc", JC,
                         {"scope": "group:otherteam", "project": "muster-chat", "body": "x"}, expect=403)
    assert out["code"] == "invalid_scope"


async def test_announce_rate_limited_separately(client):
    args = {"scope": "user:jc", "project": "muster-chat", "body": "x"}
    await announce(client, "k-jc", JC, args)
    await announce(client, "k-jc", JC, args)
    out = await announce(client, "k-jc", JC, args, expect=429)
    assert out["code"] == "message_rate_exceeded" and out["limit"] == 2
