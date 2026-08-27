"""End-to-end through the ASGI app with static keys. Three identities:
jc + ana share group 'ackstorm'; bob is in 'otherteam' (invisible to both)."""
import httpx
import pytest
from muster_api import app as app_mod, config, store
from muster_api.identity import Address

STATIC = ('{"k-jc": {"user_id": "jc", "groups": ["ackstorm"]},'
          ' "k-ana": {"user_id": "ana", "groups": ["ackstorm"]},'
          ' "k-bob": {"user_id": "bob", "groups": ["otherteam"]}}')

JC = Address("jc", "laptop", "claude", "muster-chat", "a1")
ANA = Address("ana", "devbox", "opencode", "muster-chat", "b1")
BOB = Address("bob", "other", "claude", "muster-chat", "c1")


def hdrs(key: str, addr: Address) -> dict:
    return {"x-muster-api-key": key,
            "x-muster-agent": f"{addr.host}/{addr.runtime}/{addr.project}/{addr.session}"}


@pytest.fixture
async def client(r):
    cfg = config.load(env={"MUSTER_VALKEY_URL": "redis://localhost:6379/3",
                           "MUSTER_STATIC_KEYS": STATIC, "MUSTER_CHAT_RATE": "5"})
    application = app_mod.create_app(cfg)
    application.state.redis = r
    # register the three agents as the stream would (Task 9 does this on connect)
    for addr, groups in ((JC, ["ackstorm"]), (ANA, ["ackstorm"]), (BOB, ["otherteam"])):
        await store.register_agent(r, addr, groups, {}, retention=3600)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application),
                                 base_url="http://t") as c:
        yield c


async def rpc(client, key, addr, op, args, expect=200):
    resp = await client.post("/v1/rpc", json={"op": op, "args": args}, headers=hdrs(key, addr))
    assert resp.status_code == expect, resp.text
    return resp.json()


async def test_chat_and_fetch_roundtrip(client):
    out = await rpc(client, "k-jc", JC, "chat", {"to": "b1", "body": "users.name is now display_name"})
    assert out["ok"] and out["to"] == str(ANA)
    msgs = (await rpc(client, "k-ana", ANA, "fetch", {}))["messages"]
    assert len(msgs) == 1
    assert msgs[0]["from"] == str(JC) and msgs[0]["body"] == "users.name is now display_name"
    assert (await rpc(client, "k-ana", ANA, "fetch", {}))["messages"] == []  # fetch was the ack


async def test_chat_from_is_server_stamped(client):
    await rpc(client, "k-jc", JC, "chat", {"to": "b1", "body": "hi"})
    msgs = (await rpc(client, "k-ana", ANA, "fetch", {}))["messages"]
    assert msgs[0]["from"].startswith("jc/")  # user came from the key, not the client header


async def test_chat_acl_blocks_unshared_group(client):
    out = await rpc(client, "k-bob", BOB, "chat", {"to": "b1", "body": "x"}, expect=404)
    assert out["code"] == "agent_not_found"  # ana is invisible to bob — indistinguishable from absent


async def test_ambiguous_reference_returns_candidates(client):
    out = await rpc(client, "k-jc", JC, "chat", {"to": "muster-chat", "body": "x"}, expect=409)
    assert out["code"] == "ambiguous_reference"
    # jc sees his own agent + ana (shared group), NOT bob — and both match "muster-chat"
    assert {c["addr"] for c in out["candidates"]} == {str(JC), str(ANA)}
    assert all("groups" not in c for c in out["candidates"])  # no team-list leak to the resolver


async def test_self_send_refused(client):
    out = await rpc(client, "k-jc", JC, "chat", {"to": "a1", "body": "x"}, expect=400)
    assert out["code"] == "self_send"


async def test_body_size_cap(client):
    out = await rpc(client, "k-jc", JC, "chat", {"to": "b1", "body": "x" * (256 * 1024 + 1)},
                    expect=413)
    assert out["code"] == "message_too_large"


async def test_rate_limit_machine_readable(client):
    for _ in range(5):
        await rpc(client, "k-jc", JC, "chat", {"to": "b1", "body": "spam"})
    out = await rpc(client, "k-jc", JC, "chat", {"to": "b1", "body": "spam"}, expect=429)
    assert out["code"] == "message_rate_exceeded"
    assert out["limit"] == 5 and out["window"] == 60 and out["retry_after"] > 0
    assert "loop" in out["message"]  # worded so an LLM sender recognizes the failure mode


async def test_bad_key_401(client):
    resp = await client.post("/v1/rpc", json={"op": "fetch", "args": {}},
                             headers=hdrs("nope", JC))
    assert resp.status_code == 401


async def test_readyz_reflects_valkey(client):
    ok = await client.get("/readyz")
    assert ok.status_code == 200 and ok.json()["ok"] is True
    # unreachable valkey -> 503 (readiness gate), while /healthz stays 200 (liveness)
    import redis.asyncio as redis
    from muster_api import app as app_mod, config
    cfg = config.load(env={"MUSTER_VALKEY_URL": "redis://localhost:1/0",
                           "MUSTER_STATIC_KEYS": "{}"})
    bad = app_mod.create_app(cfg)
    bad.state.redis = redis.from_url("redis://localhost:1/0",
                                     socket_connect_timeout=0.2, decode_responses=True)
    transport = __import__("httpx").ASGITransport(app=bad)
    async with __import__("httpx").AsyncClient(transport=transport, base_url="http://t") as c:
        assert (await c.get("/readyz")).status_code == 503
        assert (await c.get("/healthz")).status_code == 200


async def test_metrics_endpoint(client):
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "muster_messages_delivered_total" in resp.text
    assert "muster_sse_connections" in resp.text
