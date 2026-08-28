"""Reuses the client fixture pattern from test_rpc_chat_fetch."""
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


def hdrs(key, addr):
    return {"x-muster-api-key": key,
            "x-muster-agent": f"{addr.host}/{addr.runtime}/{addr.project}/{addr.session}"}


@pytest.fixture
async def client(r):
    cfg = config.load(env={"MUSTER_VALKEY_URL": "redis://localhost:6379/3",
                           "MUSTER_STATIC_KEYS": STATIC})
    application = app_mod.create_app(cfg)
    application.state.redis = r
    for addr, groups in ((JC, ["ackstorm"]), (ANA, ["ackstorm"]), (BOB, ["otherteam"])):
        await store.register_agent(r, addr, groups, {"branch": "main"}, retention=3600)
    await store.touch_presence(r, str(ANA), "c1", ttl=60)  # ana online, others offline
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application),
                                 base_url="http://t") as c:
        yield c


async def rpc(client, key, addr, op, args):
    resp = await client.post("/v1/rpc", json={"op": op, "args": args}, headers=hdrs(key, addr))
    assert resp.status_code == 200, resp.text
    return resp.json()


async def test_roster_is_acl_filtered(client):
    agents = (await rpc(client, "k-jc", JC, "roster", {"status": "all"}))["agents"]
    assert {a["addr"] for a in agents} == {str(JC), str(ANA)}  # bob invisible
    assert all("groups" not in a for a in agents)
    ana = next(a for a in agents if a["user"] == "ana")
    assert ana["status"] == "online" and ana["meta"]["branch"] == "main"
    assert ana["last_connect"] > 0


async def test_roster_defaults_to_online(client):
    res = await rpc(client, "k-jc", JC, "roster", {})
    assert [a["addr"] for a in res["agents"]] == [str(ANA)]  # jc itself is offline here
    assert res["hidden"] == {}  # the caller is never counted as a hidden peer


async def test_roster_hides_offline_peers_as_counts(r, client):
    other = Address("jc", "laptop", "claude", "ach-memory", "z9")
    await store.register_agent(r, other, ["ackstorm"], {}, retention=3600)
    res = await rpc(client, "k-jc", JC, "roster", {})
    assert [a["addr"] for a in res["agents"]] == [str(ANA)]
    assert res["hidden"] == {"ach-memory": 1}


async def test_roster_status_offline_lists_them(client):
    res = await rpc(client, "k-jc", JC, "roster", {"status": "offline"})
    assert [a["addr"] for a in res["agents"]] == [str(JC)]
    assert res["hidden"] == {"muster-chat": 1}  # ana, online, hidden by the inverse filter


async def test_roster_filters(client):
    agents = (await rpc(client, "k-jc", JC, "roster",
                        {"runtime": "opencode", "status": "all"}))["agents"]
    assert [a["addr"] for a in agents] == [str(ANA)]
    agents = (await rpc(client, "k-jc", JC, "roster",
                        {"project": "muster-chat", "user": "jc", "status": "all"}))["agents"]
    assert [a["addr"] for a in agents] == [str(JC)]


async def test_roster_by_group(client):
    agents = (await rpc(client, "k-jc", JC, "roster",
                        {"group": "ackstorm", "status": "all"}))["agents"]
    assert {a["addr"] for a in agents} == {str(JC), str(ANA)}


async def test_roster_by_foreign_group_refused(client):
    resp = await client.post("/v1/rpc", json={"op": "roster", "args": {"group": "otherteam"}},
                             headers=hdrs("k-jc", JC))
    assert resp.status_code == 403
    assert resp.json()["code"] == "invalid_scope"


async def test_roster_rejects_unknown_status(client):
    resp = await client.post("/v1/rpc", json={"op": "roster", "args": {"status": "live"}},
                             headers=hdrs("k-jc", JC))
    assert resp.status_code == 400
    assert resp.json()["code"] == "invalid_status"


async def test_search_op_is_gone(client):
    """roster absorbed it — search was the same call with optional filters."""
    resp = await client.post("/v1/rpc", json={"op": "search", "args": {}},
                             headers=hdrs("k-jc", JC))
    assert resp.status_code == 400
    assert resp.json()["code"] == "unknown_op"
