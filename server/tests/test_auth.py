import httpx
import pytest
from muster_api import auth, config

STATIC = '{"k-jc": {"user_id": "jc", "groups": ["ackstorm"]}}'


def make_auth(handler, static=STATIC, ttl=60):
    cfg = config.load(env={
        "MUSTER_STATIC_KEYS": static,
        "MUSTER_RESOLVER_URL": "http://resolver/v2/user/info",
        "MUSTER_AUTH_CACHE_TTL": str(ttl),
    })
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return auth.Authenticator(cfg, http=http)


async def test_static_key_short_circuits():
    a = make_auth(lambda req: pytest.fail("resolver must not be called"))
    ident = await a.resolve("k-jc")
    assert ident == auth.Identity("jc", ("ackstorm",))


async def test_resolver_success_is_cached():
    calls = []

    def handler(req):
        calls.append(req.headers["x-litellm-api-key"])
        return httpx.Response(200, json={"user_id": "ana", "teams": ["ackstorm"]})

    a = make_auth(handler)
    assert (await a.resolve("k-ana")).user_id == "ana"
    assert (await a.resolve("k-ana")).groups == ("ackstorm",)
    assert calls == ["k-ana"]  # second call served from cache


async def test_resolver_rejection_evicts_and_401s():
    responses = [httpx.Response(200, json={"user_id": "ana", "teams": []}),
                 httpx.Response(401)]
    a = make_auth(lambda req: responses.pop(0), ttl=0)  # ttl=0: every call re-resolves
    await a.resolve("k-ana")
    with pytest.raises(auth.AuthError) as e:
        await a.resolve("k-ana")
    assert e.value.status == 401 and e.value.code == "unauthorized"


async def test_resolver_down_fails_closed():
    def handler(req):
        raise httpx.ConnectError("down")

    a = make_auth(handler)
    with pytest.raises(auth.AuthError) as e:
        await a.resolve("k-unknown")
    assert e.value.status == 503 and e.value.code == "resolver_unavailable"


async def test_missing_key_401():
    a = make_auth(lambda req: httpx.Response(200))
    with pytest.raises(auth.AuthError) as e:
        await a.resolve("")
    assert e.value.status == 401
