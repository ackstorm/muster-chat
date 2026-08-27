# muster-api (Central Bus Service) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the central Muster coordination service: authenticated HTTP API (stateless RPC + SSE delivery stream) over a private Valkey, implementing spec v2 (`docs/superpowers/specs/2026-08-27-muster-v1-central-bus-spec-v2.md`).

**Architecture:** FastAPI app, session-less: every request carries `x-muster-api-key` (resolved against an external identity resolver, cached 60s) + `x-muster-agent` (host/runtime/project/session; `user` is server-stamped). Unicast chat = durable Valkey stream inbox + envelope nudge over SSE, `fetch` is the ack (single cursor). Broadcast = ephemeral full-body fan-out to online streams via Valkey pub/sub, no storage. One ACL predicate. This is Plan 1 of 3 (shims and telegram-gateway are separate plans).

**Tech Stack:** Python 3.12, FastAPI, uvicorn, redis.asyncio (Valkey), httpx. Tests: pytest + pytest-asyncio against the real Valkey from `docker compose` (test DB 3), httpx `ASGITransport` for the app.

## Global Constraints

Copied from spec v2 — every task implicitly includes these:

- Key namespace is `muster2:` only. Never touch the v0 `muster:` prefix.
- `user` address segment is server-stamped from the resolver. Clients never supply it. `x-muster-agent` carries only `host/runtime/project/session`.
- No `ack` op exists. `fetch` returns unread bodies and advances the single cursor. `deliver` events never advance it.
- Broadcast (`announce`) is ephemeral: online recipients only, full body over SSE, no inbox write, no retention, no TTL argument.
- Auth is fail-closed: resolver rejection (400/401/403) ⇒ evict cache + 401; resolver unreachable and no fresh cache ⇒ 503. No stale-while-error window.
- Rate-limited requests get machine-readable errors, never silent drops (shape in Task 6). No content-based dedup anywhere.
- Defaults (spec §17): auth cache TTL 60s · message TTL 72h · agent retention 7d (MUST be ≥ message TTL, assert at config load) · inbox MAXLEN ~1000 · unicast rate 20/60s · broadcast rate 3/60s · body cap 256 KB · SSE ping 15s · envelope subject ≤56 chars. All env-overridable.
- Presence cleanup only removes a presence record if `presence.connection_id == closing_connection_id`.
- Server location: `server/` in this repo (the spec's "sibling muster/ project" does not exist; monorepo decision recorded here).
- Tests run with: `cd server && uv run pytest ...` (uv manages the venv from `pyproject.toml`). Valkey must be up: `docker compose up -d` at repo root. Tests use DB 3 and flush it; production default is DB 1 (safe: distinct `muster2:` prefix).

## File Structure

```
server/
  pyproject.toml            # deps + pytest config
  Dockerfile                # multi-stage, explicit COPYs (Task 11)
  muster_api/
    __init__.py
    config.py               # env → frozen Config dataclass, spec defaults
    identity.py             # Address parse/format, reference matching, ACL predicate (pure)
    auth.py                 # static-key dev mode + resolver client + TTL cache
    store.py                # all Valkey ops: agents, presence, inbox, cursor, rates, pubsub
    ops.py                  # RPC handlers: chat/fetch/roster/search/announce
    stream.py               # GET /v1/stream SSE generator
    reaper.py               # GC of expired agents
    app.py                  # FastAPI factory, /v1/rpc dispatch, lifespan
  tests/
    conftest.py             # valkey + app fixtures, static test identities
    test_config.py
    test_identity.py
    test_auth.py
    test_store_registry.py
    test_store_inbox.py
    test_rpc_chat_fetch.py
    test_rpc_roster_search.py
    test_rpc_announce.py
    test_stream.py
    test_reaper.py
helm/muster-api/            # minimal chart (Task 11)
docker-compose.yml          # modified: add muster-api service (Task 11)
```

---

### Task 1: Project scaffold + config

**Files:**
- Create: `server/pyproject.toml`, `server/muster_api/__init__.py`, `server/muster_api/config.py`, `server/tests/__init__.py`, `server/tests/test_config.py`

**Interfaces:**
- Produces: `config.load(env: Mapping = os.environ) -> Config` — frozen dataclass with fields: `valkey_url: str`, `resolver_url: str | None`, `resolver_header: str`, `user_field: str`, `groups_field: str`, `static_keys: dict`, `auth_cache_ttl: int`, `message_ttl: int`, `agent_retention: int`, `inbox_maxlen: int`, `chat_rate: int`, `announce_rate: int`, `rate_window: int`, `body_max: int`, `ping_interval: int`, `presence_ttl: int`. Raises `ValueError` if `agent_retention < message_ttl`.

- [ ] **Step 1: Write `server/pyproject.toml`**

```toml
[project]
name = "muster-api"
version = "0.1.0"
description = "Muster central agent coordination bus"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.30",
    "redis>=5.0",
    "httpx>=0.27",
]

[dependency-groups]
dev = ["pytest>=8", "pytest-asyncio>=0.24"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 2: Write the failing test** — `server/tests/test_config.py`

```python
import pytest
from muster_api import config


def test_defaults_match_spec():
    cfg = config.load(env={})
    assert cfg.valkey_url == "redis://localhost:6379/1"
    assert cfg.auth_cache_ttl == 60
    assert cfg.message_ttl == 72 * 3600
    assert cfg.agent_retention == 7 * 86400
    assert cfg.inbox_maxlen == 1000
    assert cfg.chat_rate == 20
    assert cfg.announce_rate == 3
    assert cfg.rate_window == 60
    assert cfg.body_max == 256 * 1024
    assert cfg.ping_interval == 15
    assert cfg.resolver_url is None
    assert cfg.static_keys == {}


def test_env_overrides_and_static_keys():
    cfg = config.load(env={
        "MUSTER_RESOLVER_URL": "http://litellm:4000/v2/user/info",
        "MUSTER_STATIC_KEYS": '{"k1": {"user_id": "jc", "groups": ["ackstorm"]}}',
        "MUSTER_CHAT_RATE": "5",
    })
    assert cfg.resolver_url == "http://litellm:4000/v2/user/info"
    assert cfg.static_keys["k1"]["user_id"] == "jc"
    assert cfg.chat_rate == 5


def test_retention_must_cover_message_ttl():
    with pytest.raises(ValueError, match="agent_retention"):
        config.load(env={"MUSTER_AGENT_RETENTION": "3600", "MUSTER_MESSAGE_TTL": "7200"})
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_config.py -v`
Expected: FAIL (ImportError: no module `muster_api.config`)

- [ ] **Step 4: Implement** — `server/muster_api/config.py` (and empty `muster_api/__init__.py`, `tests/__init__.py`)

```python
"""Env-driven configuration. Defaults are pinned by spec v2 §17."""
import json
import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Config:
    valkey_url: str
    resolver_url: str | None
    resolver_header: str
    user_field: str
    groups_field: str
    static_keys: dict
    auth_cache_ttl: int
    message_ttl: int
    agent_retention: int
    inbox_maxlen: int
    chat_rate: int
    announce_rate: int
    rate_window: int
    body_max: int
    ping_interval: int
    presence_ttl: int


def load(env: Mapping = os.environ) -> Config:
    cfg = Config(
        valkey_url=env.get("MUSTER_VALKEY_URL", "redis://localhost:6379/1"),
        resolver_url=env.get("MUSTER_RESOLVER_URL") or None,
        resolver_header=env.get("MUSTER_RESOLVER_HEADER", "x-litellm-api-key"),
        user_field=env.get("MUSTER_USER_FIELD", "user_id"),
        groups_field=env.get("MUSTER_GROUPS_FIELD", "teams"),
        static_keys=json.loads(env.get("MUSTER_STATIC_KEYS", "{}")),
        auth_cache_ttl=int(env.get("MUSTER_AUTH_CACHE_TTL", "60")),
        message_ttl=int(env.get("MUSTER_MESSAGE_TTL", str(72 * 3600))),
        agent_retention=int(env.get("MUSTER_AGENT_RETENTION", str(7 * 86400))),
        inbox_maxlen=int(env.get("MUSTER_INBOX_MAXLEN", "1000")),
        chat_rate=int(env.get("MUSTER_CHAT_RATE", "20")),
        announce_rate=int(env.get("MUSTER_ANNOUNCE_RATE", "3")),
        rate_window=int(env.get("MUSTER_RATE_WINDOW", "60")),
        body_max=int(env.get("MUSTER_BODY_MAX", str(256 * 1024))),
        ping_interval=int(env.get("MUSTER_PING_INTERVAL", "15")),
        presence_ttl=int(env.get("MUSTER_PRESENCE_TTL", "60")),
    )
    if cfg.agent_retention < cfg.message_ttl:
        raise ValueError("agent_retention must be >= message_ttl (spec §7)")
    return cfg
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_config.py -v`
Expected: 3 PASS

- [ ] **Step 6: Commit**

```bash
git add server/pyproject.toml server/uv.lock server/muster_api/__init__.py server/muster_api/config.py server/tests/__init__.py server/tests/test_config.py
git commit -m "feat(server): scaffold muster-api with spec-pinned config"
```

---

### Task 2: Identity — address, reference matching, ACL (pure)

**Files:**
- Create: `server/muster_api/identity.py`, `server/tests/test_identity.py`

**Interfaces:**
- Produces:
  - `Address` frozen dataclass, fields `user, host, runtime, project, session` (all `str`); `str(addr)` → `"user/host/runtime/project/session"`.
  - `parse_agent_header(user: str, header: str) -> Address` — raises `AddressError` (subclass of `ValueError`) unless header is exactly 4 non-empty `/`-segments.
  - `matches(ref: str, addr: str) -> bool` — ref equals some contiguous `/`-joined slice of the 5 segments.
  - `visible(caller_user: str, caller_groups, agent_user: str, agent_groups) -> bool` — THE ACL predicate (spec §9).

- [ ] **Step 1: Write the failing test** — `server/tests/test_identity.py`

```python
import pytest
from muster_api import identity


def test_parse_agent_header_stamps_user():
    a = identity.parse_agent_header("jc", "laptop/claude/muster-chat/a3f9")
    assert str(a) == "jc/laptop/claude/muster-chat/a3f9"
    assert a.user == "jc" and a.project == "muster-chat"


@pytest.mark.parametrize("bad", ["", "laptop/claude/x", "a/b/c/d/e", "laptop//x/y", "jc/laptop/claude/x/y/z"])
def test_parse_agent_header_rejects_malformed(bad):
    with pytest.raises(identity.AddressError):
        identity.parse_agent_header("jc", bad)


def test_matches_contiguous_slices_only():
    addr = "jc/laptop/claude/muster-chat/a3f9"
    assert identity.matches("muster-chat", addr)              # single segment
    assert identity.matches("laptop/claude/muster-chat", addr)  # contiguous slice
    assert identity.matches("a3f9", addr)                       # session
    assert identity.matches(addr, addr)                         # full address
    assert not identity.matches("laptop/muster-chat", addr)     # non-contiguous
    assert not identity.matches("muster", addr)                 # no substring matching
    assert not identity.matches("", addr)


def test_visible_own_user_always_shared_group_otherwise():
    assert identity.visible("jc", [], "jc", [])                       # own agents, no groups needed
    assert identity.visible("jc", ["ackstorm"], "ana", ["ackstorm"])  # shared group
    assert not identity.visible("jc", ["ackstorm"], "bob", ["otherteam"])
    assert not identity.visible("jc", [], "ana", [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_identity.py -v`
Expected: FAIL (no module `muster_api.identity`)

- [ ] **Step 3: Implement** — `server/muster_api/identity.py`

```python
"""Address shape, reference resolution, and the one ACL predicate. Pure — no I/O.
Spec v2 §6 (identity), §6.1 (references), §9 (ACL)."""
from dataclasses import dataclass


class AddressError(ValueError):
    pass


@dataclass(frozen=True)
class Address:
    user: str
    host: str
    runtime: str
    project: str
    session: str

    def __str__(self) -> str:
        return "/".join((self.user, self.host, self.runtime, self.project, self.session))


def parse_agent_header(user: str, header: str) -> Address:
    """x-muster-agent = host/runtime/project/session. `user` comes from the resolver,
    never from the client (server-stamped, spec §6)."""
    parts = header.split("/")
    if len(parts) != 4 or not all(parts):
        raise AddressError("x-muster-agent must be host/runtime/project/session, all segments non-empty")
    return Address(user, *parts)


def matches(ref: str, addr: str) -> bool:
    """Shortest-unique-reference contract (§6.1): a ref matches iff it equals a
    contiguous '/'-joined slice of the address segments. No substring matching."""
    if not ref:
        return False
    segs = addr.split("/")
    return any("/".join(segs[i:j]) == ref for i in range(len(segs)) for j in range(i + 1, len(segs) + 1))


def visible(caller_user: str, caller_groups, agent_user: str, agent_groups) -> bool:
    """THE ACL predicate (§9). The only authorization rule in muster."""
    return caller_user == agent_user or bool(set(caller_groups) & set(agent_groups))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_identity.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add server/muster_api/identity.py server/tests/test_identity.py
git commit -m "feat(server): address parsing, reference matching, ACL predicate"
```

---

### Task 3: Auth — resolver client, cache, static dev keys

**Files:**
- Create: `server/muster_api/auth.py`, `server/tests/test_auth.py`

**Interfaces:**
- Consumes: `config.Config` (Task 1).
- Produces:
  - `Identity` frozen dataclass: `user_id: str`, `groups: tuple[str, ...]`.
  - `AuthError(status: int, code: str, message: str)` exception.
  - `Authenticator(cfg, http: httpx.AsyncClient | None)` with `async resolve(key: str) -> Identity`. Behavior: static keys first; then 60s in-memory cache keyed by sha256(key); resolver 400/401/403 ⇒ evict + `AuthError(401, "unauthorized", …)`; resolver 5xx/network error ⇒ `AuthError(503, "resolver_unavailable", …)` (fresh cache hits short-circuit before any resolver call); missing key ⇒ 401.

- [ ] **Step 1: Write the failing test** — `server/tests/test_auth.py`

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_auth.py -v`
Expected: FAIL (no module `muster_api.auth`)

- [ ] **Step 3: Implement** — `server/muster_api/auth.py`

```python
"""Delegated auth (spec §5.3–5.4): forward the caller's key to an external resolver,
cache (user_id, groups) 60s, fail closed. Muster stores no users, groups, or keys."""
import hashlib
import time
from dataclasses import dataclass

import httpx

from . import config as config_mod


@dataclass(frozen=True)
class Identity:
    user_id: str
    groups: tuple[str, ...]


class AuthError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def _dig(obj, path: str):
    for part in path.split("."):
        obj = obj[part]
    return obj


class Authenticator:
    def __init__(self, cfg: config_mod.Config, http: httpx.AsyncClient | None = None):
        self.cfg = cfg
        self.http = http or httpx.AsyncClient(timeout=5.0)
        self._cache: dict[str, tuple[Identity, float]] = {}  # sha256(key) -> (ident, fresh_until)

    async def resolve(self, key: str) -> Identity:
        if not key:
            raise AuthError(401, "unauthorized", "missing x-muster-api-key")
        if key in self.cfg.static_keys:  # local-dev mode (spec §16)
            entry = self.cfg.static_keys[key]
            return Identity(entry["user_id"], tuple(entry.get("groups", ())))
        kh = hashlib.sha256(key.encode()).hexdigest()  # never hold raw keys in memory maps/logs
        hit = self._cache.get(kh)
        if hit and hit[1] > time.monotonic():
            return hit[0]
        if not self.cfg.resolver_url:
            raise AuthError(401, "unauthorized", "no resolver configured and key not in static keys")
        try:
            resp = await self.http.get(self.cfg.resolver_url, headers={self.cfg.resolver_header: key})
        except httpx.HTTPError:
            raise AuthError(503, "resolver_unavailable", "identity resolver unreachable")  # fail closed
        if resp.status_code in (400, 401, 403):
            self._cache.pop(kh, None)  # "the resolver said no" is never served from cache
            raise AuthError(401, "unauthorized", "key refused by identity resolver")
        if resp.status_code >= 500:
            raise AuthError(503, "resolver_unavailable", f"resolver answered {resp.status_code}")
        data = resp.json()
        try:
            ident = Identity(str(_dig(data, self.cfg.user_field)),
                             tuple(_dig(data, self.cfg.groups_field) or ()))
        except (KeyError, TypeError):
            raise AuthError(502, "resolver_schema", "resolver response missing user/groups fields")
        self._cache[kh] = (ident, time.monotonic() + self.cfg.auth_cache_ttl)
        return ident
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_auth.py -v`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add server/muster_api/auth.py server/tests/test_auth.py
git commit -m "feat(server): delegated auth with resolver cache and fail-closed posture"
```

---

### Task 4: Store — registry, presence, connection race

**Files:**
- Create: `server/muster_api/store.py`, `server/tests/conftest.py`, `server/tests/test_store_registry.py`

**Interfaces:**
- Consumes: `identity.Address` (Task 2).
- Produces (all async, first arg `r` = `redis.asyncio` client, `decode_responses=True`):
  - Key helpers: `AGENTS = "muster2:agents"`, `agent_key(a)`, `presence_key(a)`, `inbox_key(a)`, `cursor_key(a)`, `rate_key(kind, a)`, `notify_channel(a)` — `a` is the full address string.
  - `register_agent(r, addr: Address, groups: list, meta: dict, retention: int) -> None`
  - `touch_presence(r, a: str, connection_id: str, ttl: int) -> None`
  - `clear_presence(r, a: str, connection_id: str) -> int` — deletes IFF connection_id matches (spec §7.1); returns 1 if deleted.
  - `list_agents(r) -> list[dict]` — dicts: `addr, user, host, runtime, project, session, groups (list), meta (dict), status ("online"|"offline")`. Skips addrs whose agent hash expired.

- [ ] **Step 1: Write the shared fixtures** — `server/tests/conftest.py`

```python
"""Shared fixtures. Requires the repo-root Valkey: `docker compose up -d`.
Tests use DB 3 and flush it — production uses DB 1."""
import pytest
import redis.asyncio as redis

TEST_VALKEY = "redis://localhost:6379/3"


@pytest.fixture
async def r():
    client = redis.from_url(TEST_VALKEY, decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()
```

- [ ] **Step 2: Write the failing test** — `server/tests/test_store_registry.py`

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `docker compose up -d && cd server && uv run pytest tests/test_store_registry.py -v`
Expected: FAIL (no module `muster_api.store`)

- [ ] **Step 4: Implement** — `server/muster_api/store.py` (registry half; inbox half comes in Task 5)

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_store_registry.py -v`
Expected: 4 PASS

- [ ] **Step 6: Commit**

```bash
git add server/muster_api/store.py server/tests/conftest.py server/tests/test_store_registry.py
git commit -m "feat(server): agent registry, presence, connection-race-safe cleanup"
```

---

### Task 5: Store — inbox, cursor, envelope, rate windows

**Files:**
- Modify: `server/muster_api/store.py` (append)
- Create: `server/tests/test_store_inbox.py`

**Interfaces:**
- Produces (appended to `store.py`):
  - `envelope(frm: str, body: str, subject: str | None, important: bool, subject_max: int = 56) -> str` — pure; v0 semantics: first line of subject-or-body, ≤56 chars, `· fetch for full` nudge when the line doesn't carry the whole body, `❗ ` prefix when important.
  - `append_message(r, to: str, fields: dict, maxlen: int, message_ttl: int) -> str` — XADD with `ts` + `expires_at` stamped; returns msg_id.
  - `publish_deliver(r, to: str, event: dict) -> None` — JSON on `notify_channel(to)`.
  - `fetch_unread(r, a: str, limit: int) -> list[dict]` — entries past cursor, advances cursor to last returned entry (fetch-as-ack, spec §11), filters messages past `expires_at`.
  - `unread_count(r, a: str) -> tuple[int, str]` — `(count, oldest_ts)` past cursor, no cursor movement (for the coalesced reconnect nudge).
  - `rate_check(r, kind: str, a: str, limit: int, window: int) -> tuple[bool, int]` — fixed-window INCR+EXPIRE; `(allowed, retry_after_seconds)`.

- [ ] **Step 1: Write the failing test** — `server/tests/test_store_inbox.py`

```python
from muster_api import store

A = "jc/laptop/claude/muster-chat/a3f9"


def test_envelope_short_body_carries_all():
    assert store.envelope("ana/x/y/z/1", "done", None, False) == "✉ ana/x/y/z/1: done"


def test_envelope_long_body_nudges_fetch():
    e = store.envelope("ana/x/y/z/1", "x" * 100, None, False)
    assert e.endswith(" · fetch for full") and "x" * 56 in e and "x" * 57 not in e


def test_envelope_important_marked():
    assert store.envelope("ana/x/y/z/1", "ship it", None, True).startswith("❗ ")


async def test_append_fetch_advances_cursor(r):
    m1 = await store.append_message(r, A, {"from": "ana/x/y/z/1", "kind": "chat", "body": "one"},
                                    maxlen=1000, message_ttl=3600)
    await store.append_message(r, A, {"from": "ana/x/y/z/1", "kind": "chat", "body": "two"},
                               maxlen=1000, message_ttl=3600)
    got = await store.fetch_unread(r, A, limit=10)
    assert [m["body"] for m in got] == ["one", "two"]
    assert got[0]["msg_id"] == m1
    assert await store.fetch_unread(r, A, limit=10) == []  # cursor advanced: nothing unread


async def test_unread_count_does_not_advance(r):
    await store.append_message(r, A, {"from": "f", "kind": "chat", "body": "b"}, 1000, 3600)
    count, oldest_ts = await store.unread_count(r, A)
    assert count == 1 and oldest_ts
    count2, _ = await store.unread_count(r, A)
    assert count2 == 1  # still unread


async def test_expired_message_not_returned(r):
    await store.append_message(r, A, {"from": "f", "kind": "chat", "body": "old"}, 1000, message_ttl=-1)
    assert await store.fetch_unread(r, A, 10) == []  # already past expires_at


async def test_rate_check_fixed_window(r):
    for _ in range(3):
        ok, _ = await store.rate_check(r, "chat", A, limit=3, window=60)
        assert ok
    ok, retry = await store.rate_check(r, "chat", A, limit=3, window=60)
    assert not ok and 0 < retry <= 60
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_store_inbox.py -v`
Expected: FAIL (AttributeError: `store` has no `envelope`)

- [ ] **Step 3: Implement** — append to `server/muster_api/store.py`

```python
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
    n = await r.incr(k)
    if n == 1:
        await r.expire(k, window)
    if n > limit:
        return False, max(await r.ttl(k), 1)
    return True, 0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_store_inbox.py -v`
Expected: 7 PASS

- [ ] **Step 5: Commit**

```bash
git add server/muster_api/store.py server/tests/test_store_inbox.py
git commit -m "feat(server): inbox streams, fetch-as-ack cursor, envelope, rate windows"
```

---

### Task 6: App skeleton + RPC chat/fetch end-to-end

**Files:**
- Create: `server/muster_api/ops.py`, `server/muster_api/app.py`, `server/tests/test_rpc_chat_fetch.py`

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces:
  - `ops.OpError(status: int, payload: dict)` — payload always has `code` and `message` keys.
  - `ops.dispatch(r, cfg, ident: Identity, sender: Address, op: str, args: dict) -> dict` — routes to `op_chat`, `op_fetch` (this task; roster/search Task 7, announce Task 8). Unknown op ⇒ `OpError(400, {"code": "unknown_op", …})`.
  - `app.create_app(cfg: Config | None = None) -> FastAPI` — factory. Routes: `POST /v1/rpc` (body `{"op": str, "args": dict}`), `GET /healthz`. App state: `app.state.cfg`, `app.state.redis`, `app.state.auth`. Auth errors → HTTP status + `{"code", "message"}` JSON; OpError → its status + payload.
  - Wire contract (used by Plan 2 shims): success bodies are `{"ok": true, …}`; `chat` returns `{"ok": true, "msg_id", "to", "status"}`; `fetch` returns `{"ok": true, "messages": [{"msg_id","from","kind","subject","body","ts","important"}]}`; error bodies `{"code", "message", …}` — rate errors add `retry_after`, `limit`, `window`; ambiguity adds `candidates` (list of agent dicts); not-found adds `visible` (list of addrs).

- [ ] **Step 1: Write the failing test** — `server/tests/test_rpc_chat_fetch.py`

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_rpc_chat_fetch.py -v`
Expected: FAIL (no module `muster_api.ops` / `muster_api.app`)

- [ ] **Step 3: Implement ops** — `server/muster_api/ops.py`

```python
"""RPC handlers. Every rule the server enforces lives here or in identity.visible —
reference resolution, ACL, size cap, self-send, rate limits, server-stamped from."""
from . import identity, store
from .auth import Identity
from .identity import Address


class OpError(Exception):
    def __init__(self, status: int, payload: dict):
        super().__init__(payload.get("message", payload["code"]))
        self.status, self.payload = status, payload


async def _visible_agents(r, ident: Identity) -> list[dict]:
    return [a for a in await store.list_agents(r)
            if identity.visible(ident.user_id, ident.groups, a["user"], a["groups"])]


def _resolve_reference(ref: str, agents: list[dict]) -> dict:
    hits = [a for a in agents if identity.matches(ref, a["addr"])]
    if not hits:
        raise OpError(404, {"code": "agent_not_found",
                            "message": f"no visible agent matches {ref!r}",
                            "visible": [a["addr"] for a in agents]})
    if len(hits) > 1:
        raise OpError(409, {"code": "ambiguous_reference",
                            "message": f"{ref!r} matches {len(hits)} agents; use a longer reference",
                            "candidates": hits})
    return hits[0]


async def _check_rate(r, kind: str, sender: str, limit: int, window: int):
    ok, retry = await store.rate_check(r, kind, sender, limit, window)
    if not ok:
        raise OpError(429, {"code": "message_rate_exceeded", "retry_after": retry,
                            "limit": limit, "window": window,
                            "message": "Unusually high agent messaging rate; possible message "
                                       "loop. Wait until the retry window expires before continuing."})


async def op_chat(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    body = args.get("body") or ""
    subject, important = args.get("subject"), bool(args.get("important"))
    if len(body.encode()) > cfg.body_max:
        raise OpError(413, {"code": "message_too_large",
                            "message": f"body is {len(body.encode())} bytes; cap is {cfg.body_max}"})
    target = _resolve_reference(args.get("to") or "", await _visible_agents(r, ident))
    if target["addr"] == str(sender):
        raise OpError(400, {"code": "self_send", "message": "target resolves to the sending agent"})
    await _check_rate(r, "chat", str(sender), cfg.chat_rate, cfg.rate_window)
    env = store.envelope(str(sender), body, subject, important)
    fields = {"from": str(sender), "kind": "chat", "body": body, "summary": env,
              "important": "1" if important else "0"}
    if subject:
        fields["subject"] = subject
    msg_id = await store.append_message(r, target["addr"], fields, cfg.inbox_maxlen, cfg.message_ttl)
    await store.publish_deliver(r, target["addr"], {
        "event": "deliver", "kind": "chat", "msg_id": msg_id,
        "from": str(sender), "envelope": env, "important": important})
    return {"ok": True, "msg_id": msg_id, "to": target["addr"], "status": target["status"]}


async def op_fetch(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    limit = min(int(args.get("limit") or 20), 100)
    msgs = await store.fetch_unread(r, str(sender), limit)
    return {"ok": True, "messages": [
        {"msg_id": m["msg_id"], "from": m.get("from", ""), "kind": m.get("kind", ""),
         "subject": m.get("subject", ""), "body": m.get("body", ""), "ts": m.get("ts", ""),
         "important": m.get("important") == "1"} for m in msgs]}


_OPS = {"chat": op_chat, "fetch": op_fetch}  # roster/search (Task 7), announce (Task 8) extend this


async def dispatch(r, cfg, ident: Identity, sender: Address, op: str, args: dict) -> dict:
    handler = _OPS.get(op)
    if handler is None:
        raise OpError(400, {"code": "unknown_op", "message": f"unknown op {op!r}",
                            "ops": sorted(_OPS)})
    return await handler(r, cfg, ident, sender, args)
```

- [ ] **Step 4: Implement app** — `server/muster_api/app.py`

```python
"""FastAPI wiring. Session-less: every request re-derives (identity, address) from
headers (spec §5). The stream route is added in Task 9, the reaper in Task 10."""
import os

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import config as config_mod, ops
from .auth import AuthError, Authenticator
from .identity import Address, AddressError, parse_agent_header


async def caller(request: Request) -> tuple:
    """(Identity, Address) for this request. user is stamped from the resolved key."""
    ident = await request.app.state.auth.resolve(request.headers.get("x-muster-api-key", ""))
    addr = parse_agent_header(ident.user_id, request.headers.get("x-muster-agent", ""))
    return ident, addr


def create_app(cfg: config_mod.Config | None = None) -> FastAPI:
    cfg = cfg or config_mod.load(os.environ)
    app = FastAPI(title="muster-api")
    app.state.cfg = cfg
    app.state.redis = redis.from_url(cfg.valkey_url, decode_responses=True)
    app.state.auth = Authenticator(cfg)

    @app.exception_handler(AuthError)
    async def _auth_err(request, exc: AuthError):
        return JSONResponse({"code": exc.code, "message": exc.message}, status_code=exc.status)

    @app.exception_handler(AddressError)
    async def _addr_err(request, exc: AddressError):
        return JSONResponse({"code": "bad_agent_header", "message": str(exc)}, status_code=400)

    @app.exception_handler(ops.OpError)
    async def _op_err(request, exc: ops.OpError):
        return JSONResponse(exc.payload, status_code=exc.status)

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.post("/v1/rpc")
    async def rpc(request: Request):
        ident, addr = await caller(request)
        payload = await request.json()
        return await ops.dispatch(request.app.state.redis, cfg, ident, addr,
                                  payload.get("op", ""), payload.get("args") or {})

    return app
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_rpc_chat_fetch.py -v`
Expected: 9 PASS. If exception handlers don't fire for `OpError` raised inside the route (FastAPI wraps some exceptions), convert `rpc` to try/except and return `JSONResponse(exc.payload, status_code=exc.status)` directly — behavior, not shape, is the contract.

- [ ] **Step 6: Run all tests**

Run: `cd server && uv run pytest -v`
Expected: all green (Tasks 1–6 suites).

- [ ] **Step 7: Commit**

```bash
git add server/muster_api/ops.py server/muster_api/app.py server/tests/test_rpc_chat_fetch.py
git commit -m "feat(server): /v1/rpc with chat + fetch, full guard rails end-to-end"
```

---

### Task 7: RPC roster + search

**Files:**
- Modify: `server/muster_api/ops.py`
- Create: `server/tests/test_rpc_roster_search.py`

**Interfaces:**
- Consumes: `_visible_agents` (Task 6).
- Produces: ops `roster {}` and `search {user?, project?, group?, runtime?, live?}` registered in `_OPS`. Both return `{"ok": true, "agents": [...]}` where each agent dict is `{addr, user, host, runtime, project, session, status, meta}` — `groups` is stripped (membership is resolver business, not roster content).

- [ ] **Step 1: Write the failing test** — `server/tests/test_rpc_roster_search.py`

```python
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
    agents = (await rpc(client, "k-jc", JC, "roster", {}))["agents"]
    assert {a["addr"] for a in agents} == {str(JC), str(ANA)}  # bob invisible
    assert all("groups" not in a for a in agents)
    ana = next(a for a in agents if a["user"] == "ana")
    assert ana["status"] == "online" and ana["meta"]["branch"] == "main"


async def test_search_filters(client):
    agents = (await rpc(client, "k-jc", JC, "search", {"runtime": "opencode"}))["agents"]
    assert [a["addr"] for a in agents] == [str(ANA)]
    agents = (await rpc(client, "k-jc", JC, "search", {"live": True}))["agents"]
    assert [a["addr"] for a in agents] == [str(ANA)]
    agents = (await rpc(client, "k-jc", JC, "search", {"project": "muster-chat", "user": "jc"}))["agents"]
    assert [a["addr"] for a in agents] == [str(JC)]


async def test_search_by_group(client):
    agents = (await rpc(client, "k-jc", JC, "search", {"group": "ackstorm"}))["agents"]
    assert {a["addr"] for a in agents} == {str(JC), str(ANA)}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_rpc_roster_search.py -v`
Expected: FAIL (`unknown_op` for roster/search → 400 assertion error)

- [ ] **Step 3: Implement** — add to `server/muster_api/ops.py`

```python
def _public(a: dict) -> dict:
    """Roster row: groups stripped — membership is resolver business, not roster content."""
    return {k: a[k] for k in ("addr", "user", "host", "runtime", "project", "session", "status", "meta")}


async def op_roster(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    return {"ok": True, "agents": [_public(a) for a in await _visible_agents(r, ident)]}


async def op_search(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    agents = await _visible_agents(r, ident)
    if args.get("user"):
        agents = [a for a in agents if a["user"] == args["user"]]
    if args.get("project"):
        agents = [a for a in agents if a["project"] == args["project"]]
    if args.get("runtime"):
        agents = [a for a in agents if a["runtime"] == args["runtime"]]
    if args.get("group"):
        agents = [a for a in agents if args["group"] in a["groups"]]
    if args.get("live"):
        agents = [a for a in agents if a["status"] == "online"]
    return {"ok": True, "agents": [_public(a) for a in agents]}
```

And extend the registry line:

```python
_OPS = {"chat": op_chat, "fetch": op_fetch, "roster": op_roster, "search": op_search}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_rpc_roster_search.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add server/muster_api/ops.py server/tests/test_rpc_roster_search.py
git commit -m "feat(server): roster and search ops, ACL-filtered directory"
```

---

### Task 8: RPC announce — ephemeral scoped broadcast

**Files:**
- Modify: `server/muster_api/ops.py`
- Create: `server/tests/test_rpc_announce.py`

**Interfaces:**
- Consumes: `store.publish_deliver`, `_visible_agents`, `_check_rate` (Tasks 5–6).
- Produces: op `announce {scope, project, body, subject?}` in `_OPS`. Scope auth (spec §12.2): `user:<x>` valid only when `x == caller.user_id`; `group:<g>` only when `g ∈ caller.groups`; else `OpError(403, {"code": "invalid_scope"})`. Eligible = visible ∩ scope ∩ `project` match ∩ online, minus sender. Full-body publish per recipient (`kind: "announce"`), **no inbox write, no cursor**. Returns `{"ok": true, "recipients": N}`. Rate: `announce_rate` (3/60s) bucket, distinct from chat.

- [ ] **Step 1: Write the failing test** — `server/tests/test_rpc_announce.py`

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_rpc_announce.py -v`
Expected: FAIL (`unknown_op`)

- [ ] **Step 3: Implement** — add to `server/muster_api/ops.py`

```python
async def op_announce(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    """Ephemeral scoped broadcast (spec §12.2): online recipients only, full body over
    SSE, no inbox write, no retention. Offline agents never receive it — by design."""
    scope, project = args.get("scope") or "", args.get("project") or ""
    body, subject = args.get("body") or "", args.get("subject")
    if len(body.encode()) > cfg.body_max:
        raise OpError(413, {"code": "message_too_large",
                            "message": f"body is {len(body.encode())} bytes; cap is {cfg.body_max}"})
    if scope == f"user:{ident.user_id}":
        def in_scope(a): return a["user"] == ident.user_id
    elif scope.startswith("group:") and scope[len("group:"):] in ident.groups:
        g = scope[len("group:"):]
        def in_scope(a): return g in a["groups"]
    else:
        raise OpError(403, {"code": "invalid_scope",
                            "message": "scope must be user:<self> or group:<one of your groups>"})
    await _check_rate(r, "announce", str(sender), cfg.announce_rate, cfg.rate_window)
    eligible = [a for a in await _visible_agents(r, ident)
                if in_scope(a) and a["project"] == project
                and a["status"] == "online" and a["addr"] != str(sender)]
    event = {"event": "deliver", "kind": "announce", "from": str(sender),
             "subject": subject or "", "body": body}
    for a in eligible:
        await store.publish_deliver(r, a["addr"], event)
    return {"ok": True, "recipients": len(eligible)}
```

And extend the registry:

```python
_OPS = {"chat": op_chat, "fetch": op_fetch, "roster": op_roster,
        "search": op_search, "announce": op_announce}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_rpc_announce.py -v`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add server/muster_api/ops.py server/tests/test_rpc_announce.py
git commit -m "feat(server): announce — ephemeral scoped broadcast to online agents"
```

---

### Task 9: SSE delivery stream

**Files:**
- Create: `server/muster_api/stream.py`, `server/tests/test_stream.py`
- Modify: `server/muster_api/app.py` (register route)

**Interfaces:**
- Consumes: `store.register_agent/touch_presence/clear_presence/unread_count/notify_channel`, `auth.Authenticator.resolve`, `app.caller`.
- Produces: `GET /v1/stream` — headers as RPC plus optional `x-muster-meta` (JSON; presence enrichment, read once at connect). On connect: register agent (retention refresh), write presence with fresh `connection_id`, subscribe pub/sub, emit coalesced `{"kind": "unread", "count", "oldest_ts"}` deliver event if unread > 0. Then: forward published deliver events verbatim; every `ping_interval`: re-resolve the key (spec §5.4 — cache-bounded; failure ⇒ `event: error` + close), refresh presence TTL, emit `: ping` comment. On disconnect: `clear_presence` guarded by connection_id.
- SSE wire format: `event: deliver\ndata: <json>\n\n` for events, `: ping\n\n` for keepalives. Response headers: `Cache-Control: no-cache`, `X-Accel-Buffering: no`.

- [ ] **Step 1: Write the failing test** — `server/tests/test_stream.py`

```python
import asyncio
import json

import httpx
import pytest
from muster_api import app as app_mod, config, store
from muster_api.identity import Address

STATIC = ('{"k-jc": {"user_id": "jc", "groups": ["ackstorm"]},'
          ' "k-ana": {"user_id": "ana", "groups": ["ackstorm"]}}')
JC = Address("jc", "laptop", "claude", "muster-chat", "a1")
ANA = Address("ana", "devbox", "opencode", "muster-chat", "b1")


def hdrs(key, addr):
    return {"x-muster-api-key": key,
            "x-muster-agent": f"{addr.host}/{addr.runtime}/{addr.project}/{addr.session}",
            "x-muster-meta": '{"branch": "main"}'}


@pytest.fixture
async def client(r):
    cfg = config.load(env={"MUSTER_VALKEY_URL": "redis://localhost:6379/3",
                           "MUSTER_STATIC_KEYS": STATIC, "MUSTER_PING_INTERVAL": "1"})
    application = app_mod.create_app(cfg)
    application.state.redis = r
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application),
                                 base_url="http://t", timeout=10) as c:
        yield c


async def read_events(response, n, timeout=5):
    """Collect n SSE deliver events (skip pings) from a streaming response."""
    events, buf = [], ""
    async with asyncio.timeout(timeout):
        async for chunk in response.aiter_text():
            buf += chunk
            while "\n\n" in buf:
                block, buf = buf.split("\n\n", 1)
                if block.startswith("event: deliver"):
                    events.append(json.loads(block.split("data: ", 1)[1]))
                if len(events) >= n:
                    return events
    return events


async def test_connect_registers_and_delivers_live_chat(client, r):
    async with client.stream("GET", "/v1/stream", headers=hdrs("k-ana", ANA)) as resp:
        assert resp.status_code == 200
        await asyncio.sleep(0.2)  # let registration land
        agents = await store.list_agents(r)
        assert agents and agents[0]["status"] == "online" and agents[0]["meta"] == {"branch": "main"}
        # jc chats ana while her stream is open → envelope arrives as deliver event
        await client.post("/v1/rpc", json={"op": "chat", "args": {"to": "b1", "body": "live one"}},
                          headers=hdrs("k-jc", JC))
        events = await read_events(resp, 1)
    assert events[0]["kind"] == "chat" and events[0]["from"] == str(JC)
    assert "live one" in events[0]["envelope"]
    # stream closed → presence cleaned up
    await asyncio.sleep(0.2)
    assert (await store.list_agents(r))[0]["status"] == "offline"


async def test_reconnect_gets_coalesced_unread_nudge(client, r):
    # two messages land while ana is offline
    for body in ("one", "two"):
        await client.post("/v1/rpc", json={"op": "chat", "args": {"to": "b1", "body": body}},
                          headers=hdrs("k-jc", JC))
    # ana must exist for jc to chat her: register her directly first
    # (order note: register, then send, then connect)
    async with client.stream("GET", "/v1/stream", headers=hdrs("k-ana", ANA)) as resp:
        events = await read_events(resp, 1)
    assert events[0]["kind"] == "unread" and events[0]["count"] == 2


@pytest.fixture
async def ana_registered(r):
    await store.register_agent(r, ANA, ["ackstorm"], {}, retention=3600)
```

Note for the implementer: `test_reconnect_gets_coalesced_unread_nudge` needs `ana_registered` before the chats — add it to that test's signature (`async def test_reconnect_gets_coalesced_unread_nudge(client, r, ana_registered)`) and keep the fixture last in the file.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_stream.py -v`
Expected: FAIL (404 — `/v1/stream` route doesn't exist)

- [ ] **Step 3: Implement** — `server/muster_api/stream.py`

```python
"""GET /v1/stream — the delivery leg (spec §5.2, §11). One long-lived SSE response per
connected agent; the serving pod subscribes to the agent's notify channel; cross-pod
fan-out rides Valkey pub/sub, never pod memory."""
import json
import time
import uuid

from fastapi import Request
from fastapi.responses import StreamingResponse

from . import store
from .auth import AuthError


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


async def stream_endpoint(request: Request, ident, addr) -> StreamingResponse:
    cfg, r = request.app.state.cfg, request.app.state.redis
    a = str(addr)
    connection_id = uuid.uuid4().hex
    try:
        meta = json.loads(request.headers.get("x-muster-meta") or "{}")
    except ValueError:
        meta = {}
    key = request.headers.get("x-muster-api-key", "")

    async def gen():
        await store.register_agent(r, addr, ident.groups, meta, cfg.agent_retention)
        await store.touch_presence(r, a, connection_id, cfg.presence_ttl)
        pubsub = r.pubsub()
        await pubsub.subscribe(store.notify_channel(a))
        try:
            count, oldest_ts = await store.unread_count(r, a)
            if count:  # coalesced nudge: a weekend of messages is ONE event (spec §11)
                yield _sse("deliver", {"event": "deliver", "kind": "unread",
                                       "count": count, "oldest_ts": oldest_ts})
            last_tick = time.monotonic()
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg:
                    yield f"event: deliver\ndata: {msg['data']}\n\n"  # forward verbatim
                if time.monotonic() - last_tick >= cfg.ping_interval:
                    try:
                        await request.app.state.auth.resolve(key)  # §5.4: cache-bounded re-auth
                    except AuthError as e:
                        yield _sse("error", {"code": e.code, "message": "stream closed: " + e.message})
                        return
                    await store.touch_presence(r, a, connection_id, cfg.presence_ttl)
                    yield ": ping\n\n"
                    last_tick = time.monotonic()
        finally:
            await pubsub.aclose()
            await store.clear_presence(r, a, connection_id)  # no-op if a successor took over

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
```

- [ ] **Step 4: Register the route** — add to `create_app` in `server/muster_api/app.py`

```python
    from . import stream as stream_mod

    @app.get("/v1/stream")
    async def stream_route(request: Request):
        ident, addr = await caller(request)
        return await stream_mod.stream_endpoint(request, ident, addr)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd server && uv run pytest tests/test_stream.py -v`
Expected: 2 PASS. Known trap: if the generator's `finally` doesn't run on client disconnect under ASGITransport, presence lingers until its TTL — assert with a small `await asyncio.sleep` and if it still fails, wrap the loop body with `if await request.is_disconnected(): return`.

- [ ] **Step 6: Run all tests**

Run: `cd server && uv run pytest -v`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add server/muster_api/stream.py server/muster_api/app.py server/tests/test_stream.py
git commit -m "feat(server): SSE delivery stream with presence, re-auth, unread nudge"
```

---

### Task 10: Reaper + app lifespan

**Files:**
- Create: `server/muster_api/reaper.py`, `server/tests/test_reaper.py`
- Modify: `server/muster_api/app.py` (lifespan)

**Interfaces:**
- Produces: `reaper.reap_once(r) -> int` — for every addr in `store.AGENTS` whose agent hash no longer exists (retention expired), delete inbox/cursor/presence keys and SREM from the set; returns number reaped. `reaper.run_forever(r, interval: int = 600)` — loop calling `reap_once`, exceptions logged, never crashes the app. `create_app` starts it in FastAPI lifespan and cancels on shutdown.

- [ ] **Step 1: Write the failing test** — `server/tests/test_reaper.py`

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && uv run pytest tests/test_reaper.py -v`
Expected: FAIL (no module `muster_api.reaper`)

- [ ] **Step 3: Implement** — `server/muster_api/reaper.py`

```python
"""GC for expired agent identities (spec §7): when the agent hash TTL has fired,
remove the orphaned inbox, cursor, presence, and directory entry. This — not
compliance machinery — is the v1 retention story."""
import asyncio
import logging

from . import store

log = logging.getLogger("muster.reaper")


async def reap_once(r) -> int:
    reaped = 0
    for a in await r.smembers(store.AGENTS):
        if not await r.exists(store.agent_key(a)):
            await r.delete(store.inbox_key(a), store.cursor_key(a), store.presence_key(a))
            await r.srem(store.AGENTS, a)
            reaped += 1
    if reaped:
        log.info("reaped %d expired agents", reaped)
    return reaped


async def run_forever(r, interval: int = 600) -> None:
    while True:
        try:
            await reap_once(r)
        except Exception:  # noqa: BLE001 — the reaper must never kill the app
            log.exception("reap cycle failed")
        await asyncio.sleep(interval)
```

- [ ] **Step 4: Wire lifespan** — modify `create_app` in `server/muster_api/app.py`: replace `app = FastAPI(title="muster-api")` with

```python
    import asyncio
    from contextlib import asynccontextmanager

    from . import reaper

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(reaper.run_forever(app.state.redis))
        yield
        task.cancel()

    app = FastAPI(title="muster-api", lifespan=lifespan)
```

(Keep the `app.state.*` assignments before the first request; assigning after `create_app` returns is fine because lifespan runs at server startup, not at factory time. If ordering bites — `app.state.redis` unset inside lifespan — move the redis client creation above the `FastAPI(...)` call and close it after `yield`.)

- [ ] **Step 5: Run all tests**

Run: `cd server && uv run pytest -v`
Expected: all green (reaper test + no regressions from the lifespan change).

- [ ] **Step 6: Commit**

```bash
git add server/muster_api/reaper.py server/muster_api/app.py server/tests/test_reaper.py
git commit -m "feat(server): reaper GC for expired agents, wired into app lifespan"
```

---

### Task 11: Packaging — Dockerfile, compose, Helm chart, smoke script

**Files:**
- Create: `server/Dockerfile`, `server/README.md`, `helm/muster-api/Chart.yaml`, `helm/muster-api/values.yaml`, `helm/muster-api/templates/deployment.yaml`, `helm/muster-api/templates/service.yaml`, `helm/muster-api/templates/ingress.yaml`
- Modify: `docker-compose.yml` (repo root)

**Interfaces:**
- Consumes: the finished app (`muster_api.app:create_app` factory).
- Produces: `docker compose up -d` at repo root serves muster-api on `127.0.0.1:8765` with static dev keys, next to Valkey (spec §16 local-dev requirement). Helm chart deploys the same image with resolver-based auth.

- [ ] **Step 1: Write `server/Dockerfile`** (multi-stage, explicit COPYs, no `COPY . .`)

```dockerfile
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project -o requirements.txt \
 && pip install --no-cache-dir --target=/app/deps -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
COPY --from=builder /app/deps /usr/local/lib/python3.12/site-packages
COPY muster_api/ ./muster_api/
EXPOSE 8765
CMD ["uvicorn", "muster_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8765"]
```

- [ ] **Step 2: Add the service to root `docker-compose.yml`** (append under `services:`)

```yaml
  muster-api:
    build: ./server
    ports:
      - "127.0.0.1:8765:8765"
    environment:
      MUSTER_VALKEY_URL: redis://valkey:6379/1
      # local-dev static keys (spec §16): no LiteLLM needed
      MUSTER_STATIC_KEYS: '{"dev-key": {"user_id": "dev", "groups": ["local"]}}'
    depends_on:
      valkey:
        condition: service_healthy
    restart: unless-stopped
```

- [ ] **Step 3: Smoke-test the composed stack**

```bash
docker compose up -d --build
for i in $(seq 1 15); do
  curl -sf http://127.0.0.1:8765/healthz && break
  sleep 2
done
[ $i -eq 15 ] && { echo "FAIL: muster-api never healthy" >&2; exit 1; }
# roster with the dev key (empty until an agent connects a stream)
curl -sf -X POST http://127.0.0.1:8765/v1/rpc \
  -H 'x-muster-api-key: dev-key' -H 'x-muster-agent: local/curl/smoke/1' \
  -H 'content-type: application/json' -d '{"op": "roster", "args": {}}'
# live stream + chat roundtrip, two fake agents:
curl -sN http://127.0.0.1:8765/v1/stream \
  -H 'x-muster-api-key: dev-key' -H 'x-muster-agent: local/curl/smoke/receiver' &
STREAM_PID=$!; sleep 1
curl -sf -X POST http://127.0.0.1:8765/v1/rpc \
  -H 'x-muster-api-key: dev-key' -H 'x-muster-agent: local/curl/smoke/sender' \
  -H 'content-type: application/json' \
  -d '{"op": "chat", "args": {"to": "receiver", "body": "smoke says hi"}}'
sleep 1; kill $STREAM_PID
```

Expected: healthz OK; roster returns `{"ok": true, "agents": [...]}` with the receiver once its stream is up; the backgrounded stream prints an `event: deliver` block containing `smoke says hi`.

- [ ] **Step 4: Write the Helm chart** (minimal; SSE ingress annotations are the load-bearing part, spec §16)

`helm/muster-api/Chart.yaml`:

```yaml
apiVersion: v2
name: muster-api
description: Muster central agent coordination bus
version: 0.1.0
appVersion: "0.1.0"
```

`helm/muster-api/values.yaml`:

```yaml
image:
  repository: muster-api
  tag: "0.1.0"
replicas: 2
valkeyUrl: redis://muster-valkey:6379/1
auth:
  resolverUrl: http://litellm.litellm.svc:4000/v2/user/info
  resolverHeader: x-litellm-api-key
  userField: user_id
  groupsField: teams
ingress:
  enabled: true
  host: muster.example.com
  className: nginx
```

`helm/muster-api/templates/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}
spec:
  replicas: {{ .Values.replicas }}
  selector:
    matchLabels: {app: {{ .Release.Name }}}
  template:
    metadata:
      labels: {app: {{ .Release.Name }}}
    spec:
      containers:
        - name: muster-api
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          ports: [{containerPort: 8765}]
          env:
            - {name: MUSTER_VALKEY_URL, value: {{ .Values.valkeyUrl | quote }}}
            - {name: MUSTER_RESOLVER_URL, value: {{ .Values.auth.resolverUrl | quote }}}
            - {name: MUSTER_RESOLVER_HEADER, value: {{ .Values.auth.resolverHeader | quote }}}
            - {name: MUSTER_USER_FIELD, value: {{ .Values.auth.userField | quote }}}
            - {name: MUSTER_GROUPS_FIELD, value: {{ .Values.auth.groupsField | quote }}}
          readinessProbe:
            httpGet: {path: /healthz, port: 8765}
```

`helm/muster-api/templates/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ .Release.Name }}
spec:
  selector: {app: {{ .Release.Name }}}
  ports: [{port: 80, targetPort: 8765}]
```

`helm/muster-api/templates/ingress.yaml`:

```yaml
{{- if .Values.ingress.enabled }}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {{ .Release.Name }}
  annotations:
    # SSE (spec §16): never buffer the stream, keep it open well past the 15s ping
    nginx.ingress.kubernetes.io/proxy-buffering: "off"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
spec:
  ingressClassName: {{ .Values.ingress.className }}
  rules:
    - host: {{ .Values.ingress.host }}
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: {{ .Release.Name }}
                port: {number: 80}
{{- end }}
```

- [ ] **Step 5: Lint the chart**

Run: `helm lint helm/muster-api` (if helm is unavailable locally, `helm template helm/muster-api` via docker: `docker run --rm -v "$PWD/helm/muster-api:/chart" alpine/helm:latest template /chart`)
Expected: 0 chart(s) failed.

- [ ] **Step 6: Write `server/README.md`**

```markdown
# muster-api

Central Muster coordination bus. Spec: `../docs/superpowers/specs/2026-08-27-muster-v1-central-bus-spec-v2.md`.

## Run locally

    docker compose up -d          # from the repo root: Valkey + muster-api on 127.0.0.1:8765
    # dev auth: MUSTER_STATIC_KEYS maps "dev-key" -> user "dev", group "local"

## Tests

    docker compose up -d valkey
    cd server && uv run pytest -v      # uses Valkey DB 3 and flushes it

## API in 30 seconds

    POST /v1/rpc     {"op": "roster"|"search"|"chat"|"fetch"|"announce", "args": {...}}
    GET  /v1/stream  SSE: deliver events (chat envelopes, announces, unread nudge) + pings
    Headers on everything: x-muster-api-key, x-muster-agent: host/runtime/project/session
    (x-muster-meta JSON on the stream connect only)
```

- [ ] **Step 7: Full verification + commit**

Run: `cd server && uv run pytest -v && cd .. && docker compose up -d --build && curl -sf http://127.0.0.1:8765/healthz`
Expected: all tests green, healthz `{"ok":true}`.

```bash
git add server/Dockerfile server/README.md helm/ docker-compose.yml
git commit -m "feat(server): Dockerfile, compose service, Helm chart with SSE ingress config"
```

---

## Self-Review (performed at plan-writing time)

- **Spec coverage:** §5 protocol → Tasks 1, 6, 9. §5.3–5.4 auth → Tasks 3, 9. §6 identity/references → Tasks 2, 6. §7 lifetimes → Tasks 4, 5, 10 (retention TTL, message TTL, presence TTL, reaper). §7.1 connection race → Task 4. §9 ACL → Tasks 2, 6, 7, 8. §10 data model → Tasks 4–5. §11 delivery semantics → Tasks 5, 6, 9 (fetch-as-ack, coalesced nudge). §12.1–12.2 flows → Tasks 6, 8. §12.3 gateway → **Plan 3, out of scope here**. §13 receiver behavior → shim concern, **Plan 2**. §14 anti-loop → Tasks 5, 6, 8 (rates, size cap, self-send; no content dedup). §15 security → Tasks 3, 6 (stamped from, key hashing). §16 deployment → Task 11. §17 defaults → Task 1. Presence join/leave events (§5.2, marked optional) → deliberately deferred, noted in spec as optional.
- **Placeholder scan:** every code step carries full code; the two "note for the implementer" blocks are instructions, not gaps.
- **Type consistency:** `Identity(user_id, groups: tuple)`, `Address` 5-field dataclass, `store` signatures, and the `{"ok": true, ...}` wire shapes are used identically across Tasks 3–9; `dispatch(r, cfg, ident, sender, op, args)` matches the app wiring in Task 6.
