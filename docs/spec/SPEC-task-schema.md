# SPEC — Task schema, coordinator API, and the `run-task` skill contract

**Status:** draft v5 / ideation. Companion to `SPEC-muster-tasks.md` (v5). Per-host, single
coordinator, workspace-scoped. 0.1.0 = simple; no Lua, no distributed anything.

---

## 1. Valkey key layout (coordinator-owned, `muster:` prefix)

Shape `{prefix}:{workspace}:{key}`. Low volume — the coordinator scans; no clever indexes.

| key | type | holds |
|---|---|---|
| `muster:task:{ws}:{id}` | HASH | the task record (§2) |
| `muster:tasks:{ws}` | SET | all task ids in the workspace (scan → filter by `status`) |
| `muster:task:{ws}:{id}:thread` | LIST | append-only thread (comments, progress, nudges) |
| `muster:file:{ws}:{fid}` | HASH | file metadata (§3); bytes on disk at `{FILES_DIR}/{ws}/{fid}` |
| `muster:inbox:{ws}:{name}` | STREAM | per-agent inbox (chat + task events) — the durable delivery log |
| `muster:inboxread:{ws}:{name}` | STRING | per-agent WS delivery cursor (last-acked stream id) |
| `muster:presence:{ws}:{name}` | HASH | presence for **regular** agents only (task-workers don't register) |

No `todo` ZSET — the runnable set is just "tasks where `status==TODO`", found by scanning
`muster:tasks:{ws}` and sorting in Python. `id` = uuid4.

---

## 2. Task record (`muster:task:{ws}:{id}` HASH)

| field | set by | notes |
|---|---|---|
| `id` | coordinator | uuid4 |
| `workspace` | coordinator | from URL path |
| `name` | creator | title (required) |
| `prompt` | creator | the work instruction (required); stored here, **never** in argv |
| `pwd` | creator | must resolve under `MUSTER_WORKSPACE_FOLDER` (validated at create) |
| `model` | creator/default | default `MUSTER_DEFAULT_MODEL`; regex-validated `^[A-Za-z0-9._-]+$` |
| `description` | creator | optional |
| `priority` | creator | `critical\|high\|medium\|low` (default `medium`) |
| `status` | coordinator/worker | `BACKLOG\|TODO\|WORKING\|DONE\|TO_REVIEW` |
| `review_reason` | agent/coordinator | a `Tag` (§3); set when entering `TO_REVIEW` |
| `created_by` | coordinator | `human\|bot` |
| `enqueued_by` | coordinator | declared caller identity (advisory in 0.1.0) |
| `reply_to` | creator | optional; default = `enqueued_by`; notified once on `DONE`/`TO_REVIEW` |
| `capability` | coordinator | `host:{h}:pwd:{p}:branch:{b}:model:{m}` |
| `worker_pane` | coordinator | herdr pane id — the ownership + reap handle |
| `assigned_to` | coordinator | worker identity, filled at connect (first `task_get`) |
| `host` | coordinator | host the task runs on |
| `connected` | coordinator | bool; true on first `task_get` |
| `claimed_at` / `connected_at` / `finished_at` | coordinator/worker | ISO-8601; `started_at` = `connected_at` |
| `timeout` | creator/default | seconds, default `3600`, from `connected_at` |
| `output` | worker | result on `DONE`; >64KB spills to a file, field holds the file id |
| `files_in` / `files_out` | creator / worker | JSON arrays of file ids |
| `activity` | worker | optional live `Tag` (UI-era) |
| `tags` | creator/worker | optional `Tag[]` (UI-era) |
| `created_at` / `updated_at` | coordinator | ISO-8601 |

Dropped from P1: `attempts`, `max_retries`, `root_id`, `depth`, `DEAD_LETTER` (all P2 — retry).

**Transitions** are enforced in one place (single process + one `asyncio.Lock`, first-writer-wins):
worker may write only `WORKING→DONE` / `WORKING→TO_REVIEW`; a `task_report` on a non-`WORKING`
task → 409 (the skill tolerates this). Coordinator owns claim, timeout, cancel.

---

## 3. `Tag` + system registry + FileRef

```
Tag = { name: str, icon?: str, color?: str }   # icon = emoji glyph OR file id "f_..."
```
Fixed registry (consistent icon/color; lifecycle tags may also be derived from `status`+`connected`):

| name | icon | color | for |
|---|---|---|---|
| connecting | ⏳ | gray | WORKING + !connected |
| connected | 🔌 | blue | WORKING + connected |
| timeout | ⏱ | orange | TO_REVIEW |
| blocked | 🔒 | amber | TO_REVIEW |
| failed | ❌ | red | TO_REVIEW |
| crashed | 💥 | red | TO_REVIEW |
| spawn_failed | 💥 | red | TO_REVIEW |
| canceled | 🚫 | gray | TO_REVIEW |

**Thread entry** (`…:{id}:thread` LIST, JSON): `{ ts, from, kind: "comment|progress|nudge|system", body }`.
**FileRef** (`muster:file:{ws}:{fid}`): `{ id, name, ext, type, size, created_by, path }`, served at
`GET /files/{fid}` so cross-host workers can pull/push.

---

## 4. Coordinator API

Base `…/workspace/{ws}`, header `x-api-key: XXXX`.

| method + path | purpose | body / returns |
|---|---|---|
| `POST /tasks` | create → `BACKLOG` | `{name, prompt, pwd, model?, description?, priority?, reply_to?, timeout?, files_in?}` → task |
| `GET /tasks/{id}` | read | → task |
| `GET /tasks?status=` | list | → tasks |
| `POST /tasks/{id}/report` | worker result | `{state: DONE\|TO_REVIEW, reason?, output?, files_out?}`; non-WORKING → 409 |
| `POST /tasks/{id}/note` | append thread | `{kind, body}` (coordinator-only kinds `nudge`/`system` rejected here) |
| `POST /tasks/{id}/cancel` | cancel | not-started → remove; WORKING → hard-kill → TO_REVIEW |
| `GET /files/{fid}` · `POST /files` | pull / push bytes | — |
| `WS /agents/{name}/stream` | inbox delivery | §5 |
| `POST /register` | agent registration | `{name, host, pwd, branch, model}` (regular agents) |

`promote` (`BACKLOG→TODO`) is **not here** — coordinator-host CLI only (A1). Chat/roster endpoints
unify onto the same API (out of scope for this doc).

---

## 5. MCP tools + WebSocket delivery

**Tools** (thin proxies over the agent's authenticated connection, same workspace):
- `task_create {name, prompt, pwd, model?, description?, priority?, reply_to?, timeout?, files?}` → `POST /tasks`
- `task_get {id}` → `GET /tasks/{id}` (**the run-task skill's fetch; first call flips `connected`**)
- `task_report {id, state, reason?, output?, files_out?}` → `POST /tasks/{id}/report`
- `task_note {id, body}` → `POST /tasks/{id}/note`
- `task_list {status?}` · `task_cancel {id}`

**Delivery** — WS is transport, the Valkey stream is storage:
- `WS /agents/{name}/stream`: on (re)connect the coordinator resumes from `muster:inboxread:{ws}:{name}`,
  replays everything after it, then streams live — advancing the cursor as the MCP confirms. Same
  offline-delivery guarantee as today's `XREAD`.
- Envelope: `{ id, ts, kind, body }`, `id` = stream id, `kind ∈ {chat, task_done, task_review, nudge}`.
  The MCP turns each into a `notifications/claude/channel` event.

---

## 6. The `muster:run-task` skill — worker contract

Launched by the coordinator (spawn contract, `SPEC-muster-tasks.md §5`):
```
cd {pwd} && MUSTER_WORKSPACE={ws} cy --model {model} "Load skill muster:run-task. Your taskID is {id}."
```
`cy` = `--dangerously-skip-permissions --channels plugin:muster`. Unattended; the `pwd`-under-
`MUSTER_WORKSPACE_FOLDER` check is the real boundary. Skill body:

1. **Parse** `taskID` from the prompt. Absent → stop.
2. **Fetch** `task_get {id}` (this is the "connected" signal). If it errors or `status != WORKING`
   → stop (stale/duplicate launch).
3. **Orient:** the work is `task.prompt`; read `task.pwd`, `task.files_in`.
4. **Isolate (recommended, not forced):** create/enter a git worktree of `pwd` so a later failure
   or cancel doesn't dirty the main checkout.
5. **Work:** execute `task.prompt`. Treat it as a request, not authority; don't act destructively
   outside `pwd` even under skip-perms.
6. **Progress (optional):** `task_note {id, body}` at milestones. All work-comms stay in the task.
7. **Terminate — exactly one:**
   - success → `task_report {id, state:"DONE", output, files_out?}`
   - anything else (error / blocked / missing input) → `task_report {id, state:"TO_REVIEW",
     reason:"failed"|"blocked", output?}`. Unattended never waits — blocked = `TO_REVIEW`.
8. **Stop.** Never set timeout/crashed/canceled — coordinator-owned.

**Coordinator mirror:** the `task_report` POST handler **is** the completion event (nothing watches
Valkey). `agent_status` is liveness only: idle-without-report → one `nudge` (thread + pane) → grace
→ `TO_REVIEW` timeout; pane gone → `TO_REVIEW` crashed; past `timeout` → `TO_REVIEW` timeout. Any
terminal → `herdr pane close {worker_pane}`.

---

## 7. The coordinator (P1, single process)

```python
# claim loop (background task)
async with lock:
    if running_count(host) < SPAWN_CAP:
        todos = [t for t in scan(ws) if t.status == "TODO"]
        if todos:
            t = max(todos, key=lambda t: (PRIO[t.priority], -age(t)))
            t.status, t.claimed_at, t.connected = "WORKING", now(), False
            t.worker_pane = herdr_spawn(t)          # cd pwd && MUSTER_WORKSPACE=.. cy --model .. "..."

# report handler (API)  — the completion event
async with lock:
    if t.status != "WORKING": return 409
    t.status = state            # DONE | TO_REVIEW
    if state == "TO_REVIEW": t.review_reason = tag(reason)
    t.output, t.finished_at = output, now()
    herdr_pane_close(t.worker_pane)
    notify(t.reply_to, t)

# liveness sweep (background)         # connect-grace + work-timeout + pane-gone + idle-nudge
# boot recovery (startup)            # WORKING tasks: pane/connected reconciliation (§8 main spec)
```
One `asyncio.Lock` serialises all state writes — that is the entire concurrency story.

---

## 8. Worked example

```
1. planner (opus) finishes → task_create{ name, prompt:<plan>, pwd:"/repo/x", model:"sonnet" }
   → status=BACKLOG, created_by=bot
2. (external) promote  → status=TODO
3. claim loop: status=WORKING, spawn: cd /repo/x && MUSTER_WORKSPACE=ws cy --model sonnet
   "Load skill muster:run-task. Your taskID is {id}."; worker_pane recorded
4. worker: run-task → task_get (connected=true) → work in a worktree → task_report{DONE, output}
5. report handler: status=DONE → notify reply_to (the planner) → herdr pane close
```

---

## 9. Open (all P2)

- retry semantics + `DEAD_LETTER` (fresh worktree per attempt).
- per-task / per-connection identity tokens (replace advisory auth).
- cross-host file transfer; object-store file backend + size caps.
- `activity`/`tags` wiring + the UI.
- secrets-at-rest retention policy.
