# SPEC — Muster Tasks (coordinator + task plane)

**Status:** draft v5 / ideation, implementation-ready for P1. Supersedes `docs/TODO-task-queue.md`
and earlier drafts. Companion: `SPEC-task-schema.md` (keys, API, skill contract, coordinator loop).
**Reference (not adopted):** legacy `../agent-backlog`.

---

## 0. The architectural inflection

Muster's chat bus is **daemon-less** (each agent's MCP talks to Valkey directly). Tasks break
that deliberately: they need cross-host reach, api-key auth, server-side logic, and a UI later.
So the task subsystem introduces a **coordinator daemon**, and **all** bus traffic (chat included)
moves behind its API. Daemon-less ends; that is a conscious trade.

**Scope discipline for 0.1.0:** single coordinator, one host, low volume. No Lua scripting, no
distributed locks, no HA, no multi-coordinator. In-process correctness only.

---

## 1. Architecture

```
┌─ Coordinator (fresh FastAPI daemon, ONE per host) ───────────────────────────┐
│    owns: Valkey (local, never exposed) · task lifecycle · the claim loop ·   │
│          spawn/reap via local herdr · api-key auth · file store              │
└──────────────────────────────────────────────────────────────────────────────┘
          ▲ HTTP + WebSocket  (x-api-key)
    ┌─────┴──────── local MCP server (per agent, stdio) — THIN CLIENT ─────────┐
    │   • WebSocket to the coordinator for this agent's inbox → emits native    │
    │     notifications/claude/channel events into the session                  │
    │   • exposes tools (chat, roster, fetch, task_*) that proxy to the API     │
    │   • NO direct Valkey                                                       │
    └── Claude session ─────────────────────────────────────────────────────────┘
```

- **Coordinator** = brain + Valkey owner + herdr driver + auth boundary. Fresh FastAPI, written
  from scratch (legacy `agent-backlog` is a reference for schema/file-handling only).
- **Local MCP** = mandatory: `notifications/claude/channel` can only be emitted from inside the
  session, so a remote coordinator can never push. The MCP becomes the coordinator's per-session
  delivery arm + tool proxy. It holds a **WebSocket** to the coordinator instead of `XREAD`.

---

## 2. Core principles (decided)

1. **Task = self-contained envelope.** `input` (prompt + files) → `output` (result + files) +
   `thread` + `status`. No work-communication outside the task.
2. **Agents create tasks too** (`task_create`, `created_by=bot`), always into `BACKLOG`.
3. **Everything behind the coordinator API** — chat, presence, tasks. MCP is a thin client.
4. **Interactive Claude workers, not `claude -p`.** Completion comes from the task record, not a
   process exit.
5. **Two separate statuses.** *Task status* (worker-updated via `task_report`) is the sole
   completion signal. *herdr `agent_status`* is worker lifecycle/reaping + liveness only.
6. **Tasks require herdr on the host; chat does not.**
7. **Worktrees by instruction** (the `run-task` skill recommends one); never harness-managed.
8. **Single coordinator, in-process correctness.** No Lua, no distributed locks (0.1.0).
9. **No retry in P1.** Anything that isn't clean success goes to `TO_REVIEW` for a human.

---

## 3. Identity, workspace, capability, auth

- **Workspace = a bare scope label.** Defines nothing (no model, policy, or repo config). Selected
  per request via the URL path `…/workspace/{name}/…`. Env `MUSTER_WORKSPACE` (default `local`).
- **Identity** = `repo`/`repo~worktree`/`basename(cwd)` + `-pid:{pid}`, host-qualified `{host}/{name}`.
- **Auth = `x-api-key` validates the peer, nothing more** (advisory / perimeter model — the
  coordinator is localhost/VPN-only in 0.1.0). Identity is declared; per-role enforcement is
  advisory. Real identity binding (per-task tokens, per-connection sessions) is P2.
- **Placement capability** = `host:{h}:pwd:{p}:branch:{b}:model:{m}` — a plain string, the only
  federation residue kept now. **Feature capability** (`requires:kubectl`) — deferred.
- **Coordinator config** (not on the workspace): `MUSTER_WORKSPACE_FOLDER` (root dir; every task
  `pwd` must resolve under it — A3), `MUSTER_DEFAULT_MODEL`, `MUSTER_SPAWN_CAP` (default 2),
  optional `MUSTER_ALLOWED_MODELS`.

---

## 4. Lifecycle — 5 states

```
create ─► BACKLOG ──(promote, external)──► TODO ──(coordinator claims)──► WORKING ─► DONE
                                                                            │
                                                                            └──► TO_REVIEW (+ reason Tag)
```

- **BACKLOG** — where every created task lands. Not runnable. **This column is the safety brake**:
  a confused/injected agent can create tasks but none execute until externally promoted.
- **TODO** — promoted (by whom = out of scope). Runnable.
- **WORKING** — claimed and executing.
- **DONE** — clean success. Terminal.
- **TO_REVIEW** — needs a human/other to act. Terminal for the coordinator. **All non-success
  outcomes fold in here** with a `review_reason` Tag: `failed | blocked | timeout | crashed |
  spawn_failed | canceled`. There is **no** `FAILED`/`TIMED_OUT`/`CANCELED`/`DEAD_LETTER` state,
  and no retry, in P1.

**Who writes each transition** (single process, one `asyncio.Lock`, first-writer-wins):

| transition | who |
|---|---|
| create → `BACKLOG` | creator (`task_create`) |
| `BACKLOG → TODO` | external promoter (out of scope; not a shared-key HTTP endpoint — A1) |
| `TODO → WORKING` | **coordinator only** — the claim (the lock) |
| `WORKING → DONE` | **agent** (`task_report{DONE}`) |
| `WORKING → TO_REVIEW` | **agent** (`task_report{TO_REVIEW, reason}`) or **coordinator** (timeout/crash/idle/cancel) |
| cancel, not started | enqueuer → remove the task |
| cancel, `WORKING` | enqueuer → hard-kill worker → `TO_REVIEW` reason=`canceled` |

The worker's `task_report` says only **`DONE`** or **`TO_REVIEW`(+reason)** — never `FAILED`.

### Tags

`Tag = { name: str, icon?: str, color?: str }`. `icon` is an emoji glyph *or* a file-store id for a
custom image. A fixed **system-tag registry** gives consistent icon/color for the lifecycle +
review reasons (see `SPEC-task-schema.md §3`). `review_reason` is a `Tag`. `activity` (live "what
am I doing") and general `tags[]` are defined but wired later (UI-era).

---

## 5. Execution

### The claim loop (single coordinator, no Lua)

```
every ~1s, if running_count(host) < MUSTER_SPAWN_CAP:
    todos = [t in workspace tasks where status == TODO]
    if not todos: continue
    t = max(todos, key = priority-then-age)
    t.status = WORKING; t.claimed_at = now; t.connected = false     # the claim = the lock
    pane = herdr spawn (§ spawn contract); t.worker_pane = pane
```
Atomic because it's a single asyncio process (no `await` mid check-set) + one `asyncio.Lock` around
all task-state writes. No queue popping tricks, no claim key, no TTL, no heartbeat.

### Spawn contract

```
cd {pwd} && MUSTER_WORKSPACE={ws} cy --model {model} "Load skill muster:run-task. Your taskID is {id}."
```
- `model` regex-validated (`^[A-Za-z0-9._-]+$`) + all interpolated values `shlex.quote`d (A4).
- `MUSTER_WORKSPACE` env so the worker's MCP hits the right scope. api-key is ambient host config.
- Coordinator records `worker_pane` (the reap handle), `host`, `capability`. `assigned_to` fills in
  at connect. **`kind=task-worker` needs no worker flag** — a pane is a task-worker iff it's a
  `worker_pane` the coordinator recorded; reaping only ever touches that set, so human panes are
  never reaped.

### Connected + two-phase liveness

- `connected` flips true on the worker's **first `task_get`** (also sets `assigned_to`, `connected_at`).
- **Connect-grace** (`WORKING` + `!connected` past ~90s) → spawn/boot failed, nothing ran →
  `TO_REVIEW` reason=`spawn_failed`.
- **Work timeout** (`WORKING` + `connected`) → the `timeout` (default 3600s) measured **from
  `connected_at`**; also idle-after-nudge and pane-gone → `TO_REVIEW` (reason `timeout`/`crashed`).

### Reaping & cancel

- Any terminal (`DONE`/`TO_REVIEW`) → `herdr pane close {worker_pane}`.
- Cancel `WORKING` → hard-kill (`pane close`) immediately → `TO_REVIEW` reason=`canceled`, note
  "tree may be dirty at `{pwd}`". No graceful handshake, no auto-cleanup.

---

## 6. Placement, files, cost

- **Placement is spawn-only in P1.** No assign-to-live-agent (that path has no reap/timeout model).
- **`pwd` boundary** = must resolve under `MUSTER_WORKSPACE_FOLDER` (A3), checked at `task_create`.
- **Files = shared store behind the API** (disk in P1 → object store later); cross-host pull/push
  via `GET/POST /files`. `output` >64KB spills to a file, `output` holds the file id.
- **Spawn cap** per host (`MUSTER_SPAWN_CAP`, default 2) — cost control; an interactive worker is a
  full Claude session (~$4/task) burning the human's own rate-limit pool.

---

## 7. Security (0.1.0 = advisory perimeter)

- **Perimeter trust.** `x-api-key` validates the peer; the coordinator is not publicly exposed.
  In-band identity is advisory; document it as such (per-task/connection tokens = P2).
- **`BACKLOG` is the brake** — created tasks never run until externally promoted (§4).
- **`promote` is not a shared-key HTTP endpoint** — coordinator-host CLI only (A1). Otherwise any
  key-holder (incl. an injected skip-perms worker via `curl`) could self-promote and self-execute.
- **`pwd` under `MUSTER_WORKSPACE_FOLDER`** is the execution boundary (A3); task-workers are always
  `--dangerously-skip-permissions`.
- **`model` validated + shell-quoted at spawn** (A4) — the one creator-supplied value that reaches
  the spawn command line.
- **Secrets at rest:** P1 = no retention, tasks persist. File store: add a size cap. (Both noted,
  not solved.)

---

## 8. Restart recovery (one boot scan)

Walk `WORKING` tasks once on boot:
- `worker_pane` empty (crashed before spawn) → back to `TODO`.
- pane gone, `!connected` (spawn failed, nothing ran) → `TO_REVIEW` reason=`spawn_failed`.
- pane gone, `connected` (ran then died) → `TO_REVIEW` reason=`crashed`.
- pane alive → adopt (resume monitoring).

No claim keys, no reconciliation protocol.

---

## 9. Federation — explicitly NOT 0.1.0

Single coordinator forever, for now. The only thing kept for a possible future mesh is the `host:`
segment in the capability string (free text). No host registry, no coordinator↔coordinator auth,
no cross-host forwarding — none of it built or scaffolded in P1.

---

## 10. Phasing

- **P1 (0.1.0) — single coordinator, one host.** Everything above: FastAPI coordinator + Valkey +
  the claim loop + 5-state lifecycle + spawn/reap via herdr + connected/two-phase-liveness + file
  store (disk) + advisory auth + MCP rewired as a WS client + chat behind the API.
- **P2** — retry + `DEAD_LETTER`, per-task/connection tokens, cross-host file transfer, UI on the API.
- **P3** — federation (multi-coordinator mesh), if ever.

---

## 11. Decisions log

Valkey per-host · workspace defines nothing (URL path) · `x-api-key` = peer-validate, advisory
security · 5 states, failure/timeout/cancel → `TO_REVIEW`+reason · coordinator sets `WORKING` (the
lock), single-process, no Lua · `connected` = first `task_get`, two-phase liveness · cancel = hard
kill, no cleanup · Tag `{name,icon,color}` + system registry · WebSocket transport + Valkey-stream
storage + `inboxread` cursor replay · spawn-only P1 · files disk behind API, output>64KB→file ·
task-workers not in roster · `reply_to` notified once on `DONE`/`TO_REVIEW` · `promote` off the
shared key (A1) · `pwd` under `MUSTER_WORKSPACE_FOLDER` (A3) · `model` validated+quoted (A4) ·
no retry/DEAD_LETTER in P1.
