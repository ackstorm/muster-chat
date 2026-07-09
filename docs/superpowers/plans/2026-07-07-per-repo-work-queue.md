# Per-repo Work Queue (v1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an async, per-repo work queue to the muster plugin — `enqueue` hands a task to another agent's queue; `queue` drains your own — so work can wait for a worker instead of a human hand-carrying it between panes.

**Architecture:** One more owned Valkey stream per agent, `muster:queue:{group}:{name}`, mirroring the existing inbox (`muster:inbox:{group}:{name}`). Anyone in the group `XADD`s into agent Y's queue; only Y reads Y's queue, draining it via a read cursor (`muster:queueread:{group}:{name}`) on the tool call. No live push, no dispatcher, no daemon — the owner self-drains at its own boot (surfaced in the startup welcome as a count). This keeps muster daemon-less; auto-spawn of an absent worker is explicitly out of scope (see `docs/TODO-task-queue.md`).

**Tech Stack:** Python 3, `redis.asyncio`, `anyio`, `mcp` low-level Server. Tests: `pytest` against a real Valkey at `redis://127.0.0.1:6379/1` (logical DB 1).

## Global Constraints

- **Version bump:** `plugins/muster/.claude-plugin/plugin.json` `0.8.0` → `0.9.0` (additive feature = minor). Release is version-gated; `claude plugin update` is a no-op without the bump.
- **Dual-import idiom (keep it):** every `mcp/*.py` starts with `try: from . import naming … except ImportError: import naming`. New code must not break flat-script vs package import.
- **Key formats are load-bearing** (daemon interop with Phase 0 store.py): new keys live only in `naming.py`, in the same `muster:{thing}:{group}:{name}` shape.
- **`enqueue` is NOT gated** — no presence check, no idle-gate. The target may be offline or not yet born; that is the point. (Contrast `chat`/`send_message`, which gates on herdr status.) Group boundary is still enforced structurally: keys use the sender's own `GROUP`.
- **Single drain, no re-surface:** `queue` advances the cursor past what it returns. A task read-but-unfinished before a crash is skipped next boot — acceptable for v1 (no retry/dead-letter). Mark this ceiling with a `ponytail:` comment.
- **Test command** (from repo root):
  ```
  uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests -v
  ```
  Single test: append `::test_name` to the file path.
- **Commit trailers** (release task): end the commit body with
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01WnAQP94CsTZx59oC94A2rX
  ```

---

### Task 1: Queue key helpers (`naming.py`)

**Files:**
- Modify: `plugins/muster/mcp/naming.py` (after `joined_key`, line 28)
- Test: `plugins/muster/tests/test_naming.py`

**Interfaces:**
- Produces: `naming.qkey(group, name) -> str` = `muster:queue:{group}:{name}`; `naming.qreadkey(group, name) -> str` = `muster:queueread:{group}:{name}`. Consumed by Task 2.

- [ ] **Step 1: Write the failing test** — append to `plugins/muster/tests/test_naming.py`:

```python
def test_queue_key_schema():
    assert naming.qkey("busX", "ach") == "muster:queue:busX:ach"
    assert naming.qreadkey("busX", "ach") == "muster:queueread:busX:ach"
```

- [ ] **Step 2: Run it, verify it fails**

Run: `uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests/test_naming.py::test_queue_key_schema -v`
Expected: FAIL — `AttributeError: module 'naming' has no attribute 'qkey'`.

- [ ] **Step 3: Implement** — in `plugins/muster/mcp/naming.py`, add after the `joined_key` line (line 28):

```python
def qkey(group, name):      return f"muster:queue:{group}:{name}"      # plugin-only: async work queue
def qreadkey(group, name):  return f"muster:queueread:{group}:{name}"  # plugin-only: queue drain cursor
```

- [ ] **Step 4: Run it, verify it passes**

Run: `uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests/test_naming.py -v`
Expected: PASS (all naming tests).

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/mcp/naming.py plugins/muster/tests/test_naming.py
git commit -m "feat(muster): queue/queueread key helpers"
```

---

### Task 2: Queue ops (`busops.py`) — enqueue, count, drain

**Files:**
- Modify: `plugins/muster/mcp/busops.py` (append after `tail_inbox`, line 99)
- Test: `plugins/muster/tests/test_busops.py`

**Interfaces:**
- Consumes: `naming.qkey`, `naming.qreadkey` (Task 1).
- Produces (consumed by Task 3):
  - `enqueue_task(r, group, to, frm, goal, subject=None) -> {"ok": True, "task_id": str, "to": str}`
  - `queue_pending(r, group, name) -> int`
  - `drain_queue(r, group, name) -> list[{"task_id","from","goal","subject","ts"}]` (oldest-first; advances the cursor)

- [ ] **Step 1: Write the failing test** — append to `plugins/muster/tests/test_busops.py`:

```python
def test_queue_enqueue_drain_and_no_resurface(r):
    async def go():
        await _clean(r, "busT")
        # enqueue is ungated: no presence written, target need not be live
        res = await busops.enqueue_task(r, "busT", "worker", "boss", "run the plan", subject="deploy")
        assert res["ok"] is True and res["to"] == "worker"
        await busops.enqueue_task(r, "busT", "worker", "boss", "second task")
        assert await busops.queue_pending(r, "busT", "worker") == 2
        tasks = await busops.drain_queue(r, "busT", "worker")
        assert [tk["goal"] for tk in tasks] == ["run the plan", "second task"]  # oldest-first
        assert tasks[0]["from"] == "boss" and tasks[0]["subject"] == "deploy"
        # drained: cursor advanced, nothing re-surfaces
        assert await busops.queue_pending(r, "busT", "worker") == 0
        assert await busops.drain_queue(r, "busT", "worker") == []
        # a NEW task after drain surfaces again (cursor only skips what was read)
        await busops.enqueue_task(r, "busT", "worker", "boss", "third")
        assert await busops.queue_pending(r, "busT", "worker") == 1
        assert [tk["goal"] for tk in await busops.drain_queue(r, "busT", "worker")] == ["third"]
    run(go())
```

- [ ] **Step 2: Run it, verify it fails**

Run: `uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests/test_busops.py::test_queue_enqueue_drain_and_no_resurface -v`
Expected: FAIL — `AttributeError: module 'busops' has no attribute 'enqueue_task'`.
(Requires Valkey up: `docker compose up -d` from repo root.)

- [ ] **Step 3: Implement** — append to `plugins/muster/mcp/busops.py`:

```python
async def enqueue_task(r, group, to, frm, goal, subject=None):
    """Append a work item to {to}'s queue. Unlike send_message there is NO presence/idle gate:
    the queue is async and its target may be offline or not yet born — a fresh agent for {to}
    drains it on startup. Group-scoped like everything else (keys use the caller's group)."""
    fields = {"from": frm, "goal": goal, "ts": str(int(time.time()))}
    if subject:
        fields["subject"] = subject
    task_id = await r.xadd(naming.qkey(group, to), fields)
    return {"ok": True, "task_id": task_id, "to": to}


async def _since_cursor(r, stream, cursor_key):
    """Stream entries newer than the persisted cursor (all of them if no cursor yet)."""
    cur = await r.get(cursor_key)
    if cur:
        return await r.xrange(stream, min="(" + cur, max="+")  # '(' = exclusive of cur
    return await r.xrange(stream, min="-", max="+")


async def queue_pending(r, group, name):
    """Count of undrained tasks for {name} — for the startup heads-up."""
    return len(await _since_cursor(r, naming.qkey(group, name), naming.qreadkey(group, name)))


async def drain_queue(r, group, name):
    """Read all undrained tasks for {name}, oldest-first, and advance the cursor past them —
    a single drain, so they do not re-surface on the next fresh start.
    ponytail: advancing on read means a task read but unfinished before a crash is skipped
    next boot (no retry/dead-letter in v1 — see docs/TODO-task-queue.md). Add XCLAIM-style
    redelivery only if lost work bites."""
    stream, cursor_key = naming.qkey(group, name), naming.qreadkey(group, name)
    entries = await _since_cursor(r, stream, cursor_key)
    tasks = [{"task_id": mid, "from": f.get("from", ""), "goal": f.get("goal", ""),
              "subject": f.get("subject", ""), "ts": f.get("ts", "")} for mid, f in entries]
    if entries:
        await r.set(cursor_key, entries[-1][0])
    return tasks
```

- [ ] **Step 4: Run it, verify it passes**

Run: `uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests/test_busops.py -v`
Expected: PASS (all busops tests — the new one plus the pre-existing ones unaffected).

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/mcp/busops.py plugins/muster/tests/test_busops.py
git commit -m "feat(muster): enqueue_task + queue drain ops (ungated, cursor-drained)"
```

---

### Task 3: `enqueue` + `queue` tools and boot heads-up (`muster_channel.py`)

**Files:**
- Modify: `plugins/muster/mcp/muster_channel.py` — `INSTRUCTIONS` (line 33), `_list_tools` (line 90), `_call_tool` (line 120), `welcome` (line 192)
- Test: `plugins/muster/tests/test_channel_tools.py` (update the surface assertion)

**Interfaces:**
- Consumes: `busops.enqueue_task`, `busops.queue_pending`, `busops.drain_queue` (Task 2).
- Produces: tool surface becomes exactly `["roster", "chat", "fetch", "enqueue", "queue"]`.

- [ ] **Step 1: Update the failing test** — in `plugins/muster/tests/test_channel_tools.py`, replace the body of `test_tool_surface_is_roster_chat_fetch` assertions:

```python
def test_tool_surface_is_roster_chat_fetch():
    # Guards the tool surface and that importing the module doesn't start the server
    # (the anyio.run(main) is behind an `if __name__ == "__main__"` guard).
    tools = anyio.run(muster_channel._list_tools)
    names = [tool.name for tool in tools]
    assert names == ["roster", "chat", "fetch", "enqueue", "queue"]
    assert "send" not in names   # clean break, no backcompat
    chat = next(tool for tool in tools if tool.name == "chat")
    assert "important" in chat.inputSchema["properties"]
    enqueue = next(tool for tool in tools if tool.name == "enqueue")
    assert enqueue.inputSchema["required"] == ["to", "goal"]
```

- [ ] **Step 2: Run it, verify it fails**

Run: `uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests/test_channel_tools.py -v`
Expected: FAIL — `assert ['roster','chat','fetch'] == ['roster','chat','fetch','enqueue','queue']`.

- [ ] **Step 3a: Add the two tool definitions** — in `_list_tools` (`plugins/muster/mcp/muster_channel.py`), insert after the `fetch` tool (before the closing `]`, line ~116):

```python
        t.Tool(name="enqueue", description=(
            "ENQUEUE — drop an async work item into another agent's queue, by name. Unlike chat "
            "this is NOT real-time and NOT gated on the target being live: the task waits in the "
            "queue and is picked up when a fresh agent for that name next starts (it drains its "
            "queue on launch). Use it to hand off work — 'run this plan', 'regen the schema' — to "
            "a peer, or to a repo whose agent isn't running yet. Put the work in `goal`."),
            inputSchema={"type": "object", "required": ["to", "goal"], "properties": {
                "to": {"type": "string", "description": "target agent/repo name, e.g. 'ach-agent'"},
                "goal": {"type": "string", "description": "the work to do — a task or plan to run"},
                "subject": {"type": "string", "description": "optional short label for the task"}}}),
        t.Tool(name="queue", description=(
            "Drain YOUR OWN work queue — async tasks other agents enqueued for you to run. Returns "
            "each task's goal and sender and marks them read, so they don't resurface on your next "
            "start. Check it when your startup heads-up says tasks are waiting, or whenever you are "
            "free to pick up queued work."),
            inputSchema={"type": "object", "properties": {}}),
```

- [ ] **Step 3b: Add the two handlers** — in `_call_tool`, insert before the final `return [t.TextContent(type="text", text=f"unknown tool {name}")]` (line ~149):

```python
    if name == "enqueue":
        res = await busops.enqueue_task(r, GROUP, args["to"], NAME, args["goal"], args.get("subject"))
        return [t.TextContent(type="text", text=(
            f"Queued for {res['to']} (task {res['task_id']}). "
            f"They pick it up when a fresh agent for {res['to']} next starts."))]
    if name == "queue":
        tasks = await busops.drain_queue(r, GROUP, NAME)
        if not tasks:
            return [t.TextContent(type="text", text="Your Muster queue is empty.")]
        lines = [f"[{tk['ts']}] from {tk['from']}: "
                 + (f"({tk['subject']}) " if tk['subject'] else "") + tk['goal'] for tk in tasks]
        return [t.TextContent(type="text", text=(
            f"Your Muster queue ({len(tasks)} task(s) — now drained):\n" + "\n".join(lines)))]
```

- [ ] **Step 3c: Surface queued count in the welcome** — in `welcome`, after the inbox `pending` block (after line 209, `tail = f" You have {pending} ..."`), add:

```python
        queued = await busops.queue_pending(r, GROUP, NAME)
        if queued:
            tail += f" {queued} task(s) in your queue — call `queue` to pull them."
```

- [ ] **Step 3d: Update the two copy strings** — in the same file:
  - `INSTRUCTIONS` (line ~39): change `follow up with the Muster tools (roster, chat, fetch)` → `follow up with the Muster tools (roster, chat, fetch, enqueue, queue)`.
  - `welcome` content (line ~216): change `Tools: roster, chat, fetch.` → `Tools: roster, chat, fetch, enqueue, queue.`

- [ ] **Step 4: Run it, verify it passes**

Run: `uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests -v`
Expected: PASS — full suite (naming + busops + channel-tools + git_identity). The surface test now sees all five tools.

- [ ] **Step 5: Commit**

```bash
git add plugins/muster/mcp/muster_channel.py plugins/muster/tests/test_channel_tools.py
git commit -m "feat(muster): enqueue/queue tools + boot queue heads-up"
```

---

### Task 4: User-facing docs + version bump

**Files:**
- Modify: `plugins/muster/.claude-plugin/plugin.json` (version + description)
- Modify: `README.md`, `plugins/muster/README.md`, `docs/GETTING-STARTED.md`, `CLAUDE.md`, `plugins/muster/skills/herdr-muster/SKILL.md`
- Modify: `docs/TODO-task-queue.md` (mark v1 shipped)

**Interfaces:** none (docs only). Verification is grep + `claude plugin validate`.

- [ ] **Step 1: Bump the manifest** — in `plugins/muster/.claude-plugin/plugin.json`: `"version": "0.8.0"` → `"0.9.0"`; in its `description`, change the tools list `roster, chat, fetch` → `roster, chat, fetch, enqueue, queue`.

- [ ] **Step 2: Document the two tools.** In each of `README.md`, `plugins/muster/README.md`, `docs/GETTING-STARTED.md` (§7), add — after the `fetch` bullet — a short pair:
  - **`enqueue {to, goal, subject?}`** — hand an async task to another agent's queue by name. Not real-time, not gated on the target being live; it's picked up when a fresh agent for that name next starts (drains its queue on launch). For handing off work ("run this plan") to a peer or a not-yet-running repo.
  - **`queue`** — drain your own queue: the tasks other agents enqueued for you. Marks them read (no re-surface next start). Your startup greeting tells you when tasks are waiting.

  Also: in each doc's "Tools: roster, chat, fetch" sample greeting line → `roster, chat, fetch, enqueue, queue`. In `README.md` §Status and `plugins/muster/README.md` Status blurb, move `enqueue`/`queue` out of the "out of scope" list (leave `ack`/`announce`/auto-spawn there). In `docs/GETTING-STARTED.md`, update the §7 TOC anchor + header only if they enumerate tool names (they read "roster / chat / fetch" — extend to include the queue, keeping link text and `#...` anchor consistent).

- [ ] **Step 3: Update `CLAUDE.md`** (repo, at build root) — in the Tools paragraph, add a line for `enqueue`/`queue` (async per-repo queue, ungated, drained on fresh start), and note `naming.qkey`/`qreadkey` + `busops.enqueue_task`/`drain_queue` as the queue primitives.

- [ ] **Step 4: Update `docs/TODO-task-queue.md`** — change Status to note **v1 (enqueue + queue tools) shipped in 0.9.0**; keep the deferred parts (herdr watcher / auto-spawn, retry/dead-letter, lifecycle UI) as the remaining roadmap.

- [ ] **Step 5: Verify no stale tool lists + manifest valid**

```bash
git grep -nE "roster.{0,3}(chat|/).{0,3}fetch" -- '*.md' | grep -v enqueue   # should return nothing (every tool list now includes the queue tools)
claude plugin validate ./plugins/muster
```
Expected: the grep prints nothing; validate passes.

- [ ] **Step 6: Commit**

```bash
git add README.md CLAUDE.md docs plugins/muster/README.md plugins/muster/.claude-plugin/plugin.json plugins/muster/skills
git commit -m "docs(muster): document enqueue/queue, bump 0.9.0"
```

---

### Task 5: Release — full suite, validate, tag, push, plugin update

**Files:** none (release actions).

- [ ] **Step 1: Full suite green**

Run: `uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests -v`
Expected: PASS — all tests (18: the prior 17 + the new queue busops test; naming gained an assertion, channel-tools surface updated).

- [ ] **Step 2: Validate the plugin**

Run: `claude plugin validate ./plugins/muster`
Expected: passes.

- [ ] **Step 3: Tag the release**

```bash
git tag v0.9.0
```

- [ ] **Step 4: Push**

```bash
git push origin main
git push origin v0.9.0
```

- [ ] **Step 5: Refresh the marketplace + update the installed plugin**

```bash
claude plugin marketplace update herdr-muster
claude plugin update muster@herdr-muster
```
Expected: `Plugin "muster" updated from 0.8.0 to 0.9.0`. (Running agents keep the old tools until relaunched.)

---

## Self-Review

- **Spec coverage:** enqueue (ungated write to Y's queue) → Task 2/3; per-repo addressing via `qkey` → Task 1; owner-only drain with cursor, no re-surface → Task 2 (`drain_queue`) + test; fresh-start heads-up → Task 3 (welcome); daemon-less preserved (no new resident process) → by construction; auto-spawn/watcher deferred → documented in Task 4. ✅
- **No placeholders:** every code step carries complete code; every run step has an exact command + expected result.
- **Type consistency:** `enqueue_task`/`queue_pending`/`drain_queue` signatures and the task dict keys (`task_id/from/goal/subject/ts`) are identical across Task 2's definition, its test, and Task 3's handlers. Tool surface `["roster","chat","fetch","enqueue","queue"]` matches the Task 3 test and the welcome/INSTRUCTIONS copy.
- **Ceiling noted:** `drain_queue` crash-skip documented with a `ponytail:` comment and in the TODO; retry/dead-letter explicitly out of v1 scope.
