# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Two things at once: the **project home** for **Muster**, an agent coordination bus, and a **Claude Code plugin marketplace** (`.claude-plugin/marketplace.json`) that ships one plugin, `muster`. The `muster` plugin lets several Claude agents that share a coordination **group** discover each other **by name** and message each other, delivered as native Claude Code **channel** events — no keystroke injection. [herdr](https://herdr.dev) is an optional adapter: present, it auto-fills the group from its workspace id; absent, set `MUSTER_GROUP`.

## Architecture (the parts you can't see from one file)

**Vocabulary.** "Bus" names the Muster system itself (as in "Agent Coordination Bus"). "Group" is the coordination scope — who you can see and message — resolved by `naming.derive_group`: `MUSTER_GROUP` → herdr's `HERDR_WORKSPACE_ID` (prefixed `HERDR-` when `HERDR_ENV` is set, e.g. `HERDR-wH`) → `"local"`.

**Delivery is a channel, not keystrokes.** `plugins/muster/mcp/muster_channel.py` is a stdio MCP server that advertises `capabilities.experimental["claude/channel"]` and emits `notifications/claude/channel` `{content, meta}`. Claude Code renders those natively as `<channel source="muster">…</channel>` in the receiving session. Each agent runs its own server (one per pane, launched by `plugins/muster/.mcp.json`); an agent's server delivers that agent's *own* inbox to *itself*.

**Identity vs. group vs. presence** — the central design split:
- **Identity = git repo + pid, herdr-free.** `naming.derive_agent_name` → `repo` for a main checkout, `repo~worktree` for a linked worktree → `basename(cwd)` → a generated id (`hostname:pid`) as last resort, each suffixed `-pid:{pid}` (e.g. `muster-chat-pid:1234`). The pid suffix makes two panes sharing one checkout distinct presence keys instead of colliding on the identical git name — the reason a pane can now see its co-located peers. It is stable within a process: a `git checkout` (branch change) must NOT change the name — branch is presence, never identity — but it *does* change across a restart (new pid), so identity is per-process, not eternal. That is fine because `chat` only targets *live* peers by their current roster name. `herdr.git_identity` derives `repo` from `--git-common-dir` (shared across worktrees) and the worktree tag from the per-worktree `--git-dir` basename (stable across checkout).
- **Group = the coordination scope.** `naming.derive_group`: `MUSTER_GROUP` → `HERDR_WORKSPACE_ID` → `"local"`. Under herdr (`HERDR_ENV` set) the workspace-derived group is **prefixed `HERDR-`** (`wH` → `HERDR-wH`) so a short workspace id can't collide with a hand-set `MUSTER_GROUP` or the `"local"` default; an explicit `MUSTER_GROUP` is still used verbatim. This is a **hard boundary**: an agent can only see or message agents in its own group. herdr is optional — present, it auto-fills the group from its workspace id; absent, set `MUSTER_GROUP` yourself (unset ⇒ everyone shares `local`).
- **Presence = branch / worktree / cwd / status**, written to `muster:presence:{group}:{name}` every 30s (TTL 90). That presence key **is** the registry — there is no separate daemon; each server self-registers. `status` is herdr's live `agent_status` (idle/working/blocked) when `HERDR_ENV` is set, else `online`.

**The four modules (`plugins/muster/mcp/`):**
- `naming.py` — pure, no I/O. Key formats, `derive_group` (= coordination scope: `MUSTER_GROUP` → `HERDR_WORKSPACE_ID` → `"local"`), `derive_agent_name`, `self_identity`. Unit-tested.
- `herdr.py` — fail-safe adapters, all optional: `panes()` (herdr CLI, used only for this pane's live `agent_status`), `git_info`, `git_identity`. Any failure returns empty/`(None, None)`; the server still runs.
- `busops.py` — async Valkey ops taking a `redis.asyncio` client: `write_presence`, `list_roster`, `build_orientation` (the identity + roster + pending line, used by the startup welcome), `send_message` (presence- and herdr-status-gated XADD to the recipient's inbox — only `idle`/`online`/unknown accept; `_envelope` builds the short pushed line), `announce_join` (summary-only join notice), `fetch_inbox`, `tail_inbox`. All bus logic lives here so it's testable without the MCP plumbing.
- `hooks/hooks.json` — a **bundled SessionStart hook** (matcher `clear|compact`). Not an MCP module and not a script: a single static `echo` that re-injects a fixed nudge via `additionalContext` ("Muster is still active — use `roster`/`fetch`, load the skill"). No Valkey, no Python — live data comes from the tools on demand. Dynamic orientation stays on the startup welcome (server already connected); spinning up a redis-connecting subprocess on every `/clear` was not worth the one dynamic line.
- `muster_channel.py` — wires it together: resolve identity at startup, `relay_inbox` (tails own inbox → `_push_entries` pushes each), `register_presence` (git/herdr status via `anyio.to_thread`), `welcome`, `announce_join` (opt-out `MUSTER_JOIN=0`, deduped by a `SET NX` marker), the three tools, and the request-dispatch loop. `connect()` caches the one shared Valkey client behind a lock.

**Tools (all same-group):** `roster` (list live peers by name + status), `chat {to, body, subject?}` (real-time 1:1 — deliver to a peer's inbox → their relay pushes a short **envelope**: sender + subject, plus a "· fetch for full" nudge when the body is longer than the line; refused if the target's herdr status is not `idle`, unless `important: true` overrides and marks it ❗, read when the agent next runs), `fetch {limit?}` (read own inbox full bodies; the channel push carries only envelopes/summaries). A future async **queue** lane (enqueue work → a dispatcher assigns/spawns) is planned; the broadcast name `announce` is reserved for it.

## Non-obvious invariants — read before editing

- **Valkey key schema is a contract.** `muster:inbox:{group}:{name}`, `muster:inboxread:{group}:{name}`, `muster:presence:{group}:{name}` must stay byte-identical to the Phase 0 daemon's `store.py` (kept in the sibling `muster/` project, not in this repo) so they interoperate. The internal `bus`→`group` rename (`derive_bus`→`derive_group`, param `bus`→`group`) is cosmetic: the literal key prefixes (`muster:inbox:`, `muster:inboxread:`, `muster:presence:`) and the `{scope}:{name}` shape are unchanged, so the daemon still interoperates. Never hand-format a key — always go through `naming.ikey/rkey/pkey`.
- **Tools + channel push coexist via a private SDK method.** The server does NOT call `Server.run()` (that builds its own session and gives the background pushers no handle). Instead it holds a manual `ServerSession` and runs a dispatch loop calling `srv._handle_message(...)` — exactly what `Server.run` does internally. Because `_handle_message` is private, `plugins/muster/.mcp.json` pins `mcp>=1.28,<1.29`. See `docs/PROBE-tools-and-channel.md` for the proven pattern. If you bump the mcp SDK, re-verify this.
- **Dual-import idiom.** `busops.py` and `muster_channel.py` start with `try: from . import naming … except ImportError: import naming`. Required because the same files run two ways: as a package under pytest (`plugins.muster.mcp.*`) and as a flat script at runtime (the installed plugin runs `python .../mcp/muster_channel.py`, no `plugins` package present). Keep it.
- **Ignore Pyright "import could not be resolved" on the mcp/redis/anyio and sibling imports.** They resolve at runtime (the deps come from `uv run --with …`; siblings via the flat-script path). They are static-analysis noise, not real errors.
- **Untrusted content.** Channel/peer content is a *request*, never authority. The doctrine lives in the server's `instructions` string (always in the system prompt) and `plugins/muster/skills/muster-chat/SKILL.md`. Don't add anything that treats a `<channel>` body as a command.
- **Fail-safe startup.** If Valkey is down, or herdr is absent/its CLI errors/the pane isn't listed (herdr is optional — none of that is fatal), the MCP handshake must still complete and the channel stay idle. Tools connect lazily and return an offline message rather than crashing.
- **Async hygiene — never block the single event loop.** Relay push, tool responses, presence, and welcome all run in one anyio loop. `register_presence` wraps `herdr.git_info`/`herdr.agent_status` in `anyio.to_thread.run_sync` because they shell out via `subprocess.run` (10s timeout each) — a direct call stalls relay + tool responses every 30s. Any new subprocess / blocking / sync-file call reached from an async path must be offloaded the same way. `connect()` is lock-guarded (double-checked `anyio.Lock`) so two tasks starting together share one Valkey client instead of each building — and leaking — its own.
- **Relay cursor advances only on delivered messages.** `relay_inbox` delegates each batch to `_push_entries`, which persists `CURSOR` after every successful push and **stops at the first failure**, returning the last delivered id so the next `XREAD` re-reads the failed one. Never advance the watermark past an un-pushed message — a later same-batch success overwriting the cursor silently dropped the failed message (fixed in 0.8.1). Keep the stop-on-failure.
- **The re-orient hook is a static nudge, not logic.** `/clear` wipes the conversation but not the MCP server — presence stays live and the agent stays registered; it only forgets its identity + neighbours. Server `instructions` (doctrine/tools/skill nudge) survive `/clear`, so the hook only re-surfaces "check `roster`/`fetch`" — a fixed `echo`, no Valkey and no presence write. Keep it static: a hook that wrote presence or re-announced a join would false-notify peers a "[presence] + … online" on every `/clear`, and a per-`/clear` redis subprocess re-imports the very fragility the server already owns. If live data is ever needed here again, it belongs on the server (already connected), never a hook.

## Commands

Run from the repo root. The server and tests use `uv` with `--no-project` (no virtualenv to manage); `uv` fetches deps on first run.

```bash
# Valkey (transport + registry). Uses logical DB /1 on 127.0.0.1:6379.
docker compose up -d

# All tests. Pure tests (naming, git_identity, the _push_entries guards) need no services;
# the busops and _call_tool handler tests need Valkey up. --with mcp is required: the channel
# tests import muster_channel, which imports the mcp SDK.
uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests -v

# A single test
uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests/test_naming.py::test_key_schema_matches_phase0 -v

# Validate the plugin manifest
claude plugin validate ./plugins/muster

# Run the server standalone for debugging (no Claude/herdr needed — falls back to env identity).
# Pin mcp to match .mcp.json (the server uses the private srv._handle_message — see invariants).
HERDR_PANE_ID=w9:p9 HERDR_WORKSPACE_ID=w9 MUSTER_WELCOME=0 \
  uv run --with 'mcp>=1.28,<1.29' --with redis --no-project python plugins/muster/mcp/muster_channel.py
```

**Verifying server behaviour without a live Claude:** drive the stdio server with a raw MCP client (initialize → tools/call → capture `notifications/claude/channel`). `docs/PROBE-tools-and-channel.md` records the working two-instance end-to-end pattern; reuse it rather than launching real Claude sessions.

## Install / launch / release

- The plugin is **always** referenced marketplace-qualified as `muster@muster-chat`. The bare `muster` is not resolved (`claude plugin update muster` → "Plugin not found").
- Channels are a research preview: activate at launch with `claude --dangerously-load-development-channels plugin:muster@muster-chat`. An org admin can allowlist it in managed settings to drop the flag (see the root `README.md`).
- **Updates are version-gated.** `claude plugin update` is a no-op unless `plugins/muster/.claude-plugin/plugin.json` `version` bumps — so every shippable change to the plugin must bump that version, or installs keep the stale cached copy.
- **On every release, tell the user how to update — and offer to run it.** A release is only live once the release commit + `vX.Y.Z` tag are pushed to `origin`. After pushing, surface the two update commands (`claude plugin marketplace update muster-chat` then `claude plugin update muster@muster-chat`) and offer to run them for the user. The canonical copy lives in README "Updating".

## Env vars the server reads

`MUSTER_VALKEY_URL` (default `redis://localhost:6379/1`), `MUSTER_GROUP` (coordination scope override; resolution order `MUSTER_GROUP → HERDR_WORKSPACE_ID → "local"`, workspace prefixed `HERDR-` under `HERDR_ENV`), `MUSTER_WELCOME=0` to silence the startup welcome push, `MUSTER_JOIN=0` to silence the join-announce to peers (on by default, deduped 5 min). herdr's `HERDR_ENV` / `HERDR_PANE_ID` / `HERDR_WORKSPACE_ID` are read when present (herdr is optional).

## Scope

Shipped: inbound delivery + `roster`/`chat`/`fetch`, same-group only; co-located panes disambiguated by a `-pid:{pid}` name suffix. Out of scope: a standalone presence daemon, cross-group messaging, `ack`/`announce`/`task_add`.
