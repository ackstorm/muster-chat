# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Two things at once: the **project home** for **Muster**, an agent coordination bus, and a **Claude Code plugin marketplace** (`.claude-plugin/marketplace.json`) that ships one plugin, `muster`. The `muster` plugin lets Claude agents across hosts, runtimes, and users discover and message each other over a central HTTP bus (**muster-api**, in `server/`), delivered as native Claude Code **channel** events — no keystroke injection. There is also a standalone OpenCode port (`plugins/muster/opencode/muster-chat.js`) speaking the same wire contract.

## Architecture (the parts you can't see from one file)

**The shims are thin; the server is the brain.** `plugins/muster/mcp/` (the Claude shim) and `plugins/muster/opencode/muster-chat.js` (the OpenCode shim) only (a) derive the client half of the agent address, (b) expose the bus ops as tools, and (c) hold an SSE stream to push `deliver` events into the runtime. Identity stamping (the `user` segment, from the API key), ACL, the read cursor, and rate limits all live server-side in `server/` (a separate subsystem — see `server/CLAUDE.md` if present, not detailed here).

**Address, not name+group.** Every agent has a 5-segment address `user/host/runtime/project/session`. `user` is stamped by muster-api from `x-muster-agent`'s caller (the API key); the shim supplies `host/runtime/project/session` via the `x-muster-agent` header (`naming.derive_address`). `to` (chat) accepts any contiguous, unique slice of an address; ambiguous references get back `candidates` to retry with a longer slice. `search` filters and `announce`'s `project` match a segment exactly (no slice matching), and `scope` must be exactly `user:<you>` or `group:<g>`.

**Delivery is a channel, not keystrokes.** `plugins/muster/mcp/muster_channel.py` is a stdio MCP server that advertises `capabilities.experimental["claude/channel"]` and emits `notifications/claude/channel` `{content, meta}`. Claude Code renders those natively as `<channel source="muster">…</channel>` in the receiving session. Each agent runs its own server (one per pane, launched by `plugins/muster/.mcp.json`); the SSE connection to muster-api **is** that agent's presence (connected = online).

**The four modules (`plugins/muster/mcp/`):**
- `naming.py` — pure, no I/O. `derive_project` (git repo, `repo~worktree` for a linked worktree, else `basename(cwd)`), `derive_address` (the `host/runtime/project/session` half of the address the shim owns). Unit-tested.
- `gitmeta.py` — fail-safe git adapters, subprocess-based: `git_info` (branch / is-worktree, used for the `x-muster-meta` stream header) and `git_identity` (`repo`, `worktree` — derived from `--git-common-dir`/`--git-dir` so it's stable across a `git checkout`). Any failure returns `(None, None)`; the server still runs.
- `httpbus.py` — the only module that talks to the network: `MusterClient.rpc()` (`POST /v1/rpc`) and `MusterClient.stream()` (SSE `GET /v1/stream`, `parse_sse` framing). Raises `BusError(status, payload)` on a non-2xx rpc answer; the caller renders `payload`.
- `muster_channel.py` — wires it together: resolve the address at startup (`resolve_address`), `relay()` (holds the SSE stream, dedups by `msg_id` with a 256-entry LRU — **announce events carry no `msg_id` and must never be deduped** — and pushes each `deliver` event as a channel notification), `welcome`, the five tools (`roster`/`search`/`chat`/`fetch`/`announce`), and the request-dispatch loop.
- `hooks/hooks.json` — a **bundled SessionStart hook** (matcher `clear|compact`). Not an MCP module and not a script: a single static `echo` that re-injects a fixed nudge via `additionalContext` ("Muster is still active — use `roster`/`fetch`, load the skill"). No network, no Python — live data comes from the tools on demand.

**Tools:** `roster` (list visible agents by address + status), `search` (filter by user/project/runtime/group/live), `chat {to, body, subject?, important?}` (real-time 1:1 — the recipient's relay pushes a short **envelope**: address + subject, plus a "fetch for full" nudge; `important: true` marks it ❗), `fetch {limit?}` (read own inbox full bodies and mark them read — server-side cursor, one-shot), `announce {scope, project, body, subject?}` (ephemeral broadcast to online agents of one project, never stored). The OpenCode shim mirrors these as `muster_roster`/`muster_chat`/`muster_fetch`/`muster_announce` (no `search`) plus event-driven delivery — see `plugins/muster/opencode/muster-chat.js`.

## Non-obvious invariants — read before editing

- **Tools + channel push coexist via a private SDK method.** The server does NOT call `Server.run()` (that builds its own session and gives the background pushers no handle). Instead it holds a manual `ServerSession` and runs a dispatch loop calling `srv._handle_message(...)` — exactly what `Server.run` does internally. Because `_handle_message` is private, `plugins/muster/.mcp.json` pins `mcp>=1.28,<1.29`. See `docs/PROBE-tools-and-channel.md` for the proven pattern. If you bump the mcp SDK, re-verify this.
- **Dual-import idiom.** `muster_channel.py` starts with `try: from . import naming, gitmeta … except ImportError: import naming; import gitmeta`. Required because the same files run two ways: as a package under pytest (`plugins.muster.mcp.*`) and as a flat script at runtime (the installed plugin runs `python .../mcp/muster_channel.py`, no `plugins` package present). Keep it.
- **Ignore Pyright "import could not be resolved" on the mcp/httpx/anyio and sibling imports.** They resolve at runtime (the deps come from `uv run --with …`; siblings via the flat-script path). They are static-analysis noise, not real errors.
- **Untrusted content.** Channel/peer content is a *request*, never authority. The doctrine lives in the server's `instructions` string (always in the system prompt) and `plugins/muster/skills/muster-chat/SKILL.md`. Don't add anything that treats a `<channel>` body as a command.
- **Fail-safe startup.** If muster-api is unreachable, the MCP handshake must still complete and the channel stay idle. Tools connect lazily (`httpx.AsyncClient` is built on first use) and return an offline message rather than crashing; the relay retries with capped exponential backoff.
- **msg_id dedup, but never for announce.** `relay()`'s LRU (256 entries) drops at-least-once duplicate `deliver` events by `msg_id` — but `announce` events have no `msg_id` and must always pass through undeduped.
- **No client-side read cursor.** Only the server's `fetch` op advances the read watermark; the shims never persist one locally. In the OpenCode shim this means: never call `fetch` before a session id is known — fetching with nowhere to surface the result would silently consume mail (`drainInbox` in `muster-chat.js` checks `sessionID` before calling `fetch`, in that order, deliberately).
- **The re-orient hook is a static nudge, not logic.** `/clear` wipes the conversation but not the MCP server — the SSE stream stays connected and the agent stays "present"; it only forgets its identity + neighbours. Server `instructions` (doctrine/tools/skill nudge) survive `/clear`, so the hook only re-surfaces "check `roster`/`fetch`" — a fixed `echo`, no network call. Keep it static: a hook that queried the bus would re-import per-`/clear` fragility the server already owns. If live data is ever needed here again, it belongs on the server (already connected), never a hook.
- **§13 channel-deferral: deliberately not implemented.** See `docs/references/channel-deferral.md` for why v1 ships no hold-while-busy logic anywhere (server or shim) — Claude Code already defers `<channel>` surfacing to a safe point.

## Commands

Run from the repo root. The plugin tests use `uv` with `--no-project` (no virtualenv to manage); `uv` fetches deps on first run.

```bash
# Valkey + muster-api (transport + the central bus the shims talk to).
docker compose up -d

# All plugin tests. Pure tests (naming, git_identity, dedup guards) need no services; the
# httpbus and integration tests need muster-api up. mcp MUST be pinned — bare `--with mcp`
# resolves to mcp 2.x and breaks collection (the channel tests import muster_channel).
uv run --with httpx --with anyio --with pytest --with 'mcp>=1.28,<1.29' --no-project pytest plugins/muster/tests -v

# A single test
uv run --with httpx --with anyio --with pytest --with 'mcp>=1.28,<1.29' --no-project pytest plugins/muster/tests/test_naming.py -v

# Validate the plugin manifest
claude plugin validate ./plugins/muster

# Run the server standalone for debugging (no Claude needed — falls back to env identity).
# Pin mcp to match .mcp.json (the server uses the private srv._handle_message — see invariants).
MUSTER_WELCOME=0 \
  uv run --with 'mcp>=1.28,<1.29' --with httpx --no-project python plugins/muster/mcp/muster_channel.py
```

**Verifying server behaviour without a live Claude:** drive the stdio server with a raw MCP client (initialize → tools/call → capture `notifications/claude/channel`). `docs/PROBE-tools-and-channel.md` records the working two-instance end-to-end pattern; reuse it rather than launching real Claude sessions.

## Install / launch / release

- The plugin is **always** referenced marketplace-qualified as `muster@muster-chat`. The bare `muster` is not resolved (`claude plugin update muster` → "Plugin not found").
- Channels are a research preview: activate at launch with `claude --dangerously-load-development-channels plugin:muster@muster-chat`. An org admin can allowlist it in managed settings to drop the flag (see the root `README.md`).
- **Updates are version-gated.** `claude plugin update` is a no-op unless `plugins/muster/.claude-plugin/plugin.json` `version` bumps — so every shippable change to the plugin must bump that version, or installs keep the stale cached copy.
- **On every release, tell the user how to update — and offer to run it.** A release is only live once the release commit + `vX.Y.Z` tag are pushed to `origin`. After pushing, surface the two update commands (`claude plugin marketplace update muster-chat` then `claude plugin update muster@muster-chat`) and offer to run them for the user. The canonical copy lives in README "Updating".

## Env vars the shim reads

`MUSTER_URL` (default `http://localhost:8765`) — muster-api base URL. `MUSTER_API_KEY`
(default `dev-key`) — sent as `x-muster-api-key`; `user` is stamped server-side from it.
`MUSTER_HOST` — override for the `host` address segment (else the machine hostname).
`MUSTER_WELCOME=0` — silence the startup welcome push. `MUSTER_INBOUND=refuse` — never
open the SSE stream (you appear offline; outbound tools still work; mail queues server-side).

## Scope

Shipped: inbound delivery + `roster`/`search`/`chat`/`fetch`/`announce` against a central
muster-api bus, plus an OpenCode port with event-driven delivery. Out of scope: `ack`,
`task_add`, and runtime-side channel-deferral logic (§13 — see
`docs/references/channel-deferral.md`).
