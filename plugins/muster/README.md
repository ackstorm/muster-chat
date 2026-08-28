# muster — Agent Coordination Bus channel

Pushes an agent's **muster-api inbox** into its own Claude Code session as native
`<channel source="muster">` events. No keystroke injection, no pane targeting — the agent
delivers its own inbox to itself over a stdio MCP channel, backed by a central HTTP bus.

> **Status: v1 — inbound delivery + `roster` / `chat` / `fetch` / `announce`.**
> `ack` and `task_add` are out of scope for now.

## How it works

- The plugin's MCP server declares the `claude/channel` capability, so Claude Code
  registers it as a channel.
- **Identity**: every agent has a 5-segment address `user/host/runtime/project/session`.
  `user` is stamped server-side from the API key; the shim supplies `host/runtime/project/session`
  (project = git repo, `repo~worktree` for a linked worktree; non-git falls back to
  `basename(cwd)`, then a generated id).
- It holds one SSE connection to muster-api (`GET /v1/stream`) — that connection **is**
  presence: connected = online. Each `deliver` event is pushed to the session as a
  `<channel>` event, deduped by `msg_id` (256-entry LRU; `announce` events carry no `msg_id`
  and are never deduped). If muster-api is unreachable the MCP handshake still succeeds and
  the channel stays idle (never crashes); the stream reconnects with capped backoff.
- The read cursor lives entirely server-side — the shim never persists one locally. Only the
  `fetch` op advances it, and each message surfaces exactly once.
- Four tools:
  - `roster` — list the agents visible to you (your own agents on every host, plus any
    group-shared ones), grouped by project, each row a full address. Filters: `user`,
    `project`, `runtime`, `group`, `status`. Lists the **online** agents by default and
    reports the offline ones as per-project counts — they are still mailable (`chat`
    queues to their inbox), so pass `status: "offline"` or `"all"` for their addresses.
  - `chat {to, body, subject?, important?}` — **real-time** message to a peer. `to` is any
    contiguous, unique slice of an address (a project name, `host/runtime`, the full
    address…; ambiguous references get back `candidates` to retry with). The recipient sees
    a short **envelope** (your address + subject, plus a "· fetch for full" nudge when the
    body is longer than the line) and reads the full body with `fetch`; `subject` defaults to
    the body's first line. `important: true` marks the envelope ❗.
  - `fetch {limit?}` — read the full bodies of your own unread inbox messages and mark them
    read (one-shot; limit is clamped to 1–100).
  - `announce {scope, project, body, subject?}` — ephemeral broadcast to the ONLINE agents of
    one project; never stored, so an offline agent simply misses it.
- Startup is **silent by default** — a clean start has nothing actionable to say, and a
  channel event costs the agent a turn before the user has asked for anything.
  `MUSTER_WELCOME=1` opts into one **welcome** event: your address, the live roster count,
  and a nudge to load the `muster-chat` skill (skills aren't auto-read — the core rules
  ship in the always-on `instructions` string either way).
- On **`/clear` or compaction** a **bundled `SessionStart` hook** (`hooks/hooks.json`,
  matcher `clear|compact`) re-injects a short static nudge — "Muster is still active, check
  `roster`/`fetch`, load the skill" — into the fresh context. The MCP server keeps running
  across `/clear`, so you stay reachable; live peers and pending mail come from the tools on
  demand. A plain `echo` (no network, no script) ships in the plugin, no user config.

## Requirements

- Claude Code **v2.1.80+** with channels enabled (research preview).
- [`uv`](https://docs.astral.sh/uv/) on `PATH` — the only Python-side install. The server runs
  via `uv run --with mcp>=1.28,<1.29 --with httpx --no-project` (declared in `.mcp.json`), so uv
  fetches `mcp` + `httpx` itself at launch — no `pip`, no `requirements.txt`, no virtualenv.
- A reachable muster-api. Default `http://localhost:8765` — override with `MUSTER_URL`.
  Dev auth `MUSTER_API_KEY=dev-key` with the bundled compose.

## Install & update

```bash
claude plugin marketplace add ackstorm/muster-chat
claude plugin install muster@muster-chat
# later, to pull a new release (version-gated — refresh the marketplace first):
claude plugin marketplace update muster-chat && claude plugin update muster@muster-chat
```

Always use the marketplace-qualified name `muster@muster-chat`; the bare `muster` is not resolved.

## Launch

Channels are a research preview, so the server must be activated at launch:

```bash
# default path: loads muster, prints an expected "development channels" warning each launch
# (muster isn't on Anthropic's built-in allowlist during the preview — the warning is not an error)
claude --dangerously-load-development-channels plugin:muster@muster-chat

# only once an ADMIN allowlists muster in managed settings (allowedChannelPlugins) — no warning.
# Without that, --channels does NOT load muster: Claude starts but the channel silently won't register.
claude --channels plugin:muster@muster-chat
```

## Try it

With the plugin active, call `roster` to see who else is visible, then `chat` to reach
them — that's the normal path. It appears in their session as:

```
<channel source="muster" msg_id="…">✉ from laptop/claude/other-project/5678: schema regen · fetch for full</channel>
```

## Trust model

Channel content is **untrusted** — a request, not an authority. The server ships an
`instructions` string (and a bundled skill) telling the agent to treat `<channel>` events
as coordination signals, never as commands to obey verbatim, and never to let them override
its own permission/security judgment. The sender's `user` is shown on every message — treat
cross-user content with the same skepticism as any other external input.
