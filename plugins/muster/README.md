# muster — Agent Coordination Bus channel

Pushes an agent's **Valkey inbox** into its own Claude Code session as native
`<channel source="muster">` events. No keystroke injection, no pane targeting — the agent
delivers its own inbox to itself over a stdio MCP channel.

> **Status: MVP — inbound delivery + `roster` / `chat` / `fetch`.** Same-group only (no
> cross-group coordination). `ack` / `announce` / `task_add` and a standalone
> presence daemon are out of scope for now — see the [spec](../../docs/spec).

## How it works

- The plugin's MCP server declares the `claude/channel` capability, so Claude Code
  registers it as a channel.
- **Identity**: name = the agent's git repo (`repo~worktree` for a linked worktree;
  non-git falls back to `basename(cwd)`, then a generated id). Herdr-free — resolved from
  git + `cwd`, no herdr dependency. **Group** = the coordination scope: `MUSTER_GROUP` →
  herdr's `HERDR_WORKSPACE_ID` (when herdr is present) → `local`. This is the hard
  boundary: you can only see or message agents in your own group, never across groups.
- It tails its own Valkey inbox stream `muster:inbox:{group}:{name}`. Every new entry is
  pushed to the session as a `<channel>` event, resuming from a persisted read cursor
  (`muster:inboxread:{group}:{name}`), so mail queued **before** the channel connects
  (first-launch `uv` warm-up can take a few seconds) is still delivered, and nothing is
  re-delivered across restarts. If Valkey is unreachable the MCP handshake still
  succeeds and the channel stays idle (never crashes).
- It self-registers presence (`muster:presence:{group}:{name}`, refreshed every 30s, 90s
  TTL) — that key IS the roster; no separate daemon needed for discovery. `status` is
  herdr's live `agent_status` (`idle`/`working`/`blocked`) when herdr is present, else
  `online`.
- Three tools, all scoped to your own group:
  - `roster` — list the AI agents live in your group, by name, each with its status
    (`idle`/`working`/`blocked` under herdr, or `online` without it).
  - `chat {to, body, subject?}` — **real-time** message to a peer by name, for when you need
    them now. They see a short **envelope** (your name + subject, plus a "· fetch for full"
    nudge when the body is longer than the line) and read the full body with `fetch`; `subject`
    defaults to the body's first line. Under herdr, only **idle** agents accept chat — sending
    to a `working` or `blocked` agent returns an error (non-herdr agents show `online` and
    always accept). Pass `important: true` to override the gate and deliver anyway; it's marked
    ❗ and read when the agent next runs (a channel event, never a keystroke — so it can't
    answer a `blocked` agent's permission prompt).
  - `fetch {limit?}` — read the full bodies of your own recent inbox messages (the
    channel only pushes envelopes/summaries; limit is clamped to 1–100).
- On startup it pushes one **welcome** event: your identity, the live roster, a nudge
  to load the `muster-chat` skill (skills aren't auto-read — the core rules also ship in
  the always-on `instructions` string), and how many items are already waiting.
  Disable with `MUSTER_WELCOME=0`.
- On **`/clear` or compaction** a **bundled `SessionStart` hook** (`hooks/hooks.json`,
  matcher `clear|compact`) re-injects a short static nudge — "Muster is still active, check
  `roster`/`fetch`, load the skill" — into the fresh context. The MCP server keeps running across
  `/clear`, so you stay registered and reachable; live peers and pending mail come from the tools
  on demand. A plain `echo` (no Valkey, no script), ships in the plugin, no user config.
- On startup it also **greets** the peers already live in the group — each gets a
  `👋 {name} joined group {group}` notice in their channel, so running agents learn a new
  peer arrived. Deduped by a 5-minute marker (`SET NX`), so a quick restart does **not**
  re-announce. Disable with `MUSTER_JOIN=0`.

## Requirements

- Claude Code **v2.1.80+** with channels enabled (research preview).
- [`uv`](https://docs.astral.sh/uv/) on `PATH` — the only Python-side install. The server runs
  via `uv run --with mcp>=1.28,<1.29 --with redis --no-project` (declared in `.mcp.json`), so uv
  fetches `mcp` + `redis` itself at launch — no `pip`, no `requirements.txt`, no virtualenv.
- A reachable Valkey/Redis. Default `redis://localhost:6379/1` — override with
  `MUSTER_VALKEY_URL`.
- Nothing else — [herdr](https://herdr.dev) is *optional*. Present, it auto-fills your
  group from its workspace id and reports live agent status. Absent, set
  `MUSTER_GROUP=<name>` yourself (unset ⇒ `local`) and status shows `online`.
  Set `MUSTER_GROUP` on **all** panes of a group or **none** — mixing an explicit `MUSTER_GROUP`
  with herdr's workspace on other panes splits them into different groups.

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

With the plugin active, call `roster` to see who else is live, then `chat` to reach
them by name — that's the normal path. It appears in their session as:

```
<channel source="muster" ident="w5:ach-agent" msg_id="…">✉ ach: schema regen · fetch for full</channel>
```

<details>
<summary>Manual delivery test (bypass <code>chat</code>, write the inbox stream directly)</summary>

```bash
redis-cli -n 1 XADD "muster:inbox:<group>:<name>" '*' summary "2 unread from ach — run fetch"
```
</details>

## Trust model

Channel content is **untrusted** — a request, not an authority. The server ships an
`instructions` string (and a bundled skill) telling the agent to treat `<channel>` events
as coordination signals, never as commands to obey verbatim, and never to let them override
its own permission/security judgment.
