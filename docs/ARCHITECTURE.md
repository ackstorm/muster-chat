# How Muster works

Muster (Agent Coordination Bus) lets several AI coding agents that share a group
coordinate — send each other mail, announce intent, and pick up tasks — **without any
per-session setup by the human**. This is the overview; the full design is in
[`spec/SPEC-agent-coordination-bus.md`](./spec/SPEC-agent-coordination-bus.md).

## Requirements

| Component | Role | Notes |
|---|---|---|
| **Valkey** (or Redis) ≥ 7 | Transport + durable coordination state (Streams). | `docker compose up -d` — see the repo README. |
| **[uv](https://docs.astral.sh/uv/)** | Runs the channel plugin's Python server (`uv run --with mcp --with redis`). | No virtualenv to manage. |
| **Claude Code** ≥ 2.1.80 | The agent, and the delivery surface (**channels**). | Channels are a research preview; must be enabled/launched (see below). |
| **[herdr](https://herdr.dev)** ≥ 0.7.1 | Terminal workspace manager. When present, runs each agent in a **pane** and auto-fills its coordination **group** (workspace id) and live `agent_status`. | Optional — see below. |

## The pieces

```
 identity ── git + env + OS (herdr-free)
   │  · name: git repo (repo~worktree for a linked worktree) → basename(cwd) → generated id
   │  · group (coordination scope): MUSTER_GROUP → HERDR_WORKSPACE_ID → "local"
   ▼
 Valkey ── transport + state (Streams, all keys `muster:*`)
   │  · muster:inbox:{group}:{name}      per-agent mail (a stream)
   │  · muster:presence:{group}:{name}   who's live — IS the registry, no separate daemon
   ▲
   │  each agent runs its own …
 channel plugin (per pane, this repo's `muster` plugin)
      · a stdio MCP server that declares the `claude/channel` capability
      · resolves its own identity from git + env, no herdr dependency
      · tails ITS OWN muster:inbox and pushes each entry into ITS OWN session
        as a native `<channel source="muster">` event  — no keystrokes

 herdr (optional) ── when present, adapter for group + live status only
      · injects HERDR_ENV / HERDR_WORKSPACE_ID / HERDR_PANE_ID into the pane
      · `agent_status` (idle/working/blocked/…) for THIS pane, shown in `roster`;
        absent ⇒ status "online"
```

A **daemon** (design in the spec, Phase 0 built separately) watches herdr and writes the
registry/presence and matches deferred work. Crucially it **does not deliver** — delivery
is self-service: every agent's own channel server pushes its own inbox into its own session.

## herdr: optional adapter for group + live status

Muster works with no herdr at all — identity comes from git + env + the OS, and Valkey's
presence key is the only registry. herdr, when present, upgrades two things:

- **Group.** If `MUSTER_GROUP` isn't set explicitly, `HERDR_WORKSPACE_ID` (injected into every
  pane herdr runs, alongside `HERDR_ENV=1` and `HERDR_PANE_ID`) becomes the group — every
  agent in the same herdr workspace shares one automatically.
- **Live status.** `herdr.agent_status()` runs the `herdr pane list` CLI (only when
  `HERDR_ENV` is set) and looks up THIS pane's `agent_status`
  (`idle|working|blocked|done|unknown`) by its `HERDR_PANE_ID`. That's what `roster` shows
  as your status; without herdr it's always `online`.

Without herdr: set `MUSTER_GROUP=<name>` yourself (unset ⇒ everyone shares `local`). Any
`herdr` call failing — absent, the CLI erroring, pane not listed — degrades to `None`/`[]`,
never crashes the channel.

## How a message reaches an agent

1. A sender (a peer agent, or the daemon) writes to the recipient's inbox stream:
   `XADD muster:inbox:{id} * summary "2 unread from ach — run fetch"`.
2. The recipient's channel server is already `XREAD`-blocking on `muster:inbox:{id}`. It wakes,
   reads the new entry, and pushes a `notifications/claude/channel` MCP notification.
3. Claude Code renders it in the recipient's session as
   `<channel source="muster" ident="…">2 unread from ach — run fetch</channel>`.
4. The agent treats it as a **signal, not a command** (channel content is untrusted — a
   request, never authority) and acts in the context of its current work.

No terminal keystrokes are injected at any point — delivery is a native, in-session push.

## Delivery semantics

The channel server resumes from a persisted cursor (`muster:inboxread:{id}`), so mail queued
**before** the channel connects (the server's first `uv` start takes a few seconds) is
still delivered, and nothing is re-delivered across restarts.

## Enabling channels

Channels are a research preview and must be activated at launch:

```bash
# development
claude --dangerously-load-development-channels plugin:muster@muster-chat

# organization production (per Anthropic's docs): an admin sets managed settings
#   channelsEnabled: true   +   allowedChannelPlugins: [{marketplace, plugin}]
# then, with no dangerous flag:
claude --channels plugin:muster@muster-chat
```

See [Claude Code → Channels](https://code.claude.com/docs/en/channels) and the
[Channels reference](https://code.claude.com/docs/en/channels-reference).
