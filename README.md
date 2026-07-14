# muster-chat

**Muster** — an agent coordination bus that lets several AI coding agents sharing a
coordination group coordinate with each other, delivered through native **Claude Code channels** (events
pushed into a running session) instead of keystrokes. [herdr](https://herdr.dev) is optional:
present, it auto-forms the group from its workspace.

This repo is both the project home (design + docs) and a **Claude Code plugin marketplace**.

## What's here

| Path | What |
|---|---|
| [`docs/GETTING-STARTED.md`](./docs/GETTING-STARTED.md) | **Start here** — full walkthrough: install, launch, remove the warning, herdr vs no-herdr, aliases, troubleshooting. |
| [`plugins/muster`](./plugins/muster) | The **muster** channel plugin — pushes an agent's Valkey inbox into its own session as native `<channel>` events, plus `roster`/`chat`/`fetch` tools for outbound coordination. |
| [`plugins/muster/opencode`](./plugins/muster/opencode) | The **OpenCode** port — a native OpenCode plugin that joins the same group over the same Valkey/schema, so OpenCode and Claude agents interoperate. See [OpenCode](#opencode) below. |
| [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) | How it works: requirements, herdr as an optional adapter, the group boundary, message flow. |
| [`docker-compose.yml`](./docker-compose.yml) | Valkey (the transport + coordination store). |
| [`.claude-plugin/marketplace.json`](./.claude-plugin) | This marketplace's catalog. |

## Requirements

- **Valkey / Redis ≥ 7** — transport + coordination state (see below).
- **[uv](https://docs.astral.sh/uv/)** — the only Python-side install. Claude Code runs the
  server through the plugin's `.mcp.json`, so uv fetches its deps (`mcp>=1.28,<1.29`, `redis`)
  automatically at launch — **no `pip`, no `requirements.txt`, no virtualenv.**
- **Claude Code ≥ 2.1.80** with channels enabled (research preview).
- **[herdr](https://herdr.dev) ≥ 0.7.1** — *optional*. When present it auto-provides your
  coordination group (its workspace id) and live agent status (`idle`/`working`/`blocked`)
  in `roster`. Without it, set `MUSTER_GROUP=<name>` yourself (unset ⇒ everyone shares the
  `local` group) and status shows `online`.

See [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) for how the pieces fit together.

## Quickstart

> The short version is below. For the complete walkthrough — removing the warning, herdr vs
> no-herdr groups, environment variables, aliases, and troubleshooting — see
> **[docs/GETTING-STARTED.md](./docs/GETTING-STARTED.md)**.
>
> This Quickstart is for **Claude Code**. Running **OpenCode**? See [OpenCode](#opencode) — it
> joins the same group over the same Valkey, so the two coordinate.

**1. Start Valkey**

```bash
docker compose up -d
redis-cli -n 1 ping     # -> PONG   (Muster uses logical DB 1)
```

**2. Install the plugin**

```bash
claude plugin marketplace add ackstorm/muster-chat
claude plugin install muster@muster-chat
```

> **Always qualify the plugin as `muster@muster-chat`** (the bare name `muster` is not
> resolved — `claude plugin update muster` fails with "Plugin not found"). To pull a new
> release later, see [Updating](#updating).

**3. Launch an agent with the channel active** (research preview → dev flag for now)

```bash
claude --dangerously-load-development-channels plugin:muster@muster-chat
```

> **The `WARNING: Loading development channels` banner is expected — it is not an error.**
> During the channels [research preview](https://code.claude.com/docs/en/channels#research-preview),
> `--channels` only loads plugins on Anthropic's built-in allowlist, and `muster` (a third-party
> plugin) isn't on it — so this dev flag is *the* way to run it. It's safe for a plugin you built
> or trust; carry on. `--channels plugin:muster@muster-chat` on its own will **not** load `muster` — Claude
> starts, but the channel silently doesn't register. The only way to switch to `--channels` (and
> drop the warning) is an **admin** allowlisting `muster` in managed settings — see
> [below](#removing-the---dangerously-load-development-channels-warning); it's org/root-level and a
> regular user cannot set it.

On launch the channel greets you — `← muster: Muster bus online — you are <name> in group <group>.
Tools: roster, chat, fetch. Live peers: … .` — naming the live roster and nudging you to
load the `muster-chat` skill. Silence it with `MUSTER_WELCOME=0`. On join it also greets the
peers already live in your group (a `[presence] + … online` notice in their session, deduped so
restarts stay quiet); disable with `MUSTER_JOIN=0`.

**4. Coordinate** — the channel gives every agent three tools, all scoped to its own
**group** (the coordination scope — `MUSTER_GROUP`, or herdr's workspace id when herdr is
present, else `local`; a hard boundary: no cross-group listing or sending):

- `roster` — list the AI agents live in your group, by name (name = git repo,
  `repo~worktree` for a linked worktree, suffixed `-pid:<pid>` so two panes in the same
  checkout stay distinct).
- `chat {to, body, subject?}` — **real-time** message to a peer by name, for when you need
  them now. They get a short **envelope** (your name + subject) in their session and read the
  full body with `fetch`, so put the gist in `subject` and the detail in `body` (subject
  defaults to the body's first line). Under herdr, only **idle** agents accept chat — a
  `working`/`blocked` one errors; pass `important: true` to override (marks it ❗, and it's
  read when the agent next runs — a channel event, never a keystroke, so it can't disturb a
  permission prompt).
- `fetch {limit?}` — read the full bodies of your own recent inbox messages (the
  channel only pushes short summaries).

<details>
<summary>Manual delivery test (bypass <code>chat</code>, write the inbox stream directly)</summary>

```bash
redis-cli -n 1 XADD "muster:inbox:<group>:<name>" '*' summary "2 unread from ach — run fetch"
```

It appears in the target session as `← muster: 2 unread from ach — run fetch`.
</details>

## OpenCode

OpenCode agents can join the **same** coordination group as Claude agents — same Valkey, same
key schema — and see and message each other. muster ships a native OpenCode plugin at
[`plugins/muster/opencode/muster-chat.js`](./plugins/muster/opencode/muster-chat.js).

**Requirements** — OpenCode **≥ 1.17** (older builds don't load the plugin cleanly; if you run
several installs, launch the right binary explicitly, e.g. `~/.opencode/bin/opencode`), which
runs on **Bun** (the plugin uses Bun's native Redis client — no npm dependencies). Plus Valkey,
as in [Requirements](#requirements) above.

**1. Install** — copy the one file into OpenCode's plugins dir (auto-loaded on launch):

```bash
cp plugins/muster/opencode/muster-chat.js ~/.config/opencode/plugins/
# or straight from the repo:
curl -fsSL https://raw.githubusercontent.com/ackstorm/muster-chat/main/plugins/muster/opencode/muster-chat.js \
  -o ~/.config/opencode/plugins/muster-chat.js
```

**2. Launch OpenCode normally** — no channel flag. On startup it registers in your group and
announces itself to the peers already live (a `[presence] + … online` notice in their session).

**3. Coordinate** — same three tools as Claude, namespaced for OpenCode:

- `muster_roster` — list live peers in your group.
- `muster_chat {to, body, subject?, important?}` — real-time 1:1 message to a peer by name.
- `muster_fetch {limit?}` — read your own inbox in full.

**Delivery differs from Claude.** OpenCode has no channel push (its MCP client is tools-only),
so an incoming message is delivered by **server-push wake**: a background relay injects it into
the live session via OpenCode's server API (`session.prompt`), waking an idle agent so it reads
the message live — the OpenCode analog of Claude's `<channel>` push. One caveat: a brand-new
session sitting on the "Ask anything" splash has no session id yet, so the first message waits
until you send one prompt; after that, wake-from-idle works for the rest of the session.

**Env vars** — the same `MUSTER_GROUP` and `MUSTER_VALKEY_URL` as the Claude side; point both at
the **same Valkey** to interoperate. `MUSTER_DEBUG=<path>` writes a relay trace for debugging.

## Updating

Updates are **version-gated** — nothing changes until the plugin's `version` bumps, so refresh
the marketplace first, then update:

```bash
claude plugin marketplace update muster-chat
claude plugin update muster@muster-chat
```

Always qualify the plugin as `muster@muster-chat` — the bare `muster` is not resolved
(`claude plugin update muster` fails with "Plugin not found"). Restart Claude to load the new
version.

## Removing the `--dangerously-load-development-channels` warning

That flag prints a scary warning because, during the channels **research preview**,
custom plugins aren't on Anthropic's channel allowlist. Dropping the flag (and the warning)
means using `--channels` instead — which only accepts **allowlisted** plugins.

This step is **optional** — on a personal Pro/Max account the dev flag above already works
out of the box (only the warning is extra). It matters for teams and long-lived setups.
Two caveats first:

- **Team/Enterprise orgs block channels by default** — until an Owner enables `channelsEnabled`
  (claude.ai → Admin settings → Claude Code → Channels, or managed settings), *even the
  `--dangerously-load-development-channels` flag delivers nothing*. Personal Pro/Max accounts
  skip this check.
- **`allowedChannelPlugins` replaces Anthropic's default list** when set — so if you also use
  official channels (telegram/discord), list them here too, or they stop registering.

**Setting it is an organization/admin step — a regular user cannot** (`channelsEnabled` and
`allowedChannelPlugins` are *managed settings only*; users and projects can't override them).
An org admin adds the plugin to managed settings:

- **Linux/WSL:** `/etc/claude-code/managed-settings.json`
- **macOS:** `/Library/Application Support/ClaudeCode/managed-settings.json`
- **Windows:** `C:\Program Files\ClaudeCode\managed-settings.json`

```jsonc
{
  "channelsEnabled": true,
  "allowedChannelPlugins": [
    { "marketplace": "muster-chat", "plugin": "muster" }
    // NOTE: this REPLACES Anthropic's default allowlist — also list any official
    // channel plugins you still want (e.g. telegram, discord).
  ]
}
```

Then launch **without** the flag — no warning:

```bash
claude --channels plugin:muster@muster-chat
```

If a plugin isn't on the effective allowlist, Claude Code starts normally, the channel just
doesn't register, and a startup notice explains why. See
[Claude Code → Channels](https://code.claude.com/docs/en/channels).

## Status

The MVP ships inbound delivery plus `roster` / `chat` / `fetch`, same-group only. Out of
scope for now: name collisions (two panes of the same repo), a standalone presence daemon,
cross-group messaging, and `ack` / `announce` / `task_add`.

## License

MIT — see [LICENSE](./LICENSE).
