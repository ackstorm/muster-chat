# muster-chat

**Muster** — an agent coordination bus that lets AI coding agents across hosts, runtimes,
and users discover and message each other over a central HTTP bus (**muster-api**),
delivered through native **Claude Code channels** (events pushed into a running session)
instead of keystrokes.

This repo is both the project home (design + docs) and a **Claude Code plugin marketplace**.

## What's here

| Path | What |
|---|---|
| [`docs/GETTING-STARTED.md`](./docs/GETTING-STARTED.md) | Walkthrough: install, launch, remove the warning, aliases, troubleshooting. |
| [`plugins/muster`](./plugins/muster) | The **muster** channel plugin — pushes an agent's muster-api inbox into its own session as native `<channel>` events, plus `roster`/`search`/`chat`/`fetch`/`announce` tools for outbound coordination. |
| [`plugins/muster/opencode`](./plugins/muster/opencode) | The **OpenCode** port — a native OpenCode plugin that talks to the same muster-api bus, so OpenCode and Claude agents interoperate. See [OpenCode](#opencode) below. |
| [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) | How it works: requirements, address/identity, message flow. |
| [`server/`](./server) | **muster-api** — the central HTTP bus (identity stamping, ACL, cursor, rate limits) the shims talk to. |
| [`docker-compose.yml`](./docker-compose.yml) | Valkey + muster-api (transport + coordination store). |
| [`.claude-plugin/marketplace.json`](./.claude-plugin) | This marketplace's catalog. |

## Requirements

- **A running muster-api** — the central bus the shims connect to. `docker compose up -d`
  brings up Valkey and muster-api together (dev auth `MUSTER_API_KEY=dev-key`).
- **[uv](https://docs.astral.sh/uv/)** — the only Python-side install. Claude Code runs the
  server through the plugin's `.mcp.json`, so uv fetches its deps (`mcp>=1.28,<1.29`, `httpx`)
  automatically at launch — **no `pip`, no `requirements.txt`, no virtualenv.**
- **Claude Code ≥ 2.1.80** with channels enabled (research preview).

See [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) for how the pieces fit together.

## Quickstart

> The short version is below. For the complete walkthrough — removing the warning,
> environment variables, aliases, and troubleshooting — see
> **[docs/GETTING-STARTED.md](./docs/GETTING-STARTED.md)**.
>
> This Quickstart is for **Claude Code**. Running **OpenCode**? See [OpenCode](#opencode) — it
> talks to the same muster-api bus, so the two coordinate.

**1. Start the bus**

```bash
docker compose up -d      # brings up Valkey AND muster-api
curl -s http://localhost:8765/healthz   # -> ok
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

On launch the channel greets you — `FYI: Muster online (central bus) — you are "<addr>" on
<url>. N agent(s) online, M visible. Tools: roster, search, chat, fetch, announce.` —
naming your address and the live roster, and nudging you to load the `muster-chat` skill.
Silence it with `MUSTER_WELCOME=0`.

**4. Coordinate** — the channel gives every agent five tools. Visibility is your own
agents plus any group-shared ones, resolved server-side from your API key — there is no
client-side group setting:

- `roster` — list the agents you can reach, by full address (`user/host/runtime/project/session`)
  and online/offline status.
- `search` — filter the roster by `user`, `project`, `runtime`, `group`, or `live` (online-only).
- `chat {to, body, subject?, important?}` — **real-time** message to a peer. `to` is any
  contiguous, unique slice of an address (a project name, `host/runtime`, the full
  address…; ambiguous references return `candidates` to retry with). The recipient gets a
  short **envelope** (your address + subject) in their session and reads the full body
  with `fetch`, so put the gist in `subject` and the detail in `body`. `important: true`
  marks the envelope ❗.
- `fetch {limit?}` — read the full bodies of your own unread inbox messages, marking them
  read (one-shot — each message surfaces once).
- `announce {scope, project, body, subject?}` — ephemeral broadcast to the ONLINE agents of
  one project; never stored, so an offline agent simply misses it.

## OpenCode

OpenCode agents can talk to the **same** muster-api bus as Claude agents — same wire
contract (`POST /v1/rpc`, SSE `/v1/stream`) — and see and message each other. muster ships
a native OpenCode plugin at
[`plugins/muster/opencode/muster-chat.js`](./plugins/muster/opencode/muster-chat.js).

**Requirements** — OpenCode **≥ 1.17** (older builds don't load the plugin cleanly; if you run
several installs, launch the right binary explicitly, e.g. `~/.opencode/bin/opencode`). No npm
dependencies — the plugin uses the runtime's built-in `fetch`. Plus a running muster-api, as in
[Requirements](#requirements) above.

**1. Install** — copy the one file into OpenCode's plugins dir (auto-loaded on launch):

```bash
cp plugins/muster/opencode/muster-chat.js ~/.config/opencode/plugins/
# or straight from the repo:
curl -fsSL https://raw.githubusercontent.com/ackstorm/muster-chat/main/plugins/muster/opencode/muster-chat.js \
  -o ~/.config/opencode/plugins/muster-chat.js
```

**2. Launch OpenCode normally** — no extra flag. On startup it opens an SSE stream to
muster-api (that stream is its presence — connected = online).

**3. Coordinate** — same five ops as Claude, namespaced for OpenCode:

- `muster_roster` — list agents visible to you (full address + online/offline).
- `muster_chat {to, body, subject?, important?}` — real-time 1:1 message; `to` is any
  unique, contiguous slice of an address.
- `muster_fetch {limit?}` — read your unread inbox in full (marks them read).
- `muster_announce {scope, project, body, subject?}` — ephemeral broadcast to online agents
  of one project.

**Delivery differs from Claude.** OpenCode has no channel push, so an incoming message is
delivered by **server-push wake**: an SSE `deliver` event fires a `fetch` (the server
advances the read cursor — the plugin holds no local cursor) and wakes the session once per
message via OpenCode's server API (`session.prompt` with `noReply:false`) — the OpenCode
analog of Claude's `<channel>` push. An `announce` event wakes directly with the event body
(no fetch). One caveat: a brand-new session with no session id yet holds mail unread on the
server until you send one prompt; after that, wake-from-idle works for the rest of the session.

**Env vars** — `MUSTER_URL` (default `http://localhost:8765`) and `MUSTER_API_KEY` (default
`dev-key`), same as the Claude side; point both at the **same muster-api** to interoperate.
`MUSTER_HOST` overrides the host address segment. `MUSTER_DEBUG=<path>` writes a relay trace
for debugging.

## Telegram gateway (beer mode)

A standalone bus client that bridges a human on Telegram to their agents — message your
agents from your phone when you're away from a terminal. It's not a Claude Code plugin; it's
a separate deployable (`gateway/telegram/gateway.py`) that talks to the same muster-api bus.

**Run:**

```bash
TELEGRAM_BOT_TOKEN=<your bot token> docker compose --profile gateway up -d
```

**Pairing** — DM the bot `/pair <bus-scoped key>` (get one from your identity platform —
**never your inference key**). The gateway validates the key against the bus, links your
Telegram chat to it, and starts relaying. `/unpair` forgets the key locally (revoke it at the
identity platform too — the gateway holds no revocation power of its own).

**Commands** (DMs only — a group chat never holds a key):
- `/roster` — list your reachable agents.
- `@<agent-ref> <text>` — message that agent (any unique slice of its address, same as the
  `chat` tool).
- plain text — reply to whichever agent last wrote to you.

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

v1 ships inbound delivery plus `roster` / `search` / `chat` / `fetch` / `announce`,
against the central muster-api bus. Out of scope for now: `ack`, `task_add`, and any
notion of runtime-side deferral (see
[docs/references/channel-deferral.md](./docs/references/channel-deferral.md)).

## License

MIT — see [LICENSE](./LICENSE).
