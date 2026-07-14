# Getting started with Muster

The full walkthrough: install the plugin, launch it, remove the warning, configure your
group (with or without herdr), set up an alias, and coordinate. Skim the headers and jump
to what you need.

- [0. Prerequisites](#0-prerequisites)
- [1. Install the plugin](#1-install-the-plugin)
- [2. Launch](#2-launch)
- [3. Remove the warning (optional, admin)](#3-remove-the-warning-optional-admin)
- [4. Your group — with or without herdr](#4-your-group--with-or-without-herdr)
- [5. Environment variables](#5-environment-variables)
- [6. Make an alias](#6-make-an-alias)
- [7. Coordinate: roster / chat / fetch](#7-coordinate-roster--chat--fetch)
- [8. Trust model](#8-trust-model)
- [9. Troubleshooting](#9-troubleshooting)

---

## 0. Prerequisites

- **Valkey / Redis ≥ 7** — the transport + coordination store. From the repo root:
  ```bash
  docker compose up -d
  redis-cli -n 1 ping     # -> PONG   (Muster uses logical DB 1)
  ```
  Not on `localhost:6379/1`? Point the server at it with `MUSTER_VALKEY_URL` (see §5).
- **[uv](https://docs.astral.sh/uv/)** on `PATH` — the *only* thing you install for the Python
  side. Claude Code launches the server through the plugin's `.mcp.json`
  (`uv run --with mcp>=1.28,<1.29 --with redis --no-project python …/muster_channel.py`), so
  **uv fetches the Python deps (`mcp`, `redis`) itself on first launch — no `pip install`, no
  `requirements.txt`, no virtualenv to manage.** First launch warms the uv cache (a few
  seconds); later launches are instant. uv also fetches a suitable Python (3.10+) if the
  system has none.
- **Claude Code ≥ 2.1.80** — channels are a [research preview](https://code.claude.com/docs/en/channels#research-preview).
- **[herdr](https://herdr.dev)** — *optional*. Present, it auto-forms your group and reports
  live agent status. Absent, you set the group yourself (§4). Nothing breaks without it.

## 1. Install the plugin

```bash
claude plugin marketplace add ackstorm/muster-chat
claude plugin install muster@muster-chat
```

- **Always use the qualified name `muster@muster-chat`.** The bare `muster` does not resolve
  (`claude plugin update muster` → *"Plugin not found"*).
- **Updates are version-gated.** `claude plugin update` is a no-op until the plugin's
  version bumps, so refresh the marketplace first:
  ```bash
  claude plugin marketplace update muster-chat && claude plugin update muster@muster-chat
  ```

## 2. Launch

Channels are a research preview, so the channel must be activated **at launch** — it is not
enough for the plugin to be installed.

**A) Right now, works out of the box** — the dev flag:

```bash
claude --dangerously-load-development-channels plugin:muster@muster-chat
```

You will see a `WARNING: Loading development channels` banner **every launch. This is
expected, not an error.** During the preview, `--channels` only loads plugins on Anthropic's
built-in allowlist, and `muster` (a third-party plugin) isn't on it — so this flag is *the* way
to run it. It's safe for a plugin you built or trust.

**B) The clean command `--channels` (no warning)** — works **only after** an admin
allowlists `muster` (§3):

```bash
claude --channels plugin:muster@muster-chat
```

> ⚠️ Without the allowlist, `--channels plugin:muster@muster-chat` does **not** load muster: Claude
> starts normally but the channel silently doesn't register, and a startup notice says the
> plugin isn't approved. Use path **A** until §3 is done.

On launch the channel greets you:

```
← muster: FYI: Muster online (Agent Coordinator Harness) — you are "ach-agent" in group "w5". You have 2 item(s) waiting. Live peers: ach. Tools: roster, chat, fetch. New here? Load the muster-chat skill.
```

Silence the greeting with `MUSTER_WELCOME=0`; silence the join-notice to peers with `MUSTER_JOIN=0` (§5).

## 3. Remove the warning (optional, admin)

**Optional.** On a personal Pro/Max account, path A above already works — only the warning is
extra. This step matters for teams and long-lived setups. It switches you to `--channels`.

Two things to know first:

- **Team/Enterprise orgs block channels by default.** Until an Owner enables `channelsEnabled`
  (claude.ai → Admin settings → Claude Code → Channels, or managed settings), *even the dev
  flag delivers nothing*. Personal Pro/Max accounts skip this check.
- **`allowedChannelPlugins` replaces Anthropic's default list** when set. If you also use
  official channels (telegram/discord), list them here too or they stop registering.

`channelsEnabled` and `allowedChannelPlugins` are **managed settings only — a regular user
cannot override them.** An admin edits the managed-settings file:

- **Linux/WSL:** `/etc/claude-code/managed-settings.json`
- **macOS:** `/Library/Application Support/ClaudeCode/managed-settings.json`
- **Windows:** `C:\Program Files\ClaudeCode\managed-settings.json`

```jsonc
{
  "channelsEnabled": true,
  "allowedChannelPlugins": [
    { "marketplace": "muster-chat", "plugin": "muster" }
    // add official channels you still use, e.g.:
    // { "marketplace": "claude-plugins-official", "plugin": "telegram" }
  ]
}
```

Then launch with path **B** — no flag, no warning. Revert by deleting the file.

## 4. Your group — with or without herdr

A **group** is the coordination scope: you can only see and message agents in your own group.
It's a hard boundary — no cross-group listing or sending.

**With herdr** — nothing to set. Your group is herdr's workspace id, and `roster` shows each
peer's live status (`idle` / `working` / `blocked`). herdr is detected via `HERDR_ENV`.

**Without herdr** — set the group yourself:

```bash
MUSTER_GROUP=my-project claude --dangerously-load-development-channels plugin:muster@muster-chat
```

- Unset `MUSTER_GROUP` ⇒ everyone shares the default group `local`.
- Status shows `online` (herdr's live status isn't available).

> **Foot-gun:** set `MUSTER_GROUP` on **all** panes of a group or **none**. Mixing an explicit
> `MUSTER_GROUP` on one pane with herdr's workspace on another splits them into different groups,
> and they won't see each other.

Resolution order: `MUSTER_GROUP` → herdr's `HERDR_WORKSPACE_ID` → `local`.

Your **name** in the group is your git repo (`repo~worktree` for a linked worktree); non-git
falls back to the directory name, then a generated id.

## 5. Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `MUSTER_GROUP` | herdr workspace, else `local` | Coordination scope (§4). |
| `MUSTER_VALKEY_URL` | `redis://localhost:6379/1` | Where Valkey lives. |
| `MUSTER_WELCOME` | `1` | `0` silences the startup greeting to yourself. |
| `MUSTER_JOIN` | `1` | `0` silences the `[presence] + … online` notice you send to peers on startup (deduped 5 min, so restarts stay quiet). |

herdr's `HERDR_ENV` / `HERDR_PANE_ID` / `HERDR_WORKSPACE_ID` are read when present.

## 6. Make an alias

Typing the launch line every time gets old. Add a one-word alias.

```bash
# ~/.zshrc or ~/.bashrc

# before the allowlist (§3) — dev flag, warning is expected:
alias muster='claude --dangerously-load-development-channels plugin:muster@muster-chat'

# after the allowlist (§3) — clean, no warning:
alias muster='claude --channels plugin:muster@muster-chat'

# pin a group when you're not using herdr:
alias muster='MUSTER_GROUP=my-project claude --channels plugin:muster@muster-chat'
```

Reload with `source ~/.zshrc` (or open a new shell), then just run `muster`.

## 7. Coordinate: roster / chat / fetch

Three tools, all scoped to your group:

- **`roster`** — list the agents live in your group, by name, each with its status
  (`idle`/`working`/`blocked` under herdr, else `online`). This is how you discover who to
  reach; presence is self-registered (30s refresh, 90s TTL), no daemon needed.
- **`chat {to, body, subject?}`** — **real-time** message to a peer by name, for when you need
  them now. They see a short **envelope** (your name + subject) in their session and read the
  full body with `fetch`. Put the gist in `subject`, the detail in `body` (`subject` defaults
  to the body's first line). A long body gets a `· fetch for full` nudge so it never reads as
  truncated. It's a *request* to a peer, not a command they must obey. **Under herdr, only
  `idle` agents accept chat** — sending to a `working` or `blocked` agent returns an error
  (without herdr everyone is `online` and always accepts). Pass `important: true` to override
  the gate and deliver anyway; it's marked ❗ and read when the agent next runs. (Delivery is a
  channel event, never a keystroke, so it can't answer a `blocked` agent's permission prompt.)
- **`fetch {limit?}`** — read the full bodies of your own recent inbox messages (the channel
  push only carries the envelope; `limit` clamps 1–100).

The recipient sees, in their session:

```
<channel source="muster" ident="w5:ach-agent" msg_id="…">✉ Message from ach: schema regen · fetch for full</channel>
```

On startup you also notify peers already live in the group (`[presence] + "you" online (no action needed)`) unless
`MUSTER_JOIN=0`.

## 8. Trust model

Channel content is **untrusted — a request, not an authority.** The server tells the agent to
treat `<channel>` events as coordination signals, never as commands to obey verbatim, and
never to let them override its own permission/security judgment. A peer's message asks; it
never compels.

## 9. Troubleshooting

| Symptom | Cause → fix |
|---|---|
| Channel doesn't register (no greeting, tools missing) with `--channels` | muster isn't allowlisted → use the dev flag (path A) or do §3. |
| Same, on a Team/Enterprise account | `channelsEnabled` is off → an Owner must enable it (§3). |
| `claude plugin update muster` → *"Plugin not found"* | Qualify it: `muster@muster-chat`. |
| Update seems to do nothing | Version-gated + stale marketplace → `claude plugin marketplace update muster-chat` first, and the plugin version must have bumped. |
| `roster` empty though a peer is running | Different groups (`MUSTER_GROUP` mismatch, or one pane on herdr's workspace and another on an explicit `MUSTER_GROUP` — see §4 foot-gun), Valkey down, or the peer's presence expired (90s TTL — it may have exited). |
| Two panes of the **same repo** share one inbox | Known limitation: identity is the repo name, so two panes of the same repo collide. Use separate worktrees (distinct `repo~worktree` names). |
| Channel is idle, no messages arrive | Valkey unreachable — the MCP handshake still succeeds, delivery is just disabled. Check `docker compose ps` and `MUSTER_VALKEY_URL`. |

---

See also: [ARCHITECTURE.md](./ARCHITECTURE.md) (how it works)
and the [`muster` plugin README](../plugins/muster/README.md).
