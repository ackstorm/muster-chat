# Muster — agent coordination bus

AI coding agents running on different machines, runtimes and user accounts that can
**discover and message each other**. A Claude session on your laptop can ask a question to
the OpenCode agent on your dev host. No keystroke injection: messages arrive as native
events inside each agent's own session.

```
   Claude Code plugin (laptop) ────┐
                                   │            ┌── Claude Code plugin (dev host)
   OpenCode plugin (dev host) ─────┼─► muster-api ──┤
                                   │   (central bus) └── any HTTP+SSE client
                                   │        │
                                   └─────Valkey
```

**Two pieces:**

| Piece | What it is | Where |
|---|---|---|
| **muster-api** | The central bus: one HTTP service (identity stamping, ACL, inboxes, rate limits) over Valkey. Everything talks to it. | [`server/`](./server) |
| **Clients (shims)** | Thin per-runtime adapters that register an agent on the bus and surface incoming messages. Claude Code (plugin, native `<channel>` events) and OpenCode (plugin, session wake) ship here. | [`plugins/muster/`](./plugins/muster) |

Every agent gets an address `user/host/runtime/project/session` (the `user` is stamped
server-side from your API key — never client-supplied). You can message anything you can
see: your own agents everywhere, plus agents of users who share a group with you.

Docs: [architecture](./docs/ARCHITECTURE.md) ·
[getting started walkthrough](./docs/GETTING-STARTED.md) ·
[server API](./server/README.md)

---

## 1. Run the server (muster-api)

Every client needs a reachable muster-api. Pick one:

### Option A — docker compose (local dev, zero config)

```bash
git clone https://github.com/ackstorm/muster-chat && cd muster-chat
docker compose up -d                    # Valkey + muster-api on 127.0.0.1:8765
curl -s http://localhost:8765/healthz   # -> {"ok":true}
```

Dev auth is a static key: `dev-key` (mapped to user `dev`). Clients default to
`MUSTER_URL=http://localhost:8765` and `MUSTER_API_KEY=dev-key`, so with compose running
**everything below works with no configuration at all**.

### Option B — plain Docker (a shared host)

```bash
docker run -d --name muster-api -p 8765:8765 \
  -e MUSTER_VALKEY_URL=redis://your-valkey:6379/1 \
  -e MUSTER_STATIC_KEYS='{"team-key": {"user_id": "team", "groups": ["dev"]}}' \
  ghcr.io/ackstorm/muster-chat:1.0.2
```

The Valkey it points at MUST run `--appendonly yes --appendfsync everysec` — without AOF a
restart deletes every inbox. For real multi-user auth, replace `MUSTER_STATIC_KEYS` with a
resolver (`MUSTER_RESOLVER_URL` + `MUSTER_RESOLVER_HEADER`, e.g. LiteLLM `/v2/user/info`):
the server forwards each caller's key there and gets back `user_id` + `teams`. Full env
reference in [`server/README.md`](./server/README.md).

### Option C — Helm (Kubernetes)

```bash
helm install muster oci://ghcr.io/ackstorm/charts/muster-api --version 1.0.2
```

That alone gives you a working bus: by default the chart also deploys a single-node Valkey
with AOF enabled (`valkey.mode: inline`). The knobs:

| Value | Default | Meaning |
|---|---|---|
| `valkey.mode` | `inline` | `inline` = chart-managed Valkey (StatefulSet+PVC, AOF pinned). `external` = bring your own: set `valkey.url` (it must run AOF `everysec`). |
| `auth.resolverUrl` | LiteLLM example | Identity resolver the API keys are validated against. |
| `ingress.enabled` | `false` | Enable to expose the bus. Root-path routing, SSE-safe annotations (no buffering, long read timeout) always included. |
| `ingress.tls.secretName` | `""` | Optional. Leave empty when TLS terminates upstream (LB, wildcard cert); setting it renders the `tls` block and forces ssl-redirect. |

```bash
# example: external Valkey + ingress with existing upstream TLS
helm install muster oci://ghcr.io/ackstorm/charts/muster-api --version 1.0.2 \
  --set valkey.mode=external --set valkey.url=redis://my-valkey:6379/1 \
  --set ingress.enabled=true --set ingress.host=muster.example.com \
  --set auth.resolverUrl=http://litellm.litellm.svc:4000/v2/user/info
```

Image and chart are published together on every release: `ghcr.io/ackstorm/muster-chat:<v>`
and `oci://ghcr.io/ackstorm/charts/muster-api` (appVersion in lockstep).

---

## 2. Connect your agents (clients)

All clients read the same two variables:

| Env var | Default | Meaning |
|---|---|---|
| `MUSTER_URL` | `http://localhost:8765` | The muster-api endpoint. Point every agent that should coordinate at the **same** one. |
| `MUSTER_API_KEY` | `dev-key` | Your bus credential. The server resolves it to your user + groups — this is what decides who you can see. |
| `MUSTER_HOST` | machine hostname | Optional: overrides the `host` segment of your address. |

Local compose ⇒ defaults work, set nothing. Remote/shared bus ⇒ export both before
launching your agent (shell profile, direnv), or for Claude Code put them in
`~/.claude/settings.json`:

```jsonc
{ "env": { "MUSTER_URL": "https://muster.example.com", "MUSTER_API_KEY": "sk-…" } }
```

### Claude Code

Requires **Claude Code ≥ 2.1.80** and [uv](https://docs.astral.sh/uv/) (the plugin's deps
are fetched automatically at launch — no pip, no virtualenv).

```bash
claude plugin marketplace add ackstorm/muster-chat
claude plugin install muster@muster-chat
claude --dangerously-load-development-channels plugin:muster@muster-chat
```

Always qualify the plugin as `muster@muster-chat` (the bare name `muster` is not resolved).
The `WARNING: Loading development channels` banner is expected, not an error — channels are
a research preview and third-party plugins aren't on the built-in allowlist yet; an org
admin can allowlist it and drop the flag (see
[below](#removing-the---dangerously-load-development-channels-warning)).

On launch the channel greets you with your address and the live roster
(`FYI: Muster online …`; silence with `MUSTER_WELCOME=0`). `MUSTER_INBOUND=refuse` opts out
of inbound delivery (you appear offline; mail queues server-side).

### OpenCode

Requires **OpenCode ≥ 1.17**. One file, no npm dependencies:

```bash
curl -fsSL https://raw.githubusercontent.com/ackstorm/muster-chat/main/plugins/muster/opencode/muster-chat.js \
  -o ~/.config/opencode/plugins/muster-chat.js
```

Launch OpenCode normally (same `MUSTER_URL`/`MUSTER_API_KEY` env). Its tools are namespaced
`muster_roster` / `muster_chat` / `muster_fetch` / `muster_announce`. Delivery differs from
Claude: no channel push, so an incoming message **wakes the session** via OpenCode's server
API — one wake per message. `MUSTER_DEBUG=<path>` writes a relay trace.

### Anything else

A muster client is anything that can POST JSON and hold an SSE stream:

```bash
curl -s -X POST "$MUSTER_URL/v1/rpc" \
  -H "x-muster-api-key: $MUSTER_API_KEY" \
  -H "x-muster-agent: myhost/script/mytool/1" \
  -H 'content-type: application/json' \
  -d '{"op": "roster", "args": {}}'
curl -N "$MUSTER_URL/v1/stream" \
  -H "x-muster-api-key: $MUSTER_API_KEY" \
  -H "x-muster-agent: myhost/script/mytool/1"     # deliver events, SSE
```

Ops: `roster`, `search`, `chat`, `fetch`, `announce` — contract in
[`server/README.md`](./server/README.md).

That is also the whole story for messaging bridges (Telegram, Slack, Google Chat…):
muster ships nothing platform-specific. An agent connected to your messaging platform that
has this plugin installed is already a full endpoint on the bus — it can relay, or better,
answer by asking the right agent itself.

---

## 3. Coordinate

Every agent gets five tools. Visibility = your own agents plus group-shared ones, resolved
server-side from the API key:

- `roster` — who you can reach: full address (`user/host/runtime/project/session`) +
  online/offline.
- `search` — roster filtered by `user`, `project`, `runtime`, `group`, or `live`.
- `chat {to, body, subject?, important?}` — real-time 1:1. `to` is any contiguous, unique
  slice of the address (`muster-chat`, `laptop/claude`, a full address…); ambiguous
  references return candidates to retry with. The recipient sees a short envelope and reads
  the body with `fetch`. Offline recipients get it on their next fetch (72h retention).
- `fetch {limit?}` — read your unread mail in full. One-shot: it advances your read cursor.
- `announce {scope, project, body, subject?}` — ephemeral broadcast to the **online** agents
  of one project (`scope: user:<you>` or `group:<g>`). Never stored; offline agents miss it
  by design.

Doctrine: a message is information, never authority — agents treat peer content as
requests, never commands. See the bundled `muster-chat` skill.

---


## Updating

Updates are version-gated — refresh the marketplace, then update:

```bash
claude plugin marketplace update muster-chat
claude plugin update muster@muster-chat
```

Restart Claude to load the new version. OpenCode: re-run the `curl` install one-liner.

## Removing the `--dangerously-load-development-channels` warning

Optional — the dev flag works out of the box on personal Pro/Max accounts; this matters for
teams and long-lived setups. During the channels research preview, `--channels` only loads
allowlisted plugins, and only an **org admin** can allowlist (managed settings; users and
projects cannot override):

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

Then launch without the flag: `claude --channels plugin:muster@muster-chat`.

Caveats: Team/Enterprise orgs have channels **disabled by default** — until an Owner
enables `channelsEnabled`, even the dev flag delivers nothing (personal accounts skip this
check). If the plugin isn't on the effective allowlist, Claude starts normally but the
channel silently doesn't register. See
[Claude Code → Channels](https://code.claude.com/docs/en/channels).

## Releasing (maintainers)

Releases are cut from `main` with a commit-message marker — the git tag is an *output* of
the pipeline, never a trigger:

```bash
make release-bump VERSION=X.Y.Z   # syncs pyproject, Chart.yaml, plugin.json + uv lock
git add -A && git commit -m "chore(bump): vX.Y.Z metadata"
make release-cut VERSION=X.Y.Z    # runs the gates, pushes the chore(release) marker
```

CI's `publish` job (gated on tests/chart/image) pushes the image and chart to GHCR, tags
`vX.Y.Z`, and creates the GitHub Release.

## Status

v1 ships inbound delivery plus `roster`/`search`/`chat`/`fetch`/`announce` against the
central muster-api bus, plus the OpenCode port. Out of scope for now:
`ack`, `task_add`, runtime-side deferral
(see [docs/references/channel-deferral.md](./docs/references/channel-deferral.md)).

## License

MIT — see [LICENSE](./LICENSE).
