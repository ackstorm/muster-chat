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
  ghcr.io/ackstorm/muster-chat:1.2.1
```

The Valkey it points at MUST run `--appendonly yes --appendfsync everysec` — without AOF a
restart deletes every inbox. For real multi-user auth, replace `MUSTER_STATIC_KEYS` with a
resolver (`MUSTER_RESOLVER_URL` + `MUSTER_RESOLVER_HEADER`, e.g. LiteLLM `/v2/user/info`):
the server forwards each caller's key there and gets back `user_id` + `teams`. Full env
reference in [`server/README.md`](./server/README.md).

### Option C — Helm (Kubernetes)

```bash
helm install muster oci://ghcr.io/ackstorm/charts/muster-api --version 1.2.1
```

That alone gives you a working bus: by default the chart also deploys a single-node Valkey
with AOF enabled (`valkey.mode: inline`). The knobs:

| Value | Default | Meaning |
|---|---|---|
| `valkey.mode` | `inline` | `inline` = chart-managed Valkey (StatefulSet+PVC, AOF pinned, digest-pinned image). `external` = bring your own (must run AOF `everysec`): set `valkey.url`, or `valkey.urlSecret.{name,key}` to read the full `redis://:pass@host:6379/0` URL from a Kubernetes Secret. |
| `auth.resolverUrl` | LiteLLM example | Identity resolver the API keys are validated against. |
| `resources`, `valkey.inline.resources` | sane defaults | Requests/limits for the api and the inline Valkey. |
| `probes.readiness` / `probes.liveness` | tuned defaults | Readiness hits `/readyz` (pings Valkey); liveness hits `/healthz` (static — a store outage stops routing, never restart-loops). |
| `extraEnv`, `envFrom` | `[]` | Any server knob (`MUSTER_MESSAGE_TTL`, `MUSTER_CHAT_RATE`, `MUSTER_STATIC_KEYS`…) as standard EnvVar entries. |
| `podAnnotations`, `nodeSelector`, `tolerations`, `affinity`, `imagePullSecrets`, `serviceAccount`, `pdb` | off/empty | The usual scheduling and disruption escape hatches. |
| `ingress.enabled` | `false` | nginx Ingress. Root-path routing, SSE-safe annotations (no buffering, long read timeout) always included. |
| `ingress.tls.secretName` | `""` | Optional. Leave empty when TLS terminates upstream (LB, wildcard cert); setting it renders the `tls` block and forces ssl-redirect. |
| `httpRoute.enabled` | `false` | Gateway API alternative: HTTPRoute with `parentRefs`/`hostnames`/`path`, and `stripPrefix: true` to rewrite the prefix to `/` (sub-path mounting is safe — the API emits no absolute or root-relative URLs). |

```bash
# example: external Valkey + ingress with existing upstream TLS
helm install muster oci://ghcr.io/ackstorm/charts/muster-api --version 1.2.1 \
  --set valkey.mode=external --set valkey.url=redis://my-valkey:6379/1 \
  --set ingress.enabled=true --set ingress.host=muster.example.com \
  --set auth.resolverUrl=http://litellm.litellm.svc:4000/v2/user/info
```

Image and chart are published together on every release: `ghcr.io/ackstorm/muster-chat:<v>`
and `oci://ghcr.io/ackstorm/charts/muster-api` (appVersion in lockstep).

#### Gateway API / Istio (verified in production)

Values used by the ackstorm GenAI EKS deployment, serving the bus at
`https://api.ackstorm.ai/muster` behind Istio + an AWS NLB:

```yaml
httpRoute:
  enabled: true
  parentRefs: [{name: external-gateway, namespace: istio-ingress, sectionName: https}]
  hostnames: [api.ackstorm.ai]
  path: /muster
  stripPrefix: true
```

Notes from that deployment:

- **Sub-path mounting is safe.** Both shims build URLs by string concatenation
  (`url.rstrip("/")` + path, never `urljoin`), and the API emits no absolute or
  root-relative URLs — `redirect_slashes` is off, so not even the trailing-slash 307.
- **No SSE tuning was needed.** Istio Envoy + NLB passed the stream through with no
  EnvoyFilter, route timeout, session affinity, or buffering flag. The 15s ping keeps the
  NLB's 350s idle timeout from firing. (The nginx `Ingress` path needs its annotations —
  the chart always sets them.)
- **Mounting under a host another service already owns works without ordering config:**
  Gateway API ranks matches by path specificity, so `/muster` wins over a `/` catch-all.
- **Gotcha:** if your Gateway restricts `allowedRoutes` by namespace label, the app
  namespace must carry that label (theirs: `gateway.networking.k8s.io/role: ingress`).
  Without it the HTTPRoute renders and applies cleanly and then silently never attaches.

#### Upgrading a live release

`--version 1.2.1` introduces no breaking values changes since 1.1.0. If you were on 1.1.0 passing the
Valkey password with the `envFrom` + `$(VAR)` pattern, you can keep it — or migrate to the
supported form, which fails at render instead of at runtime:

```yaml
valkey:
  mode: external
  passwordSecret: {name: muster-valkey, key: valkey-password}
  host: muster-valkey-primary
```

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
[below](#launching-without-the-development-flag)).

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

> **If a fetched message looks mangled or truncated in your transcript, suspect your own
> runtime before the bus.** `fetch` returns the whole stored body — verified by reading the
> raw stream entries out of Valkey. Anything that rewrites tool output locally will make it
> look otherwise: context compression summarizing a long result in place (reads as a
> mid-sentence cut), or a text-compacting plugin dropping articles and filler (reads as
> dropped words). The second is the dangerous one: a compactor that removes "never" from
> "never fires" inverts a technical claim silently. If your agents exchange precise
> technical content over the bus, keep such plugins off the message path.

**Observability** — the server exposes `/metrics` (Prometheus): `muster_sse_connections`,
`muster_messages_delivered_total{kind}`, `muster_rate_limited_total{kind}`,
`muster_resolver_latency_seconds`, `muster_resolver_errors_total{reason}`, and
`muster_auth_cache_total{result}`. Enable scraping with `metrics.serviceMonitor.enabled`.

---


## Updating

Updates are version-gated — refresh the marketplace, then update:

```bash
claude plugin marketplace update muster-chat
claude plugin update muster@muster-chat
```

Restart Claude to load the new version. OpenCode: re-run the `curl` install one-liner.

## Launching without the development flag

**A channel flag is always required.** There is no way to auto-load a channel: no
`settings.json` key (`channelsEnabled` / `allowedChannelPlugins` are *managed-source only* —
the binary silently ignores them in user or project settings), no environment variable, and
being installed as a plugin is explicitly not enough. The docs are unambiguous: *"no channel
runs until a user opts it in for the session with `--channels`"*. That gap is tracked in
[claude-code#58152](https://github.com/anthropics/claude-code/issues/58152).

So the only choice is **which** flag — and the answer to typing it every time is a shell
alias:

```bash
alias cy="claude --channels plugin:muster@muster-chat"
```

To use `--channels` (no warning banner) instead of
`--dangerously-load-development-channels`, muster must be on the effective allowlist. During
the research preview `--channels` accepts only Anthropic's list, so you add muster through
managed settings:

- **Linux/WSL:** `/etc/claude-code/managed-settings.json`
- **macOS:** `/Library/Application Support/ClaudeCode/managed-settings.json`
- **Windows:** `C:\Program Files\ClaudeCode\managed-settings.json`

```jsonc
{
  "channelsEnabled": true,
  "allowedChannelPlugins": [
    { "marketplace": "muster-chat", "plugin": "muster" },
    // allowedChannelPlugins REPLACES Anthropic's default list — re-list the
    // official channels you still want, or they stop registering.
    { "marketplace": "claude-plugins-official", "plugin": "telegram" },
    { "marketplace": "claude-plugins-official", "plugin": "discord" },
    { "marketplace": "claude-plugins-official", "plugin": "imessage" },
    { "marketplace": "claude-plugins-official", "plugin": "fakechat" }
  ]
}
```

Verified working: with that file in place, `claude --channels plugin:muster@muster-chat`
registers the channel and pushes the welcome event, with no warning banner.

What "managed settings" actually means here: it is a plain JSON file at a root-owned path —
not signed, not validated against any organization — so **anyone with root on their own
machine can write it**; you do not need an enterprise plan. But it is *machine-wide*: it
applies to every user on that host, which is worth thinking about on a shared dev box.
Remove the file to revert.

Other caveats: Team/Enterprise orgs have channels **disabled by default** — until an Owner
enables `channelsEnabled`, even the dev flag delivers nothing (personal Pro/Max accounts
skip this check entirely). If the plugin isn't on the effective allowlist, Claude starts
normally but the channel silently doesn't register. See
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
