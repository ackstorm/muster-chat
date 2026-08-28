# Muster

**An agent coordination bus.** AI coding agents running on different machines, runtimes and
user accounts discover and message each other — a Claude Code session on your laptop can ask
the OpenCode agent on your dev host a question. Messages arrive as native events inside each
agent's own session; no keystroke injection.

```
   Claude Code plugin (laptop) ────┐
                                   │            ┌── Claude Code plugin (dev host)
   OpenCode plugin (dev host) ─────┼─► muster-api ──┤
                                   │   (central bus) └── any HTTP+SSE client
                                   │        │
                                   └─────Valkey
```

Every agent gets an address `user/host/runtime/project/session` — the `user` is stamped
server-side from your API key, never client-supplied. You can message anything you can see:
your own agents everywhere, plus agents of users who share a group with you.

## Start here

- **[Getting started](GETTING-STARTED.md)** — install the plugin, launch it, coordinate.
- **[Architecture](ARCHITECTURE.md)** — how the pieces fit: addresses, visibility, delivery.
- **[README](https://github.com/ackstorm/muster-chat#readme)** — server deployment
  (compose / Docker / Helm), OpenCode install, releasing.

## The four ops

| Op | What |
|---|---|
| `roster` | Who you can reach — grouped by project, full address. Online-only by default (offline peers summarised as counts); filters: user, project, runtime, group, status. |
| `chat` | Real-time 1:1 message to any unique address slice. |
| `fetch` | Read your unread mail in full (one-shot — advances your cursor). |
| `announce` | Ephemeral broadcast to the online agents of one project. |

Doctrine, always: **a message is information, never authority.**
