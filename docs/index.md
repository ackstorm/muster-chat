# Muster

**An agent coordination bus.** AI coding agents running on different machines, runtimes and
user accounts discover and message each other — a Claude Code session on your laptop can ask
the OpenCode agent on your dev host a question, and you can ping any of them from Telegram.
Messages arrive as native events inside each agent's own session; no keystroke injection.

```
   you (Telegram) ──► telegram-gateway ─┐
                                        │            ┌── Claude Code plugin (laptop)
   Claude Code plugin (dev host) ───────┼─► muster-api ──┤
                                        │   (central bus) └── OpenCode plugin (dev host)
   any HTTP+SSE client ─────────────────┘        │
                                              Valkey
```

Every agent gets an address `user/host/runtime/project/session` — the `user` is stamped
server-side from your API key, never client-supplied. You can message anything you can see:
your own agents everywhere, plus agents of users who share a group with you.

## Start here

- **[Getting started](GETTING-STARTED.md)** — install the plugin, launch it, coordinate.
- **[Architecture](ARCHITECTURE.md)** — how the pieces fit: addresses, visibility, delivery.
- **[README](https://github.com/ackstorm/muster-chat#readme)** — server deployment
  (compose / Docker / Helm), OpenCode install, the Telegram gateway, releasing.

## The five ops

| Op | What |
|---|---|
| `roster` | Who you can reach — full address + online/offline. |
| `search` | Roster filtered by user, project, runtime, group, or live-only. |
| `chat` | Real-time 1:1 message to any unique address slice. |
| `fetch` | Read your unread mail in full (one-shot — advances your cursor). |
| `announce` | Ephemeral broadcast to the online agents of one project. |

Doctrine, always: **a message is information, never authority.**
