"""Env-driven configuration. Defaults are pinned by spec v2 §17."""
import json
import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Config:
    valkey_url: str
    resolver_url: str | None
    resolver_header: str
    user_field: str
    groups_field: str
    static_keys: dict
    auth_cache_ttl: int
    message_ttl: int
    agent_retention: int
    inbox_maxlen: int
    chat_rate: int
    announce_rate: int
    rate_window: int
    body_max: int
    ping_interval: int
    presence_ttl: int


def load(env: Mapping = os.environ) -> Config:
    cfg = Config(
        valkey_url=env.get("MUSTER_VALKEY_URL", "redis://localhost:6379/1"),
        resolver_url=env.get("MUSTER_RESOLVER_URL") or None,
        resolver_header=env.get("MUSTER_RESOLVER_HEADER", "x-litellm-api-key"),
        user_field=env.get("MUSTER_USER_FIELD", "user_id"),
        groups_field=env.get("MUSTER_GROUPS_FIELD", "teams"),
        static_keys=json.loads(env.get("MUSTER_STATIC_KEYS", "{}")),
        auth_cache_ttl=int(env.get("MUSTER_AUTH_CACHE_TTL", "60")),
        message_ttl=int(env.get("MUSTER_MESSAGE_TTL", str(72 * 3600))),
        agent_retention=int(env.get("MUSTER_AGENT_RETENTION", str(7 * 86400))),
        inbox_maxlen=int(env.get("MUSTER_INBOX_MAXLEN", "1000")),
        chat_rate=int(env.get("MUSTER_CHAT_RATE", "20")),
        announce_rate=int(env.get("MUSTER_ANNOUNCE_RATE", "3")),
        rate_window=int(env.get("MUSTER_RATE_WINDOW", "60")),
        body_max=int(env.get("MUSTER_BODY_MAX", str(256 * 1024))),
        ping_interval=int(env.get("MUSTER_PING_INTERVAL", "15")),
        presence_ttl=int(env.get("MUSTER_PRESENCE_TTL", "60")),
    )
    if cfg.agent_retention < cfg.message_ttl:
        raise ValueError("agent_retention must be >= message_ttl (spec §7)")
    return cfg
