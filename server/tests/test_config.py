import pytest
from muster_api import config


def test_defaults_match_spec():
    cfg = config.load(env={})
    assert cfg.valkey_url == "redis://localhost:6379/1"
    assert cfg.auth_cache_ttl == 60
    assert cfg.message_ttl == 72 * 3600
    assert cfg.agent_retention == 7 * 86400
    assert cfg.inbox_maxlen == 1000
    assert cfg.chat_rate == 20
    assert cfg.announce_rate == 3
    assert cfg.rate_window == 60
    assert cfg.body_max == 256 * 1024
    assert cfg.ping_interval == 15
    assert cfg.resolver_url is None
    assert cfg.static_keys == {}


def test_env_overrides_and_static_keys():
    cfg = config.load(env={
        "MUSTER_RESOLVER_URL": "http://litellm:4000/v2/user/info",
        "MUSTER_STATIC_KEYS": '{"k1": {"user_id": "jc", "groups": ["ackstorm"]}}',
        "MUSTER_CHAT_RATE": "5",
    })
    assert cfg.resolver_url == "http://litellm:4000/v2/user/info"
    assert cfg.static_keys["k1"]["user_id"] == "jc"
    assert cfg.chat_rate == 5


def test_retention_must_cover_message_ttl():
    with pytest.raises(ValueError, match="agent_retention"):
        config.load(env={"MUSTER_AGENT_RETENTION": "3600", "MUSTER_MESSAGE_TTL": "7200"})
