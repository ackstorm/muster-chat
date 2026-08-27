from muster_api import store

A = "jc/laptop/claude/muster-chat/a3f9"


def test_envelope_short_body_carries_all():
    assert store.envelope("ana/x/y/z/1", "done", None, False) == "✉ ana/x/y/z/1: done"


def test_envelope_long_body_nudges_fetch():
    e = store.envelope("ana/x/y/z/1", "x" * 100, None, False)
    assert e.endswith(" · fetch for full") and "x" * 56 in e and "x" * 57 not in e


def test_envelope_important_marked():
    assert store.envelope("ana/x/y/z/1", "ship it", None, True).startswith("❗ ")


async def test_append_fetch_advances_cursor(r):
    m1 = await store.append_message(r, A, {"from": "ana/x/y/z/1", "kind": "chat", "body": "one"},
                                    maxlen=1000, message_ttl=3600)
    await store.append_message(r, A, {"from": "ana/x/y/z/1", "kind": "chat", "body": "two"},
                               maxlen=1000, message_ttl=3600)
    got = await store.fetch_unread(r, A, limit=10)
    assert [m["body"] for m in got] == ["one", "two"]
    assert got[0]["msg_id"] == m1
    assert await store.fetch_unread(r, A, limit=10) == []  # cursor advanced: nothing unread


async def test_unread_count_does_not_advance(r):
    await store.append_message(r, A, {"from": "f", "kind": "chat", "body": "b"}, 1000, 3600)
    count, oldest_ts = await store.unread_count(r, A)
    assert count == 1 and oldest_ts
    count2, _ = await store.unread_count(r, A)
    assert count2 == 1  # still unread


async def test_expired_message_not_returned(r):
    await store.append_message(r, A, {"from": "f", "kind": "chat", "body": "old"}, 1000, message_ttl=-1)
    assert await store.fetch_unread(r, A, 10) == []  # already past expires_at


async def test_rate_check_fixed_window(r):
    for _ in range(3):
        ok, _ = await store.rate_check(r, "chat", A, limit=3, window=60)
        assert ok
    ok, retry = await store.rate_check(r, "chat", A, limit=3, window=60)
    assert not ok and 0 < retry <= 60
