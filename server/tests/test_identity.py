import pytest
from muster_api import identity


def test_parse_agent_header_stamps_user():
    a = identity.parse_agent_header("jc", "laptop/claude/muster-chat/a3f9")
    assert str(a) == "jc/laptop/claude/muster-chat/a3f9"
    assert a.user == "jc" and a.project == "muster-chat"


@pytest.mark.parametrize("bad", ["", "laptop/claude/x", "a/b/c/d/e", "laptop//x/y", "jc/laptop/claude/x/y/z"])
def test_parse_agent_header_rejects_malformed(bad):
    with pytest.raises(identity.AddressError):
        identity.parse_agent_header("jc", bad)


def test_matches_contiguous_slices_only():
    addr = "jc/laptop/claude/muster-chat/a3f9"
    assert identity.matches("muster-chat", addr)              # single segment
    assert identity.matches("laptop/claude/muster-chat", addr)  # contiguous slice
    assert identity.matches("a3f9", addr)                       # session
    assert identity.matches(addr, addr)                         # full address
    assert not identity.matches("laptop/muster-chat", addr)     # non-contiguous
    assert not identity.matches("muster", addr)                 # no substring matching
    assert not identity.matches("", addr)


def test_visible_own_user_always_shared_group_otherwise():
    assert identity.visible("jc", [], "jc", [])                       # own agents, no groups needed
    assert identity.visible("jc", ["ackstorm"], "ana", ["ackstorm"])  # shared group
    assert not identity.visible("jc", ["ackstorm"], "bob", ["otherteam"])
    assert not identity.visible("jc", [], "ana", [])
