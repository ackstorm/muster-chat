"""Address derivation (pure). Spec v2 §6: 4 client segments, '-' placeholder, sanitized."""
from plugins.muster.mcp import naming


def test_full_address_from_git_repo():
    addr = naming.derive_address({}, "claude", ("muster-chat", None), "/w/muster-chat", "laptop", 1234)
    assert addr == "laptop/claude/muster-chat/1234"


def test_worktree_and_host_override():
    addr = naming.derive_address({"MUSTER_HOST": "devbox"}, "opencode",
                                 ("muster-chat", "feat-x"), "/w/x", "ignored", 7)
    assert addr == "devbox/opencode/muster-chat~feat-x/7"


def test_non_git_falls_back_to_cwd_basename():
    addr = naming.derive_address({}, "claude", (None, None), "/tmp/scratch dir/", "h", 1)
    assert addr == "h/claude/scratch-dir/1"


def test_segments_never_empty_or_slashed():
    addr = naming.derive_address({}, "claude", (None, None), None, "", 5)
    assert addr == "-/claude/-/5"
    assert naming._seg("a/b c") == "a-b-c"
