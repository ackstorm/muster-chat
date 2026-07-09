# plugins/muster/tests/test_naming.py
from plugins.muster.mcp import naming  # run pytest from repo root

def test_key_schema_matches_phase0():
    assert naming.ikey("busX", "ach") == "muster:inbox:busX:ach"
    assert naming.rkey("busX", "ach") == "muster:inboxread:busX:ach"
    assert naming.pkey("busX", "ach") == "muster:presence:busX:ach"

def test_name_from_pkey_roundtrip():
    assert naming.name_from_pkey(naming.pkey("busX", "ach-agent")) == "ach-agent"

def test_derive_group_prefers_muster_group_then_herdr_then_local():
    assert naming.derive_group({"MUSTER_GROUP": "proj", "HERDR_WORKSPACE_ID": "w5"}) == "proj"
    assert naming.derive_group({"HERDR_WORKSPACE_ID": "w5"}) == "w5"
    assert naming.derive_group({}) == "local"
    # inside herdr (HERDR_ENV set): workspace-derived group gets the HERDR- prefix ...
    assert naming.derive_group({"HERDR_ENV": "1", "HERDR_WORKSPACE_ID": "wH"}) == "HERDR-wH"
    # ... but an explicit MUSTER_GROUP still wins verbatim, and no workspace falls to local
    assert naming.derive_group({"HERDR_ENV": "1", "MUSTER_GROUP": "proj", "HERDR_WORKSPACE_ID": "wH"}) == "proj"
    assert naming.derive_group({"HERDR_ENV": "1"}) == "local"

def test_derive_agent_name_priority():
    assert naming.derive_agent_name("ach", None, "/x/whatever", "id") == "ach"
    assert naming.derive_agent_name("ach", "wt", "/x", "id") == "ach~wt"
    assert naming.derive_agent_name(None, None, "/home/u/ach-agent", "id") == "ach-agent"
    assert naming.derive_agent_name(None, None, None, "id") == "id"

def test_derive_agent_name_pid_suffix_disambiguates_co_located_panes():
    # two panes in the same checkout get the same git name -> pid keeps them distinct
    assert naming.derive_agent_name("herdr-muster", None, "/x", "id", pid=1234) == "herdr-muster-pid:1234"
    assert naming.derive_agent_name("ach", "wt", "/x", "id", pid=7) == "ach~wt-pid:7"
    assert naming.derive_agent_name(None, None, "/home/u/ach-agent", "id", pid=9) == "ach-agent-pid:9"
    # pane_id fallback is already per-pane unique -> no suffix
    assert naming.derive_agent_name(None, None, None, "id", pid=9) == "id"
    # self_identity threads pid through
    _, name, _ = naming.self_identity({"HERDR_PANE_ID": "wH:p7"}, ("herdr-muster", None), "/x", "d", 1234)
    assert name == "herdr-muster-pid:1234"

def test_self_identity_without_herdr_uses_git_and_default_id():
    group, name, pid = naming.self_identity({}, ("myrepo", None), "/x/myrepo", "host:123")
    assert group == "local" and name == "myrepo" and pid == "host:123"

def test_self_identity_with_herdr_env():
    env = {"HERDR_PANE_ID": "w5:p2", "HERDR_WORKSPACE_ID": "w5"}
    group, name, pid = naming.self_identity(env, ("ach-agent", None), "/x/ach-agent", "host:1")
    assert group == "w5" and name == "ach-agent" and pid == "w5:p2"

def test_self_identity_muster_group_override_beats_herdr():
    env = {"HERDR_PANE_ID": "w5:p2", "HERDR_WORKSPACE_ID": "w5", "MUSTER_GROUP": "team"}
    group, _, _ = naming.self_identity(env, ("r", None), "/x/r", "d")
    assert group == "team"
