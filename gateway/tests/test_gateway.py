"""Pure logic: command parsing + pairing store. The network loops are exercised live, not here."""
import json

from gateway.telegram.gateway import PairingStore, parse_command


def test_parse_commands():
    assert parse_command("/pair sk-bus-abc") == ("pair", "sk-bus-abc")
    assert parse_command("/unpair") == ("unpair", None)
    assert parse_command("/roster") == ("roster", None)
    assert parse_command("@muster-chat how is the feature going?") == ("chat", ("muster-chat", "how is the feature going?"))
    assert parse_command("plain reply text") == ("reply", "plain reply text")
    assert parse_command("/pair") == ("help", None)          # missing arg → usage
    assert parse_command("/nonsense") == ("help", None)


def test_pairing_store_roundtrip(tmp_path):
    p = tmp_path / "pairings.json"
    s = PairingStore(str(p))
    assert s.get(11) is None
    s.set(11, "sk-bus-abc")
    s.set_last(11, "jc/laptop/claude/muster-chat/1")
    assert s.get(11)["key"] == "sk-bus-abc"
    assert s.get(11)["last"] == "jc/laptop/claude/muster-chat/1"
    # survives reload + file is private
    s2 = PairingStore(str(p))
    assert s2.get(11)["key"] == "sk-bus-abc"
    assert (p.stat().st_mode & 0o777) == 0o600
    s2.remove(11)
    assert PairingStore(str(p)).get(11) is None
    # raw file never holds anything but chat_id/key/last
    assert set(json.loads(p.read_text()).get("11", {"key": 0, "last": 0}).keys()) <= {"key", "last"}
