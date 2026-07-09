# plugins/muster/mcp/naming.py
"""Pure identity + key-format helpers for the Muster channel plugin.
Vendored from Phase 0 muster/muster/core.py + store.py so the plugin's ephemeral uv env is
self-contained. Key formats MUST stay byte-identical to store.py (daemon interop)."""
import os


def derive_group(env):
    """The coordination group (scope). Explicit MUSTER_GROUP wins verbatim; else herdr's
    workspace — prefixed 'HERDR-' when running inside herdr (HERDR_ENV set) so a short
    workspace id can't collide with a hand-set MUSTER_GROUP or the 'local' default; else
    'local' (permissive default — every agent with no group set shares it)."""
    if env.get("MUSTER_GROUP"):
        return env["MUSTER_GROUP"]
    ws = env.get("HERDR_WORKSPACE_ID")
    if ws:
        return f"HERDR-{ws}" if env.get("HERDR_ENV") else ws
    return "local"


def derive_agent_name(git_repo, git_worktree, cwd, pane_id, pid=None):
    """Per-pane identity, herdr-free. git repo (repo~worktree for a linked worktree) →
    basename(cwd) → pane_id, with a `-pid:NNNN` suffix (when pid is given) so two panes
    sharing one checkout don't collide on the identical git-repo name — each pane is its
    own presence key. The suffix is stable within a process (survives a branch checkout,
    which is presence not identity) but changes across restarts. pane_id is already
    per-pane unique, so it takes no suffix."""
    suffix = f"-pid:{pid}" if pid else ""
    if git_repo:
        return (f"{git_repo}~{git_worktree}" if git_worktree else git_repo) + suffix
    if cwd:
        return (os.path.basename(cwd.rstrip("/")) or pane_id) + suffix
    return pane_id


def ikey(group, name):  return f"muster:inbox:{group}:{name}"
def rkey(group, name):  return f"muster:inboxread:{group}:{name}"
def pkey(group, name):  return f"muster:presence:{group}:{name}"
def presence_scan_match(group):  return f"muster:presence:{group}:*"
def joined_key(group, name):  return f"muster:joined:{group}:{name}"  # plugin-only: join-announce dedup
def name_from_pkey(key):  return key.rsplit(":", 1)[-1]  # fallback only; prefer the hash 'name' field


def self_identity(env, git_id=(None, None), cwd=None, default_id="unknown", pid=None):
    """(group, name, pane_id) for THIS process — NO herdr dependency. pane_id from the herdr
    env when present, else a caller-supplied generated id. `pid` (os.getpid()) suffixes the
    name so co-located panes stay distinct. Pure: the caller runs the git/OS calls and passes
    cwd + git_id + default_id + pid in."""
    pane_id = env.get("HERDR_PANE_ID") or default_id
    return derive_group(env), derive_agent_name(git_id[0], git_id[1], cwd, pane_id, pid), pane_id
