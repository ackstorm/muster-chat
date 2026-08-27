# plugins/muster/mcp/naming.py
"""Address derivation — the client half of the 5-segment agent address (spec v2 §6).
`user` is server-stamped from the API key; the shim supplies host/runtime/project/session.
Pure — no I/O."""
import re


def _seg(value, fallback="-"):
    """One address segment: ASCII-printable only, no '/', never empty ('-' placeholder).
    Non-ASCII (e.g. a cwd basename like 'café') would otherwise reach the x-muster-agent
    header and break encoding on every request."""
    s = re.sub(r"[^\x21-\x7e]+|/", "-", str(value or "").strip())
    return s or fallback


def derive_project(git_id, cwd):
    """repo (repo~worktree for a linked worktree) → basename(cwd) → '-'. Branch is
    deliberately NOT used: it changes on checkout — that's presence, never identity."""
    repo, worktree = git_id
    if repo:
        return f"{repo}~{worktree}" if worktree else repo
    if cwd:
        base = cwd.rstrip("/").rsplit("/", 1)[-1]
        if base:
            return base
    return "-"


def derive_address(env, runtime, git_id, cwd, hostname, pid):
    """The x-muster-agent header value: host/runtime/project/session.
    host = MUSTER_HOST override or the machine hostname; session = pid (unique per
    host, stable within a process, new after restart — same tradeoff as v0)."""
    host = env.get("MUSTER_HOST") or hostname
    return "/".join((_seg(host), _seg(runtime), _seg(derive_project(git_id, cwd)), _seg(pid)))
