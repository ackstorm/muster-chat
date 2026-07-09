"""Thin herdr 0.7.1 CLI adapters + per-pane git info. Every call is fail-safe."""
import json
import os
import subprocess
import sys
import time


def log(msg):
    print(f"[muster {time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def _run(cmd, timeout=10):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=True)


def panes():
    try:
        out = _run(["herdr", "pane", "list"]).stdout
        return json.loads(out).get("result", {}).get("panes", [])
    except Exception as e:
        log(f"herdr pane list failed: {e}")
        return []


def agent_status(pane_id):
    """This pane's LIVE agent status from herdr (idle/working/blocked/…) — the one signal
    only herdr has. None if herdr is unavailable or the pane isn't listed, so callers can
    degrade gracefully (herdr optional)."""
    if not pane_id:
        return None
    for p in panes():
        if p.get("pane_id") == pane_id:
            return p.get("agent_status")
    return None


def git_info(cwd):
    """(branch, is_linked_worktree). SPEC §7.5: branch --show-current; empty = detached → short SHA
    (symbolic-ref fails exactly on detached HEAD, so v0.4 used it wrong)."""
    # ponytail: except blocks stay silent by design — this runs every 30s from
    # register_presence and a non-git cwd is a normal fallback, not an error. A genuinely
    # broken git env surfaces once at startup via git_identity's log below instead of
    # spamming a line every 30s.
    try:
        branch = _run(["git", "-C", cwd, "branch", "--show-current"]).stdout.strip() or None
        if branch is None:  # detached HEAD: show-current returns empty, not an error
            branch = _run(["git", "-C", cwd, "rev-parse", "--short", "HEAD"]).stdout.strip() or None
    except Exception:
        branch = None
    try:
        dirs = _run(["git", "-C", cwd, "rev-parse", "--git-dir", "--git-common-dir"]).stdout.split()
        is_wt = len(dirs) == 2 and dirs[0] != dirs[1]
    except Exception:
        is_wt = False
    return branch, is_wt


def git_identity(cwd):
    """(repo, worktree) — stable per-worktree identity. repo = the shared repo name (same
    across all worktrees of the repo); worktree = git's own worktree name (basename of the
    per-worktree git-dir) or None for the main checkout. Branch is deliberately NOT used
    (it changes on checkout → that's presence). Fail-safe: (None, None) outside a repo."""
    try:
        common = _run(["git", "-C", cwd, "rev-parse", "--git-common-dir"]).stdout.strip()
        gitdir = _run(["git", "-C", cwd, "rev-parse", "--git-dir"]).stdout.strip()
    except Exception as e:
        log(f"git identity lookup failed in {cwd} ({e}); falling back to non-git name")
        return None, None
    common_abs = os.path.realpath(os.path.join(cwd, common))
    gitdir_abs = os.path.realpath(os.path.join(cwd, gitdir))
    repo = os.path.basename(os.path.dirname(common_abs)) or None
    worktree = os.path.basename(gitdir_abs) if gitdir_abs != common_abs else None
    return repo, worktree
