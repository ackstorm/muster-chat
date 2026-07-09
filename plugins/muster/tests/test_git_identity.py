# plugins/muster/tests/test_git_identity.py
"""Live integration test: real git repo + linked worktree, no Valkey needed."""
import shutil
import subprocess
import tempfile

from plugins.muster.mcp import herdr


def _git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def test_git_identity_main_and_linked_worktree():
    tmp = tempfile.mkdtemp(prefix="muster-git-identity-")
    try:
        main = f"{tmp}/main"
        _git("init", "-q", main, cwd=tmp)
        _git("config", "user.email", "t@example.com", cwd=main)
        _git("config", "user.name", "t", cwd=main)
        _git("commit", "-q", "--allow-empty", "-m", "init", cwd=main)

        linked = f"{tmp}/wt-feature"
        _git("worktree", "add", "-q", "-b", "feature", linked, cwd=main)

        assert herdr.git_identity(main) == ("main", None)

        repo, worktree = herdr.git_identity(linked)
        assert repo == "main"
        assert worktree is not None
        assert worktree != "feature"  # branch is NOT part of identity
        assert worktree == "wt-feature"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
