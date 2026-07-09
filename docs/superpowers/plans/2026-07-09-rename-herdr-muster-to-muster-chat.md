# Rename Project herdr-muster → muster-chat Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebrand the project from `herdr-muster` to `muster-chat` across the marketplace identifier, GitHub repo, skill name, and all living docs — without changing any runtime logic (naming/presence/bus code is untouched; only string literals and identifiers that spell out the old project name change).

**Architecture:** This is a mechanical rename, not a feature. The plugin itself keeps its name `muster` (unchanged — it's the tool, not the project brand). What changes: the marketplace id (`.claude-plugin/marketplace.json` `name` field, currently `herdr-muster`), the GitHub repo (`ackstorm/herdr-muster` → `ackstorm/muster-chat`), the skill folder `plugins/muster/skills/herdr-muster/` → `plugins/muster/skills/muster-chat/` (and every `Skill: muster:herdr-muster` reference that points at it), and every doc/config string that spells out the qualified plugin name `muster@herdr-muster` or the bare marketplace name `herdr-muster`.

**Tech Stack:** Plain text/JSON edits, `git mv`, `gh repo rename`, existing pytest suite for regression check, `claude plugin validate` for manifest sanity.

## Global Constraints

- Plugin name stays `muster` — only the marketplace/repo/skill identifiers change.
- Two files are explicitly **excluded** from this rename (owner's call — they're dated historical records, not living docs): `docs/spec/SPEC-agent-coordination-bus.md` and `docs/superpowers/plans/2026-07-07-per-repo-work-queue.md`. Do not touch `herdr-muster` occurrences there.
- `plugins/muster/tests/test_naming.py` uses the literal string `"herdr-muster"` twice (lines 30, 36) purely as a sample repo-name fixture for `derive_agent_name` — unrelated to project branding. Do not touch it.
- No behavior change beyond the skill name and the two welcome/instructions strings in `muster_channel.py` that spell out the skill name — this still counts as "shippable" per `CLAUDE.md`, so the plugin version must bump (see Task 7).
- A release is only live once the release commit + `vX.Y.Z` tag are pushed to `origin` (per `CLAUDE.md`) — Task 7 covers this and the post-push update instructions.

---

## File Structure

- `plugins/muster/skills/herdr-muster/` → renamed to `plugins/muster/skills/muster-chat/` (content unchanged — verified zero internal `herdr-muster` references in `SKILL.md`).
- `plugins/muster/mcp/muster_channel.py` — 2 string edits (skill name in `INSTRUCTIONS` and in the welcome `content`).
- `plugins/muster/hooks/hooks.json` — 1 string edit (skill name in the re-orient nudge).
- `.claude-plugin/marketplace.json` — marketplace `name` field.
- `plugins/muster/.claude-plugin/plugin.json` — `homepage` URL + version bump (version bump isolated to Task 7, matching the repo's existing release-commit convention of a version-only diff).
- `CLAUDE.md`, `README.md`, `plugins/muster/README.md`, `docs/ARCHITECTURE.md`, `docs/GETTING-STARTED.md` — every living-doc occurrence of `herdr-muster` / `muster@herdr-muster`.

---

### Task 1: Rename the skill directory and every reference to it

**Files:**
- Modify (rename): `plugins/muster/skills/herdr-muster/` → `plugins/muster/skills/muster-chat/`
- Modify: `plugins/muster/mcp/muster_channel.py:44,240`
- Modify: `plugins/muster/hooks/hooks.json` (single `command` string)
- Modify: `CLAUDE.md:35`
- Modify: `README.md:77`
- Modify: `plugins/muster/README.md:44`
- Modify: `docs/GETTING-STARTED.md:84`

**Interfaces:**
- Produces: skill invocable as `Skill: muster:muster-chat` (was `muster:herdr-muster`); every doc/code reference to "the herdr-muster skill" now reads "the muster-chat skill".

- [ ] **Step 1: Rename the skill directory**

```bash
git mv plugins/muster/skills/herdr-muster plugins/muster/skills/muster-chat
```

- [ ] **Step 2: Verify the rename and that the file content needs no changes**

```bash
ls plugins/muster/skills/muster-chat/SKILL.md
grep -n "herdr-muster" plugins/muster/skills/muster-chat/SKILL.md
```
Expected: `ls` prints the path; `grep` prints nothing (no matches).

- [ ] **Step 3: Edit `plugins/muster/mcp/muster_channel.py` line 44**

Old:
```python
    "For the full bus doctrine, load the herdr-muster skill (Skill: muster:herdr-muster)."
```
New:
```python
    "For the full bus doctrine, load the muster-chat skill (Skill: muster:muster-chat)."
```

- [ ] **Step 4: Edit `plugins/muster/mcp/muster_channel.py` line 240**

Old:
```python
        " Tools: roster, chat, fetch. New here? Load the herdr-muster skill (Skill: muster:herdr-muster)."
```
New:
```python
        " Tools: roster, chat, fetch. New here? Load the muster-chat skill (Skill: muster:muster-chat)."
```

- [ ] **Step 5: Edit `plugins/muster/hooks/hooks.json`**

Inside the single `command` string, replace the substring:
- Old: `Skill muster:herdr-muster.`
- New: `Skill muster:muster-chat.`

(Everything else in that JSON string is unchanged.)

- [ ] **Step 6: Edit `CLAUDE.md` line 35**

Old:
```
The doctrine lives in the server's `instructions` string (always in the system prompt) and `plugins/muster/skills/herdr-muster/SKILL.md`.
```
New:
```
The doctrine lives in the server's `instructions` string (always in the system prompt) and `plugins/muster/skills/muster-chat/SKILL.md`.
```

- [ ] **Step 7: Edit `README.md` line 77**

Old: `load the \`herdr-muster\` skill.`
New: `load the \`muster-chat\` skill.`

- [ ] **Step 8: Edit `plugins/muster/README.md` line 44**

Old: `to load the \`herdr-muster\` skill (skills aren't auto-read`
New: `to load the \`muster-chat\` skill (skills aren't auto-read`

- [ ] **Step 9: Edit `docs/GETTING-STARTED.md` line 84**

Old (inside the example transcript):
```
← muster: FYI: Muster online (Agent Coordinator Harness) — you are "ach-agent" in group "w5". You have 2 item(s) waiting. Live peers: ach. Tools: roster, chat, fetch. New here? Load the herdr-muster skill.
```
New:
```
← muster: FYI: Muster online (Agent Coordinator Harness) — you are "ach-agent" in group "w5". You have 2 item(s) waiting. Live peers: ach. Tools: roster, chat, fetch. New here? Load the muster-chat skill.
```

- [ ] **Step 10: Grep-verify no stray skill references remain**

```bash
grep -rn "muster:herdr-muster\|skills/herdr-muster" . --include="*" 2>/dev/null | grep -v '^\.git/'
```
Expected: no output.

- [ ] **Step 11: Commit**

Hold this commit — it's combined with Task 2 into a single commit at Task 5 (mechanical rename, one logical change). No commit here; proceed to Task 2.

---

### Task 2: Rename the marketplace id, plugin homepage, and every qualified-name reference

**Files:**
- Modify: `.claude-plugin/marketplace.json`
- Modify: `plugins/muster/.claude-plugin/plugin.json` (homepage only — version bump is Task 7)
- Modify: `CLAUDE.md:16,70,71,73`
- Modify: `README.md:1,51,52,55,62,69,114,115,118,151,161`
- Modify: `plugins/muster/README.md:74,75,77,80,89,93`
- Modify: `docs/ARCHITECTURE.md:87,92`
- Modify: `docs/GETTING-STARTED.md:42,43,46,51,62,74,77,113,133,167,170,173,219,220`

**Interfaces:**
- Produces: marketplace id `muster-chat` (was `herdr-muster`); plugin qualified name `muster@muster-chat` (was `muster@herdr-muster`); GitHub path `ackstorm/muster-chat` (was `ackstorm/herdr-muster`).

- [ ] **Step 1: Edit `.claude-plugin/marketplace.json`**

Old:
```json
  "name": "herdr-muster",
```
New:
```json
  "name": "muster-chat",
```

- [ ] **Step 2: Edit `plugins/muster/.claude-plugin/plugin.json` homepage**

Old:
```json
  "homepage": "https://github.com/ackstorm/herdr-muster"
```
New:
```json
  "homepage": "https://github.com/ackstorm/muster-chat"
```

- [ ] **Step 3: Edit `CLAUDE.md` line 16 (identity example)**

Old substring: `(e.g., \`herdr-muster-pid:1234\`)`
New substring: `(e.g., \`muster-chat-pid:1234\`)`

- [ ] **Step 4: Edit `CLAUDE.md` lines 70-73**

Replace every occurrence of `muster@herdr-muster` → `muster@muster-chat`, `plugin:muster@herdr-muster` → `plugin:muster@muster-chat`, and `claude plugin marketplace update herdr-muster` → `claude plugin marketplace update muster-chat` within those lines. Concretely:

Line 70 old: `` referenced marketplace-qualified as `muster@herdr-muster`. ``
Line 70 new: `` referenced marketplace-qualified as `muster@muster-chat`. ``

Line 71 old: `` activate at launch with `claude --dangerously-load-development-channels plugin:muster@herdr-muster`. ``
Line 71 new: `` activate at launch with `claude --dangerously-load-development-channels plugin:muster@muster-chat`. ``

Line 73 old: `` surface the two update commands (`claude plugin marketplace update herdr-muster` then `claude plugin update muster@herdr-muster`) ``
Line 73 new: `` surface the two update commands (`claude plugin marketplace update muster-chat` then `claude plugin update muster@muster-chat`) ``

- [ ] **Step 5: Edit `README.md` — all 11 occurrences**

Line 1 old: `# herdr-muster`
Line 1 new: `# muster-chat`

Line 51 old: `claude plugin marketplace add ackstorm/herdr-muster`
Line 51 new: `claude plugin marketplace add ackstorm/muster-chat`

Line 52 old: `claude plugin install muster@herdr-muster`
Line 52 new: `claude plugin install muster@muster-chat`

Line 55 old: `` **Always qualify the plugin as `muster@herdr-muster`** ``
Line 55 new: `` **Always qualify the plugin as `muster@muster-chat`** ``

Line 62 old: `claude --dangerously-load-development-channels plugin:muster@herdr-muster`
Line 62 new: `claude --dangerously-load-development-channels plugin:muster@muster-chat`

Line 69 old: `` `--channels plugin:muster@herdr-muster` on its own will **not** load `muster` ``
Line 69 new: `` `--channels plugin:muster@muster-chat` on its own will **not** load `muster` ``

Line 114 old: `claude plugin marketplace update herdr-muster`
Line 114 new: `claude plugin marketplace update muster-chat`

Line 115 old: `claude plugin update muster@herdr-muster`
Line 115 new: `claude plugin update muster@muster-chat`

Line 118 old: `` Always qualify the plugin as `muster@herdr-muster` — the bare `muster` is not resolved ``
Line 118 new: `` Always qualify the plugin as `muster@muster-chat` — the bare `muster` is not resolved ``

Line 151 old: `{ "marketplace": "herdr-muster", "plugin": "muster" }`
Line 151 new: `{ "marketplace": "muster-chat", "plugin": "muster" }`

Line 161 old: `claude --channels plugin:muster@herdr-muster`
Line 161 new: `claude --channels plugin:muster@muster-chat`

- [ ] **Step 6: Edit `plugins/muster/README.md` — all 6 occurrences**

Line 74 old: `claude plugin marketplace add ackstorm/herdr-muster`
Line 74 new: `claude plugin marketplace add ackstorm/muster-chat`

Line 75 old: `claude plugin install muster@herdr-muster`
Line 75 new: `claude plugin install muster@muster-chat`

Line 77 old: `claude plugin marketplace update herdr-muster && claude plugin update muster@herdr-muster`
Line 77 new: `claude plugin marketplace update muster-chat && claude plugin update muster@muster-chat`

Line 80 old: `` Always use the marketplace-qualified name `muster@herdr-muster`; ``
Line 80 new: `` Always use the marketplace-qualified name `muster@muster-chat`; ``

Line 89 old: `claude --dangerously-load-development-channels plugin:muster@herdr-muster`
Line 89 new: `claude --dangerously-load-development-channels plugin:muster@muster-chat`

Line 93 old: `claude --channels plugin:muster@herdr-muster`
Line 93 new: `claude --channels plugin:muster@muster-chat`

- [ ] **Step 7: Edit `docs/ARCHITECTURE.md` — both occurrences**

Line 87 old: `claude --dangerously-load-development-channels plugin:muster@herdr-muster`
Line 87 new: `claude --dangerously-load-development-channels plugin:muster@muster-chat`

Line 92 old: `claude --channels plugin:muster@herdr-muster`
Line 92 new: `claude --channels plugin:muster@muster-chat`

- [ ] **Step 8: Edit `docs/GETTING-STARTED.md` — all 14 occurrences**

Line 42 old: `claude plugin marketplace add ackstorm/herdr-muster`
Line 42 new: `claude plugin marketplace add ackstorm/muster-chat`

Line 43 old: `claude plugin install muster@herdr-muster`
Line 43 new: `claude plugin install muster@muster-chat`

Line 46 old: `` **Always use the qualified name `muster@herdr-muster`.** ``
Line 46 new: `` **Always use the qualified name `muster@muster-chat`.** ``

Line 51 old: `claude plugin marketplace update herdr-muster && claude plugin update muster@herdr-muster`
Line 51 new: `claude plugin marketplace update muster-chat && claude plugin update muster@muster-chat`

Line 62 old: `claude --dangerously-load-development-channels plugin:muster@herdr-muster`
Line 62 new: `claude --dangerously-load-development-channels plugin:muster@muster-chat`

Line 74 old: `claude --channels plugin:muster@herdr-muster`
Line 74 new: `claude --channels plugin:muster@muster-chat`

Line 77 old: `> ⚠️ Without the allowlist, \`--channels plugin:muster@herdr-muster\` does **not** load muster:`
Line 77 new: `> ⚠️ Without the allowlist, \`--channels plugin:muster@muster-chat\` does **not** load muster:`

Line 113 old: `{ "marketplace": "herdr-muster", "plugin": "muster" }`
Line 113 new: `{ "marketplace": "muster-chat", "plugin": "muster" }`

Line 133 old: `MUSTER_GROUP=my-project claude --dangerously-load-development-channels plugin:muster@herdr-muster`
Line 133 new: `MUSTER_GROUP=my-project claude --dangerously-load-development-channels plugin:muster@muster-chat`

Line 167 old: `alias muster='claude --dangerously-load-development-channels plugin:muster@herdr-muster'`
Line 167 new: `alias muster='claude --dangerously-load-development-channels plugin:muster@muster-chat'`

Line 170 old: `alias muster='claude --channels plugin:muster@herdr-muster'`
Line 170 new: `alias muster='claude --channels plugin:muster@muster-chat'`

Line 173 old: `alias muster='MUSTER_GROUP=my-project claude --channels plugin:muster@herdr-muster'`
Line 173 new: `alias muster='MUSTER_GROUP=my-project claude --channels plugin:muster@muster-chat'`

Line 219 old: `` | `claude plugin update muster` → *"Plugin not found"* | Qualify it: `muster@herdr-muster`. | ``
Line 219 new: `` | `claude plugin update muster` → *"Plugin not found"* | Qualify it: `muster@muster-chat`. | ``

Line 220 old: `` | Update seems to do nothing | Version-gated + stale marketplace → `claude plugin marketplace update herdr-muster` first, ... ``
Line 220 new: `` | Update seems to do nothing | Version-gated + stale marketplace → `claude plugin marketplace update muster-chat` first, ... ``

---

### Task 3: Full-repo verification sweep

**Files:** none modified — read-only checks.

- [ ] **Step 1: Confirm only the accepted-historical files still mention `herdr-muster`**

```bash
grep -rln "herdr-muster" . --include="*" 2>/dev/null | grep -v '^\.git/'
```
Expected output (exactly these three lines, nothing else):
```
docs/spec/SPEC-agent-coordination-bus.md
docs/superpowers/plans/2026-07-07-per-repo-work-queue.md
plugins/muster/tests/test_naming.py
```

- [ ] **Step 2: Run the existing test suite (regression check — no logic changed, so this must still pass)**

```bash
uv run --with redis --with anyio --with pytest --with mcp --no-project pytest plugins/muster/tests -v
```
Expected: all tests pass (Valkey must be up: `docker compose up -d` first if not already running).

- [ ] **Step 3: Validate the plugin manifest**

```bash
claude plugin validate ./plugins/muster
```
Expected: validation succeeds, no errors.

---

### Task 4: Commit the rename

**Files:** all files touched in Tasks 1-2.

- [ ] **Step 1: Review the full diff**

```bash
git status
git diff --stat
```
Expected: shows the renamed skill dir plus edits to `muster_channel.py`, `hooks.json`, both `.claude-plugin/*.json` manifests, `CLAUDE.md`, `README.md`, `plugins/muster/README.md`, `docs/ARCHITECTURE.md`, `docs/GETTING-STARTED.md`. No changes to `docs/spec/SPEC-agent-coordination-bus.md`, `docs/superpowers/plans/2026-07-07-per-repo-work-queue.md`, or `plugins/muster/tests/test_naming.py`.

- [ ] **Step 2: Stage and commit**

```bash
git add plugins/muster/skills/muster-chat plugins/muster/mcp/muster_channel.py \
  plugins/muster/hooks/hooks.json .claude-plugin/marketplace.json \
  plugins/muster/.claude-plugin/plugin.json CLAUDE.md README.md \
  plugins/muster/README.md docs/ARCHITECTURE.md docs/GETTING-STARTED.md
git status
```
Expected: the old `plugins/muster/skills/herdr-muster/` path shows as deleted (captured by the `git mv` in Task 1) and the new path as added; everything else shows modified.

```bash
git commit -m "$(cat <<'EOF'
refactor(muster): rename project herdr-muster -> muster-chat

Marketplace id, GitHub repo path, and the herdr-muster skill all rename
to muster-chat. The muster plugin itself keeps its name; only the
project/marketplace/skill identifiers change. No runtime logic changed.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Rename the GitHub repo

**Files:** none (external GitHub state + local git remote).

**⚠️ Confirm with the user immediately before Step 1 — this changes a shared, external resource.**

- [ ] **Step 1: Confirm current remote matches expectation**

```bash
git remote -v
```
Expected: `origin` points at `https://github.com/ackstorm/herdr-muster.git`.

- [ ] **Step 2: Rename the repo on GitHub**

```bash
gh repo rename muster-chat --repo ackstorm/herdr-muster
```
Expected: confirmation the repo is now `ackstorm/muster-chat`. GitHub auto-redirects the old URL, but the canonical name is now `muster-chat`.

- [ ] **Step 3: Verify / fix the local remote**

```bash
git remote -v
```
If `origin` did not auto-update to `https://github.com/ackstorm/muster-chat.git`, fix it:
```bash
git remote set-url origin https://github.com/ackstorm/muster-chat.git
git remote -v
```
Expected: `origin` now points at `https://github.com/ackstorm/muster-chat.git` for both fetch and push.

- [ ] **Step 4: Confirm with gh**

```bash
gh repo view --json nameWithOwner
```
Expected: `{"nameWithOwner":"ackstorm/muster-chat"}`.

---

### Task 6: Push the rename commit

- [ ] **Step 1: Push**

```bash
git push origin main
```
Expected: pushes the Task 4 commit to `ackstorm/muster-chat` (new remote URL from Task 5).

---

### Task 7: Release — bump plugin version and tag

**Files:**
- Modify: `plugins/muster/.claude-plugin/plugin.json` (version only)

**Interfaces:**
- Consumes: the `0.9.5` version currently in `plugins/muster/.claude-plugin/plugin.json` (after Task 2's homepage edit — homepage is already `muster-chat` by this point).
- Produces: `plugins/muster/.claude-plugin/plugin.json` version `0.9.6`; git tag `v0.9.6`.

- [ ] **Step 1: Bump the version**

Old:
```json
  "version": "0.9.5",
```
New:
```json
  "version": "0.9.6",
```

- [ ] **Step 2: Commit (version-only diff, matching this repo's existing release-commit convention)**

```bash
git add plugins/muster/.claude-plugin/plugin.json
git commit -m "$(cat <<'EOF'
chore(muster): release 0.9.6

Marketplace/repo/skill rename to muster-chat (see previous commit) is a
shippable change to the skill name surfaced in the welcome message and
the /clear|compact re-orient hook, so the plugin version bumps.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Tag**

```bash
git tag v0.9.6
```

- [ ] **Step 4: Push commit and tag**

```bash
git push origin main
git push origin v0.9.6
```

- [ ] **Step 5: Tell the user how to update, and offer to run it**

Per `CLAUDE.md`, surface these two commands and offer to run them:
```bash
claude plugin marketplace update muster-chat
claude plugin update muster@muster-chat
```

---

## Self-Review

**1. Spec coverage:** Skill rename ✅ (Task 1), marketplace/plugin-homepage/qualified-name rename across all living docs ✅ (Task 2), verification sweep ✅ (Task 3), commit ✅ (Task 4), GitHub repo rename ✅ (Task 5), push ✅ (Task 6), version bump + release + update instructions ✅ (Task 7). Historical-doc exclusion honored explicitly in Global Constraints and Task 3's expected grep output.

**2. Placeholder scan:** No TBD/TODO; every edit step shows exact old/new text; every verification step shows the exact command and expected output.

**3. Consistency check:** Skill name used consistently as `muster-chat` (folder name and `Skill: muster:muster-chat` reference) throughout Tasks 1-2; marketplace id used consistently as `muster-chat` throughout Task 2 and Task 7's update commands; version bump (0.9.5 → 0.9.6) matches the version currently on disk (confirmed via `plugins/muster/.claude-plugin/plugin.json` read during planning).
