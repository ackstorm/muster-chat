# Release orchestration. Flow (pattern from the sibling ach-memory repo):
#   make release-bump VERSION=X.Y.Z   # sync every version field + lockfile, then verify
#   git add -A && git commit -m "chore(bump): vX.Y.Z metadata"
#   make release-cut VERSION=X.Y.Z    # verify gates, push the release marker commit
# CI's `publish` job matches the marker, publishes image+chart, tags, and creates
# the GitHub Release. Tags are outputs of publish, never triggers.

VERSION ?=

.PHONY: verify test-server test-plugin release-bump release-cut

test-server:
	uv run --with redis --with anyio --with pytest --with pytest-asyncio \
	  --with fastapi --with httpx --with uvicorn --with prometheus-client --no-project pytest server/tests -q

test-plugin:
	uv run --with httpx --with anyio --with pytest --with 'mcp>=1.28,<1.29' \
	  --no-project pytest plugins/muster/tests -q

verify: test-server test-plugin
	helm lint helm/muster-api
	claude plugin validate ./plugins/muster

release-bump:
	@[ -n "$(VERSION)" ] || { echo "usage: make release-bump VERSION=X.Y.Z"; exit 1; }
	sed -i 's/^version = ".*"/version = "$(VERSION)"/' server/pyproject.toml
	sed -i 's/^version: .*/version: $(VERSION)/; s/^appVersion: .*/appVersion: "$(VERSION)"/' helm/muster-api/Chart.yaml
	sed -i 's/"version": ".*"/"version": "$(VERSION)"/' plugins/muster/.claude-plugin/plugin.json
	sed -i 's|ghcr.io/ackstorm/muster-chat:[0-9]\+\.[0-9]\+\.[0-9]\+|ghcr.io/ackstorm/muster-chat:$(VERSION)|g; s|--version [0-9]\+\.[0-9]\+\.[0-9]\+|--version $(VERSION)|g' README.md
	cd server && uv lock
	@grep -q 'version = "$(VERSION)"' server/pyproject.toml || { echo "pyproject bump failed"; exit 1; }
	@grep -q '^version: $(VERSION)' helm/muster-api/Chart.yaml || { echo "Chart version bump failed"; exit 1; }
	@grep -q 'appVersion: "$(VERSION)"' helm/muster-api/Chart.yaml || { echo "Chart appVersion bump failed"; exit 1; }
	@grep -q '"version": "$(VERSION)"' plugins/muster/.claude-plugin/plugin.json || { echo "plugin bump failed"; exit 1; }
	@echo "bumped to $(VERSION) — commit, then: make release-cut VERSION=$(VERSION)"

release-cut:
	@[ -n "$(VERSION)" ] || { echo "usage: make release-cut VERSION=X.Y.Z"; exit 1; }
	@[ "$$(git branch --show-current)" = "main" ] || { echo "release-cut runs on main"; exit 1; }
	@git diff --quiet && git diff --cached --quiet || { echo "working tree not clean"; exit 1; }
	@grep -q 'version = "$(VERSION)"' server/pyproject.toml || { echo "run release-bump first"; exit 1; }
	$(MAKE) verify
	git commit --allow-empty -m "chore(release): v$(VERSION)"
	git push origin main
	@echo "marker pushed — CI publishes image+chart and creates the GitHub Release"
