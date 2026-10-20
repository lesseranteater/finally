# Review of Changes Since the Last Commit

Scope: all tracked modifications and untracked files in the working tree, including `.claude/`, `planning/REVIEW-Claude.md`, `planning/REVIEW-Codex.md`, and `planning/PLAN.md`. This review did not run tests. Findings are ordered by impact.

## Findings

### High

1. **The ignore rules hide planned frontend source.** `.gitignore` has unanchored `lib/` and `lib64/` patterns, which also match directories nested in the repository. The plan explicitly places shared frontend source in `frontend/src/lib/`; Git will ignore it. Anchor the Python packaging patterns to the repository root (for example `/lib/` and `/lib64/`). Also add the planned runtime database ignore and frontend generated dependency/build directories (`db/finally.db*`, `node_modules/`, `.svelte-kit/`) so local artifacts do not get committed.

2. **The frontend setup instructions specify the wrong configuration file.** `planning/PLAN.md` says to configure `adapter-static`, the fallback, and runes mode in `vite.config.ts` and says `svelte.config.js` should not exist. SvelteKit reads adapter and `kit` configuration from `svelte.config.js`; Vite configuration loads the SvelteKit plugin. Following the plan as written will prevent the documented static SPA configuration from being applied. Update the plan and remove the unsupported copied `frontend/AGENTS.md` claim.

3. **The new review-agent instructions can recurse or fail to produce the requested review.** `.claude/agents/change-reviewer.md` tells the agent not to review and to invoke `codex exec`; `.claude/agents/codex-reviewer.md` does the same while calling itself a different AI agent. Running either from Codex can launch Codex again instead of doing the work, and relies on a CLI being installed and usable. These instructions also don't define how to return or validate the generated file. Let the reviewer perform the review directly, or give it a supported, bounded delegation mechanism with a clear output check.

### Medium

4. **Daily percentage change is undefined and the existing price model cannot provide it.** The plan's watchlist requires daily change %, while `PriceUpdate.change_percent` is measured against the previous update. The simulator has no daily baseline, and the Massive snapshot path discards its day-change fields. Specify the baseline (for example session open for simulation and prior close for Massive) and carry it through the cache/API/SSE.

5. **Removing a watched ticker can break an open position.** Both market data source implementations remove the ticker from the active set and price cache. The plan allows watchlist management while positions remain open, so removing a held ticker can leave it unvalued and impossible to sell at a current cached price. Keep active market tracking for the union of watchlist tickers and open positions, or specify an equivalent rule.

6. **The chat failure flow is internally inconsistent.** Section 9 says validation failures are fed back to the LLM so it can inform the user, but the described flow calls the LLM once and executes trades after receiving its response. The model cannot see post-execution failures. Either make the backend add action outcomes to the returned message/result, or specify a second model call; define the `actions` result shape at the same time.

7. **Trade and API contracts leave implementation-critical behavior open.** The plan lists endpoint paths and an LLM request schema, but does not define response/error bodies, status codes, quantity bounds/precision, ticker normalization, average-cost updates, sell-to-zero behavior, missing-price behavior, or portfolio/P&L formulas. Define these before parallel frontend/backend implementation. Also specify atomic trade persistence and serialization so concurrent trades cannot both spend the same cash or shares.

8. **The plan's Docker persistence examples conflict.** It describes a project-root `db/` directory and `.gitkeep`, but the run command mounts a named volume at `/app/db`. Choose one canonical local persistence setup and align scripts/Compose examples with it. Clarify the container working layout so `/app/db` does not overlap the backend's `backend/db/` schema package. The raw Docker command also cannot promise to open a browser; reserve that best-effort behavior for the local start scripts.

9. **The chat review recommendation for persisted idempotency is disproportionate to the stated demo.** `REVIEW-Codex.md` requires persisted idempotency keys for chat execution. That adds schema and lifecycle complexity for a single-user, no-login demo. A disabled-while-sending frontend guard may be sufficient for the planned scope; if retry-safe exactly-once semantics are truly required, describe the storage and duplicate-key behavior explicitly as a deliberate requirement.

### Low

10. **The plan ends with an unexplained separator.** The tracked change adds only `---` plus blank lines after the final testing bullet. Remove it or add the intended section so this does not read like an unfinished edit.

11. **The new `reviewer` agent and `/doc-review` command overlap but have different scopes.** The agent description says it reviews `PLAN.md`, while its instructions use `$ARGUMENTS`; the command always targets a named file but gives no guidance for a missing/invalid argument or an existing review section. Consolidate the behavior and state whether to append to the target document or write a separate review artifact. The separate Codex-launching agents currently imply `planning/REVIEW.md`, while the command says append to the document itself.

12. **The Cerebras skill's dependency and runtime expectations need to match the project plan.** `.claude/skills/cerebras/SKILL.md` says the API key must be set and that `litellm` and `pydantic` must be installed, while `PLAN.md` says mock mode supports development without a key and the current `backend/pyproject.toml` does not include those packages. Align the skill and project dependency/startup instructions, including behavior when the key is absent in mock mode.

## Additional notes

- `planning/REVIEW-Claude.md` and `planning/REVIEW-Codex.md` contain useful prior feedback, but they are themselves untracked artifacts and overlap substantially. Keep one canonical review output to avoid conflicting recommendations.
- One suggestion in `REVIEW-Codex.md` is unsafe for the documented container: binding Uvicorn to `127.0.0.1` inside the container would make the published port unreachable. Keep the container listener on `0.0.0.0`; if host-only access is desired, restrict the host side of the port mapping instead.
- The stale-price recommendation in `REVIEW-Codex.md` needs a market-hours/source-aware rule. A simple wall-clock age limit would reject prices outside market hours, including weekends, even when the last quote is expected.
- The current implementation evidence for the market-data findings is in `backend/app/market/{cache.py,models.py,simulator.py,massive_client.py,stream.py}`. These observations concern behavior the plan needs to account for; the planned portfolio/API implementation is not present yet.
