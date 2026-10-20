# FinAlly Review

Scope: the uncommitted changes, `planning/PLAN.md`, `planning/REVIEW-Codex.md`, `.gitignore`, and the existing code in `backend/app/market/`. I did not run the tests. Findings are ordered most severe first.

## Uncommitted changes

- `planning/PLAN.md` changes by only +3 lines. It appends a stray `---` and blank lines after the final "SSE resilience" bullet (around line 473). Section 12 now ends with a dangling horizontal rule. Revert it or finish the section.
- `planning/REVIEW-Codex.md` is untracked. It is a decent review, but see "Problems with REVIEW-Codex.md" below. Several of its recommendations conflict with the project's goals or with Docker behavior. Do not apply them wholesale.

## High severity

1. **`.gitignore` will ignore the frontend's `src/lib/`.**
   - The Python template contains `lib/` and `lib64/` (`.gitignore`, in the "C extensions / packaging" block). A bare `lib/` pattern matches at any depth.
   - PLAN.md section 10 puts shared code in `frontend/src/lib/`. Everything there would be silently untracked.
   - Anchor the patterns (`/lib/`, `/lib64/`) or remove them.
   - Also missing: `db/finally.db*` (PLAN section 4 says it is gitignored, but no rule exists), `node_modules/` and `.svelte-kit/`. `build/` is ignored, which is correct for `frontend/build`.

2. **PLAN.md says "no `svelte.config.js`", and the adapter and runes mode are "configured in `vite.config.ts`" (PLAN.md section 3, section 4 Key Boundaries, section 10 Frontend Conventions).**
   - As far as I know this is not how SvelteKit works. `adapter-static`, `kit.*` options and `compilerOptions.runes` are read from `svelte.config.js`. `vite.config.ts` only hosts the `sveltekit()` plugin.
   - The claim appears to be copied from the "prelegal" `AGENTS.md`, which is an external reference the agents cannot see.
   - Verify against current SvelteKit docs before the frontend agent builds on it. If it is wrong, fix it in all three places. At minimum, drop the external reference.

3. **Daily change % has no data source.**
   - PLAN.md sections 2 and 10 require a "daily change %" in the watchlist.
   - `PriceUpdate.change_percent` (`backend/app/market/models.py:293-298`) is the change versus the previous tick, about 0.001% in the simulator and one 15 s poll in Massive mode.
   - The cache stores no day-open or previous-close baseline. The simulator has no concept of one either. The Massive snapshot includes `prevDay` and `todaysChangePerc`, but `massive_client.py:294-300` discards them.
   - Decide the semantics now. The simplest option is a session-open baseline for the simulator and previous close for Massive. Add `day_open` to `PriceUpdate` and to the SSE payload.

4. **Removing a ticker deletes its price while it may still be held.**
   - `SimulatorDataSource.remove_ticker` (`simulator.py:251-255`) and `MassiveDataSource.remove_ticker` (`massive_client.py:265-269`) drop the ticker from the source and the cache unconditionally.
   - PLAN.md lets the user hold a position and remove the ticker from the watchlist. After that the portfolio valuation, positions table and heatmap have no price for it. A subsequent sell has no fill price.
   - The plan must say the tracked set equals the union of the watchlist and open positions. The data source API, or the caller, must enforce it.

5. **Trades on unknown or stale prices are undefined.**
   - Simulator: `_add_ticker_internal` (`simulator.py:146-152`) gives any string a random seed price in 50–300. Typing "ZZZZ" produces a valid fake security. There is no ticker validation and no normalization. `add_ticker` does not upper-case, while Massive does.
   - Massive: `add_ticker` does not fetch a price. The ticker has no price until the next poll, up to 15 s later, so a trade or LLM add is rejected or racy. A failed first poll in `start()` is swallowed (`massive_client.py:311-314`), leaving an empty cache. Unknown tickers are simply absent from the response, so the user is never told.
   - PLAN.md specifies none of this. Add: ticker format validation (for example `^[A-Z.]{1,5}$`), a defined simulator policy (restrict to a known universe, or accept and say so), and a "no price, reject trade" rule.
   - Caution on staleness: REVIEW-Codex.md suggests rejecting stale prices. With Massive, markets close nights and weekends, and the free tier polls every 15 s. A naive threshold would block all trading. The threshold must be tied to the source and to market hours.

6. **The chat "error fed back to the LLM" flow is contradictory.**
   - PLAN.md section 9, Auto-Execution, says that if a trade fails validation, "the error is included in the chat response so the LLM can inform the user".
   - The flow is single-shot: the LLM responds once, then trades execute. The LLM never sees the failure.
   - Either have the backend append the failure to the returned message and the `actions` result, or add a second LLM call. State which. Also specify the actions result shape (see 8).

7. **Most of the platform does not exist yet, and some plan defaults are not in the repo.**
   - Missing: `app/main.py` (the app entry point with lifespan wiring), the database layer, portfolio, LLM code, the frontend, Dockerfile, scripts, `test/`, `.env.example` and `db/.gitkeep`. This is expected given CLAUDE.md, but the plan promises `.env.example` is committed.
   - `backend/pyproject.toml` lacks `litellm` and `httpx` (needed for `TestClient`), and `pydantic` for the structured-output schema. Dev tools are under `[project.optional-dependencies]`, so `uv sync` omits them. Use `[dependency-groups]`, which uv installs by default.
   - `rich` is a main dependency but only the demo script uses it. Move it to dev.

## Medium severity

8. **API and chat contracts are underspecified (PLAN.md section 8 and section 9).** There are no request or response bodies, no error format, and no status codes for trade failures (insufficient cash or shares, unknown ticker). The `actions` JSON stored in `chat_messages` is undefined. The frontend and backend agents will diverge. Add one example JSON per endpoint. This part of Codex's review is valid.

9. **Trade rules are unspecified.** Missing: minimum and maximum quantity and its precision (fractional shares are allowed), sell-to-zero deleting the position, and average cost on add-to-position (weighted). Floating-point `REAL` for money is acceptable for a simulation, but state rounding. Also missing: total value and unrealized P&L formulas.

10. **SSE stream details (`stream.py`).**
    - `_generate_events` catches `asyncio.CancelledError` and swallows it (line ~161). It should re-raise or just use `finally`.
    - There is no keepalive. Massive mode updates every 15 s; many proxies close idle streams. Send a `: ping` comment when idle.
    - `router` is module-global and routes are registered inside `create_stream_router`. Calling the factory twice, for example once per test, registers duplicate `/prices` routes on the same router.
    - Every event is a full snapshot of all tickers. This is fine for 10 tickers, but PLAN.md says "each SSE event contains ticker, price..." (singular). Decide on one wording. The frontend flash logic needs a per-ticker comparison either way.
    - `data:` lines only, with no `event:` name. This is fine, but document it.

11. **Cache rounding hides moves and skews the baseline.**
    - `PriceCache.update` (`cache.py:34-39`) rounds both price and previous price to 2 decimals. At roughly 1e-4 per tick (sub-cent), most simulator ticks round to the same price. The result is `direction == "flat"` for many ticks, so flashes are rare. Check whether the intended visual (frequent flashes) is met.
    - The simulator also rounds (`step()` returns `round(..., 2)`), but it keeps unrounded internal prices, which is good.

12. **Docker details in PLAN.md section 11.**
    - The plan's own run command is `-v finally-data:/app/db`, while section 4 describes a project-root `db/` bind directory. Pick one. A named volume is fine, but then the start scripts and `docker-compose.yml` must match, and `db/.gitkeep` is irrelevant.
    - `backend/db/` (Python schema package) versus top-level `db/` (data) is confusing, and `backend/db` inside the image could collide with the `/app/db` mount point if the backend lives at `/app`. Put the backend at `/app/backend` or rename the package.
    - `.env` is "read from project root", but inside the container only `--env-file` provides it. Say so. The `uv sync` step should use `--frozen`. Note that `uv.lock` is committed, which is good.
    - The workflows in `.github/` (`claude.yml`, `claude-code-review.yml`) were not reviewed.

13. **Scheduling and concurrency are not stated.** SQLite is accessed from async FastAPI plus background tasks (30 s snapshots). Specify WAL mode, one connection per operation (or `aiosqlite`), and a lock around trades. Two simultaneous trades from the UI and the LLM can overspend cash otherwise. Codex's point 1 is valid and cheap to adopt.

## Low severity

14. `GBMSimulator.__init__` treats `event_probability=0.001` as per-tick per-ticker. The comment ("every ~50 s") is right for 10 tickers, but with 50 tickers it would be every 10 s. Fine.
15. Random seed prices for unknown tickers (`simulator.py:151`) make runs non-reproducible. Seed with a hash of the ticker for determinism, and tests can then rely on it.
16. `PriceCache.version` is read without the lock (`cache.py:66`). Harmless in CPython, but inconsistent with the other methods.
17. `MassiveDataSource._fetch_snapshots` mutates nothing, but it reads `self._tickers` from a worker thread while `add_ticker` and `remove_ticker` rebuild it on the loop thread. It is benign since the list is replaced or appended atomically, but copy it first.
18. A bad `MASSIVE_API_KEY` (401) only logs an error every 15 s forever. Fall back to the simulator, or surface it on `/api/health`.
19. PLAN.md section 6 says Massive is "Polygon.io" but never states which Massive endpoint, the SDK version, or the rate-limit behavior. `planning/archive/MASSIVE_API.md` covers it. Link it from the plan.
20. PLAN.md section 2: the "browser opens" claim is not reliably true for a raw docker command. It is true only if the start script does it. Codex made this point; it is right.
21. The `OPENROUTER_API_KEY` requirement is unclear. Section 5 says required, but `LLM_MOCK=true` needs none. The backend must not crash at startup without the key.

## Problems with REVIEW-Codex.md

- Point 8, "default bind address loopback": inside a container, binding uvicorn to `127.0.0.1` makes the published port unreachable. The container must bind `0.0.0.0`. Restrict exposure on the host side instead: `-p 127.0.0.1:8000:8000`.
- Point 5, a chat idempotency key persisted with results, is over-engineering for a single-user, no-login course demo. Skip it or put it in a stretch list. A frontend in-flight guard (disable send while loading) covers the realistic case.
- Point 2, a staleness threshold: see finding 5 above. It is risky in its stated form.
- Points 1, 3, 4, 6, 7 and 10 are good and match findings 8, 9, 10, 13 above. Take them, but trim the scope: for example, do not require retention and query limits for snapshots.
- The review's "Implementation order" puts SSE schemas before the portfolio math. Prices are the dependency of both, so that is fine.

## Positives

- The market-data layer is clean. It has an abstract interface, a shared cache, a factory chosen by an environment variable, a correlated GBM with a Cholesky decomposition that is rebuilt on add and remove, and an async Massive poller that uses `to_thread` and survives errors.
- The plan is coherent on the single-container, SSE, static-build and market-orders-only decisions.
- The test suite layout under `backend/tests/market/` matches the modules.

## Suggested order of fixes

1. Fix `.gitignore` (finding 1) and verify the SvelteKit config claim (finding 2).
2. Add `day_open` and a tracked-set rule (findings 3, 4) before writing portfolio code.
3. Write down the API shapes, trade rules and ticker policy (findings 5, 6, 8, 9) in PLAN.md.
4. Fix the SSE issues (finding 10) and add the missing dependencies (finding 7).
