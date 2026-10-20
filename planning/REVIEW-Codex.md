# Review of `planning/PLAN.md`

## Summary

The plan gives agents a clear product direction and a sensible single-container architecture. Before implementation, close the following gaps in the specification. The first group protects portfolio correctness and trade behavior; the second makes the advertised setup and interface predictable.

## Priority actions

1. **Define atomic trade execution.** Require the balance/position check and all writes for one trade (cash, position, trade log, snapshot) to commit in a single SQLite transaction. Serialize competing trade requests or use conditional updates so simultaneous manual and chat trades cannot spend the same cash or shares. For multiple chat actions, specify sequential independent commits and return a result for each action. This gives partial failures a clear meaning.

2. **Define price availability and trade eligibility.** Specify startup bootstrap for the default watchlist, including Massive errors, empty responses, and rate limits. Represent unavailable prices explicitly and reject trades when a current price is missing or exceeds a stated staleness threshold. Set a deterministic policy for unknown tickers in simulator mode. Keep prices flowing for the union of watched tickers and held positions so removing a held ticker does not break portfolio valuation.

3. **Publish portfolio formulas and trade rules.** Define total value as cash plus the market value of open positions; define unrealized P&L and its percentage denominator. State what the API/UI do when a position price is absent. Specify ticker normalization, allowed trade sides, positive quantity bounds and precision, sell-to-zero handling, and whether trading requires watchlist membership. Remove positions below a small quantity epsilon to prevent floating-point residue from leaving ghost holdings.

4. **Specify complete API contracts.** Add example success and error bodies for trade, portfolio, watchlist, history, and chat endpoints. Define status codes and a consistent error envelope. For chat, distinguish the assistant's text from requested actions and per-action outcomes. Define deterministic mock inputs and outputs so E2E tests can rely on stable scenarios.

5. **Prevent duplicate execution after retries.** Give chat requests an idempotency key, persist it with the result, and return the stored result when the same request is retried. This prevents a timeout followed by a client retry from executing the same generated trades twice.

6. **Define SSE delivery and recovery.** Specify whether each event is a full price snapshot or a single ticker update, include a stable timestamp and change baseline, and send an initial snapshot on connect. Define heartbeat, reconnect/recovery behavior, and how unchanged or stale prices are represented. The UI should flash only when a price actually changes. Keep simulator tick cadence and Massive polling cadence distinct.

7. **Align chart history with the first-run experience.** Seed an initial portfolio snapshot so the P&L chart has a starting point. Define snapshot cadence, whether trades add points, and retention/query limits. For ticker charts, decide whether the backend exposes recent history or whether sparklines and the detail chart intentionally fill from SSE after page load.

8. **Set concrete startup and deployment defaults.** Pick one database initialization point and specify schema migration behavior. Align the named Docker volume example with the project-directory mount described elsewhere. Use one `.env` mechanism consistently, document `FRONTEND_DIR`, and define whether `OPENROUTER_API_KEY` is required at startup or only when live chat is used. Make the default bind address loopback so a no-login demo is local to the host; document deliberate network exposure separately.

9. **Clarify first launch.** A raw `docker run` command cannot reliably open a browser across local and remote environments. Name the supported start script as the canonical local experience and describe browser opening as best-effort. Ensure its URL, volume mapping, port, and environment-file behavior match Docker and Compose examples.

10. **Add tests for the contract's failure cases.** Cover concurrent trade attempts, transaction rollback, unavailable/stale prices, repeated chat delivery, partial chat-action failure, restart persistence, initial chart history, and SSE reconnect. These scenarios validate the most consequential runtime assumptions.

## Implementation order

1. Finalize storage, startup, binding, and price-source behavior.
2. Define portfolio calculations, trade rules, and transaction semantics.
3. Write the REST and SSE schemas, including failure outcomes.
4. Specify chat idempotency and mock behavior.
5. Update the frontend and tests against those contracts.
