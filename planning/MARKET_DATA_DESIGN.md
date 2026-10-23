# Market Data Backend — Design

Implementation-ready design for everything market-data related in FinAlly: the unified interface, the GBM simulator, the Massive (Polygon.io) client, the shared price cache, SSE streaming, and the glue other subsystems (watchlist, portfolio, chat) use to read prices.

This document builds on the code already in `backend/app/market/` (see `MARKET_DATA_SUMMARY.md`; the older long-form design is in `archive/MARKET_DATA_DESIGN.md`) and **closes the gaps found in `REVIEW.md` and `REVIEW-Codex.md`**. Every snippet below is either a complete replacement for an existing file or a new file. Section 14 lists exactly what changes relative to the code on disk.

Related references: `PLAN.md` §6 (market data), §8 (API), `archive/MASSIVE_API.md` (Massive endpoint reference).

---

## 1. Goals and decisions

| # | Decision | Why |
|---|---|---|
| 1 | One abstract `MarketDataSource`; simulator and Massive are interchangeable | Downstream code (SSE, portfolio, trades) is source-agnostic |
| 2 | Sources **write** to a `PriceCache`; consumers only **read** the cache | Decouples producers from consumers; supports future multi-user |
| 3 | `PriceUpdate` carries a **day baseline** (`day_open`) in addition to the previous tick | PLAN requires "daily change %" in the watchlist; tick-to-tick change is ~0.001% and useless |
| 4 | Tracked set = **watchlist ∪ open positions**, enforced by `MarketDataService` | Removing a held ticker from the watchlist must not strand portfolio valuation or a later sell |
| 5 | Strict ticker normalization/validation (`^[A-Z][A-Z.]{0,4}$`), one place | Simulator otherwise happily invents "ZZZZ"; Massive and simulator disagreed on case |
| 6 | **No price ⇒ no trade.** `get_trade_price()` raises `PriceUnavailableError` | Defined behaviour for Massive cold start, unknown tickers, rate limits |
| 7 | Simulator policy: any *syntactically valid* ticker is accepted with a **deterministic** seed price | Reproducible runs/tests; documented rather than accidental |
| 8 | Massive `add_ticker` fetches the price immediately and raises `UnknownTickerError` if absent | The API can return a 400 instead of silently tracking a nonexistent symbol |
| 9 | A bad Massive key (401/403) at startup falls back to the simulator and reports it on `/api/health` | Beats logging an error every 15 s forever |
| 10 | SSE sends full snapshots, an initial snapshot on connect, `: ping` keepalives, and per-router instances | Fixes duplicate-route and swallowed-cancel issues |

Non-goals: historical bars, WebSockets, per-user watchlists, staleness-based trade rejection (see §12 for why).

---

## 2. Architecture

```
                    ┌────────────────────────────────────────────┐
                    │              MarketDataService             │
 watchlist routes ─▶│  track(ticker, "watchlist"|"position")     │
 trade / chat    ─▶│  release(ticker, reason)                   │
                    │  get_trade_price(ticker) → float | raises  │
                    └───────┬───────────────────────┬────────────┘
                            │ add/remove            │ reads
                            ▼                       ▼
        ┌──────────────────────────────┐     ┌─────────────┐   version change   ┌──────────────────┐
        │ MarketDataSource (ABC)       │────▶│ PriceCache  │───────────────────▶│ GET /api/stream/ │
        │  ├ SimulatorDataSource (GBM) │write│ (in-memory) │                    │ prices  (SSE)    │
        │  └ MassiveDataSource (REST)  │     └─────────────┘                    └──────────────────┘
        └──────────────────────────────┘
```

One process, one event loop. The simulator runs as an `asyncio` task; the Massive client is synchronous, so it is called through `asyncio.to_thread`. `PriceCache` is lock-protected because of that thread hop.

### File layout (`backend/app/market/`)

```
__init__.py        public exports
models.py          PriceUpdate (frozen dataclass)
errors.py          MarketDataError hierarchy + normalize_ticker()          [new]
cache.py           PriceCache
interface.py       MarketDataSource (ABC)
seed_prices.py     seed prices, GBM params, correlation groups, initial_price()
simulator.py       GBMSimulator + SimulatorDataSource
massive_client.py  MassiveDataSource
factory.py         create_market_data_source()
service.py         MarketDataService (tracked-set + trade price lookup)    [new]
stream.py          create_stream_router()
```

---

## 3. Errors and ticker normalization — `errors.py` (new)

```python
"""Errors and ticker validation shared by every market data component."""

from __future__ import annotations

import re

# 1–5 letters, optional dots for share classes (BRK.B). Upper-case only after normalization.
_TICKER_RE = re.compile(r"^[A-Z][A-Z.]{0,4}$")


class MarketDataError(Exception):
    """Base class for market data failures."""


class InvalidTickerError(MarketDataError, ValueError):
    """Ticker is not syntactically valid. API layer maps to HTTP 400."""


class UnknownTickerError(MarketDataError):
    """Ticker is valid but the data source has no such security. HTTP 400/404."""


class PriceUnavailableError(MarketDataError):
    """No current price for the ticker (cold start, rate limit, not tracked). HTTP 409/503."""


class MarketDataAuthError(MarketDataError):
    """Massive rejected the API key (401/403)."""


def normalize_ticker(raw: str) -> str:
    """Strip, upper-case and validate a ticker. The ONLY place this logic lives.

    >>> normalize_ticker(" aapl ")
    'AAPL'
    """
    ticker = (raw or "").strip().upper()
    if not _TICKER_RE.fullmatch(ticker):
        raise InvalidTickerError(f"Invalid ticker: {raw!r}")
    return ticker
```

Everything that accepts a ticker from outside (REST bodies, LLM output, `source.add_ticker`) calls `normalize_ticker` first. Routes translate `InvalidTickerError` to `400 {"error": {"code": "invalid_ticker", ...}}`.

---

## 4. Data model — `models.py`

`PriceUpdate` gains `day_open`, the baseline for the daily change shown in the watchlist:

- **Simulator**: the session-open price (the seed price the ticker started the process with).
- **Massive**: the previous close (`snap.day.previous_close`), i.e. the standard "daily change" baseline.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of one ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float              # price at the previous cache update (tick baseline)
    day_open: float | None = None      # daily baseline (see above); None only in bare unit tests
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    # --- tick-to-tick (drives flash animation) ---
    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up' | 'down' | 'flat' relative to the previous update."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # --- daily (drives the "Chg %" column) ---
    @property
    def day_change(self) -> float:
        if not self.day_open:
            return 0.0
        return round(self.price - self.day_open, 4)

    @property
    def day_change_percent(self) -> float:
        if not self.day_open:
            return 0.0
        return round((self.price - self.day_open) / self.day_open * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "day_open": self.day_open,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
            "direction": self.direction,
        }
```

Example serialized value (what the frontend receives, per ticker):

```json
{"ticker": "AAPL", "price": 190.42, "previous_price": 190.41, "day_open": 190.0,
 "timestamp": 1761234567.12, "change": 0.01, "change_percent": 0.0053,
 "day_change": 0.42, "day_change_percent": 0.2211, "direction": "up"}
```

---

## 5. Price cache — `cache.py`

Behaviour changes vs. the current file: `update()` accepts `day_open`; the baseline is **sticky** (kept from the previous entry when the caller passes none); `version` is read under the lock; `timestamp=0` is no longer treated as "now".

Prices are rounded to 2 decimals (cents). The simulator keeps full-precision internal state, so rounding only affects what is published. Roughly half of simulator ticks therefore publish an unchanged price; that is fine because the SSE payload carries `direction` and the frontend flashes only on `up`/`down`.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest price per ticker.

    Writers: one MarketDataSource (event-loop task or worker thread).
    Readers: SSE endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0  # bumped on every update/remove; SSE change detection

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        day_open: float | None = None,
    ) -> PriceUpdate:
        """Record a price. Computes previous_price and the daily baseline.

        - First update for a ticker: previous_price == price (direction 'flat').
        - day_open: pass it to (re)set the baseline (Massive passes previous close on
          every poll). Omit it and the previous baseline is kept; for a brand-new ticker
          it defaults to the first price.
        """
        with self._lock:
            ts = time.time() if timestamp is None else timestamp
            price = round(price, 2)
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            if day_open is not None:
                baseline = round(day_open, 2)
            elif prev is not None and prev.day_open is not None:
                baseline = prev.day_open
            else:
                baseline = price

            update = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=previous_price,
                day_open=baseline,
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy of all current prices."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

---

## 6. Unified interface — `interface.py`

Contract additions: `add_ticker` **normalizes** and may raise `InvalidTickerError` / `UnknownTickerError` / `PriceUnavailableError`; a `status()` hook feeds `/api/health`. Callers never call `remove_ticker` directly for tickers that may still be held — they go through `MarketDataService` (§10).

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push updates into a shared PriceCache on their own schedule.
    Downstream code reads from the cache, never from the source.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", ...])     # exactly once
        await source.add_ticker("TSLA")       # may raise UnknownTickerError
        await source.remove_ticker("GOOGL")
        await source.stop()                   # idempotent
    """

    #: "simulator" | "massive" — surfaced by /api/health and the UI
    name: str = "unknown"

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing updates for `tickers` (already normalized).

        Must populate the cache for as many tickers as possible BEFORE returning, so
        the first SSE snapshot and the first trade have prices. May raise
        MarketDataAuthError (Massive only).
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop background work. Safe to call repeatedly."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker; no-op if already tracked.

        Postcondition on success: the cache holds a price for it.
        Raises InvalidTickerError, UnknownTickerError or PriceUnavailableError.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking and drop from the cache; no-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""

    def status(self) -> dict:
        """Health info for /api/health. Sources may extend it."""
        return {"source": self.name, "tickers": len(self.get_tickers())}
```

---

## 7. Seed data — `seed_prices.py`

Add one function; the rest of the file (SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, CORRELATION_GROUPS, correlation constants) is unchanged. Unknown tickers get a **deterministic** price derived from a CRC of the symbol, so a given symbol always starts at the same price (reproducible tests, stable demos).

```python
import zlib


def initial_price(ticker: str) -> float:
    """Seed price: known table first, otherwise a stable pseudo-random $20–$500."""
    if ticker in SEED_PRICES:
        return SEED_PRICES[ticker]
    return 20.0 + (zlib.crc32(ticker.encode()) % 48_000) / 100.0
```

---

## 8. Simulator — `simulator.py`

### 8.1 Math

Geometric Brownian Motion per ticker with correlated shocks:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt · Z )
Z = L · ε,   ε ~ N(0, I),   L = cholesky(correlation matrix)
dt = 0.5 s / (252 · 6.5 · 3600 s) ≈ 8.48e-8   (a trading year)
```

At σ = 0.22 and S = 190, a single tick moves ~1.2¢ one-sigma, so realistic-looking but slow. The `time_scale` option multiplies `dt` for livelier demos (e.g. `SIM_TIME_SCALE=50`); the default of 1 is "true to life".

Random events: each tick, each ticker has probability `event_probability` (default 0.001) of a sudden ±2–5% jump (≈ one dramatic move every ~50 s with 10 tickers).

Correlation: tech 0.6, finance 0.5, TSLA 0.3 with everything, cross-sector/unknown 0.3. The matrix is positive-definite for all combinations (diagonal 1, off-diagonals ≤ 0.6), so Cholesky always succeeds; it is rebuilt whenever tickers are added or removed (O(n²), n < 50).

### 8.2 `GBMSimulator`

Only three changes from the current implementation: seed via `initial_price`, `time_scale`, and a first-step `dt` multiplier.

```python
class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        time_scale: float = 1.0,
        rng: np.random.Generator | None = None,
    ) -> None:
        self._dt = dt * time_scale
        self._event_prob = event_probability
        self._rng = rng or np.random.default_rng()
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for t in tickers:
            self._add_ticker_internal(t)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance every ticker one dt. Returns {ticker: price rounded to cents}."""
        n = len(self._tickers)
        if n == 0:
            return {}
        z = self._rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        out: dict[str, float] = {}
        sqrt_dt = math.sqrt(self._dt)
        for i, ticker in enumerate(self._tickers):
            p = self._params[ticker]
            drift = (p["mu"] - 0.5 * p["sigma"] ** 2) * self._dt
            self._prices[ticker] *= math.exp(drift + p["sigma"] * sqrt_dt * z[i])

            if self._rng.random() < self._event_prob:
                shock = self._rng.uniform(0.02, 0.05) * (1 if self._rng.random() < 0.5 else -1)
                self._prices[ticker] *= 1 + shock

            out[ticker] = round(self._prices[ticker], 2)
        return out

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker], self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        p = self._prices.get(ticker)
        return None if p is None else round(p, 2)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = initial_price(ticker)
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    # _rebuild_cholesky() and _pairwise_correlation() are unchanged.
```

Passing a seeded `np.random.default_rng(42)` makes `step()` fully deterministic for tests (replacing the module-level `np.random` / `random` calls).

### 8.3 `SimulatorDataSource`

```python
class SimulatorDataSource(MarketDataSource):
    name = "simulator"

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        time_scale: float = 1.0,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._time_scale = time_scale
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        tickers = [normalize_ticker(t) for t in tickers]
        self._sim = GBMSimulator(
            tickers, event_probability=self._event_prob, time_scale=self._time_scale
        )
        for t in tickers:  # seed cache; session-open == seed price
            price = self._sim.get_price(t)
            self._cache.update(t, price, day_open=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is None:
            raise PriceUnavailableError("Simulator not started")
        if ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        price = self._sim.get_price(ticker)
        self._cache.update(ticker, price, day_open=price)  # priced immediately

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for t, p in self._sim.step().items():
                        self._cache.update(t, p)  # day_open stays sticky
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Simulator ticker policy (decision #7): any ticker passing `normalize_ticker` is accepted and gets a deterministic seed price. This is a documented simulation behaviour; `/api/health` reports `"source": "simulator"` so the UI can show a "simulated data" badge. Switch `add_ticker` to check against `SEED_PRICES` only if you want a closed universe.

---

## 9. Massive client — `massive_client.py`

### 9.1 API used

One endpoint covers everything (full reference in `archive/MASSIVE_API.md`):

`GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,...` via

```python
client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=[...])
```

Fields consumed per snapshot:

| Field | Use |
|---|---|
| `snap.ticker` | cache key |
| `snap.last_trade.price` | current price (also the fill price for trades) |
| `snap.last_trade.timestamp` | Unix **milliseconds** → divide by 1000 |
| `snap.day.previous_close` | `day_open` baseline for daily change (fallback: `snap.prev_day.close`) |

Tickers absent from the response are unknown or have no data; they stay absent from the cache.

### 9.2 Polling cadence

| Tier | Limit | `MASSIVE_POLL_INTERVAL` |
|---|---|---|
| Free | 5 req/min | 15 (default) |
| Paid | high | 2–5 |

All tracked tickers go in **one** call per poll. On-demand `add_ticker` costs one extra call (acceptable; a 429 surfaces as `PriceUnavailableError`).

### 9.3 Implementation

```python
"""Massive (Polygon.io) REST poller."""

from __future__ import annotations

import asyncio
import logging
import time

from .cache import PriceCache
from .errors import (
    MarketDataAuthError,
    PriceUnavailableError,
    UnknownTickerError,
    normalize_ticker,
)
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def _is_auth_error(exc: Exception) -> bool:
    """401/403 detection. The massive client raises on HTTP errors; the status code is
    in the exception (attribute `status` when present, otherwise its message)."""
    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    if status in (401, 403):
        return True
    msg = str(exc)
    return "401" in msg or "403" in msg or "NOT_AUTHORIZED" in msg.upper()


class MassiveDataSource(MarketDataSource):
    name = "massive"

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client = None
        self._last_success: float | None = None
        self._last_error: str | None = None

    # ---------- lifecycle ----------

    async def start(self, tickers: list[str]) -> None:
        from massive import RESTClient  # lazy: package only needed in Massive mode

        self._client = RESTClient(api_key=self._api_key)
        self._tickers = [normalize_ticker(t) for t in tickers]

        # Bootstrap poll. Auth errors are fatal (factory/lifespan falls back to the
        # simulator); anything else (network, 429) is logged and retried by the loop —
        # until the first success, trades on affected tickers are rejected with
        # PriceUnavailableError.
        await self._poll_once(raise_auth=True)
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker in self._tickers:
            return
        try:
            snaps = await asyncio.to_thread(self._fetch_snapshots, [ticker])
        except Exception as e:
            if _is_auth_error(e):
                raise MarketDataAuthError(str(e)) from e
            raise PriceUnavailableError(f"Could not fetch {ticker}: {e}") from e
        snap = next((s for s in snaps if getattr(s, "ticker", None) == ticker), None)
        if snap is None or not self._write_snapshot(snap):
            raise UnknownTickerError(ticker)
        self._tickers.append(ticker)  # only tracked once priced

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def status(self) -> dict:
        return {
            **super().status(),
            "poll_interval": self._interval,
            "last_success": self._last_success,
            "last_error": self._last_error,
        }

    # ---------- polling ----------

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self, raise_auth: bool = False) -> None:
        tickers = list(self._tickers)  # copy: the worker thread must not see mutations
        if not tickers or self._client is None:
            return
        try:
            snaps = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except Exception as e:
            self._last_error = str(e)
            if raise_auth and _is_auth_error(e):
                raise MarketDataAuthError(str(e)) from e
            logger.error("Massive poll failed: %s", e)
            return  # loop retries next interval (429, network, 5xx ...)

        written = sum(1 for s in snaps if self._write_snapshot(s))
        self._last_success = time.time()
        self._last_error = None
        logger.debug("Massive poll: %d/%d tickers updated", written, len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking REST call; always run via asyncio.to_thread."""
        from massive.rest.models import SnapshotMarketType

        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS, tickers=tickers
        )

    def _write_snapshot(self, snap) -> bool:
        """Parse one snapshot into the cache. Returns False if unusable."""
        try:
            price = float(snap.last_trade.price)
            if price <= 0:
                return False
            ts = snap.last_trade.timestamp / 1000.0  # ms → s
            prev_close = getattr(getattr(snap, "day", None), "previous_close", None)
            if not prev_close:
                prev_close = getattr(getattr(snap, "prev_day", None), "close", None)
            self._cache.update(
                snap.ticker, price, timestamp=ts, day_open=prev_close or None
            )
            return True
        except (AttributeError, TypeError, ValueError) as e:
            logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            return False
```

Notes:

- **Market closed / weekends**: `last_trade.price` is the last traded price; its timestamp may be hours old. The cache still serves it and trading is allowed (a sim with fake money filling at the last price is the sensible behaviour). See §12 on staleness.
- **Prior-close baseline while pre-market**: `day.previous_close` can be 0 early in the session; the `prev_day.close` fallback covers that, and if both are missing the cache keeps the previous baseline (or the first price).
- **`day_open` passed on every poll** so the baseline rolls over naturally at the next session.

---

## 10. Factory and service

### 10.1 `factory.py`

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty → Massive; otherwise the simulator."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        from .massive_client import MassiveDataSource  # lazy import

        interval = float(os.environ.get("MASSIVE_POLL_INTERVAL", "15"))
        return MassiveDataSource(api_key, price_cache, poll_interval=interval)

    return SimulatorDataSource(
        price_cache,
        time_scale=float(os.environ.get("SIM_TIME_SCALE", "1")),
    )
```

### 10.2 `service.py` (new) — the tracked-set rule and trade prices

`MarketDataService` is the only object routes/trade code talk to for tracking. It keeps a reason set per ticker (`"watchlist"`, `"position"`); a ticker is removed from the source only when **no reason remains**.

```python
"""Coordinates watchlist/position tracking and price lookup for trades."""

from __future__ import annotations

import asyncio
import logging

from .cache import PriceCache
from .errors import MarketDataAuthError, PriceUnavailableError, normalize_ticker
from .factory import create_market_data_source
from .interface import MarketDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

WATCHLIST = "watchlist"
POSITION = "position"


class MarketDataService:
    def __init__(self, cache: PriceCache, source: MarketDataSource) -> None:
        self.cache = cache
        self.source = source
        self._reasons: dict[str, set[str]] = {}
        self._lock = asyncio.Lock()
        self.fallback_reason: str | None = None  # set if Massive was rejected at startup

    # ---------- lifecycle ----------

    async def start(self, watchlist: list[str], positions: list[str]) -> None:
        """Start the source for watchlist ∪ positions (read from the DB by the caller)."""
        for t in watchlist:
            self._reasons.setdefault(normalize_ticker(t), set()).add(WATCHLIST)
        for t in positions:
            self._reasons.setdefault(normalize_ticker(t), set()).add(POSITION)
        tickers = sorted(self._reasons)
        try:
            await self.source.start(tickers)
        except MarketDataAuthError as e:
            logger.error("Massive rejected the API key (%s); falling back to simulator", e)
            self.fallback_reason = "massive_auth_failed"
            await self.source.stop()
            self.source = SimulatorDataSource(self.cache)
            await self.source.start(tickers)

    async def stop(self) -> None:
        await self.source.stop()

    # ---------- tracking ----------

    async def track(self, ticker: str, reason: str) -> str:
        """Ensure `ticker` is priced and tracked for `reason`. Returns the normalized ticker.
        Raises InvalidTickerError / UnknownTickerError / PriceUnavailableError."""
        ticker = normalize_ticker(ticker)
        async with self._lock:
            if ticker not in self._reasons:
                await self.source.add_ticker(ticker)  # raises before we record anything
                self._reasons[ticker] = set()
            self._reasons[ticker].add(reason)
        return ticker

    async def release(self, ticker: str, reason: str) -> None:
        """Drop one reason; stop tracking only when no reason is left."""
        ticker = normalize_ticker(ticker)
        async with self._lock:
            reasons = self._reasons.get(ticker)
            if reasons is None:
                return
            reasons.discard(reason)
            if not reasons:
                del self._reasons[ticker]
                await self.source.remove_ticker(ticker)

    def tracked(self) -> dict[str, set[str]]:
        return {t: set(r) for t, r in self._reasons.items()}

    # ---------- prices ----------

    def get_trade_price(self, ticker: str) -> float:
        """Fill price for a market order. No price ⇒ the trade must be rejected."""
        ticker = normalize_ticker(ticker)
        price = self.cache.get_price(ticker)
        if price is None or price <= 0:
            raise PriceUnavailableError(f"No current price for {ticker}")
        return price

    def status(self) -> dict:
        s = self.source.status()
        s["fallback"] = self.fallback_reason
        return s
```

### 10.3 How each caller uses it

| Caller | Call |
|---|---|
| `POST /api/watchlist {ticker}` | `await market.track(t, WATCHLIST)` → then insert DB row. `UnknownTickerError`/`InvalidTickerError` → 400 |
| `DELETE /api/watchlist/{t}` | delete DB row → `await market.release(t, WATCHLIST)`. If a position exists, the ticker keeps streaming |
| `POST /api/portfolio/trade` | `await market.track(t, POSITION)` (priced or reject) → `price = market.get_trade_price(t)` → validate + commit → on a **failed** validation or a sell-to-zero: `await market.release(t, POSITION)` |
| Chat auto-execution | same as the trade route, per action |
| Portfolio valuation | `cache.get_price(t)` per position; positions are always tracked, so never missing outside Massive cold start (then report `price: null` and exclude/flag rather than crash) |
| Startup | `await market.start(watchlist_tickers, position_tickers)` |

Pseudo-code for the trade path (sell-to-zero releases the position reason; a fresh buy tracks it):

```python
async def execute_trade(market, db, ticker, side, qty):
    ticker = await market.track(ticker, POSITION)      # raises if unpriceable
    try:
        price = market.get_trade_price(ticker)
        position_open = db.apply_trade(ticker, side, qty, price)   # atomic txn (portfolio module)
    except Exception:
        if not db.has_position(ticker):
            await market.release(ticker, POSITION)
        raise
    if not position_open:                               # sold to zero
        await market.release(ticker, POSITION)
```

---

## 11. SSE streaming — `stream.py`

Endpoint: `GET /api/stream/prices`. Wire format, one event per cache change (checked every 500 ms) plus keepalives:

```
retry: 1000

data: {"AAPL": {"ticker":"AAPL","price":190.42,"previous_price":190.41,"day_open":190.0,
                "timestamp":1761234567.12,"change":0.01,"change_percent":0.0053,
                "day_change":0.42,"day_change_percent":0.2211,"direction":"up"},
       "GOOGL": {...}, ...}

: ping

data: {...}
```

- Every `data:` event is a **full snapshot of all tracked tickers** (≈ 2 KB for 10 tickers), so a reconnecting client is consistent after the first event, and a snapshot is sent immediately on connect. (PLAN §6 said "each SSE event contains ticker, price..." — read that as "per ticker, within the snapshot".)
- No `event:` names — default `message` events.
- `: ping` comment every 15 s of silence: Massive mode updates only every 15 s, and proxies close idle streams.
- Flash animation: the client flashes a cell when `direction` is `up`/`down` **and** `price` differs from what it last rendered; `flat` never flashes.
- Connection indicator: `EventSource.onopen` → green, `onerror` while `readyState === CONNECTING` → yellow, `CLOSED` → red.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(
    price_cache: PriceCache, interval: float = 0.5, heartbeat: float = 15.0
) -> APIRouter:
    """Build a fresh router per call (no module-global router → safe in tests)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache, request: Request, interval: float, heartbeat: float
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)

    last_version = -1
    last_sent = time.monotonic()
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            now = time.monotonic()
            if version != last_version:
                last_version = version
                prices = price_cache.get_all()
                if prices:
                    payload = json.dumps({t: u.to_dict() for t, u in prices.items()})
                    yield f"data: {payload}\n\n"
                    last_sent = now
            elif now - last_sent >= heartbeat:
                yield ": ping\n\n"
                last_sent = now
            await asyncio.sleep(interval)
    finally:
        # CancelledError is deliberately NOT caught: it propagates so Starlette can
        # tear the response down; `finally` only logs.
        logger.info("SSE client disconnected: %s", client)
```

---

## 12. Edge cases and policies

| Situation | Behaviour |
|---|---|
| Startup with empty watchlist and no positions | `start([])`: simulator idles; Massive skips polling. Adding a ticker later works normally |
| Massive bootstrap poll fails (network/429) | Logged, `status()["last_error"]` set; loop retries. Tickers without a price → `PriceUnavailableError` on trade; SSE just omits them |
| Massive 401/403 at startup | `MarketDataAuthError` → service falls back to simulator, `/api/health` shows `"source":"simulator","fallback":"massive_auth_failed"` |
| Massive key revoked mid-run | Poll errors logged; `last_error` visible in `/api/health`; stale prices keep being served |
| Unknown ticker, simulator mode | Accepted if syntactically valid, deterministic seed price (decision #7) |
| Unknown ticker, Massive mode | `UnknownTickerError` → HTTP 400 `unknown_ticker`; nothing tracked |
| Watchlist removal of a held ticker | `release(t, WATCHLIST)`; `POSITION` reason keeps it streaming and priced |
| Staleness | **Not enforced.** With Massive, markets close nights/weekends and the free tier polls every 15 s, so any fixed age cutoff would block legitimate trading. If ever needed, tie it to market hours and `poll_interval` and apply only to Massive |
| Concurrent add/remove | `MarketDataService._lock` serializes tracking changes |
| Thread safety | `PriceCache` guards all state with one lock; the Massive worker thread only touches the cache through `update()` |
| Float precision | Prices are rounded to cents at the cache boundary; internal simulator state is unrounded |

`/api/health` should return `{"status": "ok", "market": market.status()}`.

---

## 13. App wiring — `main.py` (relevant part)

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    db = init_db()                                           # lazy schema + seed
    cache = PriceCache()
    market = MarketDataService(cache, create_market_data_source(cache))
    await market.start(
        watchlist=db.watchlist_tickers(),
        positions=db.position_tickers(),
    )
    app.state.market = market
    app.include_router(create_stream_router(cache))
    try:
        yield
    finally:
        await market.stop()

app = FastAPI(lifespan=lifespan)

def get_market(request: Request) -> MarketDataService:
    return request.app.state.market
```

Routes use `market: MarketDataService = Depends(get_market)`. Mount the static frontend **after** all `/api` routers.

Environment variables:

| Var | Default | Meaning |
|---|---|---|
| `MASSIVE_API_KEY` | empty | Non-empty ⇒ Massive; empty ⇒ simulator |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between snapshot polls (use 2–5 on paid tiers) |
| `SIM_TIME_SCALE` | `1` | Multiplier on the simulator `dt` (bigger ⇒ livelier prices) |

---

## 14. Delta vs. the code currently on disk

| File | Change |
|---|---|
| `errors.py` | **New** (§3) |
| `models.py` | Add `day_open`, `day_change`, `day_change_percent`; extend `to_dict()` |
| `cache.py` | `update(..., day_open=)` with sticky baseline; `version` under lock; `timestamp is None` check; bump version on `remove` |
| `interface.py` | `name`, `status()`; documented exceptions |
| `seed_prices.py` | Add `initial_price()` |
| `simulator.py` | `rng`/`time_scale`; deterministic seeds; normalize tickers; cache seeded with `day_open` |
| `massive_client.py` | Lazy `massive` import (also fixes the failing mocked tests noted in the old review); copy ticker list before the thread hop; `day_open` from previous close; on-demand priced `add_ticker`; auth-error detection; `status()` |
| `factory.py` | Env-driven interval/scale; lazy Massive import |
| `service.py` | **New** (§10.2) |
| `stream.py` | Router per call; initial snapshot; heartbeat; no swallowed `CancelledError` |
| `__init__.py` | Also export `MarketDataService`, the error classes, `normalize_ticker` |
| `pyproject.toml` | Move dev tools to `[dependency-groups] dev = [...]` (installed by `uv sync`); move `rich` to dev; add `httpx` (TestClient) |

Because `MassiveDataSource` now imports `massive` lazily inside `start()`/`_fetch_snapshots`, tests patch `app.market.massive_client.MassiveDataSource._fetch_snapshots` (see §15) instead of a module-level `RESTClient`, and the module imports even when the package is absent.

---

## 15. Testing plan (`backend/tests/market/`)

Existing suites (`test_models`, `test_cache`, `test_simulator`, `test_simulator_source`, `test_factory`, `test_massive`) are adapted to the changes above; new/changed cases:

```python
# test_errors.py
@pytest.mark.parametrize("raw,expected", [(" aapl ", "AAPL"), ("brk.b", "BRK.B")])
def test_normalize_ok(raw, expected):
    assert normalize_ticker(raw) == expected

@pytest.mark.parametrize("raw", ["", "A1", "TOOLONG", "ÄPL", "AAPL;DROP", None])
def test_normalize_rejects(raw):
    with pytest.raises(InvalidTickerError):
        normalize_ticker(raw)


# test_cache.py
def test_day_open_sticky():
    c = PriceCache()
    c.update("AAPL", 100.0, day_open=98.0)
    u = c.update("AAPL", 101.0)                    # no day_open passed
    assert u.day_open == 98.0 and u.day_change_percent == pytest.approx(3.0612, abs=1e-3)

def test_day_open_defaults_to_first_price():
    assert PriceCache().update("X", 50.0).day_open == 50.0

def test_remove_bumps_version():
    c = PriceCache(); c.update("A", 1.0); v = c.version; c.remove("A")
    assert c.version == v + 1


# test_simulator.py
def test_deterministic_with_seeded_rng():
    a = GBMSimulator(["AAPL", "MSFT"], rng=np.random.default_rng(1))
    b = GBMSimulator(["AAPL", "MSFT"], rng=np.random.default_rng(1))
    assert [a.step() for _ in range(50)] == [b.step() for _ in range(50)]

def test_unknown_ticker_seed_is_stable_and_in_range():
    assert initial_price("ZZZZ") == initial_price("ZZZZ")
    assert 20.0 <= initial_price("ZZZZ") <= 500.0

def test_prices_stay_positive_over_many_steps():
    sim = GBMSimulator(["TSLA"], event_probability=0.05, rng=np.random.default_rng(7))
    assert all(sim.step()["TSLA"] > 0 for _ in range(10_000))

def test_cholesky_rebuilt_on_add_remove():
    sim = GBMSimulator(["AAPL"]); assert sim._cholesky is None
    sim.add_ticker("MSFT"); assert sim._cholesky.shape == (2, 2)
    sim.remove_ticker("MSFT"); assert sim._cholesky is None


# test_simulator_source.py
async def test_add_ticker_priced_immediately():
    cache = PriceCache(); src = SimulatorDataSource(cache, update_interval=10)
    await src.start(["AAPL"])
    await src.add_ticker("pypl")
    assert cache.get_price("PYPL") is not None
    await src.stop()

async def test_invalid_ticker_rejected():
    src = SimulatorDataSource(PriceCache()); await src.start([])
    with pytest.raises(InvalidTickerError):
        await src.add_ticker("not a ticker")


# test_massive.py  (no `massive` package needed)
def snap(ticker, price, prev_close, ts_ms=1_700_000_000_000):
    return SimpleNamespace(
        ticker=ticker,
        last_trade=SimpleNamespace(price=price, timestamp=ts_ms),
        day=SimpleNamespace(previous_close=prev_close),
    )

async def test_poll_writes_price_and_baseline(monkeypatch):
    cache = PriceCache(); src = MassiveDataSource("k", cache)
    src._client, src._tickers = object(), ["AAPL"]
    monkeypatch.setattr(src, "_fetch_snapshots", lambda t: [snap("AAPL", 191.0, 190.0)])
    await src._poll_once()
    u = cache.get("AAPL")
    assert (u.price, u.day_open, u.timestamp) == (191.0, 190.0, 1_700_000_000.0)

async def test_add_unknown_ticker_raises_and_is_not_tracked(monkeypatch):
    src = MassiveDataSource("k", PriceCache()); src._client = object()
    monkeypatch.setattr(src, "_fetch_snapshots", lambda t: [])
    with pytest.raises(UnknownTickerError):
        await src.add_ticker("NOPE")
    assert "NOPE" not in src.get_tickers()

async def test_malformed_snapshot_skipped(monkeypatch):
    cache = PriceCache(); src = MassiveDataSource("k", cache)
    src._client, src._tickers = object(), ["AAPL", "BAD"]
    bad = SimpleNamespace(ticker="BAD", last_trade=None)
    monkeypatch.setattr(src, "_fetch_snapshots", lambda t: [snap("AAPL", 1.0, 1.0), bad])
    await src._poll_once()
    assert "AAPL" in cache and "BAD" not in cache

async def test_start_auth_error_raises(monkeypatch):
    src = MassiveDataSource("bad", PriceCache())
    monkeypatch.setattr(src, "_fetch_snapshots", lambda t: (_ for _ in ()).throw(Exception("401 NOT_AUTHORIZED")))
    monkeypatch.setitem(sys.modules, "massive", SimpleNamespace(RESTClient=lambda api_key: object()))
    with pytest.raises(MarketDataAuthError):
        await src.start(["AAPL"])


# test_service.py
async def test_held_ticker_survives_watchlist_removal():
    cache = PriceCache(); svc = MarketDataService(cache, SimulatorDataSource(cache))
    await svc.start(["AAPL"], [])
    await svc.track("AAPL", POSITION)
    await svc.release("AAPL", WATCHLIST)
    assert "AAPL" in svc.source.get_tickers() and cache.get_price("AAPL") is not None
    await svc.release("AAPL", POSITION)
    assert "AAPL" not in svc.source.get_tickers() and cache.get_price("AAPL") is None
    await svc.stop()

def test_trade_price_missing_raises():
    svc = MarketDataService(PriceCache(), SimulatorDataSource(PriceCache()))
    with pytest.raises(PriceUnavailableError):
        svc.get_trade_price("AAPL")

async def test_auth_failure_falls_back_to_simulator():
    cache = PriceCache()
    class Bad(MarketDataSource):
        name = "massive"
        async def start(self, t): raise MarketDataAuthError("401")
        async def stop(self): pass
        async def add_ticker(self, t): pass
        async def remove_ticker(self, t): pass
        def get_tickers(self): return []
    svc = MarketDataService(cache, Bad())
    await svc.start(["AAPL"], [])
    assert svc.source.name == "simulator" and svc.fallback_reason == "massive_auth_failed"
    await svc.stop()


# test_stream.py  (httpx.AsyncClient + ASGITransport, or call _generate_events with a stub Request)
class StubRequest:
    client = None
    def __init__(self, disconnect_after): self._n, self._after = 0, disconnect_after
    async def is_disconnected(self):
        self._n += 1
        return self._n > self._after

async def test_initial_snapshot_then_heartbeat():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    events = [e async for e in _generate_events(cache, StubRequest(4), interval=0.01, heartbeat=0.02)]
    assert events[0].startswith("retry:")
    assert events[1].startswith("data: ") and '"AAPL"' in events[1]
    assert any(e.startswith(": ping") for e in events[2:])

def test_two_routers_do_not_share_routes():
    cache = PriceCache()
    assert len(create_stream_router(cache).routes) == len(create_stream_router(cache).routes) == 1
```

E2E (in `test/`, simulator mode): default 10 tickers stream with changing prices; adding a ticker streams within one SSE tick; removing a held ticker keeps its price in the positions table; killing/restoring the connection flips the status dot and prices resume.
