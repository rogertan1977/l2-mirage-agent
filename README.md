<div align="center">

# 🪞 L2 Mirage Agent

### Stream-native L2 order-book anomaly detection for Binance USDⓈ-M perpetuals.
### No candles. No GPU. No API key. It runs on a potato.

Two independent WebSocket feeds, one verdict: it flags **vanishing walls** (spoofing-like liquidity mirages) and **iceberg-like absorption**, and it refuses to guess when the data cannot be trusted.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Dependencies: websockets only](https://img.shields.io/badge/dependencies-websockets%20only-brightgreen.svg)](#-quick-start)
[![GPU: not required](https://img.shields.io/badge/GPU-not%20required-brightgreen.svg)](#-the-potato-pc-manifesto)
[![Binance API key: not required](https://img.shields.io/badge/Binance%20API%20key-not%20required-brightgreen.svg)](#-security-and-trust-model)
[![Orders: never placed](https://img.shields.io/badge/orders-never%20placed-black.svg)](#-security-and-trust-model)
[![Platforms: Windows | Linux](https://img.shields.io/badge/platforms-Windows%20%7C%20Linux-lightgrey.svg)](#-quick-start)
[![Storage: SQLite WAL](https://img.shields.io/badge/storage-SQLite%20(WAL)-black.svg)](#-persistence)

[Manifesto](#-the-potato-pc-manifesto) · [Detectors](#-what-it-detects) · [Architecture](#-architecture) · [Two sockets, one truth](#-two-sockets-one-truth) · [False-positive firewall](#-the-false-positive-firewall) · [Quick start](#-quick-start) · [Limitations](#-limitations)

</div>

---

## 🧭 Why this exists

Candles are autopsy reports: by the time a bar closes, the interesting part of the order book is gone. The raw tape is different: it shows liquidity *appearing, disappearing and being absorbed* as it happens.

A common pitfall for naive "spoofing detectors" is to treat the depth feed and the trade feed as one clean clock. On Binance USDⓈ-M they are not. Since the 2026 endpoint split, **depth lives on `/public` and trades live on `/market`**: two separate connections that can drift apart exactly when the market gets violent. A wall that "vanished with no fills" may simply be a fill that has not arrived yet.

L2 Mirage Agent is built around that problem. It measures the drift, waits accordingly, and **stays silent when it cannot verify what it sees**. Missing a signal is cheaper than crying wolf.

> **Honest scope.** This tool reports *statistical patterns in public data*. It cannot prove anyone's intent, and it has not been validated for precision or recall on live markets. See [Limitations](#-limitations).

---

## 🥔 The Potato-PC Manifesto

Serious market microstructure tooling should not require a GPU cluster, a cloud bill or a paid data subscription. If it does, only a few people get to look at the market with clear eyes.

L2 Mirage Agent is designed so that a ten-year-old office PC is enough.

| | Result |
|---|---|
| **Test machine** | Intel Core i7-4770 (2013), 8 GB RAM, Windows 10/11 |
| **CPU usage** | **< 1 %** |
| **Memory** | **30-50 MB**, stable |
| **GPU / ML frameworks** | **None**. No PyTorch, TensorFlow, pandas or numpy |
| **Measured `/market` − `/public` skew** | **≈ 57 ms** in the maintainer's session |
| **Dependencies** | **One**: [`websockets`](https://pypi.org/project/websockets/) |

*These are the maintainer's measurements from a single machine, a single session and the default configuration. Your numbers will depend on your network, the symbols you watch and how busy the market is. Reproduce them yourself with `python l2_mirage_agent.py --diagnose 60`.*

**How it stays light**

- **Pure Python, single-threaded `asyncio`.** Message handlers are synchronous, so there are no locks and no data races.
- **Bounded state everywhere.** Capped deques, time-windowed accumulators and a monotonic-queue extreme tracker. Memory flattens after warm-up instead of growing with trade rate.
- **Tick data is never stored.** SQLite (WAL) only records the *events* that fire.
- **Replay benchmark (modern x86 Linux box):** 30 depth frames/s plus 550 trades/s, JSON parsing included, costs roughly **1 % of one core**.

---

## 🔎 What it detects

### Detector A: Vanishing walls (spoofing-like "mirages")

A large resting order close to price disappears almost instantly, **and the trade tape shows no matching fills**.

A candidate must satisfy all of the following before an alert fires:

| Rule | Default |
|---|---|
| Wall size (notional, per-symbol overridable) | ≥ $1,000,000 |
| Wall visible for at least | 700 ms |
| Shrinks by at least | 80 % |
| …within | ≤ 300 ms (1-3 depth frames) |
| Reduction persists to the next frame | confirm after 100 ms |
| Price is close to / approaching the wall | ≤ 0.05 % away, or ≥ 30 % closer than when the wall appeared |
| Fills at the wall's price explain at most | 20 % of the vanished size |
| Same-side liquidity re-posted elsewhere at most | 50 % |

### Detector B: Iceberg-like absorption

Heavy aggressive flow keeps hitting **one price level**, yet the *visible* quantity there barely changes (the level keeps getting refilled) and price does not break through.

| Rule | Default |
|---|---|
| Aggressive notional at a single price within the window | ≥ $10,000,000 in 60 s |
| Traded volume ÷ typical visible size | ≥ 5× |
| Current visible size ÷ typical visible size | ≥ 50 % |
| Price holds (no worse same-direction trade) | tolerance 0.01 % |
| Level still close to price | ≤ 0.1 % |
| Cooldown per level | 300 s |

Both directions are covered: sells absorbed at a bid level (support-like) and buys absorbed at an ask level (resistance-like).

> The public feed cannot tell a true iceberg order from a fast replenishing algorithm or from RPI liquidity (see [Limitations](#-limitations)). Alerts therefore say **"suspected"**.

---

## 🏗 Architecture

```text
        Binance USDⓈ-M public WebSockets (no API key)
 ┌───────────────────────────┐          ┌─────────────────────────────────┐
 │ /public                   │          │ /market                         │
 │ <sym>@depth20@100ms       │          │ <sym>@aggTrade                  │
 │ top-20 snapshot / 100 ms  │          │ <sym>@markPrice@1s  (heartbeat) │
 └─────────────┬─────────────┘          └────────────────┬────────────────┘
               │ own connection, own backoff             │ own connection, own backoff
               ▼                                         ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ Engine: single-threaded asyncio, synchronous handlers                  │
 │  • exchange-time alignment      • per-endpoint latency estimate        │
 │  • wall registry (per level)    • trade tape + extreme-price tracker   │
 │  • iceberg accumulators         • connection / loop-lag health         │
 └───────────────┬───────────────────────────────────────┬────────────────┘
                 │ shrink candidate                       │ level under watch
                 ▼                                        ▼
        ┌─────────────────────┐                 ┌───────────────────────┐
        │ Detector A          │                 │ Detector B            │
        │ deferred verdict    │                 │ absorption check      │
        │ (skew + watermark)  │                 │ (turnover, retention) │
        └──────────┬──────────┘                 └───────────┬───────────┘
                   └─────────────────┬──────────────────────┘
                                     ▼
          console warning · SQLite (WAL) · Telegram (MarkdownV2)
```

The third stream, `markPrice@1s`, is a **heartbeat**. It keeps the `/market` latency estimate alive when a symbol has no trades for a while.

---

## ⏱ Two sockets, one truth

Detector A compares *when a wall vanished* (depth feed) with *whether trades hit it* (trade feed). Those arrive over different connections. Here is how the engine keeps them honest.

1. **Exchange time, never arrival time.** Every cross-stream comparison uses event timestamps (`T` on both streams), not when a packet reached your machine.
2. **Measure the skew continuously.** For each endpoint the engine tracks `local receive time − exchange event time`. Both use the same local clock, so any clock offset cancels in the difference:
   `skew = p90(latency_market) − median(latency_public)`.
3. **Deferred verdicts.** A confirmed candidate is judged only after

   ```text
   wait = base_wait (250 ms) + skew × 1.25
   ```

   so trades published at the same moment have had time to arrive.
4. **Watermark check.** The verdict also requires that `/market` has already delivered *any* message (trade or heartbeat) with an event time later than the vanish time plus the aggregation slack. Within one ordered TCP stream, that means earlier trades have arrived.
5. **Pause, don't guess.** No verdict is issued when:
   - the required wait would exceed `max_wait_ms` (2.5 s) → *spoof detector paused*;
   - there are too few latency samples (warm-up);
   - the `/market` watermark does not advance in time;
   - either connection reconnected in the last 3 s;
   - the local event loop itself is lagging (CPU starved).
6. **Aggregation awareness.** Binance aggregates `aggTrade` per 100 ms and includes RPI fills without labels. Fill attribution uses a ±150 ms tolerance window for that reason.

> A public issue in Binance's own connector repository ([binance-connector-python #564](https://github.com/binance/binance-connector-python/issues/564)) reports `/public` vs `/market` skew in the hundreds of milliseconds to seconds under load. That report is not independently verified, but it is exactly the failure mode the pause logic exists for.

**Consequence:** alerts are *deliberately delayed* by at least ~250 ms, typically a fraction of a second, to buy correctness. This is not a zero-latency execution signal.

---

## 🛡 The false-positive firewall

| Filter | What it prevents |
|---|---|
| Confirmation frame | One-frame blinks and stream glitches |
| Minimum wall age (700 ms) | Flash orders that were never really "walls" |
| Sharpness window (≤ 300 ms) | Slow, ordinary cancellations |
| Fill attribution | Walls that were genuinely *eaten* |
| Sweep detection | Price trading *through* the wall |
| Replacement check | A wall that merely moved one tick (liquidity still there) |
| Out-of-view handling | Walls pushed beyond the top-20 window by a price move (not a cancel) |
| Proximity filter | Far-away walls that are irrelevant right now |
| Skew / watermark / quiet window / loop-lag gating | Verdicts on data that cannot be verified |
| Out-of-order and duplicate protection | Stale depth snapshots, duplicate `aggTrade` ids, crossed or empty books |
| Per-symbol cooldowns | Alert spam |
| Turnover + retention + hold checks (Detector B) | Depleting walls, broken support, thin coincidences |

Every rejection is counted. Run `--diagnose` to see why candidates were dropped.

---

## 🚀 Quick start

**Requirements:** Python 3.10+. Developed and tested on Linux (Python 3.12) and by the maintainer on Windows 10/11.

### Windows (PowerShell)

```powershell
git clone https://github.com/<your-username>/l2-mirage-agent.git
cd l2-mirage-agent
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install websockets
copy l2_config.example.yaml l2_config.yaml
```

### Linux / macOS

```bash
git clone https://github.com/<your-username>/l2-mirage-agent.git
cd l2-mirage-agent
python3 -m venv .venv && source .venv/bin/activate
pip install websockets
cp l2_config.example.yaml l2_config.yaml
```

### First-run checklist (do these in order)

```bash
python l2_mirage_agent.py --diagnose 60   # 1. look at the real data before trusting anything
python l2_mirage_agent.py --simulate      # 2. watch one spoof + one iceberg flow through the real pipeline
python l2_mirage_agent.py --test-telegram # 3. (optional) verify your alert channel
python l2_mirage_agent.py                 # 4. go live
```

**What to read in the `--diagnose` output**

- **visible span %**: `depth20` only contains the 20 levels nearest to price. For tight-tick contracts such as BTCUSDT this can be a tiny sliver of a percent. If it is below `spoof.max_distance_pct`, walls farther away are invisible to this feed.
- **estimated `/market` − `/public` skew**: your real inter-endpoint drift. If it is regularly above ~2 s, the spoof detector will pause by design.
- **spoof detector: OK / PAUSED (reason)**: whether verdicts are currently trustworthy.

---

## 🖥 CLI reference

| Command | What it does |
|---|---|
| `python l2_mirage_agent.py` | Live monitoring (Ctrl+C to stop) |
| `python l2_mirage_agent.py --diagnose [SECONDS]` | Stream for N seconds (default 30) and print message rates, visible depth span, per-endpoint latency, skew, detector state and rejection counters. **No alerts, no database.** |
| `python l2_mirage_agent.py --simulate` | Synthetic scenario through the real pipeline: one vanishing wall, one genuinely-eaten wall, one iceberg |
| `python l2_mirage_agent.py --test-telegram` | Send a test message |
| `-c FILE` | Use another config file (default `l2_config.yaml`) |
| `--db PATH` | Override `storage.path` |

---

## 🔧 Configuration

Copy `l2_config.example.yaml` to `l2_config.yaml`. Every key is documented inline. The ones you will touch first:

| Key | Default | Meaning |
|---|---:|---|
| `streams.symbols` | `BTCUSDT,ETHUSDT,SOLUSDT` | Symbols to watch (each adds a depth, a trade and a heartbeat stream) |
| `spoof.wall_min_notional_usd` | `1_000_000` | Minimum wall size; per-symbol: `wall_min_notional_overrides: "BTCUSDT:3000000"` |
| `spoof.max_distance_pct` | `0.2` | Maximum wall distance from price (**capped in practice by the visible span**) |
| `spoof.shrink_min_ratio` / `shrink_window_ms` | `0.8` / `300` | "Sharp shrink" definition |
| `spoof.max_fill_ratio` | `0.2` | Fills allowed before a vanish counts as a normal fill |
| `iceberg.min_taker_notional_usd` | `10_000_000` | Aggressive volume at one price within the window; per-symbol overrides supported |
| `iceberg.min_turnover` / `min_retention` | `5.0` / `0.5` | Absorption evidence |
| `sync.base_wait_ms` / `skew_margin` / `max_wait_ms` | `250` / `1.25` / `2500` | Verdict delay and the pause threshold |
| `*.cooldown_seconds` | `30` / `300` | Alert de-duplication |
| `storage.retention_days` | `30` | Automatic event cleanup |

Thresholds are in **notional USD** rather than coin counts so they are comparable across symbols. The config is read with a tiny built-in parser (two-level `section: key: value`); PyYAML is used automatically if it happens to be installed.

---

## 📲 Telegram alerts

Alerts are sent as MarkdownV2 from a background thread (`urllib` only), with retries, rate-limit handling and a plain-text fallback. **The current message templates are in Simplified Chinese** (translations are a welcome contribution).

Set credentials via environment variables (they take priority over the config file):

```powershell
# Windows PowerShell
$env:L2MIRAGE_TG_TOKEN = "123456789:AAxxxxxxxx"
$env:L2MIRAGE_TG_CHAT_ID = "-1001234567890"
```

```bash
# Linux / macOS
export L2MIRAGE_TG_TOKEN="123456789:AAxxxxxxxx"
export L2MIRAGE_TG_CHAT_ID="-1001234567890"
```

Set `telegram.enabled: true` in `l2_config.yaml`, then run `--test-telegram`.

**Example: vanishing wall** (synthetic data, abridged, English gloss in comments)

```text
⚠️ L2 虚假挂单撤离！检测到流动性真空区。            # Fake-wall withdrawal! Liquidity vacuum detected.
🪙 SIMUSDT  买盘墙 (bid)                            # bid-side wall
🧱 墙体: 价格 $99.97 · 数量 20,000 (≈$2.00M) · 已存在 2.0s      # wall: price, size, age
💨 撤离: 100ms 内缩减 99%（并持续 100ms 未回补），期间对应成交仅占 0%   # vanished in 100 ms, 0% explained by fills
📍 距现价: 0.030%（价格正逼近）                      # distance to price: 0.030% (price approaching)
⏱ 判定延迟 250ms（含 /public↔/market 偏差补偿 2ms）   # verdict delay incl. skew compensation
基于公开数据的统计判断，无法证明主观意图（疑似 Spoofing）。      # statistical inference; intent cannot be proven
```

**Example: iceberg-like absorption** (synthetic, abridged)

```text
🛡️ L2 冰山吸筹（疑似）！巨量主动吃单被隐蔽/补单限价单完全吸收。   # Suspected iceberg absorption
🪙 SIMUSDT  买盘价位（主动卖出被吸收，偏支撑）
🎯 价位: $99.95  距现价 0.050%
📦 10s 内该价位主动成交: 33,000 (≈$3.30M)
👁 可见挂单量: 中位 300 ｜ 吃单量/可见量 = 110×
```

---

## 🗃 Persistence

Only triggered events are stored, never ticks. SQLite in WAL mode:

```sql
CREATE TABLE l2_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,        -- exchange event time (unix seconds)
    symbol TEXT NOT NULL,
    kind TEXT NOT NULL,      -- SPOOF | ICEBERG
    side TEXT NOT NULL,      -- bid | ask
    price REAL, qty REAL, notional REAL,
    details TEXT             -- JSON: fills, skew, wait, distance, replacement ratio, ...
);
```

```bash
sqlite3 l2_mirage.db "SELECT datetime(ts,'unixepoch'), symbol, kind, side, price, notional FROM l2_events ORDER BY ts DESC LIMIT 20;"
```

Old events are purged automatically after `storage.retention_days`.

---

## 🔁 Reliability

- **Auto-reconnect** with exponential backoff (1 s → 60 s, ±20 % jitter), independently for each endpoint.
- **Stall detection:** a connection that stays open but silent for 20 s is torn down and re-established.
- **A bad frame never kills the connection.** Malformed JSON, missing fields, crossed books and unknown symbols are counted and skipped.
- **State is reset on reconnect.** The stale order-book state is discarded and verdicts are suppressed for a short quiet window.
- **Clean shutdown** on Ctrl+C / SIGTERM.

---

## 🔒 Security and trust model

- **No credentials for the exchange. Ever.** Public market-data streams only. There is no order, account or signing code in this repository.
- **Alert-only.** The program never places or manages orders. What you do with an alert is your decision and your risk.
- **Keep your Telegram token out of Git.** `l2_config.yaml` and `*.db` are already in `.gitignore`. Prefer environment variables. If a token is ever committed, revoke it with `/revoke` in [@BotFather](https://t.me/BotFather) and purge it from history.

---

## 🧪 What has (and has not) been verified

**Verified:**
- Detector logic against deterministic synthetic scenarios on a fake clock: real spoofs, genuine fills, partial fills, sweeps, flicker, repositioned walls, out-of-view walls, late-arriving trades, excessive skew, silent feeds, reconnect quiet windows, and iceberg positives and negatives.
- The network layer against local WebSocket servers: connection paths, garbage frames, exponential backoff, stall detection, unreachable hosts, state reset on reconnect.
- Resource use: bounded memory in sustained-traffic replays, and the real-world figures in the [manifesto](#-the-potato-pc-manifesto).

**Not verified:**
- **Precision and recall on live markets.** There is no labelled ground truth for "spoofing" in public data, and none is claimed.
- Behaviour during extreme exchange-side incidents or after future Binance endpoint changes.

The test harness is not yet part of this repository; adding it is on the roadmap.

---

## 🚧 Limitations

- **`depth20` is a narrow window.** It holds only the 20 levels nearest to price. For BTCUSDT-like contracts that can cover a tiny fraction of a percent, so walls farther away *cannot be seen*. Check the "visible span" in `--diagnose`.
- **Inter-endpoint skew is estimated, not known.** A delay that the estimator cannot see (for example trades delayed by seconds while heartbeats arrive on time) can still produce a false alert. The alert shows the wait and skew it used so you can judge it.
- **RPI (Retail Price Improvement) liquidity is ambiguous.** Binance's documentation says RPI orders are not shown in the depth stream (stated on the diff-depth page; the partial-depth stream is presumed to behave the same) and are aggregated into `aggTrade` without any label. Large volume at a level with little visible size is therefore ambiguous: iceberg, replenishing algorithm or RPI liquidity. Alerts say "suspected" for this reason.
- **Intent cannot be inferred.** A vanishing wall is a pattern, not proof of manipulation. Do not use alerts to accuse anyone.
- **100 ms resolution.** Both feeds are sampled or aggregated at 100 ms; the 100-300 ms shrink window means one to three frames.
- **Binance USDⓈ-M only**, and a single venue's book is not the market.
- **Alert text is currently Simplified Chinese.**

---

## 🗺 Roadmap

- [ ] Full-depth local order book (diff stream with snapshot sync) to see walls beyond the top 20 levels
- [ ] Publish the fake-clock and loopback test harness
- [ ] Optional opt-in recording of short event windows for offline precision studies
- [ ] English (and other) alert templates; Discord / webhook sinks
- [ ] Config schema validation

---

## 🤝 Contributing

Real-world evidence is the most valuable contribution.

- 📊 Share your `--diagnose` output (machine, symbols, skew, visible span). It builds the hardware-equality dataset.
- 🐛 Report suspected false positives with the matching `l2_events` row (its `details` JSON explains every decision). Never paste tokens or your `l2_config.yaml`.
- 🔧 PRs welcome for tests, translations and the roadmap items. Please keep the single-dependency constraint.

---

## 📚 References

- Binance USDⓈ-M: [Partial Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams)
- Binance USDⓈ-M: [Aggregate Trade Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams)
- Binance USDⓈ-M: [Important WebSocket Change Notice](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice) (the `/public` · `/market` · `/private` split)

---

## 📜 License

MIT. See [LICENSE](LICENSE).

## 📌 Disclaimer

L2 Mirage Agent is research and educational software that analyses public market data. **It is not investment advice, and it makes no representation about future performance or about the intent of any market participant.** Detections are statistical heuristics and can be wrong. Trading leveraged derivatives carries a substantial risk of loss, including total loss. You are solely responsible for your own decisions, and for complying with the laws and the terms of service that apply to you.

<div align="center">

*Hardware equality is a feature, not a compromise.*

</div>
