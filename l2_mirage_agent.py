#!/usr/bin/env python3
"""L2 Mirage Agent: 币安 USDⓈ-M 永续合约的 L2 订单簿异常侦测器（只发警报，绝不下单）。

数据：纯公开 WebSocket，免 API Key，不使用 K 线。
  /public  <symbol>@depth20@100ms   前 20 档订单簿快照（每条消息都是完整的前 20 档）
  /market  <symbol>@aggTrade        逐笔聚合成交（币安每 100ms 聚合一次；含 RPI 成交且无标签）
  /market  <symbol>@markPrice@1s    仅作为 /market 端点的「心跳」，用来估计两个端点的发布延迟偏差

两个侦测器：
  A  虚假挂单撤离（疑似 Spoofing）：贴近现价的大额墙在 ~100-300ms 内被撤走，且成交流里没有对应的成交
  B  冰山吸筹（疑似）：某价位被巨量主动单反复吃，可见挂单量却几乎不变（持续补单），且价格守住

关键设计（详见 README / 回复中的自审）：
  * 深度流与成交流走【两条独立连接】，高波动时可能相差数百毫秒甚至数秒。因此：
      - 所有跨流比较一律用交易所事件时间（深度用 T，成交用 T），不用本地到达时间；
      - 实时估计两个端点的发布延迟偏差 skew，判定被推迟 base_wait + skew*margin；
      - skew 大到无法可靠核对时，暂停虚假挂单侦测（宁可漏报，不乱报）；重连后静默一段时间。
  * 多重误报过滤：确认快照、墙龄、成交归因、扫穿检测、补单检测（流动性是否真的消失）、逼近检测、冷却。

用法：
  python l2_mirage_agent.py                      # 启动实时监控
  python l2_mirage_agent.py --test-telegram      # 验证警报推送链路
  python l2_mirage_agent.py --diagnose [秒]      # 只看数据：消息速率、可见深度跨度、两端点延迟与 skew（不发警报）
  python l2_mirage_agent.py --simulate           # 用合成数据走一遍完整链路
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import queue
import random
import re
import signal
import sqlite3
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path

log = logging.getLogger("l2mirage")

# ==========================================================================
# 设定（两层 key: value；优先 PyYAML，否则内建解析器，与 PanicDip / FuseTrace 一致）
# ==========================================================================
DEFAULTS: dict = {
    "streams": {
        "symbols": "BTCUSDT,ETHUSDT,SOLUSDT",
        # 2026 年起 USDⓈ-M WS 拆分为 /public /market /private：depth 在 /public，aggTrade 在 /market
        "public_ws": "wss://fstream.binance.com/public",
        "market_ws": "wss://fstream.binance.com/market",
        "depth_levels": 20,
        "depth_speed": "100ms",
        "heartbeat_markprice": True,
        "stall_seconds": 20,
        "backoff_min": 1,
        "backoff_max": 60,
    },
    "sync": {
        "agg_slack_ms": 150,
        "base_wait_ms": 250,
        "skew_margin": 1.25,
        "max_wait_ms": 2500,
        "min_latency_samples": 15,
        "resync_quiet_ms": 3000,
        "max_loop_lag_ms": 300,
    },
    "spoof": {
        "enabled": True,
        "wall_min_notional_usd": 1_000_000,
        "wall_min_notional_overrides": "",
        "max_distance_pct": 0.2,
        "near_pct": 0.05,
        "approach_frac": 0.7,
        "min_wall_age_ms": 700,
        "shrink_min_ratio": 0.8,
        "shrink_window_ms": 300,
        "confirm_ms": 100,
        "max_fill_ratio": 0.2,
        "max_replaced_ratio": 0.5,
        "cooldown_seconds": 30,
    },
    "iceberg": {
        "enabled": True,
        "window_seconds": 60,
        "min_taker_notional_usd": 10_000_000,
        "min_taker_notional_overrides": "",
        "watch_fraction": 0.3,
        "min_turnover": 5.0,
        "min_retention": 0.5,
        "hold_tolerance_pct": 0.01,
        "min_samples": 8,
        "max_distance_pct": 0.1,
        "cooldown_seconds": 300,
    },
    "storage": {"path": "l2_mirage.db", "retention_days": 30},
    "telegram": {"enabled": False, "bot_token": "", "chat_id": "", "timeout_seconds": 10},
    "logging": {"level": "INFO"},
}


def _strip_comment(line: str) -> str:
    quote = None
    for i, ch in enumerate(line):
        if quote:
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1].isspace()):
            return line[:i]
    return line


def _scalar(v: str):
    v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        return v[1:-1]
    low = v.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            pass
    return v


def _mini_yaml(text: str) -> dict:
    root: dict = {}
    section = root
    for raw in text.splitlines():
        line = _strip_comment(raw).rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        key, _, val = line.strip().partition(":")
        key = key.strip()
        if indent == 0:
            if val.strip() == "":
                section = root.setdefault(key, {})
            else:
                root[key] = _scalar(val)
                section = root
        else:
            section[key] = _scalar(val)
    return root


def load_config(path: str) -> dict:
    cfg = {k: dict(v) for k, v in DEFAULTS.items()}
    p = Path(path)
    if not p.exists():
        print(f"[warn] {path} not found, using built-in defaults", file=sys.stderr)
        return cfg
    text = p.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        user = yaml.safe_load(text) or {}
    except ImportError:
        user = _mini_yaml(text)
    for section, values in user.items():
        if isinstance(values, dict):
            cfg.setdefault(section, {}).update(values)
    return cfg


def parse_symbols(v) -> list[str]:
    items = v if isinstance(v, (list, tuple)) else re.split(r"[,\s]+", str(v))
    out: list[str] = []
    for s in items:
        s = str(s).strip().upper()
        if s and s not in out:
            out.append(s)
    return out


def parse_overrides(v) -> dict[str, float]:
    """'BTCUSDT:2000000,SOLUSDT:300000' -> {'BTCUSDT': 2000000.0, ...}"""
    out: dict[str, float] = {}
    for part in re.split(r"[,\s]+", str(v or "").strip()):
        if ":" in part:
            k, _, val = part.partition(":")
            try:
                out[k.strip().upper()] = float(val.replace("_", ""))
            except ValueError:
                continue
    return out


class Params:
    """把配置展开成属性（热路径里避免反复查字典）。"""

    def __init__(self, cfg: dict) -> None:
        s, y, sp, ic = cfg["streams"], cfg["sync"], cfg["spoof"], cfg["iceberg"]
        self.symbols = parse_symbols(s["symbols"])
        self.public_ws, self.market_ws = str(s["public_ws"]).rstrip("/"), str(s["market_ws"]).rstrip("/")
        self.levels, self.depth_speed = int(s["depth_levels"]), str(s["depth_speed"])
        self.heartbeat = bool(s["heartbeat_markprice"])
        self.stall_s, self.bo_min, self.bo_max = float(s["stall_seconds"]), float(s["backoff_min"]), float(s["backoff_max"])
        self.agg_slack, self.base_wait = int(y["agg_slack_ms"]), int(y["base_wait_ms"])
        self.skew_margin, self.max_wait = float(y["skew_margin"]), int(y["max_wait_ms"])
        self.min_lat, self.quiet_ms, self.max_loop_lag = int(y["min_latency_samples"]), int(y["resync_quiet_ms"]), float(y["max_loop_lag_ms"])
        self.spoof_on = bool(sp["enabled"])
        self.wall_default = float(sp["wall_min_notional_usd"])
        self.wall_over = parse_overrides(sp["wall_min_notional_overrides"])
        self.max_dist, self.near_pct, self.approach_frac = float(sp["max_distance_pct"]), float(sp["near_pct"]), float(sp["approach_frac"])
        self.min_wall_age, self.shrink_ratio = int(sp["min_wall_age_ms"]), float(sp["shrink_min_ratio"])
        self.shrink_win, self.confirm_ms = int(sp["shrink_window_ms"]), int(sp["confirm_ms"])
        self.max_fill, self.max_replaced = float(sp["max_fill_ratio"]), float(sp["max_replaced_ratio"])
        self.spoof_cool = float(sp["cooldown_seconds"]) * 1000.0
        self.ice_on = bool(ic["enabled"])
        self.ice_win = int(float(ic["window_seconds"]) * 1000)
        self.ice_default = float(ic["min_taker_notional_usd"])
        self.ice_over = parse_overrides(ic["min_taker_notional_overrides"])
        self.watch_frac, self.min_turnover, self.min_retention = float(ic["watch_fraction"]), float(ic["min_turnover"]), float(ic["min_retention"])
        self.hold_tol, self.min_samples = float(ic["hold_tolerance_pct"]) / 100.0, int(ic["min_samples"])
        self.ice_max_dist, self.ice_cool = float(ic["max_distance_pct"]), float(ic["cooldown_seconds"]) * 1000.0

    def urls(self) -> tuple[str, str]:
        syms = [s.lower() for s in self.symbols]
        pub = "/".join(f"{s}@depth{self.levels}@{self.depth_speed}" for s in syms)
        mkt = [f"{s}@aggTrade" for s in syms] + ([f"{s}@markPrice@1s" for s in syms] if self.heartbeat else [])
        return f"{self.public_ws}/stream?streams={pub}", f"{self.market_ws}/stream?streams={'/'.join(mkt)}"


# ==========================================================================
# SQLite（WAL）：只记录「触发的事件」，不存任何 tick
# ==========================================================================
class Database:
    def __init__(self, path: str) -> None:
        p = Path(path)
        if p.parent != Path("."):
            p.parent.mkdir(parents=True, exist_ok=True)
        self.path = str(p)
        self.conn = sqlite3.connect(self.path, timeout=10)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS l2_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,            -- unix 秒（交易所事件时间）
                symbol TEXT NOT NULL,
                kind TEXT NOT NULL,          -- SPOOF | ICEBERG
                side TEXT NOT NULL,          -- bid | ask
                price REAL, qty REAL, notional REAL,
                details TEXT                 -- JSON
            );
            CREATE INDEX IF NOT EXISTS idx_l2_ts ON l2_events(ts);
            CREATE INDEX IF NOT EXISTS idx_l2_sym ON l2_events(symbol, kind, ts);
        """)
        self.conn.commit()

    def add_event(self, ev: dict) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO l2_events(ts, symbol, kind, side, price, qty, notional, details) VALUES (?,?,?,?,?,?,?,?)",
                (ev["T"] / 1000.0, ev["sym"], ev["kind"], ev["side"], ev["price"], ev["qty"], ev["notional"],
                 json.dumps({k: v for k, v in ev.items() if k not in ("T", "sym", "kind", "side", "price", "qty", "notional")})))

    def purge(self, days: float) -> None:
        if days > 0:
            with self.conn:
                self.conn.execute("DELETE FROM l2_events WHERE ts < ?", (time.time() - days * 86400,))
            self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def close(self) -> None:
        try:
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            self.conn.close()


# ==========================================================================
# Telegram：纯 urllib + 背景线程（与 PanicDip 相同）+ MarkdownV2 简体中文模板
# ==========================================================================
_MD_SPECIAL = re.compile(r"([_*\[\]()~`>#+\-=|{}.!\\])")


def md(s) -> str:
    return _MD_SPECIAL.sub(r"\\\1", str(s))


def _plain(text: str) -> str:
    return re.sub(r"\\(.)", r"\1", text).replace("*", "").replace("`", "")


class TelegramNotifier:
    API = "https://api.telegram.org"

    def __init__(self, token: str, chat_id: str, timeout: float = 10.0) -> None:
        self.token, self.chat_id, self.timeout = token, chat_id, timeout
        self.q: queue.Queue = queue.Queue(maxsize=100)
        self.thread = threading.Thread(target=self._worker, name="tg-notifier", daemon=True)
        self.thread.start()

    def send(self, text: str) -> None:
        try:
            self.q.put_nowait(text)
        except queue.Full:
            log.warning("telegram queue full, message dropped")

    def close(self, timeout: float = 8.0) -> None:
        try:
            self.q.put(None, timeout=1)
        except queue.Full:
            return
        self.thread.join(timeout)

    def _worker(self) -> None:
        while True:
            item = self.q.get()
            if item is None:
                return
            try:
                self.deliver(item)
            except Exception:
                log.exception("telegram worker error")

    def _post(self, text: str, parse_mode: str | None) -> None:
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        req = urllib.request.Request(f"{self.API}/bot{self.token}/sendMessage", data=json.dumps(payload).encode("utf-8"),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            resp.read()

    def deliver(self, text: str) -> bool:
        parse_mode: str | None = "MarkdownV2"
        for attempt in range(3):
            try:
                self._post(text, parse_mode)
                return True
            except urllib.error.HTTPError as e:
                try:
                    body = json.loads(e.read().decode("utf-8", "replace"))
                except Exception:
                    body = {}
                desc = body.get("description", "")
                if e.code == 429:
                    time.sleep(min(float(body.get("parameters", {}).get("retry_after", 3)), 30))
                    continue
                if e.code == 400 and "parse" in desc.lower() and parse_mode:
                    log.warning("telegram rejected MarkdownV2 (%s); resending as plain text", desc)
                    text, parse_mode = _plain(text), None
                    continue
                log.warning("telegram HTTP %s: %s", e.code, desc or e.reason)
                return False
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                log.warning("telegram network error (%s), attempt %d/3", type(e).__name__, attempt + 1)
                time.sleep(2 ** attempt)
        return False


def make_notifier(cfg: dict, force: bool = False) -> TelegramNotifier | None:
    t = cfg.get("telegram", {})
    if not (t.get("enabled") or force):
        return None
    token = os.environ.get("L2MIRAGE_TG_TOKEN") or os.environ.get("PANICDIP_TG_TOKEN") or str(t.get("bot_token") or "")
    chat = os.environ.get("L2MIRAGE_TG_CHAT_ID") or os.environ.get("PANICDIP_TG_CHAT_ID") or str(t.get("chat_id") or "")
    if ":" not in token or not chat or chat.startswith("YOUR_"):
        log.warning("telegram enabled but bot_token / chat_id not configured -> notifications off")
        return None
    return TelegramNotifier(token, chat, float(t.get("timeout_seconds", 10)))


def fmt_usd(v: float) -> str:
    if v >= 1e9:
        return f"{v / 1e9:.2f}B"
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    if v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return f"{v:.0f}"


def fmt_qty(q: float) -> str:
    return f"{q:,.0f}" if q >= 100 else f"{q:.3f}".rstrip("0").rstrip(".")


def fmt_px(p: float) -> str:
    return f"{p:,.2f}" if p >= 1000 else (f"{p:.4f}" if p >= 1 else f"{p:.6g}")


def msg_spoof(ev: dict) -> str:
    side = "买盘墙 (bid)" if ev["side"] == "bid" else "卖盘墙 (ask)"
    return "\n".join([
        f"*{md('⚠️ L2 虚假挂单撤离！检测到流动性真空区。')}*",
        f"🪙 *{md(ev['sym'])}*  {md(side)}",
        "",
        f"🧱 {md('墙体')}: {md('价格 $%s · 数量 %s (≈$%s) · 已存在 %.1fs' % (fmt_px(ev['price']), fmt_qty(ev['ref_q']), fmt_usd(ev['notional']), ev['wall_age_ms'] / 1000))}",
        f"💨 {md('撤离')}: {md('%dms 内缩减 %.0f%%（并持续 %dms 未回补），期间对应成交仅占 %.0f%%（≈%s）' % (ev['drop_ms'], 100 * (1 - ev['qty_after'] / ev['ref_q']), ev['hold_ms'], 100 * ev['fill_ratio'], fmt_qty(ev['explained'])))}",
        f"📍 {md('距现价')}: {md('%.3f%%%s' % (ev['dist_pct'], '（价格正逼近）' if ev['approached'] else ''))}",
        f"🕳️ {md('同侧可见深度净减少 %.0f%%（补单比例 %.0f%%）' % (ev['net_drop_pct'], 100 * ev['replaced_ratio']))}",
        f"⏱ {md('判定延迟 %dms（含 /public↔/market 偏差补偿 %dms）' % (ev['wait_ms'], ev['skew_ms']))}",
        "",
        f"_{md('基于公开数据的统计判断，无法证明主观意图（疑似 Spoofing）。深度流不含 RPI 订单，而成交流包含 RPI 成交。')}_",
    ])


def msg_iceberg(ev: dict) -> str:
    where = "买盘价位（主动卖出被吸收，偏支撑）" if ev["side"] == "bid" else "卖盘价位（主动买入被吸收，偏阻力）"
    vis = "无可见挂单（可能是隐藏流动性或 RPI）" if ev["visible_zero"] else "中位 %s ｜ 吃单量/可见量 = %.0f×" % (fmt_qty(ev["v_med"]), ev["turnover"])
    return "\n".join([
        f"*{md('🛡️ L2 冰山吸筹（疑似）！巨量主动吃单被隐蔽/补单限价单完全吸收。')}*",
        f"🪙 *{md(ev['sym'])}*  {md(where)}",
        "",
        f"🎯 {md('价位')}: *{md('$' + fmt_px(ev['price']))}*  {md('距现价 %.3f%%' % ev['dist_pct'])}",
        f"📦 {md('%ds 内该价位主动成交' % ev['window_s'])}: {md('%s (≈$%s)' % (fmt_qty(ev['qty']), fmt_usd(ev['notional'])))}",
        f"👁 {md('可见挂单量')}: {md(vis)}",
        f"🧷 {md('当前可见量 / 中位 = %.0f%%' % (100 * ev['retention']))}  ·  {md('价格守住（无更低价的同向成交）')}",
        "",
        f"_{md('公开数据无法区分：冰山单 / 高速补单算法 / RPI 流动性。仅表示该价位在持续吸收。')}_",
    ])


# ==========================================================================
# 延迟估计（用本地接收时间 - 交易所事件时间；两个流用同一个本地时钟，所以时钟偏移会在相减时抵消）
# ==========================================================================
class Lat:
    def __init__(self, n: int = 300) -> None:
        self.d: deque[float] = deque(maxlen=n)

    def add(self, x: float) -> None:
        self.d.append(x)

    def __len__(self) -> int:
        return len(self.d)

    def median(self) -> float:
        return statistics.median(self.d) if self.d else 0.0

    def pct(self, q: float) -> float:
        if not self.d:
            return 0.0
        s = sorted(self.d)
        return s[min(len(s) - 1, int(q * len(s)))]


class Wall:
    __slots__ = ("side", "price", "first_T", "dist_first", "hist", "cand", "reported_until", "low_since")

    def __init__(self, side: str, price: float, T: int, dist_first: float) -> None:
        self.side, self.price, self.first_T, self.dist_first = side, price, T, dist_first
        self.hist: deque[tuple[int, float]] = deque(maxlen=60)
        self.cand: dict | None = None
        self.reported_until = 0
        self.low_since: int | None = None


class LevelAcc:
    """某个价位上、滚动窗口内的主动成交累计。"""
    __slots__ = ("dq", "qty", "notional", "last_eval")

    def __init__(self) -> None:
        self.dq: deque[tuple[int, float, float]] = deque()
        self.qty = self.notional = 0.0
        self.last_eval = 0

    def add(self, T: int, q: float, p: float) -> None:
        self.dq.append((T, q, p))
        self.qty += q
        self.notional += q * p

    def prune(self, min_T: int) -> None:
        while self.dq and self.dq[0][0] < min_T:
            _, q, p = self.dq.popleft()
            self.qty -= q
            self.notional -= q * p
        if not self.dq:
            self.qty = self.notional = 0.0

    @property
    def first_T(self) -> int:
        return self.dq[0][0] if self.dq else 0


class Extreme:
    """滑动窗口内的极值价格（单调队列）。内存只与「不同极值的个数」有关，与成交笔数无关；
    查询「自某时刻 t0 以来的最低 / 最高价」是 O(队列长度)，通常只有几个元素。"""
    __slots__ = ("dq", "is_min")

    def __init__(self, is_min: bool) -> None:
        self.dq: deque[tuple[int, float]] = deque()
        self.is_min = is_min

    def push(self, T: int, p: float) -> None:
        dq = self.dq
        if self.is_min:
            while dq and dq[-1][1] >= p:
                dq.pop()
        else:
            while dq and dq[-1][1] <= p:
                dq.pop()
        dq.append((T, p))

    def prune(self, min_T: int) -> None:
        dq = self.dq
        while dq and dq[0][0] < min_T:
            dq.popleft()

    def since(self, t0: int) -> float | None:
        for t, p in self.dq:
            if t >= t0:
                return p
        return None


class SymState:
    def __init__(self, sym: str) -> None:
        self.sym = sym
        self.last_depth_T = 0
        self.mid = self.best_bid = self.best_ask = self.low_bid = self.high_ask = 0.0
        self.bid_total = self.ask_total = 0.0
        self.span_pct = 0.0
        self.book_hist: deque[tuple[int, float, float]] = deque(maxlen=60)
        self.walls: dict[tuple[str, float], Wall] = {}
        self.trades: deque[tuple[int, float, float, bool]] = deque(maxlen=8000)   # (T, price, qty, taker_sell)
        self.last_agg_id = -1
        self.trade_wm = 0
        self.taker: dict[str, dict[float, LevelAcc]] = {"bid": {}, "ask": {}}
        self.extreme = {"bid": Extreme(True), "ask": Extreme(False)}   # bid 吸收：卖单的最低成交价；ask 吸收：买单的最高成交价
        self.watches: dict[tuple[str, float], deque] = {}                            # 可见挂单量样本 (T, qty)
        self.n_depth = self.n_trade = 0


# ==========================================================================
# 引擎：全部是同步处理（asyncio 单线程里没有 await），所以不存在数据竞争；
# 唯一的「竞态」是两条连接之间的到达顺序，用事件时间 + 偏差补偿 + 水位线式的延迟判定来处理。
# ==========================================================================
class Engine:
    def __init__(self, P: Params, db: Database | None = None, notifier: TelegramNotifier | None = None,
                 quiet: bool = False, mono_ms=None) -> None:
        self.P, self.db, self.notifier, self.quiet = P, db, notifier, quiet
        self.mono = mono_ms or (lambda: int(time.monotonic() * 1000))
        self.syms = {s: SymState(s) for s in P.symbols}
        self.lat_pub, self.lat_mkt = Lat(), Lat()
        self.pending: deque[dict] = deque()
        self.cool: dict[tuple, int] = {}
        self.conn_up = {"public": False, "market": False}
        self.quiet_until = 0
        self.degraded_until = 0
        self.loop_lag = 0.0
        self._skew_cache: tuple[int, float | None] = (-10**9, None)
        self.mkt_wm_E = 0            # /market 端点已发布消息的最大事件时间（成交或心跳）：同一条 TCP 流有序，
                                     # 收到事件时间晚于 t 的消息 => 发布时间早于它的成交一定已经到达
        self.alerts: deque[dict] = deque(maxlen=200)
        self.stats: dict[str, int] = {}
        self._last_sweep = 0

    def _inc(self, k: str, n: int = 1) -> None:
        self.stats[k] = self.stats.get(k, 0) + n

    # ------------------------------------------------------------ 连接状态
    def on_connect(self, name: str) -> None:
        self.conn_up[name] = True
        self.quiet_until = self.mono() + self.P.quiet_ms          # 重连后一段时间内的判定一律不可信
        if name == "market":
            self.mkt_wm_E = 0
        if name == "public":                                      # 重连前的订单簿状态已作废
            for st in self.syms.values():
                st.walls.clear()
                st.watches.clear()
                st.last_depth_T = 0
        self.pending.clear()

    def on_disconnect(self, name: str) -> None:
        self.conn_up[name] = False
        self.pending.clear()

    def note_loop_lag(self, lag_ms: float) -> None:
        self.loop_lag = 0.8 * self.loop_lag + 0.2 * max(0.0, lag_ms)
        if lag_ms > self.P.max_loop_lag:                          # CPU 被占满：事件循环自己落后了，判定不可信
            self.degraded_until = self.mono() + 2000
            self._inc("loop_lag_events")

    # ------------------------------------------------------------ 消息入口
    def on_public_msg(self, raw, recv_wall_ms: int) -> None:
        m = json.loads(raw)
        d = m.get("data", m)
        if d.get("e") == "depthUpdate":
            self.on_depth(d["s"], d, recv_wall_ms)

    def on_market_msg(self, raw, recv_wall_ms: int) -> None:
        m = json.loads(raw)
        d = m.get("data", m)
        e = d.get("e")
        if e == "aggTrade":
            self.on_trade(d["s"], d, recv_wall_ms)
        elif e == "markPriceUpdate":
            E = int(d["E"])
            self.lat_mkt.add(recv_wall_ms - E)                    # 心跳：即使没有成交也能持续测量 /market 的延迟
            self.mkt_wm_E = max(self.mkt_wm_E, E)

    # ------------------------------------------------------------ 成交流
    def on_trade(self, sym: str, d: dict, recv_wall_ms: int) -> None:
        st = self.syms.get(sym)
        if st is None:
            return
        try:
            E, T, a = int(d["E"]), int(d["T"]), int(d["a"])
            p, q, taker_sell = float(d["p"]), float(d["q"]), bool(d["m"])
        except (KeyError, TypeError, ValueError):
            self._inc("bad_trade")
            return
        self.lat_mkt.add(recv_wall_ms - E)
        self.mkt_wm_E = max(self.mkt_wm_E, E)
        if a <= st.last_agg_id:                                    # 去重 / 乱序保护
            self._inc("dup_trade")
            return
        st.last_agg_id = a
        st.n_trade += 1
        st.trade_wm = max(st.trade_wm, T)
        st.trades.append((T, p, q, taker_sell))
        if not self.P.ice_on:
            return
        side = "bid" if taker_sell else "ask"                      # 主动卖出吃的是买盘价位
        acc = st.taker[side].get(p)
        if acc is None:
            acc = st.taker[side][p] = LevelAcc()
        acc.add(T, q, p)
        acc.prune(T - self.P.ice_win)
        st.extreme[side].push(T, p)
        thr = self.P.ice_over.get(sym, self.P.ice_default)
        if acc.notional >= thr * self.P.watch_frac and (side, p) not in st.watches and len(st.watches) < 12:
            st.watches[(side, p)] = deque(maxlen=800)
        if acc.notional >= thr:
            self._eval_iceberg(st, side, p, acc, T)

    # ------------------------------------------------------------ 深度流
    def on_depth(self, sym: str, d: dict, recv_wall_ms: int) -> None:
        st = self.syms.get(sym)
        if st is None:
            return
        try:
            E = int(d["E"])
            T = int(d.get("T") or E)
            bids = sorted(((float(p), float(q)) for p, q in d["b"]), reverse=True)
            asks = sorted((float(p), float(q)) for p, q in d["a"])
        except (KeyError, TypeError, ValueError):
            self._inc("bad_depth")
            return
        self.lat_pub.add(recv_wall_ms - E)
        if T <= st.last_depth_T:
            self._inc("stale_depth")
            return
        bids = [x for x in bids if x[1] > 0]
        asks = [x for x in asks if x[1] > 0]
        if not bids or not asks or bids[0][0] >= asks[0][0]:
            self._inc("bad_depth")                                 # 空簿 / 交叉簿：丢弃，不更新状态
            return
        st.last_depth_T = T
        st.n_depth += 1
        st.best_bid, st.best_ask = bids[0][0], asks[0][0]
        st.low_bid, st.high_ask = bids[-1][0], asks[-1][0]
        st.mid = (st.best_bid + st.best_ask) / 2
        st.bid_total = sum(q for _, q in bids)
        st.ask_total = sum(q for _, q in asks)
        st.span_pct = ((st.best_bid - st.low_bid) + (st.high_ask - st.best_ask)) / 2 / st.mid * 100
        st.book_hist.append((T, st.bid_total, st.ask_total))
        maps = None
        if self.P.spoof_on:
            maps = {"bid": dict(bids), "ask": dict(asks)}
            self._update_walls(st, bids, asks, maps, T)
        if st.watches:
            maps = maps or {"bid": dict(bids), "ask": dict(asks)}
            for (side, price), samples in st.watches.items():
                inside = (st.low_bid <= price <= st.best_bid) if side == "bid" else (st.best_ask <= price <= st.high_ask)
                if inside:                                         # 价位在可见区间内才采样（区间外 = 未知，不是 0）
                    samples.append((T, maps[side].get(price, 0.0)))

    # ------------------------------------------------------------ 侦测器 A：虚假挂单
    def _update_walls(self, st: SymState, bids, asks, maps, T: int) -> None:
        P = self.P
        thr = P.wall_over.get(st.sym, P.wall_default)
        keys = set(st.walls)
        for side, levels in (("bid", bids), ("ask", asks)):
            for price, qty in levels:
                if price * qty >= thr:
                    keys.add((side, price))
        for key in keys:
            side, price = key
            qty = maps[side].get(price, 0.0)
            w = st.walls.get(key)
            if w is None:
                w = st.walls[key] = Wall(side, price, T, abs(price - st.mid) / st.mid * 100)
            if qty == 0.0:
                inside = (price >= st.low_bid) if side == "bid" else (price <= st.high_ask)
                if not inside:                                     # 被挤出前 20 档之外：不是撤单，是「看不见了」
                    st.walls.pop(key, None)
                    self._inc("out_of_view")
                    continue
            w.hist.append((T, qty))
            self._check_shrink(st, w, qty, T, thr)
            if qty * price < 0.2 * thr:                            # 墙已不存在一段时间后清理
                if w.low_since is None:
                    w.low_since = T
                elif T - w.low_since > 3000 and w.cand is None:
                    st.walls.pop(key, None)
            else:
                w.low_since = None

    def _check_shrink(self, st: SymState, w: Wall, qty: float, T: int, thr_notional: float) -> None:
        P = self.P
        c = w.cand
        if c is not None:
            if qty > (1 - P.shrink_ratio) * c["ref_q"]:            # 墙又回来了：闪烁 / 数据毛刺，不算
                w.cand = None
                self._inc("flicker")
                return
            if T - c["T_first_drop"] >= P.confirm_ms:              # 连续多帧都处于缩减状态 -> 确认
                w.cand = None
                w.reported_until = T + 2000
                self._promote(st, w, c, qty, T)
            return
        if T < w.reported_until:
            return
        lo = T - P.shrink_win
        win = [(t, q) for t, q in w.hist if lo <= t < T]
        ref_q = max((q for _, q in win), default=0.0)
        if ref_q * w.price < thr_notional or qty > (1 - P.shrink_ratio) * ref_q:
            return
        full = [t for t, q in win if q >= 0.95 * ref_q]
        ref_T, full_from = max(full), min(full)       # 最后一次 / 最早一次「满额」快照
        if ref_T - w.first_T < P.min_wall_age:                     # 墙太「年轻」：可能只是一闪而过的噪音
            self._inc("too_young")
            return
        if self.mono() < self.cool.get(("SPOOF", st.sym, w.side), 0):
            return
        w.cand = {"ref_q": ref_q, "ref_T": ref_T, "full_from": full_from, "T_first_drop": T, "qty_first": qty}

    def _promote(self, st: SymState, w: Wall, c: dict, qty: float, T: int) -> None:
        bt_ref = at_ref = None
        for t, bt, at in st.book_hist:                              # 取墙消失前那一帧的同侧可见总量
            if t <= c["ref_T"]:
                bt_ref, at_ref = bt, at
        if bt_ref is None:
            self._inc("no_ref_book")
            return
        before = bt_ref if w.side == "bid" else at_ref
        after = st.bid_total if w.side == "bid" else st.ask_total
        dist = abs(w.price - st.mid) / st.mid * 100
        if len(self.pending) >= 500:                                # 待判定队列有上限：异常行情 / 事件循环卡顿时不会无限堆积
            self._inc("pending_overflow")
            return
        self.pending.append({
            "sym": st.sym, "side": w.side, "price": w.price, "ref_q": c["ref_q"], "qty_after": qty,
            "ref_T": c["ref_T"], "full_from": c["full_from"], "T_first_drop": c["T_first_drop"], "T_confirm": T,
            "arrive": self.mono(),
            "wall_age_ms": c["ref_T"] - w.first_T, "dist_pct": dist, "dist_first": w.dist_first,
            "before_total": before, "after_total": after})

    # ------------------------------------------------------------ 判定（延迟到成交流「追上」之后）
    def skew_ms(self) -> float | None:
        mono = self.mono()
        if mono - self._skew_cache[0] >= 200:                      # 百分位需要排序：每 200ms 重算一次就够了
            if len(self.lat_pub) < self.P.min_lat or len(self.lat_mkt) < self.P.min_lat:
                v = None
            else:
                v = max(0.0, self.lat_mkt.pct(0.9) - self.lat_pub.median())
            self._skew_cache = (mono, v)
        return self._skew_cache[1]

    def wait_ms(self) -> tuple[float | None, str]:
        """深度快照到达本地的时刻 A = t + L_pub；同一时刻 t 的成交到达本地的时刻 = t + L_mkt（+ 聚合最多 100ms）。
        所以在 A 之后还要再等 max(0, L_mkt - L_pub) + 聚合延迟 + 余量，才能认为「该来的成交都到了」。"""
        sk = self.skew_ms()
        if sk is None:
            return None, "warmup"
        w = self.P.base_wait + sk * self.P.skew_margin
        if w > self.P.max_wait:
            return None, "skew"
        return w, ""

    def tick(self) -> None:
        mono = self.mono()
        if self.pending:
            wait, why = self.wait_ms()
            keep: deque[dict] = deque()
            for p in self.pending:
                age = mono - p["arrive"]
                if wait is None:
                    self._inc("spoof_unverifiable_" + why)         # 无法核对成交：宁可漏报
                elif age < wait:
                    keep.append(p)
                elif self.P.heartbeat and self.mkt_wm_E < p["T_confirm"] + self.P.agg_slack:
                    # 估计的等待已到，但 /market 还没有发布过「晚于撤单时刻」的任何消息：成交可能还在路上
                    if age >= self.P.max_wait:
                        self._inc("spoof_unverifiable_watermark")
                    else:
                        keep.append(p)
                else:
                    self._finalize_spoof(p, mono, wait)
            self.pending = keep
        if mono - self._last_sweep >= 1000:                        # 每秒一次的清理 + 冰山评估
            self._last_sweep = mono
            self._sweep()

    def _finalize_spoof(self, p: dict, mono: int, wait: float) -> None:
        P = self.P
        if not (self.conn_up["public"] and self.conn_up["market"]) or mono < self.quiet_until or mono < self.degraded_until:
            self._inc("spoof_unverifiable_state")
            return
        st = self.syms[p["sym"]]
        side, price = p["side"], p["price"]
        T_lo, T_hi = p["full_from"] - P.agg_slack, p["T_confirm"] + P.agg_slack   # 归因窗口覆盖整个观察窗口
        sweep_hi = p["T_first_drop"] + P.agg_slack
        explained, swept = 0.0, False
        eps = price * 1e-9
        for T, tp, q, taker_sell in reversed(st.trades):           # 从最新一端往回扫，遇到窗口之外立即停止
            if T < T_lo:
                break
            if T > T_hi:
                continue
            if side == "bid" and taker_sell:
                if abs(tp - price) <= eps:
                    explained += q                                  # 恰好成交在墙的价位：认为是墙被吃掉
                elif tp < price and T <= sweep_hi:
                    swept = True                                    # 墙被扫穿（只看撤离前后：撤离之后价格下滑不算）
            elif side == "ask" and not taker_sell:
                if abs(tp - price) <= eps:
                    explained += q
                elif tp > price and T <= sweep_hi:
                    swept = True
        removed = p["ref_q"] - p["qty_after"]
        fill_ratio = explained / removed if removed > 0 else 1.0
        net_removed = p["before_total"] - p["after_total"]
        replaced = min(1.0, max(0.0, (removed - net_removed) / removed)) if removed > 0 else 1.0
        approached = p["dist_pct"] <= P.near_pct or p["dist_pct"] <= P.approach_frac * p["dist_first"]
        if swept:
            self._inc("spoof_rejected_swept")
        elif fill_ratio > P.max_fill:
            self._inc("spoof_rejected_filled")
        elif replaced > P.max_replaced:
            self._inc("spoof_rejected_replaced")
        elif not approached or p["dist_pct"] > P.max_dist:
            self._inc("spoof_rejected_not_near")
        else:
            self.cool[("SPOOF", st.sym, side)] = mono + int(P.spoof_cool)
            self._emit({
                "kind": "SPOOF", "T": p["T_confirm"], "sym": st.sym, "side": side, "price": price, "qty": removed,
                "notional": p["ref_q"] * price, "ref_q": p["ref_q"], "qty_after": p["qty_after"],
                "drop_ms": p["T_first_drop"] - p["ref_T"], "hold_ms": p["T_confirm"] - p["T_first_drop"],
                "wall_age_ms": p["wall_age_ms"], "explained": explained,
                "fill_ratio": fill_ratio, "dist_pct": p["dist_pct"], "approached": approached,
                "replaced_ratio": replaced, "net_drop_pct": 100 * net_removed / p["before_total"] if p["before_total"] else 0.0,
                "wait_ms": int(wait), "skew_ms": int(self.skew_ms() or 0)})

    # ------------------------------------------------------------ 侦测器 B：冰山吸筹
    def _eval_iceberg(self, st: SymState, side: str, price: float, acc: LevelAcc, T: int) -> None:
        P = self.P
        mono = self.mono()
        if mono - acc.last_eval < 500:                              # 限频：同一价位最多每 0.5 秒评估一次
            return
        acc.last_eval = mono
        samples = st.watches.get((side, price))
        if samples is None or len(samples) < P.min_samples:
            return
        key = ("ICE", st.sym, side, price)
        if mono < self.cool.get(key, 0) or st.mid <= 0:
            return
        vis = [q for _, q in samples]
        v_med, v_last = statistics.median(vis), vis[-1]
        visible_zero = v_med <= 0
        turnover = float("inf") if visible_zero else acc.qty / v_med
        retention = 1.0 if visible_zero else v_last / v_med
        if turnover < P.min_turnover or retention < P.min_retention:
            self._inc("ice_rejected_depleted")
            return
        first_T = acc.first_T                                       # 价格必须「守住」：自这个价位开始被吃以来，没有更差价格的同向成交
        ext = st.extreme[side].since(first_T)
        held = ext is None or (ext >= price * (1 - P.hold_tol) if side == "bid" else ext <= price * (1 + P.hold_tol))
        if not held:
            self._inc("ice_rejected_pierced")
            return
        dist = abs(price - st.mid) / st.mid * 100
        if dist > P.ice_max_dist:
            self._inc("ice_rejected_far")
            return
        self.cool[key] = mono + int(P.ice_cool)
        ev = {"kind": "ICEBERG", "T": T, "sym": st.sym, "side": side, "price": price, "qty": acc.qty,
              "notional": acc.notional, "window_s": int(P.ice_win / 1000), "v_med": v_med, "v_last": v_last,
              "turnover": 0.0 if visible_zero else turnover, "retention": retention, "dist_pct": dist,
              "visible_zero": visible_zero}
        acc.dq.clear()
        acc.qty = acc.notional = 0.0
        st.watches.pop((side, price), None)
        self._emit(ev)

    def _sweep(self) -> None:
        P = self.P
        for st in self.syms.values():
            wm = st.trade_wm
            while st.trades and st.trades[0][0] < wm - 30_000:
                st.trades.popleft()
            for side in ("bid", "ask"):
                st.extreme[side].prune(wm - P.ice_win)
                for price in list(st.taker[side]):
                    acc = st.taker[side][price]
                    acc.prune(wm - P.ice_win)
                    if not acc.dq:
                        del st.taker[side][price]
                        st.watches.pop((side, price), None)
                    elif P.ice_on and acc.notional >= P.ice_over.get(st.sym, P.ice_default):
                        self._eval_iceberg(st, side, price, acc, wm)

    # ------------------------------------------------------------ 输出
    def _emit(self, ev: dict) -> None:
        self.alerts.append(ev)
        self._inc("alert_" + ev["kind"].lower())
        if self.quiet:
            return
        if ev["kind"] == "SPOOF":
            log.warning("⚠️  [SPOOF] %s %s wall @%s qty %s (≈$%s) vanished in %dms (held %dms), fills %.0f%%, near %.3f%%, replaced %.0f%%, wait %dms (skew %dms)",
                        ev["sym"], ev["side"], fmt_px(ev["price"]), fmt_qty(ev["ref_q"]), fmt_usd(ev["notional"]), ev["drop_ms"], ev["hold_ms"],
                        100 * ev["fill_ratio"], ev["dist_pct"], 100 * ev["replaced_ratio"], ev["wait_ms"], ev["skew_ms"])
        else:
            log.warning("🛡️  [ICEBERG?] %s %s level @%s absorbed %s (≈$%s) in %ds, visible median %s, turnover %.0fx, retention %.0f%%",
                        ev["sym"], ev["side"], fmt_px(ev["price"]), fmt_qty(ev["qty"]), fmt_usd(ev["notional"]), ev["window_s"],
                        fmt_qty(ev["v_med"]), ev["turnover"], 100 * ev["retention"])
        if self.db:
            try:
                self.db.add_event(ev)
            except Exception:
                log.exception("db write failed")
        if self.notifier:
            try:
                self.notifier.send(msg_spoof(ev) if ev["kind"] == "SPOOF" else msg_iceberg(ev))
            except Exception:
                log.exception("failed to build/send telegram message")

    def snapshot(self) -> dict:
        sk = self.skew_ms()
        wait, why = self.wait_ms()
        return {
            "lat_pub_med": self.lat_pub.median(), "lat_pub_p90": self.lat_pub.pct(0.9),
            "lat_mkt_med": self.lat_mkt.median(), "lat_mkt_p90": self.lat_mkt.pct(0.9),
            "skew_ms": sk, "wait_ms": wait, "spoof_state": "OK" if wait is not None else f"PAUSED ({why})",
            "loop_lag_ms": self.loop_lag, "pending": len(self.pending), "stats": dict(self.stats),
            "symbols": {s: {"depth": st.n_depth, "trades": st.n_trade, "span_pct": st.span_pct, "walls": len(st.walls),
                            "watches": len(st.watches)} for s, st in self.syms.items()}}


# ==========================================================================
# 网络层：两条独立连接，指数退避 + 抖动自动重连；任何异常都不会让进程崩溃
# ==========================================================================
async def stream_loop(name: str, url: str, handler, engine: Engine, stop: asyncio.Event, P: Params) -> None:
    try:
        import websockets
    except ImportError:
        log.error("missing dependency: pip install websockets")
        engine._inc("fatal_missing_websockets")
        stop.set()
        return
    backoff = P.bo_min
    n_conn = 0
    errors = 0
    while not stop.is_set():
        reason = "closed"
        up_at = None
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=512, close_timeout=2) as ws:
                n_conn += 1
                up_at = time.monotonic()
                engine.on_connect(name)
                (log.info if n_conn == 1 else log.warning)("%s stream connected (#%d)", name, n_conn)
                while not stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=P.stall_s)
                    recv_wall = int(time.time() * 1000)
                    try:
                        handler(raw, recv_wall)
                    except Exception:                                   # 单条坏消息绝不能杀掉连接
                        errors += 1
                        engine._inc("handler_errors")
                        if errors <= 5:
                            log.exception("%s: bad message skipped", name)
                    if up_at is not None and time.monotonic() - up_at > 30:
                        backoff = P.bo_min                               # 连接稳定了，重置退避
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            reason = f"stalled (no data for {P.stall_s:.0f}s)"
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
        engine.on_disconnect(name)
        if stop.is_set():
            break
        delay = backoff * (0.8 + 0.4 * random.random())
        log.warning("%s stream down (%s); reconnecting in %.1fs", name, reason, delay)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, P.bo_max)


async def ticker(engine: Engine, stop: asyncio.Event, db: Database | None, retention: float, diagnose: bool) -> None:
    period = 0.05
    last = time.monotonic()
    t_start, last_beat, last_purge, span_logged = last, last, 0.0, False
    while not stop.is_set():
        await asyncio.sleep(period)
        now = time.monotonic()
        engine.note_loop_lag((now - last - period) * 1000)
        last = now
        try:
            engine.tick()
        except Exception:
            log.exception("tick failed")
        if not span_logged and not diagnose and now - t_start > 12:
            span_logged = True
            log_span_report(engine)
        if now - last_beat >= 60 and not diagnose:
            last_beat = now
            s = engine.snapshot()
            log.info("heartbeat: spoof=%s skew=%s lat_pub=%.0f/%.0fms lat_mkt=%.0f/%.0fms loop_lag=%.0fms pending=%d alerts(S/I)=%d/%d depth=%s",
                     s["spoof_state"], "n/a" if s["skew_ms"] is None else f"{s['skew_ms']:.0f}ms", s["lat_pub_med"], s["lat_pub_p90"],
                     s["lat_mkt_med"], s["lat_mkt_p90"], s["loop_lag_ms"], s["pending"], s["stats"].get("alert_spoof", 0),
                     s["stats"].get("alert_iceberg", 0), {k: v["depth"] for k, v in s["symbols"].items()})
        if db and now - last_purge > 3600:
            last_purge = now
            try:
                db.purge(retention)
            except Exception:
                log.exception("purge failed")


def log_span_report(engine: Engine) -> None:
    """depth20 只覆盖盘口最靠近现价的 20 档。对 BTC 这类最小跳动单位很小的币种，它可能只覆盖 ~0.003% 的价格区间。"""
    P = engine.P
    for s, st in engine.syms.items():
        if st.span_pct <= 0:
            continue
        note = ""
        if st.span_pct < P.max_dist:
            note = f" -> 前 20 档只覆盖 ±{st.span_pct:.3f}%，小于 max_distance_pct={P.max_dist}%：更远的墙在这个流里【看不见】"
        log.info("visible depth span %s: %.4f%% of price%s", s, st.span_pct, note)


async def run(cfg: dict, diagnose: int | None = None) -> int:
    P = Params(cfg)
    if not P.symbols:
        print("config error: streams.symbols is empty")
        return 1
    db = None if diagnose else Database(cfg["storage"]["path"])
    notifier = None if diagnose else make_notifier(cfg)
    engine = Engine(P, db, notifier, quiet=bool(diagnose))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    pub_url, mkt_url = P.urls()
    log.info("L2 Mirage started | %s | spoof=%s iceberg=%s | db=%s | telegram=%s", ",".join(P.symbols),
             "on" if P.spoof_on else "off", "on" if P.ice_on else "off", db.path if db else "-", "on" if notifier else "off")
    tasks = [asyncio.create_task(stream_loop("public", pub_url, engine.on_public_msg, engine, stop, P)),
             asyncio.create_task(stream_loop("market", mkt_url, engine.on_market_msg, engine, stop, P)),
             asyncio.create_task(ticker(engine, stop, db, float(cfg["storage"]["retention_days"]), bool(diagnose)))]
    try:
        if diagnose:
            await asyncio.sleep(diagnose)
            print_diagnosis(engine, diagnose)
        else:
            await stop.wait()
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if notifier:
            notifier.close()
        if db:
            db.close()
        log.info("shutdown complete")
    return 1 if engine.stats.get("fatal_missing_websockets") else 0


def print_diagnosis(engine: Engine, seconds: int) -> None:
    s = engine.snapshot()
    print(f"\n=== L2 Mirage diagnosis ({seconds}s) ===")
    print(f"{'symbol':<12}{'depth/s':>9}{'trades/s':>10}{'visible span %':>16}{'walls':>7}")
    for sym, v in s["symbols"].items():
        print(f"{sym:<12}{v['depth'] / seconds:>9.1f}{v['trades'] / seconds:>10.1f}{v['span_pct']:>16.4f}{v['walls']:>7}")
    print(f"\nlatency (local receive time − exchange event time E), ms:")
    print(f"  /public  depth    median {s['lat_pub_med']:.0f}   p90 {s['lat_pub_p90']:.0f}   samples {len(engine.lat_pub)}")
    print(f"  /market  trade+hb median {s['lat_mkt_med']:.0f}   p90 {s['lat_mkt_p90']:.0f}   samples {len(engine.lat_mkt)}")
    sk = s["skew_ms"]
    print(f"  estimated /market − /public skew: {'n/a (not enough samples)' if sk is None else f'{sk:.0f} ms'}")
    print(f"  spoof detector: {s['spoof_state']}" + ("" if s["wait_ms"] is None else f"  (verdict wait {s['wait_ms']:.0f} ms)"))
    print(f"  event-loop lag: {s['loop_lag_ms']:.1f} ms")
    cand = {k: v for k, v in s["stats"].items() if k.startswith(("alert_", "spoof_", "ice_", "flicker", "too_young", "out_of_view", "stale", "dup", "bad", "handler"))}
    print(f"  counters: {cand or 'none'}")
    print("\nNote: absolute latency includes your local clock offset; only the DIFFERENCE between the two endpoints is meaningful.")
    print("depth20 covers only the visible top-20 levels: if 'visible span %' is below spoof.max_distance_pct, walls farther away cannot be seen.")


# ==========================================================================
# 合成数据演练：走一遍真实的处理链路（含 Telegram 模板），不需要网络
# ==========================================================================
async def simulate(cfg: dict) -> int:
    cfg = {k: dict(v) for k, v in cfg.items()}
    cfg["streams"]["symbols"] = "SIMUSDT"
    cfg["sync"].update(min_latency_samples=3, resync_quiet_ms=0)
    cfg["spoof"].update(wall_min_notional_usd=1_000_000, min_wall_age_ms=700)
    cfg["iceberg"].update(window_seconds=10, min_taker_notional_usd=3_000_000, min_samples=5)
    cfg["storage"]["path"] = "l2_mirage_sim.db"
    P = Params(cfg)
    db = Database(cfg["storage"]["path"])
    notifier = make_notifier(cfg)
    eng = Engine(P, db, notifier)
    eng.on_connect("public")
    eng.on_connect("market")
    eng.quiet_until = 0
    aid = [0]

    def book(bid_wall: float, ask_wall: float, vis95: float = 300.0) -> dict:
        now = int(time.time() * 1000)
        bids = [[f"{99.99:.2f}", "300"], ["99.98", "300"], ["99.97", str(bid_wall)], ["99.96", "300"], ["99.95", str(vis95)]]
        bids += [[f"{99.94 - 0.01 * i:.2f}", "300"] for i in range(15)]
        asks = [["100.01", "300"], ["100.02", "300"], ["100.03", str(ask_wall)]] + [[f"{100.04 + 0.01 * i:.2f}", "300"] for i in range(17)]
        return {"e": "depthUpdate", "E": now, "T": now, "s": "SIMUSDT", "b": bids, "a": asks}

    def trade(price: float, qty: float, taker_sell: bool) -> dict:
        now = int(time.time() * 1000)
        aid[0] += 1
        return {"e": "aggTrade", "E": now, "T": now, "s": "SIMUSDT", "a": aid[0], "p": f"{price:.2f}", "q": str(qty), "m": taker_sell}

    def feed(depth: dict | None = None, trades: list | None = None, hb: bool = False) -> None:
        now = int(time.time() * 1000)
        if depth:
            eng.on_depth("SIMUSDT", depth, now)
        for t in trades or []:
            eng.on_trade("SIMUSDT", t, now)
        if hb:                                                     # 走真实的消息入口（含组合流外壳），测试分发逻辑
            eng.on_market_msg(json.dumps({"stream": "simusdt@markPrice@1s",
                                          "data": {"e": "markPriceUpdate", "E": now, "s": "SIMUSDT", "p": "100"}}), now)

    log.info("[simulate] 15s scenario: 2.5s calm -> bid wall VANISHES (no fills) -> ask wall is genuinely EATEN -> iceberg absorption at 99.95")
    t0 = time.monotonic()
    step = 0
    bid_wall, ask_wall, vis95 = 20000.0, 20000.0, 300.0
    while time.monotonic() - t0 < 15:
        el = time.monotonic() - t0
        trades: list = []
        if 2.5 <= el < 5.0:
            bid_wall = 200.0                                          # 撤单：缩减 99%，没有任何成交
        elif 5.0 <= el < 5.2:
            bid_wall = 20000.0
        if 5.2 <= el < 5.9:
            qty = 3000.0
            trades.append(trade(100.03, qty, False))                  # 真实吃单：卖墙被吃掉
            ask_wall = max(200.0, ask_wall - qty)
        elif 5.9 <= el < 6.5:
            ask_wall = 20000.0
        if 8.0 <= el < 14.0 and step % 2 == 0:
            trades.append(trade(99.95, 3000.0, True))                 # 99.95 被反复砸，可见量始终 ~300
        feed(book(bid_wall, ask_wall, vis95), trades, hb=(step % 5 == 0))
        eng.tick()
        step += 1
        await asyncio.sleep(0.1)
    for _ in range(10):
        eng.tick()
        await asyncio.sleep(0.1)
    spoofs = [a for a in eng.alerts if a["kind"] == "SPOOF"]
    ices = [a for a in eng.alerts if a["kind"] == "ICEBERG"]
    print(f"\n[simulate] alerts: SPOOF={len(spoofs)} (expected 1)  ICEBERG={len(ices)} (expected 1)")
    print(f"[simulate] rejected as explained by fills: {eng.stats.get('spoof_rejected_filled', 0) + eng.stats.get('spoof_rejected_swept', 0)}  counters: {eng.stats}")
    if notifier:
        notifier.close()
    db.close()
    return 0 if len(spoofs) == 1 and len(ices) == 1 else 1


def test_telegram(cfg: dict) -> int:
    n = make_notifier(cfg, force=True)
    if n is None:
        print("Telegram not configured. Set [telegram] in the config, or env L2MIRAGE_TG_TOKEN / L2MIRAGE_TG_CHAT_ID.")
        return 1
    ok = n.deliver("\n".join(["🧪 *L2 Mirage 测试讯息*", md("如果你能看到这条讯息，说明 Telegram 推送已正常工作。✅")]))
    n.close(1)
    print("✅ test message sent" if ok else "❌ failed to send (see log above)")
    return 0 if ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="L2 Mirage Agent - spoofing / iceberg-absorption detector (alert-only)")
    ap.add_argument("-c", "--config", default="l2_config.yaml")
    ap.add_argument("--db", help="override storage.path")
    ap.add_argument("--test-telegram", action="store_true", help="send a test message and exit")
    ap.add_argument("--diagnose", nargs="?", const=30, type=int, metavar="SECONDS",
                    help="stream for N seconds and print rates, visible depth span, per-endpoint latency and skew (no alerts)")
    ap.add_argument("--simulate", action="store_true", help="run a synthetic scenario through the real pipeline")
    args = ap.parse_args()
    cfg = load_config(args.config)
    logging.basicConfig(level=getattr(logging, str(cfg["logging"]["level"]).upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    if args.db:
        cfg["storage"]["path"] = args.db
    if args.test_telegram:
        sys.exit(test_telegram(cfg))
    try:
        if args.simulate:
            sys.exit(asyncio.run(simulate(cfg)))
        sys.exit(asyncio.run(run(cfg, args.diagnose)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
