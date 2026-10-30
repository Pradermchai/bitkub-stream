#!/usr/bin/env python3
"""
bitkub_stream_collector.py — second Bitkub data collector (fast stream, for a future market-making study).

Paper-trading research, not financial advice. Reads PUBLIC Bitkub market data only (WebSocket streams + two public
REST calls). No orders, no accounts, no exchange API keys. Standard library only, except `decrypt` on the owner's PC.

40 Bitkub "exchange" THB coins (real local order books; frozen Test P list, 2026-10-30) plus USDT_THB as the
THB/USD reference. Connections (checked live 2026-10-29; see knowledge/bitkub-stream-data.md):
  wss://api.bitkub.com/websocket-api/market.trade.thb_x,...,market.ticker.thb_x,...   (1 connection, all coins)
  wss://api.bitkub.com/websocket-api/orderbook/<pairing_id>                         (1 connection per coin)
(The docs write `orderbook.<id>`; only the `orderbook/<id>` path answers. The docs also call market.trade closed since
2026-05-18, yet it still streams; if it stops, trades_ob below still records every trade with its side.)

  python bitkub_stream_collector.py run --minutes 330 --handoff-at 320 --handoff-cmd "gh workflow run stream.yml"
  python bitkub_stream_collector.py decrypt DOWNLOADS_DIR RUNS_DIR   # owner's PC (key: STREAM_DATA_KEY or prompt)
  python bitkub_stream_collector.py merge RUNS_DIR MERGED_DIR         # join runs, remove hand-over duplicates
  python bitkub_stream_collector.py download --repo OWNER/REPO --out DOWNLOADS_DIR   # public release assets

Output of one run (UTC days, by receive time), in OUT/<run_id>/:
  <day>/trades_<SYM>.csv.gz     recv_ms,ts_ms,txn,price,amount,side   (trade stream; side added from the order-book
                                                                       stream's trade list, blank if not matched)
  <day>/trades_ob_<SYM>.csv.gz  recv_ms,ts_s,price,amount,side         (order-book stream's trade list, de-duplicated)
  <day>/book10_<SYM>.csv.gz     recv_ms,b1_p,b1_q..b10_q,a1_p..a10_q    (top 10 levels, written when they change)
  <day>/bbo_<SYM>.csv.gz        recv_ms,bid,bid_qty,ask,ask_qty,last    (ticker stream, written when it changes)
  gaps.csv        connection drops: conn, symbols, start/end of the silence, reason
  heartbeat.csv   hourly: per coin row counts and exchange->receive delay; ALL row with clock offset and reconnects
  run.csv         one row: start/end, messages, reconnects, gaps, hand-over result
recv_ms = this machine's clock when the message arrived; ts_ms/ts_s = Bitkub's trade time. Books carry no exchange time.
"""

from __future__ import annotations

import argparse
import base64
import csv
import getpass
import gzip
import hashlib
import io
import json
import os
import socket
import ssl
import struct
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
import zlib
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# Frozen Test P list (reports/test_p_plan.md): output of select_exchange_coins.py run 2026-10-30 ≈ 06:40 Bangkok.
# Rule: active, unfrozen "exchange" THB pairs, no pegged coins, median daily THB value 23-29 Oct 2026 >= 200k, top 40.
# The old Test J coins and USDT pairs were Bitkub "broker" pairs (hidden orders, knowledge/bitkub-broker-pairs.md).
COINS = ["BTC_THB", "NEAR_THB", "ETH_THB", "XRP_THB", "DOGE_THB", "WLD_THB", "SOL_THB", "IOST_THB", "CRV_THB",
         "ADA_THB", "LUNA_THB", "XLM_THB", "OP_THB", "KUB_THB", "BNB_THB", "EPIC_THB", "GALA_THB", "ENA_THB",
         "ONDO_THB", "LINK_THB", "UNI_THB", "HBAR_THB", "SUI_THB", "REALX_THB", "MAVIA_THB", "BCH_THB", "ORDER_THB",
         "HYPE_THB", "REZ_THB", "SAND_THB", "ZIL_THB", "DOT_THB", "MOVR_THB", "SQD_THB", "ARB_THB", "BMT_THB",
         "QNT_THB", "JFIN_THB", "ZRC_THB", "SOPH_THB"]
SYMBOLS = COINS + ["USDT_THB"]
HOST = "api.bitkub.com"
REST = f"https://{HOST}/api/v3"
WS_PATH = "/websocket-api/"
LEVELS = 10
SIDE_WAIT_MS = 60000       # a trade waits this long for its side (the order-book trade list came 3-35 s late live)
DATA_TIMEOUT_S = 120       # no data message for this long = dead connection, reconnect
FLUSH_EVERY_S = 30         # gzip sync-flush: a killed run loses at most this much
HEARTBEAT_EVERY_S = 3600
KDF_ITER = 200000          # must match `openssl enc -pbkdf2 -iter` in the workflow

HEADERS = {
    "trades": ["recv_ms", "ts_ms", "txn", "price", "amount", "side"],
    "trades_ob": ["recv_ms", "ts_s", "price", "amount", "side"],
    "book10": ["recv_ms"] + [f"{s}{i}_{x}" for s in "ba" for i in range(1, LEVELS + 1) for x in "pq"],
    "bbo": ["recv_ms", "bid", "bid_qty", "ask", "ask_qty", "last"],
}
GAP_HEADER = ["conn", "syms", "start_ms", "end_ms", "start_utc", "end_utc", "reason"]
HB_HEADER = ["recv_ms", "utc", "sym", "trades", "side_blank", "trades_ob", "book10", "bbo", "rest_fills",
             "delay_p50_ms", "delay_p90_ms", "conns_up", "reconnects", "clock_offset_ms"]
REST_FILL_EVERY_MS = 5000  # at most one REST trade-list call per coin per 5 s (limit: 100 calls/s)


def now_ms() -> int:
    return int(time.time() * 1000)


def utc(ms: Optional[int]) -> str:
    return "" if ms is None else time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000))


def day_of(ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ms / 1000))


def http_json(url: str, timeout: float = 20) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "quant-brain-research (public data, paper study)"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ---------------------------------------------------------------- minimal WebSocket client (RFC 6455, text only)

class ConnectionClosed(Exception):
    pass


class WebSocket:
    """Client side of a WebSocket over any socket-like object (recv/sendall/settimeout/close). Frames are taken from
    the buffer only when complete, so a socket timeout never loses half a frame."""

    def __init__(self, sock: Any, buf: bytes = b"", data_timeout: float = DATA_TIMEOUT_S,
                 clock: Callable[[], float] = time.monotonic):
        self.sock, self.buf, self.data_timeout, self.clock = sock, buf, data_timeout, clock
        self.pos = 0  # start of unread bytes: frames are read in place, the buffer is copied once per network read
        self.parts: List[bytes] = []

    @classmethod
    def connect(cls, path: str, timeout: float = 20) -> "WebSocket":
        raw = socket.create_connection((HOST, 443), timeout=timeout)
        sock = ssl.create_default_context().wrap_socket(raw, server_hostname=HOST)
        key = base64.b64encode(os.urandom(16)).decode()
        sock.sendall((f"GET {path} HTTP/1.1\r\nHost: {HOST}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
                      "User-Agent: quant-brain-research (public data, paper study)\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = sock.recv(4096)
            if not d:
                raise ConnectionClosed("closed during handshake")
            buf += d
        head, buf = buf.split(b"\r\n\r\n", 1)
        # Only the status is checked: Bitkub's CDN answers with a Sec-WebSocket-Accept that is not derived from our
        # key (seen 2026-10-29), and a wrong path answers "200 OK" instead of 101.
        if b" 101 " not in head.split(b"\r\n")[0]:
            sock.close()
            raise ConnectionClosed("handshake refused: " + head.split(b"\r\n")[0].decode(errors="replace"))
        sock.settimeout(5)  # short reads so a stop request is noticed quickly
        return cls(sock, buf)

    def _frame(self) -> Optional[Tuple[bool, int, bytes]]:
        b, s = self.buf, self.pos
        if len(b) - s < 2:
            return None
        n, i = b[s + 1] & 127, s + 2
        if n == 126:
            if len(b) - s < 4:
                return None
            n, i = struct.unpack(">H", b[s + 2:s + 4])[0], s + 4
        elif n == 127:
            if len(b) - s < 10:
                return None
            n, i = struct.unpack(">Q", b[s + 2:s + 10])[0], s + 10
        mask = b""
        if b[s + 1] & 128:
            mask, i = b[i:i + 4], i + 4
        if len(b) < i + n:
            return None
        p = b[i:i + n]
        if mask:
            p = bytes(x ^ mask[k % 4] for k, x in enumerate(p))
        self.pos = i + n
        return bool(b[s] & 128), b[s] & 15, p

    def send(self, opcode: int, payload: bytes = b"") -> None:
        m = os.urandom(4)  # clients must mask
        self.sock.sendall(bytes([0x80 | opcode, 0x80 | len(payload)]) + m +
                          bytes(x ^ m[k % 4] for k, x in enumerate(payload)))

    def recv(self, stop: Callable[[], bool] = lambda: False) -> Optional[str]:
        """Next text message; None if `stop()` became true. Answers pings. Raises on close or a silent connection."""
        last_data = self.clock()
        while True:
            f = self._frame()
            if f is None:
                if stop():
                    return None
                if self.clock() - last_data > self.data_timeout:
                    raise TimeoutError(f"no data for {self.data_timeout:.0f}s")
                try:
                    d = self.sock.recv(65536)
                except socket.timeout:
                    continue
                if not d:
                    raise ConnectionClosed("socket closed")
                self.buf, self.pos = self.buf[self.pos:] + d, 0
                continue
            fin, op, p = f
            if op == 9:
                self.send(10, p)
            elif op == 8:
                raise ConnectionClosed("close frame " + repr(p[:60]))
            elif op in (0, 1, 2):
                self.parts.append(p)
                if fin:
                    msg, self.parts = b"".join(self.parts), []
                    return msg.decode("utf-8", errors="replace")

    def close(self) -> None:
        try:
            self.sock.close()
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------- message parsing (pure functions)

def loads(line: str) -> Any:
    """JSON with decimals kept as the exact text Bitkub sent (no float rounding in the files)."""
    return json.loads(line, parse_float=str)


def split_lines(text: str) -> List[str]:
    return [x for x in text.split("\n") if x.strip()]


def norm_sym(s: str) -> str:
    """Bitkub stream form (quote first) -> ours: 'THB_PEPE' / 'market.trade.thb_pepe' -> 'PEPE_THB',
    'USDT_BTC' -> 'BTC_USDT', 'THB_USDT' -> 'USDT_THB'."""
    s = s.split(".")[-1].upper()
    a, b = s.split("_", 1)
    return f"{b}_{a}" if a in ("THB", "USDT") else s


def stream_name(kind: str, sym: str) -> str:
    coin, quote = sym.lower().split("_")
    return f"market.{kind}.{quote}_{coin}"


def parse_trade(m: Dict[str, Any], recv: int) -> Tuple[str, List[Any]]:
    """market.trade message -> (SYM, [recv_ms, ts_ms, txn, price, amount, side=''])."""
    return norm_sym(m["sym"]), [recv, int(m["ts"]), str(m["txn"]), str(m["rat"]), str(m["amt"]), ""]


def parse_ticker(m: Dict[str, Any]) -> Tuple[str, List[str]]:
    return norm_sym(m["stream"]), [str(m.get(k, "")) for k in
                                   ("highestBid", "highestBidSize", "lowestAsk", "lowestAskSize", "last")]


def parse_depth(m: Dict[str, Any], levels: int = LEVELS) -> List[str]:
    """depthchanged -> flat [b1_p, b1_q, ..., a10_q]; missing levels are blank."""
    out: List[str] = []
    for side in ("bids", "asks"):
        rows = (m["data"].get(side) or [])[:levels]
        for i in range(levels):
            out += [str(rows[i]["price"]), str(rows[i]["base_volume"])] if i < len(rows) else ["", ""]
    return out


def parse_tradeschanged(m: Dict[str, Any]) -> List[Tuple[int, str, str, str]]:
    """tradeschanged -> [(ts_s, price, amount, side)], newest first as sent."""
    return [(int(t[0]), str(t[1]), str(t[2]), str(t[3])) for t in (m["data"][0] or [])]


# ---------------------------------------------------------------- trade side matching and de-duplication

class SideMatcher:
    """The trade stream has a millisecond time and an id but no side; the order-book stream's trade list has the
    side but whole seconds (live check 2026-10-29: the same trades, one for one). Match on (second +-1, price, amount);
    wait up to SIDE_WAIT_MS, then write it with a blank side (trades_ob still has it)."""

    def __init__(self, wait_ms: int = SIDE_WAIT_MS, keep_s: int = 300):
        self.wait_ms, self.keep_s = wait_ms, keep_s
        self.sides: Dict[Tuple[int, float, float], set] = {}
        self.pending: List[List[Any]] = []
        self.newest = 0

    def add_book_trades(self, trades: Iterable[Tuple[int, str, str, str]]) -> None:
        for ts, p, a, side in trades:
            self.sides.setdefault((ts, float(p), float(a)), set()).add(side)
            self.newest = max(self.newest, ts)
        if len(self.sides) > 5000:
            self.sides = {k: v for k, v in self.sides.items() if k[0] >= self.newest - self.keep_s}

    def offer(self, row: List[Any]) -> None:
        self.pending.append(row)

    def _side(self, row: List[Any]) -> Optional[str]:
        sec, p, a = int(row[1]) // 1000, float(row[3]), float(row[4])
        for s in (sec, sec - 1, sec + 1):
            got = self.sides.get((s, p, a))
            if got:
                return next(iter(got)) if len(got) == 1 else ""  # both sides at once: ambiguous, leave blank
        return None

    def ready(self, now: int, force: bool = False) -> List[List[Any]]:
        out, keep = [], []
        for r in self.pending:
            s = self._side(r)
            if s is not None:
                r[5] = s
                out.append(r)
            elif force or now - r[0] >= self.wait_ms:
                out.append(r)
            else:
                keep.append(r)
        self.pending = keep
        return out


def dedup_snapshot(state: Dict[str, Any], keys: List[Tuple[int, str, str, str]], full: int = 30
                   ) -> Tuple[List[Tuple[int, str, str, str]], Optional[Tuple[int, int]], Dict[str, Any]]:
    """New trades in a 'last N trades' list (no ids), a possible gap (last_saved_ts, oldest_in_list), new state.
    Same counting rule as Test J's collector: identical trades in the last saved second are matched by count."""
    if not keys:
        return [], None, state
    last = state.get("last_ts")
    saved: Counter = state.get("last_keys", Counter())
    oldest, newest = min(k[0] for k in keys), max(k[0] for k in keys)
    gap = (last, oldest) if (last is not None and len(keys) >= full and oldest > last) else None
    if last is None:
        new = list(keys)
    else:
        new = [k for k in keys if k[0] > last]
        for k, n in Counter(k for k in keys if k[0] == last).items():
            new += [k] * max(0, n - saved.get(k, 0))
    if last is not None and newest < last:
        newest = last
    top = Counter(k for k in keys if k[0] == newest)
    if newest == last:
        top = Counter({k: max(top.get(k, 0), saved.get(k, 0)) for k in set(top) | set(saved)})
    new.sort(key=lambda k: k[0])
    return new, gap, {"last_ts": newest, "last_keys": top}


# ---------------------------------------------------------------- output files

class GzWriter:
    """One open gzip file per (kind, sym, day); a new UTC day closes the old file (rotation). flush() writes a
    sync point so a cut-off file is still readable up to the last flush. Thread-safe."""

    def __init__(self, root: Path):
        self.root = root
        self.files: Dict[Tuple[str, str], Tuple[str, Any]] = {}
        self.rows: Counter = Counter()
        self.lock = threading.Lock()

    def write(self, kind: str, sym: str, rows: List[List[Any]]) -> None:
        if not rows:
            return
        with self.lock:
            by_day: Dict[str, List[List[Any]]] = defaultdict(list)
            for r in rows:
                by_day[day_of(int(r[0]))].append(r)
            for day, rs in sorted(by_day.items()):
                cur = self.files.get((kind, sym))
                if cur is None or cur[0] != day:
                    if cur is not None:
                        cur[1].close()
                    path = self.root / day / f"{kind}_{sym}.csv.gz"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    new = not path.exists()
                    f = gzip.open(path, "ab")
                    if new:
                        f.write((",".join(HEADERS[kind]) + "\n").encode())
                    self.files[(kind, sym)] = cur = (day, f)
                buf = io.StringIO()
                csv.writer(buf, lineterminator="\n").writerows(rs)
                cur[1].write(buf.getvalue().encode())
                self.rows[(kind, sym)] += len(rs)

    def flush(self) -> None:
        with self.lock:
            for _, f in self.files.values():
                f.flush()

    def close(self) -> None:
        with self.lock:
            for _, f in self.files.values():
                f.close()
            self.files.clear()


def append_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new:
            w.writerow(header)
        w.writerows(rows)


# ---------------------------------------------------------------- recorder (state shared by connection threads)

class Recorder:
    def __init__(self, out: Path, symbols: List[str]):
        self.out, self.symbols = out, symbols
        self.w = GzWriter(out)
        self.lock = threading.Lock()
        self.match = {s: SideMatcher() for s in symbols}
        self.ob_state: Dict[str, Dict[str, Any]] = {s: {} for s in symbols}
        self.last_book: Dict[str, List[str]] = {}
        self.last_bbo: Dict[str, List[str]] = {}
        self.hour: Dict[str, Counter] = {s: Counter() for s in symbols}
        self.delays: Dict[str, List[int]] = {s: [] for s in symbols}
        self.gaps: List[List[Any]] = []
        self.messages = 0
        self.fill_due: Dict[str, int] = {}   # sym -> earliest time of the next REST side backfill
        self.fill_last: Dict[str, int] = {}

    def gap(self, conn: str, syms: str, start: Optional[int], end: int, reason: str) -> None:
        row = [conn, syms, "" if start is None else start, end, utc(start), utc(end), reason[:200]]
        with self.lock:
            self.gaps.append(row)
            append_csv(self.out / "gaps.csv", GAP_HEADER, [row])

    def on_market(self, text: str, recv: int) -> None:
        for line in split_lines(text):
            m = loads(line)
            st = str(m.get("stream", ""))
            if st.startswith("market.trade."):
                sym, row = parse_trade(m, recv)
                if sym in self.match:
                    with self.lock:
                        self.messages += 1
                        self.match[sym].offer(row)
                        self.delays[sym].append(recv - row[1])
            elif st.startswith("market.ticker."):
                sym, row = parse_ticker(m)
                with self.lock:
                    self.messages += 1
                    if sym not in self.hour or self.last_bbo.get(sym) == row:
                        continue
                    self.last_bbo[sym] = row
                    self.hour[sym]["bbo"] += 1
                self.w.write("bbo", sym, [[recv] + row])

    def on_book(self, sym: str, text: str, recv: int) -> None:
        for line in split_lines(text):
            if '"global.ticker"' in line or '"event":"ticker"' in line:  # all-market flood / duplicate of bbo
                continue
            m = loads(line)
            ev = m.get("event")
            if ev == "depthchanged":
                row = parse_depth(m)
                with self.lock:
                    self.messages += 1
                    if self.last_book.get(sym) == row:
                        continue
                    self.last_book[sym] = row
                    self.hour[sym]["book10"] += 1
                self.w.write("book10", sym, [[recv] + row])
            elif ev == "tradeschanged":
                trades = parse_tradeschanged(m)
                with self.lock:
                    self.messages += 1
                    self.match[sym].add_book_trades(trades)
                    new, gap, self.ob_state[sym] = dedup_snapshot(self.ob_state[sym], trades)
                    self.hour[sym]["trades_ob"] += len(new)
                if gap:
                    self.gap("trades_ob", sym, gap[0] * 1000, gap[1] * 1000, "trade list did not reach back")
                    with self.lock:
                        self.fill_due[sym] = max(recv, self.fill_last.get(sym, 0) + REST_FILL_EVERY_MS)
                self.w.write("trades_ob", sym, [[recv, *k] for k in new])

    def rest_fill(self, now: int, fetch: Callable[[str], Any] = http_json) -> None:
        """Busy bursts overflow the order-book stream's trade list, so some trades would get no side. Then pull the
        public REST trade list (it has the side) once and give it to the matcher."""
        with self.lock:
            due = [s for s, t in self.fill_due.items() if t <= now]
            for s in due:
                del self.fill_due[s]
                self.fill_last[s] = now
        for sym in due:
            try:
                res = fetch(f"{REST}/market/trades?sym={sym.lower()}&lmt=500")["result"]
                trades = [(int(t[0]) // 1000 if int(t[0]) > 10 ** 11 else int(t[0]), str(t[1]), str(t[2]), str(t[3]))
                          for t in res]
            except Exception:  # noqa: BLE001 - the trades are still saved; only their side may stay blank
                continue
            with self.lock:
                self.match[sym].add_book_trades(trades)
                self.hour[sym]["rest_fills"] += 1

    def release_trades(self, now: int, force: bool = False) -> None:
        for sym in self.symbols:
            with self.lock:
                rows = self.match[sym].ready(now, force)
                self.hour[sym]["trades"] += len(rows)
                self.hour[sym]["side_blank"] += sum(1 for r in rows if not r[5])
            self.w.write("trades", sym, rows)

    def heartbeat(self, now: int, conns_up: int, reconnects: int, offset: Optional[int]) -> List[List[Any]]:
        rows = []
        with self.lock:
            for sym in self.symbols:
                h, d = self.hour[sym], sorted(self.delays[sym])
                p50 = d[len(d) // 2] if d else ""
                p90 = d[int(len(d) * 0.9)] if d else ""
                rows.append([now, utc(now), sym, h["trades"], h["side_blank"], h["trades_ob"], h["book10"],
                             h["bbo"], h["rest_fills"], p50, p90, "", "", ""])
                self.hour[sym], self.delays[sym] = Counter(), []
        tot = [sum(int(r[i]) for r in rows) for i in range(3, 9)]
        rows.append([now, utc(now), "ALL", *tot, "", "", conns_up, reconnects, "" if offset is None else offset])
        append_csv(self.out / "heartbeat.csv", HB_HEADER, rows)
        return rows


def stream_loop(name: str, path: str, handler: Callable[[str, int], None], rec: Recorder, syms: str,
                stop: threading.Event, status: Dict[str, Any],
                connect: Callable[[str], Any] = WebSocket.connect, clock: Callable[[], int] = now_ms,
                max_backoff: float = 60) -> None:
    """Keep one connection alive until `stop`; every silence between two received messages caused by a drop or a
    failed connect is logged once, with its start (last message) and end (first message after reconnecting)."""
    last: Optional[int] = None
    down_reason: Optional[str] = None
    down_since: Optional[int] = None
    backoff = 1.0
    while not stop.is_set():
        ws = None
        try:
            ws = connect(path)
            status[name] = True
            while not stop.is_set():
                text = ws.recv(stop.is_set)
                if text is None:
                    break
                t = clock()
                if down_reason is not None:
                    rec.gap(name, syms, last if last is not None else down_since, t, down_reason)
                    down_reason = None
                    backoff = 1.0
                last = t
                try:
                    handler(text, t)
                except Exception as exc:  # noqa: BLE001 - one odd message must not drop the connection
                    status["parse_errors"] = status.get("parse_errors", 0) + 1
                    status["last_parse_error"] = repr(exc)[:200]
        except Exception as exc:  # noqa: BLE001 - logged as a gap, then reconnect
            if down_reason is None:
                down_reason, down_since = repr(exc)[:200], clock()
            status["reconnects"] = status.get("reconnects", 0) + 1
        finally:
            status[name] = False
            if ws is not None:
                ws.close()
        if not stop.is_set():
            stop.wait(backoff)
            backoff = min(backoff * 2, max_backoff)
    if down_reason is not None:
        rec.gap(name, syms, last if last is not None else down_since, clock(), down_reason + " (still down at stop)")


def conns_up(status: Dict[str, Any]) -> int:
    return sum(1 for k, v in status.items() if (k == "market" or k.startswith("ob_")) and v is True)


def clock_offset_ms(fetch: Callable[[str], Any] = http_json) -> Optional[int]:
    """Bitkub server time minus this machine's clock (ms), from one round trip; None if unreachable."""
    try:
        t0 = now_ms()
        server = int(fetch(f"{REST}/servertime"))
        return server - (t0 + now_ms()) // 2
    except Exception:  # noqa: BLE001
        return None


def run(out_root: Path, minutes: float, handoff_at: Optional[float], handoff_cmd: str,
        symbols: List[str] = SYMBOLS) -> int:
    start = now_ms()
    run_id = time.strftime("%Y%m%d-%H%M%S", time.gmtime(start / 1000))
    out = out_root / run_id
    out.mkdir(parents=True, exist_ok=True)
    pairs: Dict[str, int] = {}
    for attempt in range(3):
        try:
            pairs = {s["symbol"]: int(s["pairing_id"]) for s in http_json(f"{REST}/market/symbols")["result"]}
            break
        except Exception as exc:  # noqa: BLE001
            print(f"symbols attempt {attempt + 1}: {exc!r}", flush=True)
            time.sleep(10)
    if not pairs:
        print("Bitkub symbol list unreachable; stopping.", flush=True)
        return 1
    missing = [s for s in symbols if s not in pairs]
    if missing:
        print("Not listed on Bitkub (skipped):", missing, flush=True)
    symbols = [s for s in symbols if s in pairs]
    rec = Recorder(out, symbols)
    stop = threading.Event()
    status: Dict[str, Any] = {}
    market_path = WS_PATH + ",".join([stream_name("trade", s) for s in symbols] +
                                     [stream_name("ticker", s) for s in symbols])
    threads = [threading.Thread(target=stream_loop, daemon=True,
                                args=("market", market_path, rec.on_market, rec, "ALL", stop, status))]
    for s in symbols:
        threads.append(threading.Thread(target=stream_loop, daemon=True,
                                        args=(f"ob_{s}", f"{WS_PATH}orderbook/{pairs[s]}",
                                              lambda text, t, s=s: rec.on_book(s, text, t), rec, s, stop, status)))
    for t in threads:
        t.start()
    print(f"run {run_id}: {len(symbols)} symbols, {len(threads)} connections, {minutes:.0f} min", flush=True)
    end = start + int(minutes * 60000)
    next_flush = next_hb = start
    next_hb += HEARTBEAT_EVERY_S * 1000
    handoff = "not due" if handoff_at is not None else "off"
    code = 0
    while True:
        t = now_ms()
        rec.rest_fill(t)
        rec.release_trades(t)
        if t >= next_flush:
            rec.w.flush()
            next_flush = t + FLUSH_EVERY_S * 1000
        if t >= next_hb:
            up = conns_up(status)
            rows = rec.heartbeat(t, up, status.get("reconnects", 0), clock_offset_ms())
            print(f"heartbeat {utc(t)}: {rows[-1][3:8]} conns_up={up}", flush=True)
            next_hb = t + HEARTBEAT_EVERY_S * 1000
        if handoff == "not due" and t >= start + handoff_at * 60000:
            handoff = run_handoff(handoff_cmd, out_root)
            print("hand-over:", handoff, flush=True)
        if t - start > 120000 and rec.messages == 0:
            print("No market data in the first 2 minutes (Bitkub unreachable?); stopping.", flush=True)
            code = 1
            break
        if t >= end:
            break
        time.sleep(1)
    stop.set()
    for th in threads:
        th.join(timeout=15)
    t = now_ms()
    rec.release_trades(t, force=True)
    rec.heartbeat(t, conns_up(status), status.get("reconnects", 0), clock_offset_ms())
    rec.w.close()
    append_csv(out / "run.csv", ["run_id", "start_utc", "end_utc", "messages", "reconnects", "gaps", "parse_errors",
                                 "last_parse_error", "handoff", "exit"],
               [[run_id, utc(start), utc(t), rec.messages, status.get("reconnects", 0), len(rec.gaps),
                 status.get("parse_errors", 0), status.get("last_parse_error", ""), handoff, code]])
    print(f"run {run_id} done: {rec.messages} messages, {status.get('reconnects', 0)} reconnects, "
          f"{len(rec.gaps)} gaps, {status.get('parse_errors', 0)} parse errors, hand-over: {handoff}", flush=True)
    return code


def run_handoff(cmd: str, out_root: Path) -> str:
    """Start the next run (workflow_dispatch) so the two runs overlap; a marker file tells the workflow it was done."""
    if not cmd:
        return "no command"
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        if r.returncode == 0:
            (out_root / "HANDOFF_DONE").write_text(utc(now_ms()), encoding="utf-8")
            said = (r.stdout or "").strip().splitlines()
            return said[-1][:150] if said else "started next run"   # handoff.sh says started / skipped
        return f"failed ({r.returncode}): {(r.stderr or r.stdout).strip()[:150]}"
    except Exception as exc:  # noqa: BLE001
        return f"failed: {exc!r}"[:160]


# ---------------------------------------------------------------- owner's PC: download, decrypt, merge

def download(repo: str, out: Path, fetch: Callable[[str], Any] = http_json) -> int:
    out.mkdir(parents=True, exist_ok=True)
    n = 0
    for rel in fetch(f"https://api.github.com/repos/{repo}/releases?per_page=100"):
        for a in rel.get("assets", []):
            dest = out / a["name"]
            if a["name"].startswith("stream_") and a["name"].endswith(".tar.enc") and not dest.exists():  # not bnth_
                urllib.request.urlretrieve(a["browser_download_url"], dest)
                n += 1
                print("downloaded", a["name"], flush=True)
    return n


def openssl_decrypt(blob: bytes, passphrase: str, iterations: int = KDF_ITER) -> bytes:
    """Reverse of `openssl enc -aes-256-cbc -salt -pbkdf2 -iter N -md sha256`. Needs the `cryptography` package."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes  # owner's PC only
    if blob[:8] != b"Salted__":
        raise ValueError("not an openssl-encrypted file")
    salt, body = blob[8:16], blob[16:]
    kiv = hashlib.pbkdf2_hmac("sha256", passphrase.encode(), salt, iterations, 48)
    d = Cipher(algorithms.AES(kiv[:32]), modes.CBC(kiv[32:])).decryptor()
    plain = d.update(body) + d.finalize()
    pad = plain[-1]
    if not 1 <= pad <= 16 or plain[-pad:] != bytes([pad]) * pad:
        raise ValueError("wrong key (bad padding)")
    return plain[:-pad]


def decrypt_dir(src: Path, dest: Path, passphrase: str) -> int:
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in sorted(src.glob("stream_*.tar.enc")):   # the Binance TH recorder's files are bnth_*.tar.enc
        run_id = f.name[len("stream_"):-len(".tar.enc")]
        if (dest / run_id).exists():
            continue
        with tarfile.open(fileobj=io.BytesIO(openssl_decrypt(f.read_bytes(), passphrase))) as tar:
            if hasattr(tarfile, "data_filter"):
                tar.extractall(dest, filter="data")
            else:  # pragma: no cover - old Python
                tar.extractall(dest)
        n += 1
        print("decrypted", f.name, flush=True)
    return n


def inflate_all(raw: bytes) -> bytes:
    """Decompress every gzip member; on damage keep everything before the first bad byte."""
    out: List[bytes] = []
    while raw:
        d, i, step = zlib.decompressobj(31), 0, 1 << 16
        while i < len(raw) and not d.eof:
            saved = d.copy()
            try:
                out.append(d.decompress(raw[i:i + step]))
                i += step
            except zlib.error:
                if step == 1:
                    return b"".join(out)
                d, step = saved, 1  # replay this chunk byte by byte up to the damage
        if not d.eof:
            break
        raw = d.unused_data + raw[i:]
    return b"".join(out)


def read_gz_csv(path: Path) -> List[List[str]]:
    """Rows without the header. Reads every gzip member and keeps what is readable of a file cut off mid-write."""
    data = inflate_all(path.read_bytes())
    lines = data.decode("utf-8", errors="replace").split("\n")
    if not data.endswith(b"\n"):
        lines = lines[:-1]  # incomplete last line of a killed run
    return [r for r in csv.reader([x for x in lines if x]) if r and r[0] != "recv_ms"]


def merge_rows(kind: str, runs: List[List[List[str]]]) -> List[List[str]]:
    """Join the same file from overlapping runs (runs in start order) without duplicates.
    trades: one row per txn id (earliest receive, a filled side wins). trades_ob: per identical trade key the largest
    count seen by any one run. book10/bbo (no exchange id): run k's rows until run k+1's first row, then run k+1."""
    runs = [r for r in runs if r]
    if kind == "trades":
        best: Dict[str, List[str]] = {}
        for rows in runs:
            for r in rows:
                b = best.get(r[2])
                if b is None:
                    best[r[2]] = list(r)
                else:
                    if int(r[0]) < int(b[0]):
                        b[0] = r[0]
                    if not b[5] and r[5]:
                        b[5] = r[5]
        return sorted(best.values(), key=lambda r: (int(r[1]), r[2]))
    if kind == "trades_ob":
        need: Counter = Counter()
        first: Dict[Tuple[str, ...], int] = {}
        for rows in runs:
            c = Counter(tuple(r[1:]) for r in rows)
            for k, n in c.items():
                need[k] = max(need[k], n)
            for r in rows:
                first[tuple(r[1:])] = min(first.get(tuple(r[1:]), int(r[0])), int(r[0]))
        out = [[str(first[k]), *k] for k, n in need.items() for _ in range(n)]
        return sorted(out, key=lambda r: (int(r[1]), int(r[0])))
    runs = sorted(runs, key=lambda rows: int(rows[0][0]))
    out: List[List[str]] = []
    for i, rows in enumerate(runs):
        cut = int(runs[i + 1][0][0]) if i + 1 < len(runs) else None
        out += [r for r in rows if cut is None or int(r[0]) < cut]
    return out


def note_spans(spans: Dict[str, List[int]], paths: List[Path], runs: List[List[List[str]]]) -> None:
    """First and last receive time seen in each run folder (for run_holes)."""
    for p, rows in zip(paths, runs):
        if rows:
            t = [int(r[0]) for r in rows]
            s = spans.setdefault(p.parent.parent.name, [min(t), max(t)])
            s[0], s[1] = min(s[0], min(t)), max(s[1], max(t))


def run_holes(spans: Dict[str, List[int]], min_ms: int = 60000) -> List[Tuple[int, int]]:
    """Periods covered by no run (e.g. a failed hand-over, the timer restarting the chain up to an hour later):
    (last row of the runs so far, first row of the next run), when longer than min_ms."""
    out: List[Tuple[int, int]] = []
    covered: Optional[int] = None
    for s, e in sorted(spans.values()):
        if covered is not None and s - covered > min_ms:
            out.append((covered, s))
        covered = e if covered is None else max(covered, e)
    return out


def merge_dir(runs_dir: Path, dest: Path, hole_conn: str = "market") -> int:
    """RUNS_DIR/<run_id>/<day>/<kind>_<SYM>.csv.gz -> DEST/<day>/<kind>_<SYM>.csv.gz, plus all runs' gaps/heartbeat.
    Periods when no run was recording are added to gaps.csv as `hole_conn` gaps (run_id "merge")."""
    groups: Dict[Tuple[str, str], List[Path]] = defaultdict(list)
    for p in sorted(runs_dir.glob("*/*/*.csv.gz")):
        groups[(p.parent.name, p.name)].append(p)  # run folders sort by start time
    spans: Dict[str, List[int]] = {}
    for (day, name), paths in sorted(groups.items()):
        kind = next(k for k in sorted(HEADERS, key=len, reverse=True) if name.startswith(k + "_"))
        runs = [read_gz_csv(p) for p in paths]
        note_spans(spans, paths, runs)
        rows = merge_rows(kind, runs)
        target = dest / day / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(target, "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(HEADERS[kind])
            w.writerows(rows)
    merge_logs(runs_dir, dest, HB_HEADER, run_holes(spans), hole_conn)
    return len(groups)


def merge_logs(runs_dir: Path, dest: Path, hb_header: List[str], holes: List[Tuple[int, int]], hole_conn: str) -> None:
    for log, header in (("gaps.csv", ["run_id"] + GAP_HEADER), ("heartbeat.csv", ["run_id"] + hb_header)):
        rows = []
        for p in sorted(runs_dir.glob(f"*/{log}")):
            with open(p, newline="", encoding="utf-8") as f:
                rows += [[p.parent.name] + r for r in list(csv.reader(f))[1:]]
        if log == "gaps.csv":
            rows += [["merge", hole_conn, "ALL", s, e, utc(s), utc(e), "no run recording (hand-over gap)"]
                     for s, e in holes]
        if rows:
            (dest / log).unlink(missing_ok=True)
            append_csv(dest / log, header, rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="record (GitHub Actions)")
    r.add_argument("--minutes", type=float, default=330.0)
    r.add_argument("--handoff-at", type=float, default=None, help="minute at which to start the next run")
    r.add_argument("--handoff-cmd", default="")
    r.add_argument("--out", default="out")
    d = sub.add_parser("decrypt", help="decrypt downloaded .tar.enc files (owner's PC)")
    d.add_argument("src")
    d.add_argument("dest")
    m = sub.add_parser("merge", help="join decrypted runs into one file per day/kind/coin")
    m.add_argument("runs_dir")
    m.add_argument("dest")
    g = sub.add_parser("download", help="download the encrypted release files of a public repo")
    g.add_argument("--repo", required=True)
    g.add_argument("--out", default="downloads")
    a = ap.parse_args()
    if a.cmd == "run":
        sys.exit(run(Path(a.out), a.minutes, a.handoff_at, a.handoff_cmd))
    if a.cmd == "decrypt":
        key = os.environ.get("STREAM_DATA_KEY") or getpass.getpass("Data key (STREAM_DATA_KEY): ")
        print(decrypt_dir(Path(a.src), Path(a.dest), key), "files decrypted")
    elif a.cmd == "merge":
        print(merge_dir(Path(a.runs_dir), Path(a.dest)), "files merged")
    elif a.cmd == "download":
        print(download(a.repo, Path(a.out)), "new files")


if __name__ == "__main__":
    main()
