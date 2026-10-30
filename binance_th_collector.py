#!/usr/bin/env python3
"""
binance_th_collector.py — Binance TH THB-pair recorder (fast stream, for the Test Q market-making study).

Paper-trading research, not financial advice. Reads PUBLIC Binance TH market data only (WebSocket streams + public REST
snapshots). No orders, no accounts, no exchange API keys. Standard library only; the generic parts (WebSocket frames,
reconnect loop, encryption, gzip repair) come from bitkub_stream_collector.py in the same folder.

Frozen Test Q coins (select_bnth_coins.py -> reports/test_q_coins.csv, 2026-10-31) plus USDTTHB as the THB/USD
reference. The USDT pairs of the same coins are recorded as the world-price reference (Binance TH's USDT books mirror
Binance's global book; knowledge/venue-audit-2026-10-30.md). Connections (checked live 2026-10-31):
  wss://nbstream.binance.th/w3w/wsa/stream + SUBSCRIBE <sym>@depth@100ms, <sym>@trade   ("thb" connection)
  wss://nbstream.binance.th/w3w/wsa/stream + SUBSCRIBE <sym>@bookTicker                 ("ref" connection)
  https://api.binance.th/api/v1/depth?limit=1000 (book snapshot), /trades?limit=1000 (missed trades), /time

  python binance_th_collector.py run --minutes 330 --handoff-at 320 --handoff-cmd "gh workflow run binance_th.yml"
  python binance_th_collector.py download --repo OWNER/REPO --out DOWNLOADS_DIR   # only bnth_*.tar.enc files
  python binance_th_collector.py decrypt DOWNLOADS_DIR RUNS_DIR   # owner's PC (key: STREAM_DATA_KEY or prompt)
  python binance_th_collector.py merge RUNS_DIR MERGED_DIR         # join runs, remove hand-over duplicates

Output of one run (UTC days, by receive time), in OUT/<run_id>/:
  <day>/trades_<SYM>.csv.gz  recv_ms,ts_ms,id,price,amount,side,src   side = TAKER side (buy/sell, from isBuyerMaker);
                                                                      src = ws, or rest for a trade the stream missed
  <day>/book10_<SYM>.csv.gz  recv_ms,b1_p,b1_q..b10_q,a1_p..a10_q,ex_ms  top 10 levels of the rebuilt book, written when
                                                                      they change; ex_ms = exchange event time
  <day>/ref_<SYM>.csv.gz     recv_ms,bid,bid_qty,ask,ask_qty  USDT-pair best quotes: on a price change, at most once per
                                                              REF_EVERY_MS (the latest quote is kept and written later)
  gaps.csv        conn thb/ref = connection drop; book_<SYM> = book unknown or possibly wrong (reconnect, failed
                  snapshot check: from the last good check) until resynced; trades_<SYM> = trade ids the REST list
                  could not recover
  heartbeat.csv   hourly: per symbol row counts, id jumps, snapshot checks (good/bad), exchange->receive delay of
                  book events; ALL row with clock offset
  run.csv         one row: start/end, messages, reconnects, gaps, hand-over result
recv_ms = this machine's clock when the message arrived (for a rest trade: when it was fetched).
"""

from __future__ import annotations

import argparse
import base64
import csv
import getpass
import gzip
import heapq
import io
import json
import os
import socket
import ssl
import struct
import sys
import tarfile
import threading
import time
import urllib.request
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import bitkub_stream_collector as bk
from bitkub_stream_collector import append_csv, day_of, http_json, now_ms, utc

# Frozen Test Q list: select_bnth_coins.py run 2026-10-31 (THB pairs, TRADING, not pegged, median daily THB value
# 23-29 Oct 2026 >= 200k). USDTTHB is recorded as the THB/USD reference, not traded in the study.
COINS = ["BTCTHB", "ETHTHB", "XRPTHB", "VELOTHB", "SOLTHB", "BNBTHB"]
SYMBOLS = COINS + ["USDTTHB"]
REFS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "BNBUSDT"]   # VELOUSDT is not listed on Binance TH
REST = "https://api.binance.th/api/v1"
WS_HOST = "nbstream.binance.th"
WS_PATH = "/w3w/wsa/stream"
LEVELS = 10
SNAPSHOT_LIMIT = 100        # levels per side in a snapshot (Binance weight 5; the top 10 is what is written)
REF_EVERY_MS = 500          # reference quotes change ~150 times/s on BTCUSDT; 2 rows/s is plenty for a world price
BUF_MAX = 2000              # depth diffs kept per coin while waiting for a snapshot
RETRY_MS = 2000             # wait before another snapshot / REST trade call for the same coin
RETRY_MAX_MS = 60000        # snapshot back-off cap after repeated failures
OLD_MAX = 100               # diffs in a row older than the book (≈ 10 s) = book frozen, resync
HIST_MAX = 3000             # book states kept per coin for checks (≈ 5 min of BTC diffs)
VERIFY_MS = 60000           # check each book against a REST snapshot every minute ...
VERIFY_SOON_MS = 3000       # ... and this soon after a sync or an id jump (but at most once per VERIFY_MIN_MS)
VERIFY_MIN_MS = 10000
VERIFY_WAIT_MS = 5000       # a check snapshot ahead of the stream waits this long for it, then is compared anyway
TRADE_CHECK_MS = 600000     # REST trade-list check per coin every 10 min, even without a visible skip
FLUSH_EVERY_S = 30
HEARTBEAT_EVERY_S = 3600

HEADERS = {
    "trades": ["recv_ms", "ts_ms", "id", "price", "amount", "side", "src"],
    "book10": ["recv_ms"] + [f"{s}{i}_{x}" for s in "ba" for i in range(1, LEVELS + 1) for x in "pq"] + ["ex_ms"],
    "ref": ["recv_ms", "bid", "bid_qty", "ask", "ask_qty"],
}
GAP_HEADER = bk.GAP_HEADER
HB_HEADER = ["recv_ms", "utc", "sym", "trades", "rest_trades", "book10", "depth", "id_jumps", "resyncs", "checks",
             "bad_checks", "check_skips", "ref", "delay_p50_ms", "delay_p90_ms", "conns_up", "reconnects",
             "clock_offset_ms"]


def trim(x: Any) -> str:
    """'41.39000000' -> '41.39', '100.0' -> '100': exact decimal text, shorter files."""
    s = str(x)
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


# ---------------------------------------------------------------- WebSocket with Binance's SUBSCRIBE message

class BnWebSocket(bk.WebSocket):
    @classmethod
    def open(cls, streams: List[str], timeout: float = 20) -> "BnWebSocket":
        raw = socket.create_connection((WS_HOST, 443), timeout=timeout)
        sock = ssl.create_default_context().wrap_socket(raw, server_hostname=WS_HOST)
        key = base64.b64encode(os.urandom(16)).decode()
        sock.sendall((f"GET {WS_PATH} HTTP/1.1\r\nHost: {WS_HOST}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
                      "User-Agent: quant-brain-research (public data, paper study)\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = sock.recv(4096)
            if not d:
                raise bk.ConnectionClosed("closed during handshake")
            buf += d
        head, buf = buf.split(b"\r\n\r\n", 1)
        if b" 101 " not in head.split(b"\r\n")[0]:
            sock.close()
            raise bk.ConnectionClosed("handshake refused: " + head.split(b"\r\n")[0].decode(errors="replace"))
        sock.settimeout(5)
        ws = cls(sock, buf)
        ws.send_text(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
        return ws

    def send_text(self, text: str) -> None:
        """Masked text frame of any length (the base class only sends frames under 126 bytes)."""
        p, m = text.encode(), os.urandom(4)
        n = len(p)
        if n < 126:
            head = bytes([0x81, 0x80 | n])
        elif n < 65536:
            head = bytes([0x81, 0x80 | 126]) + struct.pack(">H", n)
        else:
            head = bytes([0x81, 0x80 | 127]) + struct.pack(">Q", n)
        self.sock.sendall(head + m + bytes(x ^ m[k % 4] for k, x in enumerate(p)))


# ---------------------------------------------------------------- local order book (pure, no network)

class Book:
    """One coin's book rebuilt from a REST snapshot plus depth diffs (each diff carries the new absolute quantity of
    every level it touches). Binance's documented rule "each diff's U = previous u + 1" does NOT hold on Binance TH for
    every coin: live 2026-10-31, VELOTHB skipped ids on the stream (868 -> 870) and the snapshot id jumped ~180 with no
    diff in between, while BTC/ETH/XRP/SOL/BNB/USDT followed it. So every newer diff is applied and the book is
    CHECKED against REST snapshots (`check`, Recorder.verify). Trust rule: the book is trusted while ids follow on
    (Binance's own guarantee) and from each passed check; from an id jump (jump_at) until a passed check it MAY be
    wrong and that stretch is logged as a gap; a failed check is logged from jump_at, or from the last good check.
    last_u None = not synced: diffs are buffered until the next snapshot. Before a run's first sync there are no
    rows (no rows = unknown; the previous run covers that time)."""

    def __init__(self) -> None:
        self.bids: Dict[float, Tuple[str, str]] = {}
        self.asks: Dict[float, Tuple[str, str]] = {}
        self.last_u: Optional[int] = None
        self.buf: List[Dict[str, Any]] = []
        self.hist: deque = deque(maxlen=HIST_MAX)   # (U, u, top 10, recv) after each applied diff, for `check`
        self.last_recv: Optional[int] = None   # receive time of the last update applied
        self.lost_at: Optional[int] = None     # start of the current "book unknown" gap (None = start of the run)
        self.ok_at: Optional[int] = None       # receive time of the last book state proven right (sync or check)
        self.jump_at: Optional[int] = None     # receive time of the first id jump not yet covered by a passed check
        self.next_try = 0
        self.fails = 0                         # snapshot tries in a row that failed (sets the back-off)
        self.olds = 0                          # diffs in a row older than the book
        self.verify_due = 0
        self.checked_at = 0
        self.pending: Optional[Tuple[Dict[str, Any], int]] = None   # check snapshot waiting for the stream

    def _apply(self, ev: Dict[str, Any]) -> None:
        for side, rows in ((self.bids, ev["b"]), (self.asks, ev["a"])):
            for p, q in rows:
                if float(q) == 0:
                    side.pop(float(p), None)
                else:
                    side[float(p)] = (trim(p), trim(q))

    def suspect_since(self) -> Optional[int]:
        """Start of the stretch that may be wrong if the book turns out wrong now: the first unchecked id jump, or
        (no jump: ids followed on) the last good check."""
        return self.jump_at if self.jump_at is not None else self.ok_at

    def lose(self, start: Optional[int] = None) -> None:
        """Book unknown from `start` (default: the first unchecked id jump, else the last applied update)."""
        if self.last_u is not None:
            self.lost_at = start if start is not None else (self.jump_at if self.jump_at is not None
                                                            else self.last_recv)
        self.last_u, self.buf, self.pending, self.jump_at = None, [], None, None
        self.hist.clear()

    def on_diff(self, ev: Dict[str, Any]) -> str:
        """'applied', 'jump' (applied, but ids were skipped), 'old' (already in the book), 'buffered' (waiting for a
        snapshot) or 'lost' (book frozen: diffs keep looking older than it)."""
        if self.last_u is None:
            self.buf.append(ev)
            if len(self.buf) > BUF_MAX:
                del self.buf[0]
            return "buffered"
        if ev["u"] <= self.last_u:
            self.olds += 1
            if self.olds <= OLD_MAX:
                return "old"
            self.lose()      # a snapshot id ahead of the stream's ids would freeze the book silently
            self.buf = [ev]
            return "lost"
        jump = ev["U"] > self.last_u + 1
        recv = ev.get("recv", 0)
        if jump and self.jump_at is None:
            self.jump_at = recv
        self._apply(ev)
        self.last_u, self.olds = ev["u"], 0
        self.hist.append((ev["U"], self.last_u, self.top(), recv))
        return "jump" if jump else "applied"

    def on_snapshot(self, snap: Dict[str, Any], t: int = 0) -> None:
        """Book = snapshot (received at t) + the buffered diffs newer than it."""
        lid = int(snap["lastUpdateId"])
        rest = [e for e in self.buf if e["u"] > lid]
        self.bids = {float(p): (trim(p), trim(q)) for p, q in snap["bids"] if float(q) != 0}
        self.asks = {float(p): (trim(p), trim(q)) for p, q in snap["asks"] if float(q) != 0}
        self.last_u, self.buf, self.olds, self.pending, self.jump_at = lid, [], 0, None, None
        self.hist.clear()
        self.hist.append((lid, lid, self.top(), t))
        for e in rest:
            self.on_diff(dict(e, recv=t))   # rows start at t: an earlier jump counts from t
        self.ok_at = t

    def check(self, snap: Dict[str, Any], force: bool = False) -> Optional[Tuple[bool, int]]:
        """(top 10 equal?, receive time of the compared state): the snapshot against the book as it was at the
        snapshot's id. None = cannot tell: the stream has not reached that id yet (with force, ids up to it are taken
        as silently skipped), the id falls inside one diff's id range U..u (state never seen), or it is older than
        the kept history."""
        lid = int(snap["lastUpdateId"])
        if self.last_u is None or (lid > self.last_u and not force):
            return None
        h = list(self.hist)
        i = next((k for k in range(len(h) - 1, -1, -1) if h[k][1] <= lid), None)
        if i is None or (i + 1 < len(h) and h[i + 1][0] <= lid):
            return None
        ref = Book()
        ref.bids = {float(p): (trim(p), trim(q)) for p, q in snap["bids"] if float(q) != 0}
        ref.asks = {float(p): (trim(p), trim(q)) for p, q in snap["asks"] if float(q) != 0}
        return ref.top() == h[i][2], h[i][3]

    def top(self, n: int = LEVELS) -> List[str]:
        out: List[str] = []
        for side, best in ((self.bids, heapq.nlargest), (self.asks, heapq.nsmallest)):
            keys = best(n, side)
            for i in range(n):
                out += list(side[keys[i]]) if i < len(keys) else ["", ""]
        return out


# ---------------------------------------------------------------- output files

class GzWriter(bk.GzWriter):
    """bitkub_stream_collector.GzWriter with this file's headers (one gzip file per kind, symbol and UTC day)."""

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


# ---------------------------------------------------------------- recorder (state shared by connection threads)

class Recorder:
    def __init__(self, out: Path, symbols: List[str], refs: List[str]):
        self.out, self.symbols, self.refs = out, symbols, refs
        self.w = GzWriter(out)
        self.lock = threading.RLock()
        self.books = {s: Book() for s in symbols}
        self.last_row: Dict[str, List[str]] = {}
        self.first_id: Dict[str, Optional[int]] = {s: None for s in symbols}  # last trade id before the run started
        self.done: Dict[str, Set[int]] = {s: set() for s in symbols}
        self.lost_ids: Dict[str, Set[int]] = {s: set() for s in symbols}   # already logged as unrecoverable
        self.fill_due: Dict[str, int] = {}
        self.fill_last: Dict[str, int] = {}
        self.ref_last: Dict[str, Tuple[int, Tuple[str, ...]]] = {}
        self.ref_pending: Dict[str, Tuple[int, Tuple[str, ...]]] = {}
        self.hour: Dict[str, Counter] = {s: Counter() for s in symbols + refs}
        self.delays: Dict[str, List[int]] = {s: [] for s in symbols}
        self.gaps: List[List[Any]] = []
        self.messages = 0
        self.clock: Callable[[], int] = now_ms   # receive time of REST answers (tests replace it)

    def gap(self, conn: str, syms: str, start: Optional[int], end: int, reason: str) -> None:
        row = [conn, syms, "" if start is None else start, end, utc(start), utc(end), reason[:200]]
        with self.lock:
            self.gaps.append(row)
            append_csv(self.out / "gaps.csv", GAP_HEADER, [row])

    def on_connect(self, conn: str, now: int) -> None:
        """A (re)connected THB stream starts with unknown books and possibly missed trades."""
        if conn != "thb":
            return
        with self.lock:
            for s, b in self.books.items():
                b.lose()
                b.next_try = now + 1000   # let the stream buffer diffs before the snapshot
                self.fill_due[s] = now + 3000

    def on_message(self, text: str, recv: int) -> None:
        m = json.loads(text)
        st, d = str(m.get("stream", "")), m.get("data")
        if d is None:
            return  # subscribe reply
        if st.endswith("@depth@100ms"):
            self.on_depth(d, recv)
        elif st.endswith("@trade"):
            self.on_trade(d, recv)
        elif st.endswith("@bookTicker"):
            self.on_ref(d, recv)

    def on_depth(self, d: Dict[str, Any], recv: int) -> None:
        sym, ex = d["s"], int(d["E"])
        ev = {"U": int(d["U"]), "u": int(d["u"]), "b": d.get("b") or [], "a": d.get("a") or [], "recv": recv}
        with self.lock:
            if sym not in self.books:
                return
            self.messages += 1
            self.hour[sym]["depth"] += 1
            self.delays[sym].append(recv - ex)
            b = self.books[sym]
            res = b.on_diff(ev)
            if res == "lost":
                self.hour[sym]["resyncs"] += 1
                b.next_try = recv
            if res == "jump":
                self.hour[sym]["id_jumps"] += 1
                b.verify_due = min(b.verify_due, max(recv + VERIFY_SOON_MS, b.checked_at + VERIFY_MIN_MS))
            if res not in ("applied", "jump"):
                return
            b.last_recv = recv
            row = b.top()
            if self.last_row.get(sym) == row:
                return
            self.last_row[sym] = row
            self.hour[sym]["book10"] += 1
            self.w.write("book10", sym, [[recv] + row + [ex]])   # under the lock: rows stay in book order

    def resync(self, now: int, up: bool, fetch: Callable[[str], Any] = http_json) -> None:
        """Snapshot every unsynced book whose stream is up; a coin that keeps failing backs off up to RETRY_MAX_MS."""
        if not up:
            return
        with self.lock:
            due = [s for s, b in self.books.items() if b.last_u is None and b.next_try <= now]
            for s in due:
                b = self.books[s]
                b.next_try = now + min(RETRY_MS << min(b.fails, 5), RETRY_MAX_MS)
                b.fails += 1
        for sym in due:
            try:
                snap = fetch(f"{REST}/depth?symbol={sym}&limit={SNAPSHOT_LIMIT}")
            except Exception:  # noqa: BLE001 - tried again after the back-off
                continue
            with self.lock:
                t = self.clock()
                b = self.books[sym]
                if b.last_u is not None:
                    continue
                b.on_snapshot(snap, t)
                lost, b.lost_at, b.last_recv, b.fails = b.lost_at, None, t, 0
                b.verify_due = max(t + VERIFY_SOON_MS, b.checked_at + VERIFY_MIN_MS)   # check the new book soon
                row = b.top()
                self.last_row[sym] = row
                self.hour[sym]["book10"] += 1
                self.w.write("book10", sym, [[t] + row + [""]])
                if lost is not None:
                    self.gap(f"book_{sym}", sym, lost, t, "book resynced (reconnect, failed check or frozen ids)")

    def verify(self, now: int, up: bool, fetch: Callable[[str], Any] = http_json) -> None:
        """Compare each synced book with a REST snapshot (every VERIFY_MS, sooner after a sync or an id jump).
        Pass after an id jump: the jump -> check stretch is logged as a gap (the book may have been wrong and fixed
        itself). Fail: logged from the jump (or the last good check if ids followed on) and the book is resynced.
        Cannot tell (None): tried again after RETRY_MS."""
        if not up:
            return
        with self.lock:
            due = [s for s, b in self.books.items()
                   if b.last_u is not None and b.pending is None and b.verify_due <= now]
            for s in due:
                b = self.books[s]
                b.checked_at, b.verify_due = now, now + VERIFY_MS
        for sym in due:
            try:
                snap = fetch(f"{REST}/depth?symbol={sym}&limit={SNAPSHOT_LIMIT}")
            except Exception:  # noqa: BLE001
                with self.lock:
                    self.books[sym].verify_due = now + RETRY_MS
                continue
            with self.lock:
                b = self.books[sym]
                if b.last_u is not None:
                    b.pending = (snap, self.clock())
        with self.lock:
            for sym, b in self.books.items():
                if b.pending is None:
                    continue
                snap, t = b.pending
                res = b.check(snap, force=now - t >= VERIFY_WAIT_MS)
                if res is None and now - t < VERIFY_WAIT_MS:
                    continue            # the stream has not reached the snapshot's id yet
                b.pending = None
                if res is None:
                    self.hour[sym]["check_skips"] += 1
                    b.verify_due = now + RETRY_MS   # id inside a diff's range, or too old: try again soon
                    continue
                self.hour[sym]["checks"] += 1
                match, state_t = res
                if not match:
                    self.hour[sym]["bad_checks"] += 1
                    b.lose(b.suspect_since())
                    b.next_try = now
                    continue
                b.ok_at = max(b.ok_at or 0, state_t)
                if b.jump_at is not None and state_t >= b.jump_at:
                    self.gap(f"book_{sym}", sym, b.jump_at, state_t, "id jump, unchecked until a passed check")
                    b.jump_at = None
                elif b.jump_at is not None:
                    b.verify_due = now + RETRY_MS   # the compared state was before the jump: check again

    def close_gaps(self, now: int, run_start: int) -> None:
        """At stop: a book that is still unknown is logged too (from its loss, or from the run start if it never
        synced), and so is a book with an id jump no check has covered yet."""
        with self.lock:
            for sym, b in self.books.items():
                if b.last_u is None:
                    self.gap(f"book_{sym}", sym, b.lost_at if b.lost_at is not None else run_start, now,
                             "book still unknown at stop")
                elif b.jump_at is not None:
                    self.gap(f"book_{sym}", sym, b.jump_at, now, "id jump still unchecked at stop")

    def on_trade(self, d: Dict[str, Any], recv: int) -> None:
        sym, tid = d["s"], int(d["t"])
        row = [recv, int(d["T"]), tid, trim(d["p"]), trim(d["q"]), "sell" if d["m"] else "buy", "ws"]
        with self.lock:
            if sym not in self.done:
                return
            self.messages += 1
            done = self.done[sym]
            if tid in done:
                return
            known = list(done) + ([] if self.first_id[sym] is None else [self.first_id[sym]])
            if known and tid > max(known) + 1:
                self.fill_due.setdefault(sym, recv + RETRY_MS)  # ids skipped: fetch them from the REST list
            done.add(tid)
            self.hour[sym]["trades"] += 1
            self.w.write("trades", sym, [row])

    def fill_trades(self, now: int, fetch: Callable[[str], Any] = http_json) -> None:
        """REST trade list (last 1000, with ids and sides) for coins that may have missed trades. The first call of a
        run only notes the newest id (older trades belong to the previous run). Ids that cannot be recovered are
        logged as a trades_<SYM> gap."""
        with self.lock:
            for s in self.symbols:   # routine check: a feed that stops sending trades would show no id skip
                if s in self.fill_last and now - self.fill_last[s] >= TRADE_CHECK_MS:
                    self.fill_due.setdefault(s, now)
            due = [s for s, t in self.fill_due.items() if t <= now and now - self.fill_last.get(s, 0) >= RETRY_MS]
            for s in due:
                del self.fill_due[s]
                self.fill_last[s] = now
        for sym in due:
            try:
                res = fetch(f"{REST}/trades?symbol={sym}&limit=1000")
            except Exception:  # noqa: BLE001
                with self.lock:
                    self.fill_due.setdefault(sym, now + RETRY_MS)
                continue
            with self.lock:
                t = self.clock()
                rows: List[List[Any]] = []
                done = self.done[sym]
                if self.first_id[sym] is None:
                    ids = [int(x["id"]) for x in res]
                    newest = max(ids) if ids else 0
                    self.first_id[sym] = min([newest] + [i - 1 for i in done])
                first = self.first_id[sym]
                for x in sorted(res, key=lambda x: int(x["id"])):
                    i = int(x["id"])
                    if i > first and i not in done:
                        done.add(i)
                        rows.append([t, int(x["time"]), i, trim(x["price"]), trim(x["qty"]),
                                     "sell" if x["isBuyerMaker"] else "buy", "rest"])
                self.hour[sym]["rest_trades"] += len(rows)
                self.w.write("trades", sym, rows)
                lost = self.lost_ids[sym]
                new = [i for i in range(first + 1, max(done)) if i not in done and i not in lost] if done else []
                if new:
                    lost.update(new)
                    self.gap(f"trades_{sym}", sym, None, t,
                             f"{len(new)} trade ids not in the REST list (ids {new[0]}-{new[-1]})")

    def on_ref(self, d: Dict[str, Any], recv: int) -> None:
        sym = d["s"]
        q = (trim(d["b"]), trim(d["B"]), trim(d["a"]), trim(d["A"]))
        with self.lock:
            if sym not in self.hour:
                return
            self.messages += 1
            last = self.ref_last.get(sym)
            if last is not None and (last[1][0], last[1][2]) == (q[0], q[2]):
                self.ref_pending.pop(sym, None)  # back to the written prices: nothing new to write
                return
            if last is not None and recv - last[0] < REF_EVERY_MS:
                self.ref_pending[sym] = (recv, q)
                return
            self.ref_pending.pop(sym, None)
            self.ref_last[sym] = (recv, q)
            self.hour[sym]["ref"] += 1
            self.w.write("ref", sym, [[recv, *q]])

    def flush_ref(self, now: int) -> None:
        with self.lock:
            due = [(s, p) for s, p in self.ref_pending.items() if now - self.ref_last[s][0] >= REF_EVERY_MS]
            for s, (t, q) in due:
                del self.ref_pending[s]
                self.ref_last[s] = (t, q)
                self.hour[s]["ref"] += 1
                self.w.write("ref", s, [[t, *q]])

    def heartbeat(self, now: int, conns_up: int, reconnects: int, offset: Optional[int]) -> List[List[Any]]:
        rows = []
        with self.lock:
            for sym in self.symbols + self.refs:
                h, d = self.hour[sym], sorted(self.delays.get(sym, []))
                p50 = d[len(d) // 2] if d else ""
                p90 = d[int(len(d) * 0.9)] if d else ""
                rows.append([now, utc(now), sym, *(h[k] for k in HB_HEADER[3:13]), p50, p90, "", "", ""])
                self.hour[sym] = Counter()
                if sym in self.delays:
                    self.delays[sym] = []
        tot = [sum(int(r[i]) for r in rows) for i in range(3, 13)]
        rows.append([now, utc(now), "ALL", *tot, "", "", conns_up, reconnects, "" if offset is None else offset])
        append_csv(self.out / "heartbeat.csv", HB_HEADER, rows)
        return rows


def connector(rec: Recorder, conn: str, streams: List[str],
              open_ws: Callable[[List[str]], Any] = BnWebSocket.open) -> Callable[[str], Any]:
    """`connect` for bitkub_stream_collector.stream_loop: open, subscribe, then reset what a reconnect invalidates."""
    def connect(_path: str) -> Any:
        ws = open_ws(streams)
        rec.on_connect(conn, now_ms())
        return ws
    return connect


def conns_up(status: Dict[str, Any]) -> int:
    return sum(1 for k in ("thb", "ref") if status.get(k) is True)


def clock_offset_ms(fetch: Callable[[str], Any] = http_json) -> Optional[int]:
    """Binance TH server time minus this machine's clock (ms), from one round trip; None if unreachable."""
    try:
        t0 = now_ms()
        server = int(fetch(f"{REST}/time")["serverTime"])
        return server - (t0 + now_ms()) // 2
    except Exception:  # noqa: BLE001
        return None


def run(out_root: Path, minutes: float, handoff_at: Optional[float], handoff_cmd: str,
        symbols: List[str] = SYMBOLS, refs: List[str] = REFS) -> int:
    start = now_ms()
    run_id = time.strftime("%Y%m%d-%H%M%S", time.gmtime(start / 1000))
    out = out_root / run_id
    out.mkdir(parents=True, exist_ok=True)
    listed: Set[str] = set()
    for attempt in range(3):
        try:
            listed = {s["symbol"] for s in http_json(f"{REST}/exchangeInfo")["symbols"] if s.get("status") == "TRADING"}
            break
        except Exception as exc:  # noqa: BLE001
            print(f"exchangeInfo attempt {attempt + 1}: {exc!r}", flush=True)
            time.sleep(10)
    if not listed:
        print("Binance TH symbol list unreachable; stopping.", flush=True)
        return 1
    missing = [s for s in symbols + refs if s not in listed]
    if missing:
        print("Not trading on Binance TH (skipped):", missing, flush=True)
    symbols = [s for s in symbols if s in listed]
    refs = [s for s in refs if s in listed]
    rec = Recorder(out, symbols, refs)
    stop = threading.Event()
    status: Dict[str, Any] = {}
    thb_streams = [f"{s.lower()}@depth@100ms" for s in symbols] + [f"{s.lower()}@trade" for s in symbols]
    ref_streams = [f"{s.lower()}@bookTicker" for s in refs]
    threads = [threading.Thread(target=bk.stream_loop, daemon=True,
                                args=(name, WS_PATH, rec.on_message, rec, name.upper(), stop, status),
                                kwargs={"connect": connector(rec, name, streams)})
               for name, streams in (("thb", thb_streams), ("ref", ref_streams)) if streams]
    for t in threads:
        t.start()
    print(f"run {run_id}: {len(symbols)} THB symbols, {len(refs)} references, {minutes:.0f} min", flush=True)
    end = start + int(minutes * 60000)
    next_flush = start
    next_hb = start + HEARTBEAT_EVERY_S * 1000
    handoff = "not due" if handoff_at is not None else "off"
    code = 0
    while True:
        t = now_ms()
        rec.resync(t, status.get("thb") is True)
        rec.verify(t, status.get("thb") is True)
        rec.fill_trades(t)
        rec.flush_ref(t)
        if t >= next_flush:
            rec.w.flush()
            next_flush = t + FLUSH_EVERY_S * 1000
        if t >= next_hb:
            up = conns_up(status)
            rows = rec.heartbeat(t, up, status.get("reconnects", 0), clock_offset_ms())
            print(f"heartbeat {utc(t)}: {rows[-1][3:13]} conns_up={up}", flush=True)
            next_hb = t + HEARTBEAT_EVERY_S * 1000
        if handoff == "not due" and t >= start + handoff_at * 60000:
            handoff = bk.run_handoff(handoff_cmd, out_root)
            print("hand-over:", handoff, flush=True)
        if t - start > 120000 and rec.messages == 0:
            print("No market data in the first 2 minutes (Binance TH unreachable?); stopping.", flush=True)
            code = 1
            break
        if t >= end:
            break
        time.sleep(0.5)
    stop.set()
    for th in threads:
        th.join(timeout=15)
    t = now_ms()
    rec.flush_ref(t + REF_EVERY_MS)
    rec.close_gaps(t, start)
    rec.heartbeat(t, conns_up(status), status.get("reconnects", 0), clock_offset_ms())
    rec.w.close()
    err_type = str(status.get("last_parse_error", "")).split("(")[0]   # type only: the text may hold a price
    append_csv(out / "run.csv", ["run_id", "start_utc", "end_utc", "messages", "reconnects", "gaps", "parse_errors",
                                 "last_parse_error", "handoff", "exit"],
               [[run_id, utc(start), utc(t), rec.messages, status.get("reconnects", 0), len(rec.gaps),
                 status.get("parse_errors", 0), err_type, handoff, code]])
    print(f"run {run_id} done: {rec.messages} messages, {status.get('reconnects', 0)} reconnects, "
          f"{len(rec.gaps)} gaps, {status.get('parse_errors', 0)} parse errors, hand-over: {handoff}", flush=True)
    return code


# ---------------------------------------------------------------- owner's PC: download, decrypt, merge

PREFIX = "bnth_"   # release files of this recorder; the Bitkub recorder's are stream_*.tar.enc


def download(repo: str, out: Path, fetch: Callable[[str], Any] = http_json) -> int:
    out.mkdir(parents=True, exist_ok=True)
    n = 0
    for rel in fetch(f"https://api.github.com/repos/{repo}/releases?per_page=100"):
        for a in rel.get("assets", []):
            dest = out / a["name"]
            if a["name"].startswith(PREFIX) and a["name"].endswith(".tar.enc") and not dest.exists():
                urllib.request.urlretrieve(a["browser_download_url"], dest)
                n += 1
                print("downloaded", a["name"], flush=True)
    return n


def decrypt_dir(src: Path, dest: Path, passphrase: str) -> int:
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in sorted(src.glob(f"{PREFIX}*.tar.enc")):
        run_id = f.name[len(PREFIX):-len(".tar.enc")]
        if (dest / run_id).exists():
            continue
        with tarfile.open(fileobj=io.BytesIO(bk.openssl_decrypt(f.read_bytes(), passphrase))) as tar:
            if hasattr(tarfile, "data_filter"):
                tar.extractall(dest, filter="data")
            else:  # pragma: no cover - old Python
                tar.extractall(dest)
        n += 1
        print("decrypted", f.name, flush=True)
    return n


def merge_rows(kind: str, runs: List[List[List[str]]]) -> List[List[str]]:
    """trades: one row per trade id (earliest receive). book10/ref: run k's rows until run k+1's first row."""
    if kind != "trades":
        return bk.merge_rows("book10", runs)
    best: Dict[int, List[str]] = {}
    for rows in runs:
        for r in rows:
            b = best.get(int(r[2]))
            if b is None or int(r[0]) < int(b[0]):
                best[int(r[2])] = list(r)
    return [best[i] for i in sorted(best)]


def merge_dir(runs_dir: Path, dest: Path) -> int:
    """RUNS_DIR/<run_id>/<day>/<kind>_<SYM>.csv.gz -> DEST/<day>/<kind>_<SYM>.csv.gz, plus all runs' gaps/heartbeat."""
    groups: Dict[Tuple[str, str], List[Path]] = defaultdict(list)
    for p in sorted(runs_dir.glob("*/*/*.csv.gz")):
        groups[(p.parent.name, p.name)].append(p)  # run folders sort by start time
    spans: Dict[str, List[int]] = {}
    for (day, name), paths in sorted(groups.items()):
        kind = next(k for k in sorted(HEADERS, key=len, reverse=True) if name.startswith(k + "_"))
        runs = [bk.read_gz_csv(p) for p in paths]
        bk.note_spans(spans, paths, runs)
        rows = merge_rows(kind, runs)
        target = dest / day / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(target, "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(HEADERS[kind])
            w.writerows(rows)
    bk.merge_logs(runs_dir, dest, HB_HEADER, bk.run_holes(spans), "thb")   # a hole = every THB book and trade unknown
    return len(groups)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="record (GitHub Actions)")
    r.add_argument("--minutes", type=float, default=330.0)
    r.add_argument("--handoff-at", type=float, default=None, help="minute at which to start the next run")
    r.add_argument("--handoff-cmd", default="")
    r.add_argument("--out", default="out")
    d = sub.add_parser("decrypt", help="decrypt downloaded bnth_*.tar.enc files (owner's PC)")
    d.add_argument("src")
    d.add_argument("dest")
    m = sub.add_parser("merge", help="join decrypted runs into one file per day/kind/symbol")
    m.add_argument("runs_dir")
    m.add_argument("dest")
    g = sub.add_parser("download", help="download this recorder's encrypted release files from a public repo")
    g.add_argument("--repo", required=True)
    g.add_argument("--out", default="downloads_bnth")
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
