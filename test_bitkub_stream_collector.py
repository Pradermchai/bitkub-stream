"""Unit tests for bitkub_stream_collector.py (synthetic data only, no network).
Run: python -m unittest -v test_bitkub_stream_collector.py"""
from __future__ import annotations

import base64
import gzip
import json
import socket
import struct
import sys
import tempfile
import threading
import unittest
from pathlib import Path

import bitkub_stream_collector as c

DAY_MS = 86400 * 1000
T0 = 1793200000000  # 2026-10-28 13:46:40 UTC


def frame(op, payload, fin=True):
    n = len(payload)
    head = bytes([(0x80 if fin else 0) | op])
    head += bytes([n]) if n < 126 else bytes([126]) + struct.pack(">H", n)
    return head + payload


class FakeSock:
    """Feeds bytes in fixed chunks; a None chunk = socket timeout."""

    def __init__(self, chunks):
        self.chunks, self.sent = list(chunks), []

    def recv(self, n):
        if not self.chunks:
            return b""
        ch = self.chunks.pop(0)
        if ch is None:
            raise socket.timeout()
        return ch

    def sendall(self, b):
        self.sent.append(b)

    def close(self):
        pass


class TestWebSocket(unittest.TestCase):
    def test_fragmented_message_split_over_reads_and_timeouts(self):
        data = frame(1, b'{"a":', fin=False) + frame(9, b"hi") + frame(0, b"1}")
        ws = c.WebSocket(FakeSock([data[:3], None, data[3:9], None, data[9:]]))
        self.assertEqual(ws.recv(), '{"a":1}')
        sent = ws.sock.sent[0]                        # pong for the ping, masked as clients must
        self.assertEqual(sent[0], 0x8A)
        self.assertEqual(sent[1], 0x80 | 2)
        mask = sent[2:6]
        self.assertEqual(bytes(x ^ mask[i % 4] for i, x in enumerate(sent[6:])), b"hi")

    def test_long_frame_and_two_messages_in_one_read(self):
        big = b"x" * 300
        ws = c.WebSocket(FakeSock([frame(1, big) + frame(1, b"second")]))
        self.assertEqual(ws.recv(), "x" * 300)
        self.assertEqual(ws.recv(), "second")

    def test_close_frame_and_eof_raise(self):
        with self.assertRaises(c.ConnectionClosed):
            c.WebSocket(FakeSock([frame(8, b"\x03\xe8")])).recv()
        with self.assertRaises(c.ConnectionClosed):
            c.WebSocket(FakeSock([])).recv()

    def test_silent_connection_times_out_and_stop_returns_none(self):
        t = [0.0]

        def clock():
            t[0] += 50
            return t[0]

        ws = c.WebSocket(FakeSock([None] * 10 + [frame(9, b"")] * 5), data_timeout=120, clock=clock)
        with self.assertRaises(TimeoutError):
            ws.recv()
        self.assertIsNone(c.WebSocket(FakeSock([None])).recv(stop=lambda: True))


class TestParsing(unittest.TestCase):
    def test_symbols(self):
        self.assertEqual(c.norm_sym("THB_PEPE"), "PEPE_THB")
        self.assertEqual(c.norm_sym("market.trade.thb_usdt"), "USDT_THB")
        self.assertEqual(c.stream_name("trade", "F_THB"), "market.trade.thb_f")
        self.assertEqual(c.norm_sym("USDT_BTC"), "BTC_USDT")              # USDT pairs: quote first, like THB
        self.assertEqual(c.norm_sym("market.ticker.usdt_xaut"), "XAUT_USDT")
        self.assertEqual(c.stream_name("trade", "BTC_USDT"), "market.trade.usdt_btc")

    def test_symbol_list(self):
        self.assertEqual(len(set(c.COINS)), 40)                           # frozen Test P list, unchanged
        self.assertTrue(all(s.endswith("_THB") for s in c.COINS))
        self.assertNotIn("PEPE_THB", c.COINS)                             # old broker coins are gone
        self.assertEqual(c.SYMBOLS[-1], "USDT_THB")
        self.assertEqual(len(set(c.SYMBOLS)), 41)

    def test_usdt_pair_trade_is_recorded_under_its_own_name(self):
        with tempfile.TemporaryDirectory() as d:
            r = c.Recorder(Path(d), ["BTC_USDT", "USDT_THB"])
            r.on_market(json.dumps({"stream": "market.trade.usdt_btc", "sym": "USDT_BTC", "txn": "b1",
                                    "rat": 80263.6, "amt": 0.010932, "ts": T0}), T0 + 200)
            r.release_trades(T0 + 200, force=True)
            r.w.close()
            day = Path(d) / c.day_of(T0)
            self.assertEqual(c.read_gz_csv(day / "trades_BTC_USDT.csv.gz")[0][2:5], ["b1", "80263.6", "0.010932"])
            self.assertFalse((day / "trades_USDT_THB.csv.gz").exists())

    def test_trade_keeps_exact_decimal_text(self):
        m = c.loads('{"stream":"market.trade.thb_pepe","sym":"THB_PEPE","txn":"228","rat":0.00017058,'
                    '"amt":2157213,"bid":"0","sid":"0","ts":1793219357992}')
        sym, row = c.parse_trade(m, 1793219358100)
        self.assertEqual(sym, "PEPE_THB")
        self.assertEqual(row, [1793219358100, 1793219357992, "228", "0.00017058", "2157213", ""])

    def test_ticker(self):
        m = c.loads('{"stream":"market.ticker.thb_usdt","highestBid":33.7,"highestBidSize":10,'
                    '"lowestAsk":33.71,"lowestAskSize":5.5,"last":33.71}')
        self.assertEqual(c.parse_ticker(m), ("USDT_THB", ["33.7", "10", "33.71", "5.5", "33.71"]))

    def test_depth_top_levels_and_blank_padding(self):
        m = {"event": "depthchanged", "data": {
            "bids": [{"price": str(100 - i), "base_volume": "1", "quote_volume": "1"} for i in range(15)],
            "asks": [{"price": "101", "base_volume": "2", "quote_volume": "2"}]}}
        row = c.parse_depth(m)
        self.assertEqual(len(row), 40)
        self.assertEqual(row[:2], ["100", "1"])
        self.assertEqual(row[18:20], ["91", "1"])       # 10th bid level kept, 11th dropped
        self.assertEqual(row[20:22], ["101", "2"])
        self.assertEqual(row[22:], [""] * 18)

    def test_tradeschanged(self):
        m = c.loads('{"event":"tradeschanged","pairing_id":8,"data":[[[1793219257,33.71,496.13,"BUY",0,0,true,'
                    'false,false]],[],[]]}')
        self.assertEqual(c.parse_tradeschanged(m), [(1793219257, "33.71", "496.13", "BUY")])


def trow(recv, ts_ms, txn, price="1.5", amt="2"):
    return [recv, ts_ms, txn, price, amt, ""]


class TestSideMatcher(unittest.TestCase):
    def test_match_within_one_second(self):
        m = c.SideMatcher(wait_ms=60000)
        m.add_book_trades([(1000, "1.5", "2", "SELL")])
        m.offer(trow(1001500, 1001200, "a"))          # second 1001 vs list second 1000
        self.assertEqual([r[5] for r in m.ready(1001600)], ["SELL"])

    def test_waits_for_late_list_then_blank_after_timeout(self):
        m = c.SideMatcher(wait_ms=60000)
        m.offer(trow(1000000, 999900, "a"))
        m.offer(trow(1000000, 999900, "b", price="9"))
        self.assertEqual(m.ready(1030000), [])        # list not here yet: keep waiting
        m.add_book_trades([(999, "1.5", "2", "BUY")])
        out = m.ready(1031000)
        self.assertEqual([(r[2], r[5]) for r in out], [("a", "BUY")])
        out = m.ready(1060000)                        # 'b' never matched: written with a blank side
        self.assertEqual([(r[2], r[5]) for r in out], [("b", "")])

    def test_both_sides_same_key_is_left_blank_and_force_flushes(self):
        m = c.SideMatcher()
        m.add_book_trades([(5, "1.5", "2", "BUY"), (5, "1.5", "2", "SELL")])
        m.offer(trow(5100, 5000, "a"))
        self.assertEqual(m.ready(5200)[0][5], "")
        m.offer(trow(9000, 8000, "z", price="7"))
        self.assertEqual(len(m.ready(9001, force=True)), 1)


def k(ts, price="1", amt="1", side="BUY"):
    return (ts, price, amt, side)


class TestDedupSnapshot(unittest.TestCase):
    def test_overlapping_lists_are_saved_once(self):
        new, gap, st = c.dedup_snapshot({}, [k(11), k(10)])
        self.assertEqual([x[0] for x in new], [10, 11])
        new, gap, st = c.dedup_snapshot(st, [k(13), k(12), k(11), k(10)])
        self.assertEqual([x[0] for x in new], [12, 13])
        self.assertIsNone(gap)

    def test_identical_trades_in_last_second_are_counted(self):
        _, _, st = c.dedup_snapshot({}, [k(20), k(19)])
        new, _, st = c.dedup_snapshot(st, [k(20), k(20), k(20), k(19)])
        self.assertEqual(len(new), 2)
        new, _, _ = c.dedup_snapshot(st, [k(20), k(20), k(20)])
        self.assertEqual(new, [])

    def test_gap_when_full_list_does_not_reach_back(self):
        _, _, st = c.dedup_snapshot({}, [k(5)])
        new, gap, _ = c.dedup_snapshot(st, [k(1000 + i) for i in range(30)], full=30)
        self.assertEqual(gap, (5, 1000))
        _, gap, _ = c.dedup_snapshot(st, [k(50), k(40)], full=30)
        self.assertIsNone(gap)


class TempDirCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()


class TestWriterAndReader(TempDirCase):
    def test_rotation_at_utc_midnight(self):
        w = c.GzWriter(self.dir)
        midnight = (T0 // DAY_MS + 1) * DAY_MS
        w.write("bbo", "PEPE_THB", [[midnight - 1, "1", "1", "2", "1", "1"], [midnight, "1", "1", "2", "2", "1"]])
        w.write("bbo", "PEPE_THB", [[midnight + 5, "1", "1", "2", "3", "1"]])
        w.close()
        d1, d2 = c.day_of(midnight - 1), c.day_of(midnight)
        self.assertNotEqual(d1, d2)
        a = self.dir / d1 / "bbo_PEPE_THB.csv.gz"
        b = self.dir / d2 / "bbo_PEPE_THB.csv.gz"
        self.assertEqual(len(c.read_gz_csv(a)), 1)
        self.assertEqual([r[4] for r in c.read_gz_csv(b)], ["2", "3"])
        with gzip.open(b, "rt") as f:
            self.assertEqual(f.readline().strip(), ",".join(c.HEADERS["bbo"]))  # header once, at the top

    def test_cut_off_file_is_readable_up_to_last_flush(self):
        w = c.GzWriter(self.dir)
        w.write("trades", "USDT_THB", [[T0, T0 - 5, "t1", "33.7", "1", "BUY"]])
        w.flush()
        path = self.dir / c.day_of(T0) / "trades_USDT_THB.csv.gz"
        good = path.read_bytes()
        w.write("trades", "USDT_THB", [[T0 + 1, T0, "t2", "33.7", "1", "SELL"]])
        w.close()
        path.write_bytes(good + b"\x00\x01garbage")      # simulate a run killed mid-write
        self.assertEqual([r[2] for r in c.read_gz_csv(path)], ["t1"])


class TestRecorder(TempDirCase):
    def test_book_written_on_change_only_and_flood_skipped(self):
        r = c.Recorder(self.dir, ["PEPE_THB"])
        depth = json.dumps({"event": "depthchanged", "pairing_id": 472,
                            "data": {"bids": [{"price": 1.0, "base_volume": 5, "quote_volume": 5}],
                                     "asks": [{"price": 1.1, "base_volume": 3, "quote_volume": 3}]}})
        flood = json.dumps({"event": "global.ticker", "data": {"id": 1}})
        r.on_book("PEPE_THB", depth + "\n" + flood, T0)
        r.on_book("PEPE_THB", depth, T0 + 2000)          # same book: not written again
        r.w.close()
        rows = c.read_gz_csv(self.dir / c.day_of(T0) / "book10_PEPE_THB.csv.gz")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][:5], [str(T0), "1.0", "5", "", ""])
        self.assertEqual(rows[0][21:23], ["1.1", "3"])

    def test_trades_get_side_and_list_is_deduplicated(self):
        r = c.Recorder(self.dir, ["USDT_THB"])
        tr = {"stream": "market.trade.thb_usdt", "sym": "THB_USDT", "txn": "x1", "rat": 33.71, "amt": 14.79,
              "ts": T0 - 300}
        tl = {"event": "tradeschanged", "pairing_id": 8,
              "data": [[[(T0 - 300) // 1000, 33.71, 14.79, "BUY", 0, 0, True, False, False]], [], []]}
        r.on_market(json.dumps(tr), T0)
        r.on_book("USDT_THB", json.dumps(tl), T0 + 3000)
        r.on_book("USDT_THB", json.dumps(tl), T0 + 6000)  # same list again
        r.release_trades(T0 + 7000)
        r.w.close()
        d = self.dir / c.day_of(T0)
        self.assertEqual(c.read_gz_csv(d / "trades_USDT_THB.csv.gz"),
                         [[str(T0), str(T0 - 300), "x1", "33.71", "14.79", "BUY"]])
        self.assertEqual(len(c.read_gz_csv(d / "trades_ob_USDT_THB.csv.gz")), 1)
        hb = r.heartbeat(T0 + 8000, 22, 0, -1000)
        self.assertEqual(hb[0][2:8], ["USDT_THB", 1, 0, 1, 0, 0])
        self.assertEqual(hb[0][9], 300)                   # receive delay p50 (ms)
        self.assertEqual(len(hb[0]), len(c.HB_HEADER))
        self.assertEqual(len(hb[-1]), len(c.HB_HEADER))
        self.assertEqual(hb[-1][2], "ALL")
        self.assertEqual(hb[-1][-1], -1000)

    def test_overflowing_trade_list_triggers_one_rest_side_fill(self):
        r = c.Recorder(self.dir, ["CHIP_THB"])
        sec = T0 // 1000
        r.on_book("CHIP_THB", json.dumps({"event": "tradeschanged", "data": [[[sec, 1, 1, "BUY"]], [], []]}), T0)
        burst = [[sec + 100 - i, 1, 1, "BUY"] for i in range(30)]   # 30 newer trades: the list skipped some
        r.on_book("CHIP_THB", json.dumps({"event": "tradeschanged", "data": [burst, [], []]}), T0 + 100000)
        self.assertEqual(len(r.gaps), 1)
        r.on_market(json.dumps({"stream": "market.trade.thb_chip", "sym": "THB_CHIP", "txn": "lost", "rat": 2.5,
                                "amt": 7, "ts": (sec + 50) * 1000 + 10}), T0 + 100001)
        calls = []

        def fetch(url):
            calls.append(url)
            return {"error": 0, "result": [[(sec + 50) * 1000, 2.5, 7, "SELL"]]}  # REST may send ms or s

        r.rest_fill(T0 + 100002, fetch)
        r.rest_fill(T0 + 100003, fetch)                # nothing due any more: no second call
        self.assertEqual(len(calls), 1)
        self.assertIn("sym=chip_thb", calls[0])
        r.release_trades(T0 + 100004)
        r.w.close()
        rows = c.read_gz_csv(self.dir / c.day_of(T0) / "trades_CHIP_THB.csv.gz")
        self.assertEqual([(x[2], x[5]) for x in rows], [("lost", "SELL")])

    def test_bbo_only_on_change(self):
        r = c.Recorder(self.dir, ["USDT_THB"])
        t = {"stream": "market.ticker.thb_usdt", "highestBid": 33.7, "highestBidSize": 1, "lowestAsk": 33.71,
             "lowestAskSize": 2, "last": 33.7}
        r.on_market(json.dumps(t), T0)
        r.on_market(json.dumps(t), T0 + 1)
        t["highestBidSize"] = 9
        r.on_market(json.dumps(t), T0 + 2)
        r.w.close()
        self.assertEqual(len(c.read_gz_csv(self.dir / c.day_of(T0) / "bbo_USDT_THB.csv.gz")), 2)


class FakeWS:
    def __init__(self, msgs, then_raise=True):
        self.msgs, self.then_raise = list(msgs), then_raise

    def recv(self, stop):
        if self.msgs:
            return self.msgs.pop(0)
        if self.then_raise:
            raise c.ConnectionClosed("dropped")
        return None

    def close(self):
        pass


class TestStreamLoop(TempDirCase):
    def test_drop_and_reconnect_logs_one_gap_with_start_and_end(self):
        rec = c.Recorder(self.dir, ["PEPE_THB"])
        stop = threading.Event()
        times = iter(range(1000, 100000, 1000))
        got = []
        conns = [FakeWS(["m1", "m2"]), "fail", FakeWS(["m3"], then_raise=False)]

        def connect(path):
            x = conns.pop(0)
            if x == "fail":
                raise OSError("refused")
            if not conns:
                stop.set()  # last connection: deliver m3, then stop
            return x

        c.stream_loop("ob_PEPE_THB", "/p", lambda text, t: got.append((text, t)), rec, "PEPE_THB", stop, {},
                      connect=connect, clock=lambda: next(times), max_backoff=0)
        self.assertEqual([g[0] for g in got], ["m1", "m2"])  # stop was set before m3 could be read
        self.assertEqual(len(rec.gaps), 1)
        g = rec.gaps[0]
        self.assertEqual(g[0:2], ["ob_PEPE_THB", "PEPE_THB"])
        self.assertEqual(g[2], got[-1][1])                   # gap starts at the last message received
        self.assertIn("still down at stop", g[6])

    def test_gap_closed_by_first_message_after_reconnect(self):
        rec = c.Recorder(self.dir, ["PEPE_THB"])
        stop = threading.Event()
        times = iter(range(1000, 100000, 1000))
        got = []
        conns = [FakeWS(["m1"]), FakeWS(["m2"], then_raise=False)]

        def handler(text, t):
            got.append((text, t))
            if text == "m2":
                stop.set()

        c.stream_loop("market", "/p", handler, rec, "ALL", stop, {}, connect=lambda p: conns.pop(0),
                      clock=lambda: next(times), max_backoff=0)
        self.assertEqual(len(rec.gaps), 1)
        self.assertEqual(rec.gaps[0][2], got[0][1])
        self.assertEqual(rec.gaps[0][3], got[1][1])
        self.assertIn("dropped", rec.gaps[0][6])
        with open(self.dir / "gaps.csv", encoding="utf-8") as f:
            self.assertEqual(f.readline().strip(), ",".join(c.GAP_HEADER))


class TestMerge(TempDirCase):
    def test_trades_one_row_per_txn_and_side_filled(self):
        a = [["100", "90", "t1", "1", "1", ""], ["110", "105", "t2", "1", "1", "BUY"]]
        b = [["108", "105", "t2", "1", "1", "BUY"], ["120", "118", "t3", "1", "1", "SELL"],
             ["99", "90", "t1", "1", "1", "SELL"]]
        out = c.merge_rows("trades", [a, b])
        self.assertEqual([r[2] for r in out], ["t1", "t2", "t3"])
        self.assertEqual(out[0][0], "99")
        self.assertEqual(out[0][5], "SELL")

    def test_trades_ob_keeps_max_count_per_key(self):
        a = [["1", "10", "1", "1", "BUY"], ["1", "10", "1", "1", "BUY"]]
        b = [["2", "10", "1", "1", "BUY"], ["2", "11", "1", "1", "SELL"]]
        out = c.merge_rows("trades_ob", [a, b])
        self.assertEqual(len(out), 3)
        self.assertEqual(sum(1 for r in out if r[1] == "10"), 2)

    def test_book_cut_over_at_next_runs_first_row(self):
        a = [[str(t)] + ["a"] * 4 for t in (100, 200, 300, 400)]
        b = [[str(t)] + ["b"] * 4 for t in (250, 350, 450)]
        out = c.merge_rows("bbo", [b, a])               # order of runs does not matter
        self.assertEqual([(r[0], r[1]) for r in out], [("100", "a"), ("200", "a"), ("250", "b"), ("350", "b"),
                                                       ("450", "b")])

    def test_merge_dir_end_to_end(self):
        day = c.day_of(T0)
        for run, rows in (("20261028-000000", [[T0, T0 - 9, "t1", "1", "1", "BUY"]]),
                          ("20261028-053000", [[T0 + 5, T0 - 9, "t1", "1", "1", "BUY"],
                                               [T0 + 6, T0 + 1, "t2", "1", "1", "SELL"]])):
            w = c.GzWriter(self.dir / "runs" / run)
            w.write("trades", "PEPE_THB", rows)
            w.close()
            c.append_csv(self.dir / "runs" / run / "gaps.csv", c.GAP_HEADER, [["market", "ALL", 1, 2, "", "", "x"]])
        n = c.merge_dir(self.dir / "runs", self.dir / "merged")
        self.assertEqual(n, 1)
        out = c.read_gz_csv(self.dir / "merged" / day / "trades_PEPE_THB.csv.gz")
        self.assertEqual([r[2] for r in out], ["t1", "t2"])
        with open(self.dir / "merged" / "gaps.csv", encoding="utf-8") as f:
            self.assertEqual(len(f.read().strip().split("\n")), 3)  # header + one gap per run


class TestHandoffAndDecrypt(TempDirCase):
    def test_handoff_marker_only_on_success(self):
        ok = c.run_handoff(f'"{sys.executable}" -c "pass"', self.dir)
        self.assertEqual(ok, "started next run")
        self.assertTrue((self.dir / "HANDOFF_DONE").exists())
        (self.dir / "HANDOFF_DONE").unlink()
        bad = c.run_handoff(f'"{sys.executable}" -c "raise SystemExit(3)"', self.dir)
        self.assertTrue(bad.startswith("failed (3)"))
        self.assertFalse((self.dir / "HANDOFF_DONE").exists())

    def test_openssl_vector(self):
        try:
            import cryptography  # noqa: F401
        except ImportError:
            self.skipTest("cryptography not installed (only needed on the owner's PC)")
        # made with: openssl enc -aes-256-cbc -salt -pbkdf2 -iter 200000 -md sha256 -pass env:STREAM_DATA_KEY
        blob = base64.b64decode("U2FsdGVkX18y2USnU8YUARe56RcXwLNj2Bm0y83toDk=")
        self.assertEqual(c.openssl_decrypt(blob, "unit-test-key"), b"hello bitkub\n")
        with self.assertRaises(ValueError):
            c.openssl_decrypt(blob, "wrong-key")


if __name__ == "__main__":
    unittest.main()
