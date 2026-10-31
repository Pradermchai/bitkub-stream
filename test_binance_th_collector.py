"""Unit tests for binance_th_collector.py: synthetic messages only, no network, no keys."""

import csv
import gzip
import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import binance_th_collector as c
import bitkub_stream_collector as bk


def depth(sym, U, u, b=(), a=(), E=1_000):
    return json.dumps({"stream": f"{sym.lower()}@depth@100ms",
                       "data": {"e": "depthUpdate", "E": E, "s": sym, "U": U, "u": u, "b": list(b), "a": list(a)}})


def trade(sym, tid, p="10.5", q="2", maker_buyer=True, T=900):
    return json.dumps({"stream": f"{sym.lower()}@trade",
                       "data": {"e": "trade", "E": T + 1, "s": sym, "t": tid, "p": p, "q": q, "T": T,
                                "m": maker_buyer, "M": True}})


def ref(sym, b, a, B="1", A="1"):
    return json.dumps({"stream": f"{sym.lower()}@bookTicker", "data": {"s": sym, "b": b, "B": B, "a": a, "A": A,
                                                                        "u": 1}})


def rows(path):
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.reader(f))


def plain(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


SNAP = {"lastUpdateId": 100, "bids": [["10.00000000", "5.0"], ["9.90", "1"]], "asks": [["10.20", "3"]]}


class BookTests(unittest.TestCase):
    def test_snapshot_then_buffered_diffs(self):
        b = c.Book()
        for U, u, bids in ((99, 100, [["9.0", "1"]]), (101, 101, [["10.10", "2"]]), (102, 102, [["9.90", "0"]])):
            self.assertEqual(b.on_diff({"U": U, "u": u, "b": bids, "a": []}), "buffered")
        b.on_snapshot(SNAP)                        # 99-100 dropped (in the snapshot), 101 and 102 applied
        self.assertEqual(b.last_u, 102)
        top = b.top(3)
        self.assertEqual(top[:6], ["10.1", "2", "10", "5", "", ""])   # 9.90 removed by qty 0; text trimmed
        self.assertEqual(top[6:8], ["10.2", "3"])

    def test_id_jump_is_applied_and_flagged(self):
        b = c.Book()
        b.on_snapshot(SNAP)
        self.assertEqual(b.on_diff({"U": 100, "u": 100, "b": [], "a": []}), "old")
        self.assertEqual(b.on_diff({"U": 101, "u": 101, "b": [], "a": [["10.3", "1"]]}), "applied")
        self.assertEqual(b.on_diff({"U": 103, "u": 103, "b": [], "a": [["10.4", "1"]]}), "jump")   # 102 skipped
        self.assertEqual(b.last_u, 103)
        self.assertEqual(b.top(3)[6:], ["10.2", "3", "10.3", "1", "10.4", "1"])

    def test_check_against_snapshot(self):
        b = c.Book()
        b.on_snapshot(SNAP)
        b.on_diff({"U": 101, "u": 101, "b": [["10.0", "6"]], "a": []})
        b.on_diff({"U": 102, "u": 102, "b": [["9.5", "1"]], "a": []})
        at101 = dict(SNAP, lastUpdateId=101, bids=[["10", "6"], ["9.9", "1"]])
        self.assertEqual(b.check(at101), (True, 0))                # compared with the book as it was at id 101
        self.assertFalse(b.check(dict(at101, bids=[["10", "7"]]))[0])
        self.assertTrue(b.check(dict(SNAP))[0])                    # id 100 = the snapshot state
        ahead = dict(at101, lastUpdateId=110, bids=[["10", "6"], ["9.9", "1"], ["9.5", "1"]])
        self.assertIsNone(b.check(ahead))                          # stream not there yet
        self.assertTrue(b.check(ahead, force=True)[0])             # 103-110 taken as silently skipped
        self.assertIsNone(b.check(dict(SNAP, lastUpdateId=5)))     # older than the kept history

    def test_check_inside_a_diffs_id_range_is_not_judged(self):
        b = c.Book()
        b.on_snapshot(SNAP, 1)
        b.on_diff({"U": 101, "u": 110, "b": [["10", "6"]], "a": [], "recv": 7})
        mid = dict(SNAP, lastUpdateId=105, bids=[["10", "6"], ["9.9", "1"]])
        self.assertIsNone(b.check(mid))                            # state at 105 was never seen
        self.assertEqual(b.check(dict(mid, lastUpdateId=110)), (True, 7))

    def test_snapshot_ahead_of_stream_is_not_kept_frozen(self):
        b = c.Book()
        b.on_snapshot(dict(SNAP, lastUpdateId=10 ** 9))   # ids from another sequence: every diff looks old
        res = [b.on_diff({"U": i, "u": i, "b": [], "a": []}) for i in range(1, c.OLD_MAX + 2)]
        self.assertEqual(res[:-1], ["old"] * c.OLD_MAX)
        self.assertEqual(res[-1], "lost")
        self.assertIsNone(b.last_u)

    def test_overlapping_diff_is_applied(self):
        b = c.Book()
        b.on_snapshot(SNAP)
        self.assertEqual(b.on_diff({"U": 99, "u": 101, "b": [["10.0", "6"]], "a": []}), "applied")
        self.assertEqual(b.top(1)[:2], ["10", "6"])


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name)
        self.rec = c.Recorder(self.out, ["XRPTHB"], ["XRPUSDT"])
        self.now = 1000
        self.rec.clock = lambda: self.now

    def tearDown(self):
        self.rec.w.close()
        self.tmp.cleanup()

    def file(self, kind, sym, day="1970-01-01"):
        self.rec.w.close()
        return rows(self.out / day / f"{kind}_{sym}.csv.gz")

    def test_book_rows_written_on_change_only(self):
        r = self.rec
        r.on_connect("thb", 0)
        r.on_message(depth("XRPTHB", 101, 101, b=[["10.1", "1"]]), 1500)
        r.resync(1000, up=True, fetch=lambda url: SNAP)
        r.on_message(depth("XRPTHB", 102, 102, a=[["11", "1"]], E=1700), 2000)   # new ask level: a change
        r.on_message(depth("XRPTHB", 103, 103, b=[["5", "1"]]), 2100)            # level 4: still a change
        r.on_message(depth("XRPTHB", 104, 104, b=[["5", "1"]]), 2200)            # same top 10: not written
        r.on_message(depth("XRPTHB", 106, 106, b=[["6", "1"]]), 2300)            # 105 skipped: applied, flagged
        self.assertEqual(r.books["XRPTHB"].last_u, 106)
        self.assertEqual(r.hour["XRPTHB"]["id_jumps"], 1)
        self.assertEqual(r.books["XRPTHB"].verify_due, 10_000)   # sync + 3 s, but at most one check per 10 s
        self.assertEqual(r.books["XRPTHB"].jump_at, 2300)
        book = self.file("book10", "XRPTHB")
        self.assertEqual(book[0], c.HEADERS["book10"])
        self.assertEqual(len(book), 1 + 4)                 # snapshot, 102, 103, 106
        self.assertEqual(book[1][1:3], ["10.1", "1"])      # snapshot + buffered diff 101
        self.assertEqual(book[1][-1], "")                  # snapshot rows carry no exchange time
        self.assertEqual(book[2][-1], "1700")
        self.assertFalse((self.out / "gaps.csv").exists())

    def test_failed_check_logs_gap_from_last_good_check_and_resyncs(self):
        r = self.rec
        r.on_connect("thb", 0)
        r.resync(1000, up=True, fetch=lambda url: SNAP)            # synced at 1000, check due at 10 000
        r.on_message(depth("XRPTHB", 101, 101, b=[["9.8", "1"]]), 5000)
        self.now = 10_000
        at101 = dict(SNAP, lastUpdateId=101, bids=[["10", "5"], ["9.9", "1"], ["9.8", "1"]])
        r.verify(10_000, up=True, fetch=lambda url: at101)          # matches the state received at 5000
        self.assertEqual(r.books["XRPTHB"].ok_at, 5000)
        self.assertEqual(r.hour["XRPTHB"]["checks"], 1)
        r.on_message(depth("XRPTHB", 102, 102, b=[["9.6", "1"]]), 20_000)
        self.now = 70_000
        wrong = dict(at101, lastUpdateId=102, bids=[["10", "5"], ["9.9", "1"], ["9.8", "1"], ["9.7", "1"]])
        r.verify(70_000, up=True, fetch=lambda url: wrong)          # exchange has 9.7, the book has 9.6
        self.assertIsNone(r.books["XRPTHB"].last_u)
        self.assertEqual(r.hour["XRPTHB"]["bad_checks"], 1)
        self.now = 71_000
        r.resync(71_000, up=True, fetch=lambda url: wrong)
        gaps = plain(self.out / "gaps.csv")
        self.assertEqual(gaps[1][:4], ["book_XRPTHB", "XRPTHB", "5000", "71000"])  # ids followed on: from ok_at

    def test_jump_logged_until_a_passed_check(self):
        r = self.rec
        r.on_connect("thb", 0)
        r.resync(1000, up=True, fetch=lambda url: SNAP)
        r.on_message(depth("XRPTHB", 103, 103, b=[["9.8", "1"]]), 5000)     # 101-102 skipped
        self.now = 10_000
        good = dict(SNAP, lastUpdateId=103, bids=[["10", "5"], ["9.9", "1"], ["9.8", "1"]])
        r.verify(10_000, up=True, fetch=lambda url: good)
        gaps = plain(self.out / "gaps.csv")
        self.assertEqual(gaps[1][:4], ["book_XRPTHB", "XRPTHB", "5000", "5000"])   # jump -> checked state
        self.assertIsNone(r.books["XRPTHB"].jump_at)
        r.on_message(depth("XRPTHB", 105, 105, b=[["9.7", "1"]]), 12_000)   # another jump, never checked
        r.close_gaps(15_000, run_start=0)
        self.assertEqual(plain(self.out / "gaps.csv")[2][:4], ["book_XRPTHB", "XRPTHB", "12000", "15000"])

    def test_check_waits_for_the_stream(self):
        r = self.rec
        r.on_connect("thb", 0)
        r.resync(1000, up=True, fetch=lambda url: SNAP)
        ahead = dict(SNAP, lastUpdateId=102, bids=[["10", "5"], ["9.9", "1"], ["9.8", "1"]])
        self.now = 10_000
        r.verify(10_000, up=True, fetch=lambda url: ahead)
        self.assertIsNotNone(r.books["XRPTHB"].pending)            # book still at 100
        r.on_message(depth("XRPTHB", 101, 102, b=[["9.8", "1"]]), 10_500)
        r.verify(10_600, up=True, fetch=lambda url: 1 / 0)
        self.assertIsNone(r.books["XRPTHB"].pending)
        self.assertEqual((r.hour["XRPTHB"]["checks"], r.hour["XRPTHB"]["bad_checks"]), (1, 0))

    def test_unknown_books_logged_at_stop_and_snapshot_backs_off(self):
        r = c.Recorder(self.out, ["XRPTHB", "BTCTHB"], [])
        r.clock = lambda: self.now
        r.on_connect("thb", 0)
        r.on_message(depth("XRPTHB", 101, 101), 500)
        r.resync(1000, up=True, fetch=lambda url: SNAP if "XRPTHB" in url else 1 / 0)   # BTC snapshot fails
        self.assertEqual(r.books["BTCTHB"].next_try, 3000)
        r.resync(3000, up=True, fetch=lambda url: 1 / 0)
        self.assertEqual(r.books["BTCTHB"].next_try, 7000)       # 2 s, then 4 s, ... up to 60 s
        r.on_message(depth("XRPTHB", 102, 102, b=[["1", "1"]]), 4000)
        r.on_connect("thb", 4100)                                 # reconnect: XRP unknown after its update at 4000
        r.close_gaps(9000, run_start=100)
        gaps = plain(self.out / "gaps.csv")
        self.assertEqual([g[:4] for g in gaps[1:]], [["book_XRPTHB", "XRPTHB", "4000", "9000"],
                                                     ["book_BTCTHB", "BTCTHB", "100", "9000"]])
        r.w.close()

    def test_no_snapshot_while_stream_down(self):
        calls = []
        self.rec.resync(10_000, up=False, fetch=lambda url: calls.append(url) or SNAP)
        self.assertEqual(calls, [])

    def test_trades_side_and_rest_fill(self):
        r = self.rec
        r.on_connect("thb", 0)
        # first REST call of the run: notes the newest id only
        r.fill_trades(3000, fetch=lambda url: [{"id": 50, "price": "10", "qty": "1", "time": 1, "isBuyerMaker": True}])
        self.assertEqual(r.first_id["XRPTHB"], 50)
        r.on_message(trade("XRPTHB", 51, maker_buyer=True), 4000)
        r.on_message(trade("XRPTHB", 51), 4001)                         # duplicate id: ignored
        r.on_message(trade("XRPTHB", 54, maker_buyer=False), 4100)      # 52, 53 skipped -> REST fill due
        self.assertIn("XRPTHB", r.fill_due)
        rest = [{"id": i, "price": "10.50", "qty": "1.0", "time": 800 + i, "isBuyerMaker": i == 52}
                for i in (50, 51, 52, 53, 54)]
        r.fill_trades(10_000, fetch=lambda url: rest)
        got = self.file("trades", "XRPTHB")
        self.assertEqual(got[0], c.HEADERS["trades"])
        self.assertEqual([(x[2], x[5], x[6]) for x in got[1:]],
                         [("51", "sell", "ws"), ("54", "buy", "ws"), ("52", "sell", "rest"), ("53", "buy", "rest")])
        self.assertEqual(got[3][3:5], ["10.5", "1"])
        self.assertFalse((self.out / "gaps.csv").exists())

    def test_unrecovered_trade_ids_logged(self):
        r = self.rec
        r.first_id["XRPTHB"] = 10
        r.on_message(trade("XRPTHB", 11), 100)
        r.on_message(trade("XRPTHB", 15), 200)
        r.fill_trades(10_000, fetch=lambda url: [{"id": 14, "price": "1", "qty": "1", "time": 1,
                                                   "isBuyerMaker": False}])
        gaps = plain(self.out / "gaps.csv")
        self.assertEqual(gaps[1][0], "trades_XRPTHB")
        self.assertIn("2 trade ids", gaps[1][6])

    def test_skip_right_after_first_fill_and_no_double_logging(self):
        r = self.rec
        r.on_connect("thb", 0)
        r.fill_trades(3000, fetch=lambda url: [{"id": 50, "price": "1", "qty": "1", "time": 1, "isBuyerMaker": True}])
        r.on_message(trade("XRPTHB", 53), 4000)          # 51, 52 skipped: first trade of the run
        self.assertIn("XRPTHB", r.fill_due)
        r.fill_trades(10_000, fetch=lambda url: [])       # REST cannot give them
        r.on_message(trade("XRPTHB", 55), 11_000)        # 54 skipped too
        r.fill_trades(20_000, fetch=lambda url: [{"id": 54, "price": "1", "qty": "1", "time": 1,
                                                   "isBuyerMaker": True}])
        gaps = plain(self.out / "gaps.csv")
        self.assertEqual(len(gaps), 2)                    # 51-52 logged once, 54 recovered
        self.assertIn("2 trade ids", gaps[1][6])
        self.assertIn("51-52", gaps[1][6])

    def test_routine_trade_check(self):
        r = self.rec
        r.on_connect("thb", 0)
        r.fill_trades(3000, fetch=lambda url: [])
        calls = []
        r.fill_trades(3000 + c.TRADE_CHECK_MS - 1, fetch=lambda url: calls.append(url) or [])
        r.fill_trades(3000 + c.TRADE_CHECK_MS, fetch=lambda url: calls.append(url) or [])
        self.assertEqual(len(calls), 1)

    def test_first_fill_does_not_skip_trades_seen_before_it(self):
        r = self.rec
        r.on_connect("thb", 0)
        r.on_message(trade("XRPTHB", 49), 100)          # arrives before the first REST call
        r.fill_trades(10_000, fetch=lambda url: [{"id": 49, "price": "1", "qty": "1", "time": 1,
                                                   "isBuyerMaker": False}])
        self.assertEqual(r.first_id["XRPTHB"], 48)
        self.assertFalse((self.out / "gaps.csv").exists())

    def test_reference_quotes_throttled(self):
        r = self.rec
        r.on_message(ref("XRPUSDT", "1.25", "1.26"), 1000)
        r.on_message(ref("XRPUSDT", "1.25", "1.26", B="9"), 1100)   # size only: ignored
        r.on_message(ref("XRPUSDT", "1.24", "1.26"), 1200)          # too soon: kept as pending
        r.on_message(ref("XRPUSDT", "1.23", "1.26"), 1300)          # replaces the pending one
        r.flush_ref(1400)
        r.flush_ref(1600)
        r.on_message(ref("XRPUSDT", "1.22", "1.25"), 2400)
        got = self.file("ref", "XRPUSDT")
        self.assertEqual([x[:2] for x in got[1:]], [["1000", "1.25"], ["1300", "1.23"], ["2400", "1.22"]])

    def test_unknown_and_reply_messages(self):
        self.rec.on_message(json.dumps({"id": 1, "result": None}), 1)
        self.rec.on_message(trade("BTCTHB", 1), 2)
        self.assertEqual(self.rec.messages, 0)

    def test_heartbeat(self):
        self.rec.on_message(depth("XRPTHB", 1, 1, E=900), 1000)
        rows_ = self.rec.heartbeat(5000, 2, 0, -12)
        self.assertEqual(rows_[0][2:15], ["XRPTHB", 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 100, 100])
        self.assertEqual(rows_[-1][2], "ALL")
        self.assertEqual(rows_[-1][-1], -12)


class WireTests(unittest.TestCase):
    def test_long_subscribe_frame_is_valid(self):
        sent = []

        class Sock:
            def sendall(self, b):
                sent.append(b)

        ws = c.BnWebSocket(Sock())
        text = json.dumps({"method": "SUBSCRIBE", "params": [f"s{i}@depth@100ms" for i in range(20)], "id": 1})
        self.assertGreater(len(text), 125)
        ws.send_text(text)
        reader = bk.WebSocket(None, sent[0])
        fin, op, payload = reader._frame()
        self.assertEqual((fin, op, payload.decode()), (True, 1, text))

    def test_connector_resets_books(self):
        rec = c.Recorder(Path(tempfile.mkdtemp()), ["XRPTHB"], [])
        rec.books["XRPTHB"].on_snapshot(SNAP)
        opened = []
        conn = c.connector(rec, "thb", ["xrpthb@trade"], open_ws=lambda s: opened.append(s) or "ws")
        self.assertEqual(conn("ignored"), "ws")
        self.assertEqual(opened, [["xrpthb@trade"]])
        self.assertIsNone(rec.books["XRPTHB"].last_u)
        self.assertIn("XRPTHB", rec.fill_due)


class MergeTests(unittest.TestCase):
    def test_merge_runs(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for run, trades, books in (
                    ("20261031-000000", [[100, 90, 7, "1", "1", "buy", "ws"], [300, 290, 9, "1", "1", "sell", "ws"]],
                     [[100] + ["1"] * 40 + [90], [400] + ["2"] * 40 + [390]]),
                    ("20261031-052000", [[250, 290, 9, "1", "1", "sell", "rest"], [350, 340, 10, "1", "1", "buy", "ws"]],
                     [[300] + ["3"] * 40 + [290]])):
                rec = c.Recorder(root / run, ["XRPTHB"], [])
                rec.w.write("trades", "XRPTHB", trades)
                rec.w.write("book10", "XRPTHB", books)
                rec.w.close()
            # the 1970 day folder comes from ms timestamps near 0
            self.assertEqual(c.merge_dir(root, root / "merged"), 2)
            t = rows(root / "merged" / "1970-01-01" / "trades_XRPTHB.csv.gz")
            self.assertEqual([(x[0], x[2], x[6]) for x in t[1:]], [("100", "7", "ws"), ("250", "9", "rest"),
                                                                 ("350", "10", "ws")])
            b = rows(root / "merged" / "1970-01-01" / "book10_XRPTHB.csv.gz")
            self.assertEqual([x[0] for x in b[1:]], ["100", "300"])   # run 1 until run 2's first row
            self.assertFalse((root / "merged" / "gaps.csv").exists())  # the runs overlap: no hole

    def test_hole_between_runs(self):
        self.assertEqual(bk.run_holes({"a": [0, 1000], "b": [500, 70_000], "c": [200_000, 300_000]}),
                         [(70_000, 200_000)])
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for run, t in (("r1", [100, 5000]), ("r2", [200_000, 250_000])):
                rec = c.Recorder(root / run, ["XRPTHB"], [])
                rec.w.write("book10", "XRPTHB", [[x] + ["1"] * 40 + [x] for x in t])
                rec.w.close()
            c.merge_dir(root, root / "merged")
            g = plain(root / "merged" / "gaps.csv")
            self.assertEqual(g[1][:5], ["merge", "thb", "ALL", "5000", "200000"])
            self.assertEqual(g[2][:5], ["merge", "book_XRPTHB", "XRPTHB", "5000", "200000"])

    def test_book_coverage_gaps(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            # r1 records XRP from 100; r2 overlaps it but syncs XRP only at 9000 after r1 ended at 6000; BTC only in r1
            for run, trade_t, books in (("r1", [100, 6000], {"XRPTHB": [100, 5000], "BTCTHB": [200]}),
                                        ("r2", [4000, 20_000], {"XRPTHB": [9000, 20_000]})):
                rec = c.Recorder(root / run, ["XRPTHB"], [])
                rec.w.write("trades", "XRPTHB", [[t, t, t, "1", "1", "buy", "ws"] for t in trade_t])
                for sym, ts in books.items():
                    rec.w.write("book10", sym, [[x] + ["1"] * 40 + [x] for x in ts])
                rec.w.close()
            c.merge_dir(root, root / "merged")
            g = [r[1:5] for r in plain(root / "merged" / "gaps.csv")[1:]]
            self.assertEqual(g, [["book_BTCTHB", "BTCTHB", "100", "200"], ["book_BTCTHB", "BTCTHB", "6000", "20000"],
                                 ["book_XRPTHB", "XRPTHB", "6000", "9000"]])

    def test_decrypt_only_takes_this_recorders_files(self):
        with tempfile.TemporaryDirectory() as d:
            src = Path(d) / "dl"
            src.mkdir()
            (src / "stream_20261031-000000.tar.enc").write_bytes(b"not ours")
            self.assertEqual(c.decrypt_dir(src, Path(d) / "runs", "k" * 20), 0)
            (src / "stream_20261031-000000.tar.enc").unlink()
            (src / "bnth_20261031-000000.tar.enc").write_bytes(b"not Bitkub's")
            self.assertEqual(bk.decrypt_dir(src, Path(d) / "runs", "k" * 20), 0)   # and the other way round

    def test_downloads_are_kept_apart(self):
        rel = [{"assets": [{"name": "stream_1.tar.enc", "browser_download_url": "x"},
                           {"name": "bnth_1.tar.enc", "browser_download_url": "y"}]}]
        for mod, want in ((c, "y"), (bk, "x")):
            got = []
            with tempfile.TemporaryDirectory() as d, \
                    unittest.mock.patch.object(mod.urllib.request, "urlretrieve", lambda u, p: got.append(u)):
                self.assertEqual(mod.download("o/r", Path(d), fetch=lambda url: rel), 1)
            self.assertEqual(got, [want])


if __name__ == "__main__":
    unittest.main()
