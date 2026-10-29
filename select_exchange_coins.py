#!/usr/bin/env python3
"""
select_exchange_coins.py — frozen coin-selection rule for the Bitkub exchange-pair liquidity study (Test P).

Paper-trading research, not financial advice. Public Bitkub data only (symbol list + daily bars), no keys.

Rule (written 2026-10-30 before any order-book data of these coins was recorded; see reports/test_p_plan.md):
  1. GET /api/v3/market/symbols: quote_asset THB, source "exchange", status "active", freeze_buy and freeze_sell false.
  2. Drop bases pegged to a currency or to gold (PEGGED below): their spread is not a crypto price risk.
  3. Daily bars from /tradingview/history (UTC days). THB value of a day = base volume x (open+high+low+close)/4.
     Keep coins whose MEDIAN daily THB value over the 7 complete UTC days 2026-10-23 .. 2026-10-29 is >= THB 200,000.
  4. Rank by that median, keep the top 40 (Bitkub opens one order-book connection per coin; 31 ran fine live).

  python bitkub_stream/select_exchange_coins.py            # prints the table and the Python list
  python bitkub_stream/select_exchange_coins.py --save reports/test_p_coins.csv   # also saves every passer + tick
"""

from __future__ import annotations

import json
import statistics
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List

HOST = "https://api.bitkub.com"
PEGGED = {"USDT", "USDC", "DAI", "FDUSD", "USD1", "RLUSD", "PYUSD", "TUSD", "USDE", "USDS", "BUSD", "XAUT", "PAXG"}
DAYS = [datetime(2026, 10, d, tzinfo=timezone.utc) for d in range(23, 30)]
MIN_MEDIAN_THB = 200_000
TOP_N = 40


def get(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "quant-brain-research/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def day_values(sym: str) -> Dict[int, float]:
    frm = int(DAYS[0].timestamp())
    to = int(DAYS[-1].timestamp()) + 86400
    b = get(f"{HOST}/tradingview/history?symbol={sym}&resolution=1D&from={frm}&to={to}")
    if b.get("s") != "ok":
        return {}
    return {int(t): float(v) * (float(o) + float(h) + float(lo) + float(c)) / 4
            for t, v, o, h, lo, c in zip(b["t"], b["v"], b["o"], b["h"], b["l"], b["c"])}


def select() -> List[Dict[str, Any]]:
    syms = get(f"{HOST}/api/v3/market/symbols")["result"]
    cand = [s for s in syms if s.get("quote_asset") == "THB" and s.get("source") == "exchange"
            and s.get("status") == "active" and not s.get("freeze_buy") and not s.get("freeze_sell")
            and s.get("base_asset") not in PEGGED]
    rows = []
    for s in cand:
        vals = day_values(s["symbol"])
        per_day = [vals.get(int(d.timestamp()), 0.0) for d in DAYS]   # a missing day counts as zero
        rows.append({"symbol": s["symbol"], "median_thb": statistics.median(per_day),
                     "min_thb": min(per_day), "max_thb": max(per_day), "price_step": s.get("price_step", "")})
        time.sleep(0.1)
    keep = sorted((r for r in rows if r["median_thb"] >= MIN_MEDIAN_THB), key=lambda r: -r["median_thb"])
    for i, r in enumerate(keep, 1):
        r["rank"], r["selected"] = i, i <= TOP_N
    print(f"{len(cand)} candidate exchange THB pairs, {len(keep)} pass the volume rule, keeping {min(len(keep), TOP_N)}")
    return keep


if __name__ == "__main__":
    import csv
    import sys
    passed = select()
    for r in passed:
        print(f"{r['rank']:2d} {r['symbol']:<12} median THB {r['median_thb']:>14,.0f}  min {r['min_thb']:>14,.0f}  "
              f"max {r['max_thb']:>14,.0f}  tick {r['price_step']}  {'SELECTED' if r['selected'] else ''}")
    print([r["symbol"] for r in passed if r["selected"]])
    if "--save" in sys.argv:
        path = sys.argv[sys.argv.index("--save") + 1]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([f"# saved {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by select_exchange_coins.py"])
            w.writerow(["rank", "symbol", "selected", "median_thb", "min_thb", "max_thb", "price_step"])
            for r in passed:
                w.writerow([r["rank"], r["symbol"], r["selected"], round(r["median_thb"]), round(r["min_thb"]),
                            round(r["max_thb"]), r["price_step"]])
        print("saved", path)
