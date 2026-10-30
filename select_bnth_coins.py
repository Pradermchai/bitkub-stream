#!/usr/bin/env python3
"""
select_bnth_coins.py — frozen coin-selection rule for the Binance TH THB-pair liquidity study (Test Q).

Paper-trading research, not financial advice. Public Binance TH data only (exchangeInfo + daily bars), no keys.

Rule (written 2026-10-31 before any order-book data of these coins was recorded; reports/next_session_plan.md B2):
  1. GET /api/v1/exchangeInfo (the `symbol=` parameter is ignored there, so the full list is read): quote THB,
     status TRADING.
  2. Drop bases pegged to a currency or to gold (PEGGED below).
  3. Daily bars from /api/v1/klines?interval=1d (UTC days); THB value of a day = quote volume.
     Keep pairs whose MEDIAN daily THB value over the 7 complete UTC days 2026-10-23 .. 2026-10-29 is >= THB 200,000
     (the same window and bar as Test P).
  4. Keep all passers (only 11 THB pairs are listed).

  python bitkub_stream/select_bnth_coins.py                                   # prints the table and the Python list
  python bitkub_stream/select_bnth_coins.py --save reports/test_q_coins.csv   # also saves every pair + its filters
"""

from __future__ import annotations

import json
import statistics
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List

REST = "https://api.binance.th/api/v1"
PEGGED = {"USDT", "USDC", "DAI", "FDUSD", "USD1", "RLUSD", "PYUSD", "TUSD", "USDE", "USDS", "BUSD", "XAUT", "PAXG"}
DAYS = [datetime(2026, 10, d, tzinfo=timezone.utc) for d in range(23, 30)]
MIN_MEDIAN_THB = 200_000


def get(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "quant-brain-research/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def day_values(sym: str) -> Dict[int, float]:
    frm = int(DAYS[0].timestamp() * 1000)
    to = int(DAYS[-1].timestamp() * 1000) + 86400000 - 1
    bars = get(f"{REST}/klines?symbol={sym}&interval=1d&startTime={frm}&endTime={to}&limit=10")
    return {int(b[0]) // 1000: float(b[7]) for b in bars}


def filters(s: Dict[str, Any]) -> Dict[str, str]:
    f = {x["filterType"]: x for x in s.get("filters", [])}
    return {"tick": f.get("PRICE_FILTER", {}).get("tickSize", ""), "step": f.get("LOT_SIZE", {}).get("stepSize", ""),
            "min_notional": f.get("NOTIONAL", f.get("MIN_NOTIONAL", {})).get("minNotional", "")}


def select() -> List[Dict[str, Any]]:
    syms = get(f"{REST}/exchangeInfo")["symbols"]
    thb = [s for s in syms if s.get("quoteAsset") == "THB"]
    cand = [s for s in thb if s.get("status") == "TRADING" and s.get("baseAsset") not in PEGGED]
    rows = []
    for s in cand:
        vals = day_values(s["symbol"])
        per_day = [vals.get(int(d.timestamp()), 0.0) for d in DAYS]   # a missing day counts as zero
        rows.append({"symbol": s["symbol"], "type": s.get("symbolType", ""), "median_thb": statistics.median(per_day),
                     "min_thb": min(per_day), "max_thb": max(per_day), **filters(s)})
        time.sleep(0.1)
    rows.sort(key=lambda r: -r["median_thb"])
    for i, r in enumerate(rows, 1):
        r["rank"], r["selected"] = i, r["median_thb"] >= MIN_MEDIAN_THB
    print(f"{len(thb)} THB pairs, {len(cand)} candidates, {sum(r['selected'] for r in rows)} pass the volume rule")
    return rows


if __name__ == "__main__":
    import csv
    import sys
    allrows = select()
    for r in allrows:
        print(f"{r['rank']:2d} {r['symbol']:<10} {r['type']:<7} median THB {r['median_thb']:>14,.0f}  "
              f"min {r['min_thb']:>14,.0f}  max {r['max_thb']:>14,.0f}  tick {r['tick']}  step {r['step']}  "
              f"{'SELECTED' if r['selected'] else ''}")
    print([r["symbol"] for r in allrows if r["selected"]])
    if "--save" in sys.argv:
        path = sys.argv[sys.argv.index("--save") + 1]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([f"# saved {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by select_bnth_coins.py"])
            w.writerow(["rank", "symbol", "selected", "median_thb", "min_thb", "max_thb", "price_step", "qty_step",
                        "min_notional", "symbol_type"])
            for r in allrows:
                w.writerow([r["rank"], r["symbol"], r["selected"], round(r["median_thb"]), round(r["min_thb"]),
                            round(r["max_thb"]), r["tick"], r["step"], r["min_notional"], r["type"]])
        print("saved", path)
