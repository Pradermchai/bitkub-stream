# Bitkub stream recorder

Paper-trading research, not financial advice. This repository only **records public Bitkub market data** (every trade,
the top 10 order-book levels, the best bid/ask) for 40 Bitkub "exchange" THB coins plus USDT/THB, through Bitkub's
public WebSocket streams. It places no orders and holds no exchange API keys.

- `bitkub_stream_collector.py`: the recorder (Python standard library only).
- `test_bitkub_stream_collector.py`: its tests (synthetic data; run before every recording).
- `select_exchange_coins.py`: the frozen rule that picked the 40 coins (kept for the record; not run by the workflow).
- `.github/workflows/stream.yml`: runs the recorder back-to-back on GitHub Actions (5.5-hour runs that overlap by a
  few minutes; an hourly timer restarts the chain if it ever stops).

The recorded files are **encrypted** before upload (GitHub Releases named `data-YYYYMM`). Bitkub's User Agreement
lets users keep price data for their own investing or education, not redistribute it, so nothing readable is
published here.

## Binance TH recorder (second, separate chain)
- `binance_th_collector.py`: records the public order books (rebuilt top 10 levels) and every trade with its taker
  side for the 6 Binance TH THB coins of the frozen Test Q list, plus USDT/THB, and the best quotes of the matching
  USDT pairs as a world-price reference. Same encryption and key; releases named `bnth-YYYYMM`, files `bnth_*.tar.enc`.
- `test_binance_th_collector.py`: its tests. `select_bnth_coins.py`: the frozen coin rule (not run by the workflow).
- `.github/workflows/binance_th.yml`: same 5.5-hour hand-over chain as `stream.yml`, independent of it.

`handoff.py` (used by both workflows) starts the next run near the end of each run, unless a newer run of the same
recorder is already recording; so if the backup timer ever starts a second chain, the two shrink back to one.
