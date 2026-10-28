# Bitkub stream recorder

Paper-trading research, not financial advice. This repository only **records public Bitkub market data** (every trade,
the top 10 order-book levels, the best bid/ask) for 21 coins through Bitkub's public WebSocket streams. It places no
orders and holds no exchange API keys.

- `bitkub_stream_collector.py`: the recorder (Python standard library only).
- `test_bitkub_stream_collector.py`: its tests (synthetic data; run before every recording).
- `.github/workflows/stream.yml`: runs the recorder back-to-back on GitHub Actions (5.5-hour runs that overlap by a
  few minutes; an hourly timer restarts the chain if it ever stops).

The recorded files are **encrypted** before upload (GitHub Releases named `data-YYYYMM`). Bitkub's User Agreement
lets users keep price data for their own investing or education, not redistribute it, so nothing readable is
published here.
