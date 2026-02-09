\# Daily Stock OHLCV Pipeline (SQLite)



A production-style data pipeline for \*\*daily stock/ETF OHLCV\*\* data using \*\*Stooq CSV\*\*. Built to be portfolio-ready with incremental loads and idempotent merges.



\## Features

\- \*\*Multi-symbol watchlist\*\* (e.g., `AAPL.US`, `MSFT.US`)

\- \*\*RAW → STG → CUR\*\* layers

\- \*\*Grain:\*\* 1 row per `(provider, symbol, trading\_date)`

\- \*\*Idempotent upsert\*\* into curated using `(provider, symbol, trading\_date)`

\- \*\*Incremental loads\*\* using per-symbol \*\*watermark + safety window\*\*

\- \*\*Revision detection\*\* using `row\_hash`



\## Expected CSV format (Stooq)

Header: Date,Open,High,Low,Close,Volume



Note: `Volume` is stored as \*\*REAL\*\* because some series contain fractional values.



\## How to run



\### 1) Add symbols to the watchlist



python pipeline.py --add-symbol AAPL.US

python pipeline.py --add-symbol MSFT.US



\### 2) Place CSV files in the project folder

Naming convention:



\*AAPL.US → aapl\_us\_d.csv



\*MSFT.US → msft\_us\_d.csv



\### 3) Run the pipeline



python pipeline.py



\## Output



Creates/updates stock.db with:



\*watchlist\_symbols



\*watermark\_daily\_prices



\*raw\_stooq\_daily\_prices



\*stg\_daily\_prices



\*cur\_daily\_prices

