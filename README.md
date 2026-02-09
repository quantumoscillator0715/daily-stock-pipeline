\# Daily Stock OHLCV Pipeline (SQLite)



Production-style pipeline for \*\*daily stock/ETF OHLCV\*\* data using \*\*Stooq CSV\*\*.



\## Features

\- Multi-symbol watchlist (e.g., `AAPL.US`, `MSFT.US`)

\- RAW → STG → CUR layers

\- Grain: 1 row per `(provider, symbol, trading\_date)`

\- Idempotent upsert into curated

\- Incremental loads via per-symbol watermark + safety window

\- Revision detection using `row\_hash`



\## CSV format (Stooq)



Header must be:



&nbsp;   Date,Open,High,Low,Close,Volume



Note: `Volume` is stored as REAL because some series contain fractional values.



\## How to run



\### 1) Add symbols

&nbsp;   python pipeline.py --add-symbol AAPL.US

&nbsp;   python pipeline.py --add-symbol MSFT.US



\### 2) Put CSV files in the project folder

Naming convention:

\- `AAPL.US` → `aapl\_us\_d.csv`

\- `MSFT.US` → `msft\_us\_d.csv`



\### 3) Run the pipeline

&nbsp;   python pipeline.py



\## Output (SQLite)

Creates/updates `stock.db` with tables:

\- `watchlist\_symbols`

\- `watermark\_daily\_prices`

\- `raw\_stooq\_daily\_prices`

\- `stg\_daily\_prices`

\- `cur\_daily\_prices`

