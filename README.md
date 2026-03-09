# Daily Stock Pipeline

A modular end-to-end daily market data pipeline built with Python and SQLite.

This project downloads daily OHLCV data from Stooq for a watchlist of symbols, loads it into a raw layer, rebuilds a symbol-scoped staging layer, merges into curated tables with revision tracking, and exposes a simple watchlist report.

## Features

- Watchlist-driven multi-symbol pipeline
- Incremental loading with watermark + safety overlap
- Raw / staging / curated data model
- Row-hash-based revision detection
- Run-level and symbol-level logging
- Cached CSV downloads with optional refresh
- CLI for running, reporting, and managing symbols

## Project structure

```text
daily-stock/
├── pipeline.py
├── stock.db
├── data/
└── daily_stock/
    ├── __init__.py
    ├── util.py
    ├── extract.py
    ├── db.py
    ├── load.py
    ├── transform.py
    ├── runlog.py
    ├── watermarks.py
    ├── watchlist.py
    └── report.py
'''

## Pipeline flow

For each active symbol:

1. Read the last successful watermark
2. Build an incremental date window with a safety overlap
3. Download or reuse a cached CSV
4. Load rows into `raw_stooq_daily_prices`
5. Rebuild `stg_daily_prices` for that symbol
6. Merge into `cur_daily_prices`
7. Update the watermark and run logs

## Data model

### Raw

`raw_stooq_daily_prices`

Append-oriented landing table containing downloaded source rows plus ingestion metadata and a row hash.

### Staging

`stg_daily_prices`

Rebuilt per symbol for the current run. Since staging does not key on `run_id`, the pipeline deletes existing staging rows for that symbol before rebuilding.

### Curated

`cur_daily_prices`

Analysis-ready daily prices keyed by provider + symbol + trading date, with revision metadata such as `revision_count` and `is_revised`.

## CLI usage

Run the pipeline:

```bash
python pipeline.py
'''

Force refresh downloads:

'''bash
python pipeline.py --refresh
'''

Verbose mode:

'''bash
python pipeline.py --verbose
'''

Show report:

'''bash
python pipeline.py --report
'''

Add a symbol:

'''bash
python pipeline.py --add-symbol NVDA.US
'''

Remove a symbol:

'''bash
python pipeline.py --remove-symbol NVDA.US
'''

##Example Behavior

###Existing symbols on rerun
- reload recent window into raw
- rebuild staging
- refresh unchanged curated rows

###New symbols
- backfill full history
- insert all available dates into curated
- initialize a watermark for future incremental runs

##Why this project matters
This was built as a data engineering portfolio project, not just a finance script.
The focus is on pipeline design and operational behavior:
- layered storage
- incremental processing
- rerunnable loads
- revision tracking
- logging and recoverability
- modular code structure

##Tech stack
- Python
- SQLite
- CSV ingestion
- Local file caching

##Next improvements
- Add automated tests
- Move from SQLite to Postgres
- Containerize the pipeline
- Schedule daily runs
- Add charts or dashboard output
