import sqlite3
import argparse
from datetime import datetime, timezone, timedelta

from daily_stock.util import utc_now_iso, make_run_id, parse_yyyy_mm_dd, iso_yyyy_mm_dd
from daily_stock.extract import extract_stooq_csv
from daily_stock.db import init_db
from daily_stock.load import load_csv_to_raw
from daily_stock.transform import rebuild_staging_for_run_symbol, merge_stg_to_curated
from daily_stock.runlog import insert_run_start, finalize_run, insert_symbol_start, finalize_symbol
from daily_stock.watermarks import get_last_success_date, upsert_watermark_success, upsert_watermark_failed
from daily_stock.watchlist import get_active_watchlist, upsert_watchlist_symbol, mark_removed_watchlist, seed_watchlist_if_empty
from daily_stock.report import fetch_watchlist_report, print_watchlist_report


#Constants
SAFETY_DAYS = 10
DB_PATH = "stock.db"
PROVIDER = "stooq"



def run_one_symbol(conn: sqlite3.Connection, provider_symbol: str, run_id: str, force_download: bool = False, verbose: bool = False) -> None:
    # 0) Determine incremental window first (so it is logged even if download fails)
    last_success = get_last_success_date(conn, PROVIDER, provider_symbol)
    if last_success is None:
        date_from = "1900-01-01"
    else:
        date_from = iso_yyyy_mm_dd(parse_yyyy_mm_dd(last_success) - timedelta(days=SAFETY_DAYS))
    date_to = iso_yyyy_mm_dd(datetime.now(timezone.utc).date())

    sym_started_at = utc_now_iso()
    insert_symbol_start(conn, run_id, PROVIDER, provider_symbol, date_from, date_to, sym_started_at)
    conn.commit()

    raw_count = stg_count = ins = upd = same = 0
    try:
        # 1) Extract latest CSV for this symbol
        csv_path = extract_stooq_csv(provider_symbol, force=force_download)

        print(f"\n=== {provider_symbol} window: {date_from} to {date_to} ===")

        # 2) Run pipeline steps for this symbol
        raw_count = load_csv_to_raw(conn, csv_path, PROVIDER, provider_symbol, run_id, date_from, date_to)
        stg_count = rebuild_staging_for_run_symbol(conn, run_id, PROVIDER, provider_symbol)  # staging is symbol-scoped
        ins, upd, same = merge_stg_to_curated(conn, run_id, PROVIDER, provider_symbol)

        # 3) Update watermark on success
        max_date = conn.execute("""
            SELECT MAX(trading_date)
            FROM cur_daily_prices
            WHERE provider = ? AND provider_symbol = ?;
        """, (PROVIDER, provider_symbol)).fetchone()[0]

        upsert_watermark_success(conn, PROVIDER, provider_symbol, max_date, run_id, utc_now_iso())

        # 4) Finalize symbol log (success)
        finalize_symbol(
            conn,
            run_id=run_id,
            provider=PROVIDER,
            provider_symbol=provider_symbol,
            ended_at=utc_now_iso(),
            status="success",
            date_from=date_from,
            date_to=date_to,
            raw_count=raw_count,
            stg_count=stg_count,
            ins=ins,
            upd=upd,
            same=same,
            error_message=None
        )
        conn.commit()

        # ---- your existing prints + samples (unchanged) ----
        print("curated inserted:", ins)
        print("curated updated (revised):", upd)
        print("curated unchanged (metadata refreshed):", same)

        cur_cnt = conn.execute("SELECT COUNT(*) FROM cur_daily_prices;").fetchone()[0]
        print("curated total rows:", cur_cnt)
        print("run_id:", run_id)
        print("raw rows for this run+symbol:", raw_count)
        print("stg rows for this run:", stg_count)

        if verbose:
            print("\nRAW sample:")
            for r in conn.execute("""
                SELECT provider_symbol, trading_date, open, close, volume, ingested_at, run_id, row_hash
                FROM raw_stooq_daily_prices
                WHERE run_id = ? AND provider_symbol = ?
                ORDER BY trading_date ASC
                LIMIT 2;
            """, (run_id, provider_symbol)):
                print(r)
    
            print("\nSTG sample:")
            for r in conn.execute("""
                SELECT provider_symbol, trading_date, open, close, volume, raw_row_count, raw_max_ingested_at
                FROM stg_daily_prices
                WHERE run_id = ? AND provider_symbol = ?
                ORDER BY trading_date ASC
                LIMIT 2;
            """, (run_id, provider_symbol)):
                print(r)

    except Exception as e:
        # Finalize symbol log (failed) with whatever counts were reached so far
        finalize_symbol(
            conn,
            run_id=run_id,
            provider=PROVIDER,
            provider_symbol=provider_symbol,
            ended_at=utc_now_iso(),
            status="failed",
            date_from=date_from,
            date_to=date_to,
            raw_count=raw_count,
            stg_count=stg_count,
            ins=ins,
            upd=upd,
            same=same,
            error_message=str(e)
        )
        conn.commit()
        raise



def main(force_download: bool = False, verbose: bool = False) -> None:
    run_id = make_run_id()

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        seed_watchlist_if_empty(conn, PROVIDER)

        symbols = get_active_watchlist(conn, PROVIDER)
        print("active symbols:", symbols)
        if not symbols:
            raise ValueError("No active symbols in watchlist_symbols.")

        run_started_at = utc_now_iso()
        insert_run_start(conn, run_id, run_started_at, symbols_total=len(symbols))
        conn.commit()

        symbols_success = 0
        symbols_failed = 0
        
        for sym in symbols:
            try:
                run_one_symbol(conn, sym, run_id, force_download=force_download, verbose=verbose)
                symbols_success += 1
            except Exception as e:
                upsert_watermark_failed(conn, PROVIDER, sym, run_id, utc_now_iso())
                conn.commit()
                symbols_failed += 1
                print(f"!!! FAILED symbol {sym}: {e}")
                #keep going for other symbols
                continue
        
        finalize_run(conn, run_id, utc_now_iso(), symbols_success, symbols_failed)
        conn.commit()
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--add-symbol")
    parser.add_argument("--remove-symbol")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--report", action="store_true")
    
    args = parser.parse_args()

    if args.add_symbol:
        with sqlite3.connect(DB_PATH) as conn:
            init_db(conn)
            upsert_watchlist_symbol(conn, PROVIDER, args.add_symbol)
        print(f"Added/activated {args.add_symbol}")
    elif args.remove_symbol:
        with sqlite3.connect(DB_PATH) as conn:
            init_db(conn)
            mark_removed_watchlist(conn, PROVIDER, args.remove_symbol, utc_now_iso())
            conn.commit()
        print(f"Removed/deactivated {args.remove_symbol}")
    elif args.report:
        with sqlite3.connect(DB_PATH) as conn:
            init_db(conn)
            rows = fetch_watchlist_report(conn)
        print_watchlist_report(rows)
    else:
        main(force_download=args.refresh, verbose=args.verbose)