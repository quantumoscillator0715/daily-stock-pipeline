import sqlite3
import argparse
import math
from datetime import datetime, timezone, date, timedelta

from daily_stock.util import utc_now_iso, make_run_id, compute_row_hash, parse_yyyy_mm_dd, iso_yyyy_mm_dd
from daily_stock.extract import extract_stooq_csv
from daily_stock.db import init_db
from daily_stock.load import load_csv_to_raw

#constants
SAFETY_DAYS = 10
DB_PATH = "stock.db"
PROVIDER = "stooq"
DATA_DIR = Path("data")





def rebuild_staging_for_run_symbol(conn: sqlite3.Connection, run_id: str, provider_symbol: str) -> int:
    """
    Rebuild STG for ONE symbol and ONE run_id.
    """
    stg_ingested_at = utc_now_iso()

    # Delete by provider + symbol (because STG PK does not include run_id)
    conn.execute("""
        DELETE FROM stg_daily_prices
        WHERE provider = ? AND provider_symbol = ?;
    """, (PROVIDER, provider_symbol))

    conn.execute("""
    INSERT INTO stg_daily_prices (
      provider, provider_symbol, trading_date,
      open, high, low, close, volume, row_hash,
      run_id, ingested_at, raw_max_ingested_at, raw_row_count
    )
    SELECT
      r.provider,
      r.provider_symbol,
      r.trading_date,
      r.open,
      r.high,
      r.low,
      r.close,
      r.volume,
      r.row_hash,
      r.run_id,
      ? AS ingested_at,
      x.raw_max_ingested_at,
      x.raw_row_count
    FROM raw_stooq_daily_prices r
    JOIN (
      SELECT
        provider,
        provider_symbol,
        trading_date,
        MAX(ingested_at) AS raw_max_ingested_at,
        COUNT(*) AS raw_row_count
      FROM raw_stooq_daily_prices
      WHERE run_id = ?
        AND provider = ?
        AND provider_symbol = ?
      GROUP BY provider, provider_symbol, trading_date
    ) x
      ON r.provider = x.provider
     AND r.provider_symbol = x.provider_symbol
     AND r.trading_date = x.trading_date
     AND r.ingested_at = x.raw_max_ingested_at
    WHERE r.run_id = ?
      AND r.provider = ?
      AND r.provider_symbol = ?;
    """, (stg_ingested_at, run_id, PROVIDER, provider_symbol, run_id, PROVIDER, provider_symbol))

    cur = conn.execute("""
        SELECT COUNT(*)
        FROM stg_daily_prices
        WHERE run_id = ? AND provider = ? AND provider_symbol = ?;
    """, (run_id, PROVIDER, provider_symbol))
    return cur.fetchone()[0]

def merge_stg_to_curated(conn: sqlite3.Connection, run_id: str, provider_symbol: str) -> tuple[int, int, int]:
    """
    Merge STG rows for one (provider, provider_symbol) and one run_id into CUR.

    Returns: (inserted_rows, updated_rows, unchanged_rows)
    """

    provider = PROVIDER  # keep it explicit for clarity

    # 1) Safety check per provider+symbol
    cur = conn.execute("""
        SELECT COUNT(*)
        FROM stg_daily_prices
        WHERE run_id = ? AND provider = ? AND provider_symbol = ?;
    """, (run_id, provider, provider_symbol))
    stg_count = cur.fetchone()[0]
    if stg_count == 0:
        raise ValueError(f"No staging rows found for provider={provider} symbol={provider_symbol} run_id={run_id}")

    # 2) INSERT brand-new keys into curated
    conn.execute("""
    INSERT INTO cur_daily_prices (
      provider, provider_symbol, trading_date,
      open, high, low, close, volume, row_hash,
      first_ingested_at, last_ingested_at, last_run_id,
      revision_count, is_revised
    )
    SELECT
      s.provider, s.provider_symbol, s.trading_date,
      s.open, s.high, s.low, s.close, s.volume, s.row_hash,
      s.ingested_at, s.ingested_at, s.run_id,
      0, 0
    FROM stg_daily_prices s
    WHERE s.run_id = ?
      AND s.provider = ?
      AND s.provider_symbol = ?
      AND NOT EXISTS (
        SELECT 1
        FROM cur_daily_prices c
        WHERE c.provider = s.provider
          AND c.provider_symbol = s.provider_symbol
          AND c.trading_date = s.trading_date
      );
    """, (run_id, provider, provider_symbol))
    inserted = conn.execute("SELECT changes();").fetchone()[0]

    # 3) UPDATE changed rows (hash differs): overwrite measures, bump revision_count, mark revised
    conn.execute("""
    UPDATE cur_daily_prices AS c
    SET
      (open, high, low, close, volume, row_hash, last_ingested_at, last_run_id) = (
        SELECT
          s.open, s.high, s.low, s.close, s.volume, s.row_hash, s.ingested_at, s.run_id
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
          AND s.row_hash <> c.row_hash
      ),
      revision_count = revision_count + 1,
      is_revised = 1
    WHERE c.provider = ?
      AND c.provider_symbol = ?
      AND EXISTS (
        SELECT 1
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
          AND s.row_hash <> c.row_hash
      );
    """, (
        run_id, provider, provider_symbol,          # subquery params
        provider, provider_symbol,                  # outer row filter
        run_id, provider, provider_symbol           # EXISTS params
    ))
    updated = conn.execute("SELECT changes();").fetchone()[0]

    # 4) UPDATE unchanged rows (hash same): refresh only metadata
    conn.execute("""
    UPDATE cur_daily_prices AS c
    SET
      (last_ingested_at, last_run_id) = (
        SELECT
          s.ingested_at, s.run_id
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
          AND s.row_hash = c.row_hash
      )
    WHERE c.provider = ?
      AND c.provider_symbol = ?
      AND EXISTS (
        SELECT 1
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
          AND s.row_hash = c.row_hash
      );
    """, (
        run_id, provider, provider_symbol,          # subquery params
        provider, provider_symbol,                  # outer row filter
        run_id, provider, provider_symbol           # EXISTS params
    ))
    unchanged = conn.execute("SELECT changes();").fetchone()[0]

    return inserted, updated, unchanged



def get_last_success_date(conn: sqlite3.Connection, provider: str, provider_symbol: str) -> str | None:
    """Return last_success_date (YYYY-MM-DD) from watermarks, or None if missing."""
    cur = conn.execute("""
        SELECT last_success_date
        FROM pipeline_watermarks
        WHERE provider = ? AND provider_symbol = ?;
    """, (provider, provider_symbol))
    row = cur.fetchone()
    return row[0] if row else None

def upsert_watermark_success(conn: sqlite3.Connection, provider: str, provider_symbol: str,
                            last_success_date: str, run_id: str, updated_at: str) -> None:
    """Insert/update watermark row to success."""
    conn.execute("""
        INSERT INTO pipeline_watermarks (
          provider, provider_symbol, last_success_date, last_run_id, last_updated_at, status
        )
        VALUES (?, ?, ?, ?, ?, 'success')
        ON CONFLICT(provider, provider_symbol) DO UPDATE SET
          last_success_date = excluded.last_success_date,
          last_run_id       = excluded.last_run_id,
          last_updated_at   = excluded.last_updated_at,
          status            = 'success';
    """, (provider, provider_symbol, last_success_date, run_id, updated_at))


def upsert_watermark_failed(conn: sqlite3.Connection, provider: str, provider_symbol: str,
                           run_id: str, updated_at: str) -> None:
    """Mark watermark row as failed (keep last_success_date unchanged if it exists)."""
    conn.execute("""
        INSERT INTO pipeline_watermarks (
          provider, provider_symbol, last_success_date, last_run_id, last_updated_at, status
        )
        VALUES (?, ?, '1900-01-01', ?, ?, 'failed')
        ON CONFLICT(provider, provider_symbol) DO UPDATE SET
          last_run_id       = excluded.last_run_id,
          last_updated_at   = excluded.last_updated_at,
          status            = 'failed';
    """, (provider, provider_symbol, run_id, updated_at))
    
def get_active_watchlist(conn: sqlite3.Connection, provider: str) -> list[str]:
    cur = conn.execute("""
      SELECT provider_symbol
      FROM watchlist_symbols
      WHERE provider = ? AND is_active = 1
      ORDER BY provider_symbol;
    """, (provider,))
    return [r[0] for r in cur.fetchall()]


def mark_removed_watchlist(conn: sqlite3.Connection, provider: str, provider_symbol: str, removed_at: str) -> None:
    conn.execute("""
      UPDATE watchlist_symbols
      SET is_active = 0, removed_at = ?
      WHERE provider = ? AND provider_symbol = ?;
    """, (removed_at, provider, provider_symbol))

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
        raw_count = load_csv_to_raw(conn, csv_path, provider_symbol, run_id, date_from, date_to)
        stg_count = rebuild_staging_for_run_symbol(conn, run_id, provider_symbol)  # staging is symbol-scoped
        ins, upd, same = merge_stg_to_curated(conn, run_id, provider_symbol)

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

        rev_cnt_run = conn.execute("""
            SELECT COUNT(*)
            FROM cur_daily_prices
            WHERE provider = ? AND provider_symbol = ? AND is_revised = 1 AND last_run_id = ?;
        """, (PROVIDER, provider_symbol, run_id)).fetchone()[0]
        print("curated revised rows (this run):", rev_cnt_run)

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
        # Finalize symbol log (failed) — keep counts 0 because we may fail before load starts
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


def seed_watchlist_if_empty(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT COUNT(*) FROM watchlist_symbols;")
    n = cur.fetchone()[0]
    if n == 0:
        conn.execute("""
            INSERT INTO watchlist_symbols (provider, provider_symbol, is_active, added_at, notes)
            VALUES (?, ?, 1, ?, ?);
        """, (PROVIDER, "AAPL.US", utc_now_iso(), "seed symbol"))
        print("Seeded watchlist with AAPL.US")

def upsert_watchlist_symbol(conn: sqlite3.Connection, provider: str, provider_symbol: str, notes: str = "") -> None:
    now = utc_now_iso()
    conn.execute("""
        INSERT INTO watchlist_symbols (provider, provider_symbol, is_active, added_at, notes)
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT(provider, provider_symbol) DO UPDATE SET
            is_active = 1,
            removed_at = NULL,
            notes = CASE WHEN excluded.notes != '' THEN excluded.notes ELSE watchlist_symbols.notes END;
    """, (provider, provider_symbol, now, notes))




def insert_run_start(conn: sqlite3.Connection, run_id: str, started_at: str, symbols_total: int) -> None:
    conn.execute("""
        INSERT INTO run_log (run_id, started_at, ended_at, status, symbols_total, symbols_success, symbols_failed)
        VALUES (?, ?, NULL, 'partial', ?, 0, 0)
        ON CONFLICT(run_id) DO NOTHING;
    """, (run_id, started_at, symbols_total))


def finalize_run(conn: sqlite3.Connection, run_id: str, ended_at: str, symbols_success: int, symbols_failed: int) -> None:
    status = "success" if symbols_failed == 0 else ("failed" if symbols_success == 0 else "partial")
    conn.execute("""
        UPDATE run_log
        SET ended_at = ?,
            status = ?,
            symbols_success = ?,
            symbols_failed = ?
        WHERE run_id = ?;
    """, (ended_at, status, symbols_success, symbols_failed, run_id))


def insert_symbol_start(
    conn: sqlite3.Connection,
    run_id: str,
    provider: str,
    provider_symbol: str,
    date_from: str,
    date_to: str,
    started_at: str
) -> None:
    conn.execute("""
        INSERT INTO run_symbol_log (
            run_id, provider, provider_symbol,
            status, date_from, date_to,
            raw_count, stg_count, ins, upd, same,
            error_message, started_at, ended_at
        ) VALUES (?, ?, ?, 'partial', ?, ?, 0, 0, 0, 0, 0, NULL, ?, NULL)
        ON CONFLICT(run_id, provider, provider_symbol) DO NOTHING;
    """, (run_id, provider, provider_symbol, date_from, date_to, started_at))


def finalize_symbol(
    conn: sqlite3.Connection,
    run_id: str,
    provider: str,
    provider_symbol: str,
    ended_at: str,
    status: str,
    date_from: str,
    date_to: str,
    raw_count: int,
    stg_count: int,
    ins: int,
    upd: int,
    same: int,
    error_message: str | None
) -> None:
    conn.execute("""
        UPDATE run_symbol_log
        SET ended_at = ?,
            status = ?,
            date_from = ?,
            date_to = ?,
            raw_count = ?,
            stg_count = ?,
            ins = ?,
            upd = ?,
            same = ?,
            error_message = ?
        WHERE run_id = ? AND provider = ? AND provider_symbol = ?;
    """, (
        ended_at, status, date_from, date_to,
        raw_count, stg_count, ins, upd, same, error_message,
        run_id, provider, provider_symbol
    ))



def main(force_download: bool = False, verbose: bool = False):
    run_id = make_run_id()

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        seed_watchlist_if_empty(conn)

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
            init_db(conn)  # ensures views exist

            rows = conn.execute("""
                SELECT provider_symbol, latest_date, latest_close, daily_return, sma_20, sma_50, var_20d
                FROM vw_watchlist_report
                ORDER BY provider_symbol;
            """).fetchall()
            
        print("\n=== Watchlist Report ===")
        print("symbol | date | close | ret% | sma20 | sma50 | vol20%")
        
        for sym, dt, close, ret, sma20, sma50, var20 in rows:
            vol20 = None
            if var20 is not None and var20 > 0:
                vol20 = math.sqrt(var20)

            ret_pct = None if ret is None else ret * 100.0
            vol_pct = None if vol20 is None else vol20 * 100.0

            # format nicely; keep blanks for None
            def fmt(x, nd=2):
                if x is None:
                    return ""
                if isinstance(x, (int, float)):
                    return f"{x:.{nd}f}"
                return str(x)
        
            print(" | ".join([
                sym,
                dt,
                fmt(close, 2),
                fmt(ret_pct, 2),
                fmt(sma20, 2),
                fmt(sma50, 2),
                fmt(vol_pct, 2),
            ]))
    else:
        main(force_download=args.refresh, verbose=args.verbose)