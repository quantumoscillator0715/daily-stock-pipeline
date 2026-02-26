import csv
import hashlib
import sqlite3
import argparse
from datetime import datetime, timezone
from pathlib import Path
import urllib.request
import urllib.error

from datetime import datetime, timezone, date, timedelta  # add date, timedelta

#constants
SAFETY_DAYS = 10
DB_PATH = "stock.db"
PROVIDER = "stooq"
DATA_DIR = Path("data")


def utc_now_iso() -> str:
    """Return current UTC time as ISO string like 2026-02-07T03:45:01Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_run_id(pipeline_name: str = "stock_daily", env: str = "dev") -> str:
    """Create a sortable run_id."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{pipeline_name}_{env}"


def compute_row_hash(provider_symbol: str, trading_date: str,
                     open_s: str, high_s: str, low_s: str, close_s: str, volume_s: str) -> str:
    """
    Hash the canonical string:
    provider_symbol|trading_date|open|high|low|close|volume
    using sha256.
    """
    canonical = f"{provider_symbol}|{trading_date}|{open_s}|{high_s}|{low_s}|{close_s}|{volume_s}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist."""
    conn.execute("PRAGMA foreign_keys = ON;")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_stooq_daily_prices (
      raw_id           INTEGER PRIMARY KEY,
      provider         TEXT NOT NULL,
      provider_symbol  TEXT NOT NULL,
      trading_date     TEXT NOT NULL,

      open             REAL NOT NULL,
      high             REAL NOT NULL,
      low              REAL NOT NULL,
      close            REAL NOT NULL,
      volume           REAL NOT NULL,

      source_url       TEXT NOT NULL,
      ingested_at      TEXT NOT NULL,
      run_id           TEXT NOT NULL,
      row_hash         TEXT NOT NULL,

      CHECK (volume >= 0),
      CHECK (high >= low),
      UNIQUE (provider, provider_symbol, trading_date, run_id)
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS stg_daily_prices (
      provider            TEXT NOT NULL,
      provider_symbol     TEXT NOT NULL,
      trading_date        TEXT NOT NULL,

      open                REAL NOT NULL,
      high                REAL NOT NULL,
      low                 REAL NOT NULL,
      close               REAL NOT NULL,
      volume              REAL NOT NULL,
      row_hash            TEXT NOT NULL,

      run_id              TEXT NOT NULL,
      ingested_at         TEXT NOT NULL,
      raw_max_ingested_at TEXT NOT NULL,
      raw_row_count       INTEGER NOT NULL,

      CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
      CHECK (high >= low),
      CHECK (volume >= 0),

      PRIMARY KEY (provider, provider_symbol, trading_date)
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS cur_daily_prices (
      provider           TEXT NOT NULL,
      provider_symbol    TEXT NOT NULL,
      trading_date       TEXT NOT NULL,

      open               REAL NOT NULL,
      high               REAL NOT NULL,
      low                REAL NOT NULL,
      close              REAL NOT NULL,
      volume             REAL NOT NULL,
      row_hash           TEXT NOT NULL,

      first_ingested_at  TEXT NOT NULL,
      last_ingested_at   TEXT NOT NULL,
      last_run_id        TEXT NOT NULL,

      revision_count     INTEGER NOT NULL DEFAULT 0,
      is_revised         INTEGER NOT NULL DEFAULT 0,

      CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
      CHECK (high >= low),
      CHECK (volume >= 0),
      CHECK (revision_count >= 0),
      CHECK (is_revised IN (0,1)),

      PRIMARY KEY (provider, provider_symbol, trading_date)
    );
    """)
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_watermarks (
      provider          TEXT NOT NULL,
      provider_symbol   TEXT NOT NULL,
      last_success_date TEXT NOT NULL,
      last_run_id       TEXT NOT NULL,
      last_updated_at   TEXT NOT NULL,
      status            TEXT NOT NULL,
      PRIMARY KEY (provider, provider_symbol),
      CHECK (status IN ('success','failed'))
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS watchlist_symbols (
      provider         TEXT NOT NULL,
      provider_symbol  TEXT NOT NULL,
      is_active        INTEGER NOT NULL DEFAULT 1,
      added_at         TEXT NOT NULL,
      removed_at       TEXT,
      notes            TEXT,
      PRIMARY KEY (provider, provider_symbol),
      CHECK (is_active IN (0,1))
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS run_log (
      run_id          TEXT PRIMARY KEY,
      started_at      TEXT NOT NULL,
      ended_at        TEXT,
      status          TEXT NOT NULL,
      symbols_total   INTEGER NOT NULL,
      symbols_success INTEGER NOT NULL,
      symbols_failed  INTEGER NOT NULL,
      CHECK (status IN ('success', 'failed', 'partial'))
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS run_symbol_log (
      run_id          TEXT NOT NULL,
      provider        TEXT NOT NULL,
      provider_symbol TEXT NOT NULL,
      status          TEXT NOT NULL,
      date_from       TEXT NOT NULL,
      date_to         TEXT NOT NULL,
      raw_count       INTEGER NOT NULL,
      stg_count       INTEGER NOT NULL,
      ins             INTEGER NOT NULL,
      upd             INTEGER NOT NULL,
      same            INTEGER NOT NULL,
      error_message   TEXT,
      started_at      TEXT NOT NULL,
      ended_at        TEXT,
      PRIMARY KEY (run_id, provider, provider_symbol),
      CHECK (status IN ('success', 'failed', 'partial'))
    );
    """)
      
      
    
    conn.commit()


def load_csv_to_raw(conn: sqlite3.Connection, csv_path: str, provider_symbol: str, run_id: str, date_from: str, date_to: str) -> int:
    """
    Read the Stooq CSV and append rows into RAW.
    Returns number of inserted rows.
    """
    ingested_at = utc_now_iso()

    # You can store the exact URL you used. For manual download, store a descriptive placeholder.
    source_url = f"manual_file://{Path(csv_path).name}"

    inserted = 0

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        date_from_d = parse_yyyy_mm_dd(date_from)
        date_to_d   = parse_yyyy_mm_dd(date_to)
        
        # Contract check: header must be exactly these fieldnames in this order
        expected = ["Date", "Open", "High", "Low", "Close", "Volume"]
        if reader.fieldnames != expected:
            raise ValueError(f"Bad header. Expected {expected} but got {reader.fieldnames}")

        for row in reader:
            trading_date = row["Date"]
            trading_date_d = parse_yyyy_mm_dd(trading_date)
            if trading_date_d < date_from_d or trading_date_d > date_to_d:
                continue
            open_s = row["Open"]
            high_s = row["High"]
            low_s = row["Low"]
            close_s = row["Close"]
            volume_s = row["Volume"]

            row_hash = compute_row_hash(
                provider_symbol=provider_symbol,
                trading_date=trading_date,
                open_s=open_s,
                high_s=high_s,
                low_s=low_s,
                close_s=close_s,
                volume_s=volume_s
            )

            # Insert into RAW. If you rerun with same run_id, UNIQUE constraint will protect you.
            conn.execute("""
                INSERT OR IGNORE INTO raw_stooq_daily_prices (
                    provider, provider_symbol, trading_date,
                    open, high, low, close, volume,
                    source_url, ingested_at, run_id, row_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                PROVIDER, provider_symbol, trading_date,
                float(open_s), float(high_s), float(low_s), float(close_s), float(volume_s),
                source_url, ingested_at, run_id, row_hash
            ))

            # sqlite3 doesn't directly tell "ignored vs inserted" unless we check changes()
            inserted += conn.total_changes  # we'll fix this below

    conn.commit()

    # conn.total_changes is cumulative, so the above is not ideal.
    # Instead, compute inserted rows by querying counts for this run:
    cur = conn.execute("""
        SELECT COUNT(*)
        FROM raw_stooq_daily_prices
        WHERE run_id = ? AND provider_symbol = ?;
    """, (run_id, provider_symbol))
    return cur.fetchone()[0]


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

    conn.commit()

    cur = conn.execute("""
        SELECT COUNT(*)
        FROM stg_daily_prices
        WHERE run_id = ? AND provider = ? AND provider_symbol = ?;
    """, (run_id, PROVIDER, provider_symbol))
    return cur.fetchone()[0]

def merge_stg_to_curated(conn: sqlite3.Connection, run_id: str, provider_symbol: str) -> tuple[int, int, int]:
    """
    Merge STG rows for a given run_id into CUR (latest-state table).

    Returns: (inserted_rows, updated_rows, unchanged_rows)
      - inserted_rows: new (provider, provider_symbol, trading_date)
      - updated_rows: existing key but row_hash changed -> revise + overwrite OHLCV
      - unchanged_rows: existing key and row_hash same -> only refresh last_ingested_at/last_run_id
    """

    # 1) Safety check per symbol
    cur = conn.execute("SELECT COUNT(*) FROM stg_daily_prices WHERE run_id = ? AND provider_symbol = ?;", (run_id, provider_symbol))
    stg_count = cur.fetchone()[0]
    if stg_count == 0:
        raise ValueError(f"No staging rows found for run_id={run_id}. Did you rebuild staging?")

    # 2) Insert NEW keys into curated (first time we see that (symbol, date)).
    #    We set first_ingested_at = stg.ingested_at for brand-new rows.
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
      AND s.provider_symbol = ?
      AND NOT EXISTS (
        SELECT 1
        FROM cur_daily_prices c
        WHERE c.provider = s.provider
          AND c.provider_symbol = s.provider_symbol
          AND c.trading_date = s.trading_date
      );
    """, (run_id, provider_symbol))
    inserted = conn.execute("SELECT changes();").fetchone()[0]

    # 3) Update CHANGED rows (row_hash differs): overwrite measures, bump revision_count, mark revised.
    conn.execute("""
    UPDATE cur_daily_prices AS c
    SET
      (open, high, low, close, volume, row_hash, last_ingested_at, last_run_id) = (
        SELECT
          s.open, s.high, s.low, s.close, s.volume, s.row_hash, s.ingested_at, s.run_id
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
      ),
      revision_count = revision_count + 1,
      is_revised = 1
    WHERE c.provider_symbol = ?
      AND EXISTS (
        SELECT 1
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
          AND s.row_hash <> c.row_hash
      );
    """, (run_id, provider_symbol, provider_symbol, run_id, provider_symbol))
    updated = conn.execute("SELECT changes();").fetchone()[0]

    # 4) Update UNCHANGED rows: same hash, so refresh only metadata (last_ingested_at, last_run_id).
    conn.execute("""
    UPDATE cur_daily_prices AS c
    SET
      (last_ingested_at, last_run_id) = (
        SELECT
          s.ingested_at, s.run_id
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
      )
    WHERE c.provider_symbol = ?
      AND EXISTS (
        SELECT 1
        FROM stg_daily_prices AS s
        WHERE s.run_id = ?
          AND s.provider_symbol = ?
          AND s.provider = c.provider
          AND s.trading_date = c.trading_date
          AND s.row_hash = c.row_hash
      );
    """, (run_id, provider_symbol, provider_symbol, run_id, provider_symbol))
    unchanged = conn.execute("SELECT changes();").fetchone()[0]

    conn.commit()
    return inserted, updated, unchanged

def parse_yyyy_mm_dd(s: str) -> date:
    """Parse ISO date string YYYY-MM-DD into a date object."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def iso_yyyy_mm_dd(d: date) -> str:
    """Convert a date object into YYYY-MM-DD string."""
    return d.strftime("%Y-%m-%d")


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
        conn.commit()
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
    conn.commit()

def stooq_symbol_for_url(provider_symbol: str) -> str:
    """
    Stooq expects lowercase symbols in the URL, e.g. 'AAPL.US' -> 'aapl.us'
    """
    return provider_symbol.strip().lower()


def csv_path_for_symbol(provider_symbol: str) -> str:
    """
    Your file naming convention: 'AAPL.US' -> 'data/aapl_us_d.csv'
    """
    safe = provider_symbol.strip().lower().replace(".", "_")
    return str(DATA_DIR / f"{safe}_d.csv")


def stooq_daily_csv_url(provider_symbol: str) -> str:
    """
    Stooq daily CSV endpoint:
    https://stooq.com/q/d/l/?s=aapl.us&i=d
    """
    s = stooq_symbol_for_url(provider_symbol)
    return f"https://stooq.com/q/d/l/?s={s}&i=d"


def extract_stooq_csv(provider_symbol: str, force: bool = False) -> str:
    """
    Downloads the daily CSV for provider_symbol into data/ and returns the local path.
    Uses a cache check + atomic write to avoid partial/corrupt files.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    url = stooq_daily_csv_url(provider_symbol)
    out_path = csv_path_for_symbol(provider_symbol)

    out_p = Path(out_path)

    # 1) Cache: if we already have a non-trivial file, reuse it
    if (not force) and out_p.exists() and out_p.stat().st_size > 100:
        print(f"cached: {provider_symbol} -> {out_path}")
        return out_path

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; daily-stock-pipeline/1.0)"}
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read()

        # 2) Stooq sometimes returns an HTML error page; quick sanity check.
        head = content[:300].lower()
        if (
            not content
            or b"<html" in head
            or b"<!doctype html" in head
        ):
            raise RuntimeError(f"Unexpected response when downloading {provider_symbol} from Stooq.")

        # 3) Atomic write: write to .tmp then rename into place
        tmp_path = out_p.with_suffix(out_p.suffix + ".tmp")  # e.g. aapl_us_d.csv.tmp
        tmp_path.write_bytes(content)
        tmp_path.replace(out_p)

        print(f"downloaded: {provider_symbol} -> {out_path} ({len(content)} bytes)")
        return out_path

    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP error downloading {provider_symbol}: {e.code} {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error downloading {provider_symbol}: {e.reason}") from e

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
    else:
        main(force_download=args.refresh, verbose=args.verbose)