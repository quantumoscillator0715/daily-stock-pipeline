import sqlite3
from daily_stock.util import utc_now_iso

def rebuild_staging_for_run_symbol(conn: sqlite3.Connection, run_id: str, provider: str, provider_symbol: str) -> int:
    """
    Rebuild STG for ONE symbol and ONE run_id.
    """
    print("DEBUG transform file:", __file__)
    print("DEBUG utc_now_iso in globals:", "utc_now_iso" in globals())
    print("DEBUG utc_now_iso object:", globals().get("utc_now_iso"))
    
    stg_ingested_at = utc_now_iso()

    # Delete by provider + symbol (because STG PK does not include run_id)
    conn.execute("""
        DELETE FROM stg_daily_prices
        WHERE provider = ? AND provider_symbol = ?;
    """, (provider, provider_symbol))

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
    """, (stg_ingested_at, run_id, provider, provider_symbol, run_id, provider, provider_symbol))

    cur = conn.execute("""
        SELECT COUNT(*)
        FROM stg_daily_prices
        WHERE run_id = ? AND provider = ? AND provider_symbol = ?;
    """, (run_id, provider, provider_symbol))
    return cur.fetchone()[0]

def merge_stg_to_curated(conn: sqlite3.Connection, run_id: str, provider: str, provider_symbol: str) -> tuple[int, int, int]:
    """
    Returns: (inserted_rows, updated_rows, unchanged_rows)
    """

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