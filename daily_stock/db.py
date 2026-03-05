import sqlite3

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

    conn.executescript("""
    DROP VIEW IF EXISTS vw_latest_close;

    CREATE VIEW vw_latest_close AS
    SELECT
      c.provider,
      c.provider_symbol,
      c.trading_date AS latest_date,
      c.close AS latest_close,
      c.volume AS latest_volume,
      c.last_ingested_at,
      c.last_run_id
    FROM cur_daily_prices c
    JOIN (
      SELECT provider, provider_symbol, MAX(trading_date) AS max_date
      FROM cur_daily_prices
      GROUP BY provider, provider_symbol
    ) m
      ON c.provider = m.provider
     AND c.provider_symbol = m.provider_symbol
     AND c.trading_date = m.max_date;
    """)

    conn.executescript("""
    DROP VIEW IF EXISTS vw_daily_returns;
    
    CREATE VIEW vw_daily_returns AS
    SELECT
      provider,
      provider_symbol,
      trading_date,
      close,
      LAG(close) OVER (PARTITION BY provider, provider_symbol ORDER BY trading_date) AS prev_close,
      CASE
        WHEN LAG(close) OVER (PARTITION BY provider, provider_symbol ORDER BY trading_date) IS NULL THEN NULL
        WHEN LAG(close) OVER (PARTITION BY provider, provider_symbol ORDER BY trading_date) = 0 THEN NULL
        ELSE (close / LAG(close) OVER (PARTITION BY provider, provider_symbol ORDER BY trading_date)) - 1
      END AS daily_return
    FROM cur_daily_prices;
    """)


    conn.executescript("""
    DROP VIEW IF EXISTS vw_tech_indicators;

    CREATE VIEW vw_tech_indicators AS
    WITH base AS (
      SELECT
        provider,
        provider_symbol,
        trading_date,
        close,
        daily_return
      FROM vw_daily_returns
    ),
    w AS (
      SELECT
        provider,
        provider_symbol,
        trading_date,
        close,
        daily_return,
    
        AVG(close) OVER (
          PARTITION BY provider, provider_symbol
          ORDER BY trading_date
          ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20,
    
        AVG(close) OVER (
          PARTITION BY provider, provider_symbol
          ORDER BY trading_date
          ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
    
        COUNT(daily_return) OVER (
          PARTITION BY provider, provider_symbol
          ORDER BY trading_date
          ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS n_ret_20,
    
        AVG(daily_return) OVER (
          PARTITION BY provider, provider_symbol
          ORDER BY trading_date
          ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS mean_ret_20,
    
        AVG(daily_return * daily_return) OVER (
          PARTITION BY provider, provider_symbol
          ORDER BY trading_date
          ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS mean_ret2_20
      FROM base
    ),
    final AS (
      SELECT
        provider,
        provider_symbol,
        trading_date,
        close,
        daily_return,
        sma_20,
        sma_50,
        CASE
          WHEN n_ret_20 < 20 THEN NULL
          ELSE (mean_ret2_20 - mean_ret_20 * mean_ret_20)
        END AS var_20d
      FROM w
    )
    SELECT
      provider,
      provider_symbol,
      trading_date,
      close,
      daily_return,
      sma_20,
      sma_50,
      var_20d
    FROM final;
    """)

    conn.executescript("""
    DROP VIEW IF EXISTS vw_watchlist_report;
    
    CREATE VIEW vw_watchlist_report AS
    SELECT
      t.provider,
      t.provider_symbol,
      t.trading_date AS latest_date,
      t.close AS latest_close,
      t.daily_return,
      t.sma_20,
      t.sma_50,
      t.var_20d
    FROM vw_tech_indicators t
    JOIN (
      SELECT provider, provider_symbol, MAX(trading_date) AS max_date
      FROM vw_tech_indicators
      GROUP BY provider, provider_symbol
    ) m
      ON t.provider = m.provider
     AND t.provider_symbol = m.provider_symbol
     AND t.trading_date = m.max_date
    WHERE EXISTS (
      SELECT 1
      FROM watchlist_symbols w
      WHERE w.provider = t.provider
        AND w.provider_symbol = t.provider_symbol
        AND w.is_active = 1
    );
    """)
    
    conn.commit()
