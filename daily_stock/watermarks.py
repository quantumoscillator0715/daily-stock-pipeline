import sqlite3

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