import sqlite3

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