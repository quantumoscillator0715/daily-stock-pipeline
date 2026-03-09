import sqlite3
from daily_stock.util import utc_now_iso

DEFAULT_SYMBOLS = [
    "AAPL.US",
    "GLD.US",
    "MSFT.US",
    "QQQ.US",
    "SLV.US",
    "SPY.US",
]


def get_active_watchlist(conn: sqlite3.Connection, provider: str) -> list[str]:
    cur = conn.execute(
        """
        SELECT provider_symbol
        FROM watchlist_symbols
        WHERE provider = ? AND is_active = 1
        ORDER BY provider_symbol;
        """,
        (provider,)
    )
    return [r[0] for r in cur.fetchall()]


def upsert_watchlist_symbol(
    conn: sqlite3.Connection,
    provider: str,
    provider_symbol: str,
    notes: str | None = None
) -> None:
    now = utc_now_iso()
    
    conn.execute(
        """
        INSERT INTO watchlist_symbols (
            provider, provider_symbol, is_active, added_at, removed_at, notes
        )
        VALUES (?, ?, 1, ?, NULL, ?)
        ON CONFLICT(provider, provider_symbol) DO UPDATE SET
            is_active = 1,
            removed_at = NULL,
            notes = COALESCE(excluded.notes, watchlist_symbols.notes)
        """,
        (provider, provider_symbol, now, notes)
    )


def mark_removed_watchlist(
    conn: sqlite3.Connection,
    provider: str,
    provider_symbol: str,
    removed_at: str
) -> None:
    conn.execute(
        """
        UPDATE watchlist_symbols
        SET is_active = 0,
            removed_at = ?
        WHERE provider = ? AND provider_symbol = ?;
        """,
        (removed_at, provider, provider_symbol)
    )


def seed_watchlist_if_empty(
    conn: sqlite3.Connection,
    provider: str,
    default_symbols: list[str] | None = None
) -> None:
    symbols = DEFAULT_SYMBOLS if default_symbols is None else default_symbols

    cur = conn.execute(
        """
        SELECT COUNT(*)
        FROM watchlist_symbols
        WHERE provider = ?;
        """,
        (provider,)
    )
    n = cur.fetchone()[0]

    if n == 0:
        for sym in symbols:
            note = "seed symbol" if sym == "AAPL.US" else "seeded default symbol"
            conn.execute(
                """
                INSERT INTO watchlist_symbols (
                    provider, provider_symbol, is_active, added_at, removed_at, notes
                )
                VALUES (?, ?, 1, ?, NULL, ?);
                """,
                (provider, sym, utc_now_iso(), note)
            )
        print(f"Seeded watchlist with {len(symbols)} symbols")