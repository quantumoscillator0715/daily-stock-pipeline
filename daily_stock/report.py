import math
import sqlite3


def fetch_watchlist_report(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        """
        SELECT
            provider_symbol,
            latest_date,
            latest_close,
            daily_return,
            sma_20,
            sma_50,
            var_20d
        FROM vw_watchlist_report
        ORDER BY provider_symbol;
        """
    ).fetchall()


def _fmt(x, nd: int = 2) -> str:
    if x is None:
        return ""
    if isinstance(x, (int, float)):
        return f"{x:.{nd}f}"
    return str(x)


def print_watchlist_report(rows: list[tuple]) -> None:
    print("\n=== Watchlist Report ===")
    print("symbol | date | close | ret% | sma20 | sma50 | vol20%")

    for sym, dt, close, ret, sma20, sma50, var20 in rows:
        vol20 = math.sqrt(var20) if var20 is not None and var20 > 0 else None
        ret_pct = None if ret is None else ret * 100.0
        vol_pct = None if vol20 is None else vol20 * 100.0

        print(" | ".join([
            sym,
            dt,
            _fmt(close, 2),
            _fmt(ret_pct, 2),
            _fmt(sma20, 2),
            _fmt(sma50, 2),
            _fmt(vol_pct, 2),
        ]))