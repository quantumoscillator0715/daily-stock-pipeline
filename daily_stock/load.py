import csv

def load_csv_to_raw(conn: sqlite3.Connection, csv_path: str, provider_symbol: str, run_id: str, date_from: str, date_to: str) -> int:
    """
    Read the Stooq CSV and append rows into RAW.
    Returns number of inserted rows.
    """
    ingested_at = utc_now_iso()

    # You can store the exact URL you used. For manual download, store a descriptive placeholder.
    #source_url = f"manual_file://{Path(csv_path).name}"
    source_url = "stooq_download"
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
                PROVIDER,
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

    # conn.total_changes is cumulative, so the above is not ideal.
    # Instead, compute inserted rows by querying counts for this run:
    cur = conn.execute("""
        SELECT COUNT(*)
        FROM raw_stooq_daily_prices
        WHERE run_id = ? AND provider_symbol = ?;
    """, (run_id, provider_symbol))
    return cur.fetchone()[0]