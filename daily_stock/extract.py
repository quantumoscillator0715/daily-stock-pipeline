from pathlib import Path
import urllib.request
import urllib.error

#constants
DATA_DIR = Path("data")

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