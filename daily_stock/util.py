import hashlib
from datetime import datetime, timezone, date

#constants

def utc_now_iso() -> str:
    """Return current UTC time as ISO string like 2026-02-07T03:45:01Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_run_id(pipeline_name: str = "stock_daily", env: str = "dev") -> str:
    """Create a sortable run_id."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{pipeline_name}_{env}"


def compute_row_hash(provider: str, provider_symbol: str, trading_date: str,
                     open_s: str, high_s: str, low_s: str, close_s: str, volume_s: str) -> str:
    """
    Hash the canonical string:
    provider|provider_symbol|trading_date|open|high|low|close|volume
    using sha256.
    """
    canonical = f"{provider}|{provider_symbol}|{trading_date}|{open_s}|{high_s}|{low_s}|{close_s}|{volume_s}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def parse_yyyy_mm_dd(s: str) -> date:
    """Parse ISO date string YYYY-MM-DD into a date object."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def iso_yyyy_mm_dd(d: date) -> str:
    """Convert a date object into YYYY-MM-DD string."""
    return d.strftime("%Y-%m-%d")

