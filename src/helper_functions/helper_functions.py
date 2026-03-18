import logging
from typing import Optional, List, Any
from datetime import datetime


def setup_logging(enabled: bool = True):
    """Configure logging for the application.

    Sets up basic logging configuration with a standardized format.
    Prevents duplicate handlers if called multiple times.

    Args:
        enabled: Whether to enable logging. If False, disables all logging.
    """
    if logging.getLogger().hasHandlers():
        return  # prevent duplicate handlers

    if not enabled:
        logging.disable(logging.CRITICAL)
        return

    # Level string set to DEBUG for debuging prints
    level = "INFO"
    level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,  # <-- critical to override prior handlers
    )


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO datetime string into a datetime object.

    Handles UTC timezone ('Z' suffix) by converting to '+00:00'.

    TODO: Avoid making own parser and use "from dateutil.parser import parse" instead

    Args:
        value: ISO datetime string, or None.

    Returns:
        Parsed datetime object, or None if input was None.
    """
    if value is None:
        return None
    # Handle 'Z' (UTC) suffix
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_list(value: Any) -> List[Any]:
    """Ensure a value is wrapped in a list.

    If the value is already a list, returns it unchanged.
    Otherwise, wraps the value in a single-element list.

    Args:
        value: Any value to potentially wrap in a list.

    Returns:
        A list containing the value.
    """
    if isinstance(value, list):
        return value
    return [value]


import calendar

def month_range_to_datetime(start_month: str, end_month: str) -> str:
    # Validate input format
    try:
        start_year, start_m = map(int, start_month.split("-"))
        end_year, end_m = map(int, end_month.split("-"))
    except ValueError:
        raise ValueError("Input must be in 'YYYY-MM' format")

    # First day of the start month
    start_date = f"{start_year:04d}-{start_m:02d}-01T00:00:00Z"

    # Determine last day of end month
    last_day = calendar.monthrange(end_year, end_m)[1]
    end_date = f"{end_year:04d}-{end_m:02d}-{last_day:02d}T00:00:00Z"

    return f"{start_date}/{end_date}", start_date, end_date
