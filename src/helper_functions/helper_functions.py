import logging
from typing import Optional
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

    Args:
        value: ISO datetime string, or None.

    Returns:
        Parsed datetime object, or None if input was None.
    """
    if value is None:
        return None
    # Handle 'Z' (UTC) suffix
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_list(value):
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
