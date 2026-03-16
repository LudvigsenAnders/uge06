import logging
from typing import Optional
from datetime import datetime


def setup_logging(enabled: bool = True):
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
    if value is None:
        return None
    # Handle 'Z' (UTC) suffix
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_list(value):
    if isinstance(value, list):
        return value
    return [value]
