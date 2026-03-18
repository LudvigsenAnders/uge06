"""
Database initialization utilities.

This module provides functions to create database tables and set up
the initial database schema.
"""

from db.connection import engine
from models.sqlalchemy_orm.base import Base

# Ensure all SQLAlchemy models import and register with Base.metadata
# e.g., `from models.sqlalchemy import stations, observations` in this module or in models/__init__.py
from models.sqlalchemy_orm import stations, observations  # noqa: F401
from models.sqlalchemy_orm import url_checkpoint  # noqa: F401


async def init_db() -> None:
    """Create all database tables defined in the SQLAlchemy models.

    Uses the Base.metadata to create all tables that have been imported
    and registered with the metadata registry.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
