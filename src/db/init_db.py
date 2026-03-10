from db.connection import engine
from models.sqlalchemy.base import Base

# Ensure all SQLAlchemy models import and register with Base.metadata
# e.g., `from models.sqlalchemy import stations, observations` in this module or in models/__init__.py
#import models.sqlalchemy.stations
#import models.sqlalchemy.observations


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
