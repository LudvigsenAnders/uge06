from db.connection import engine
from models.sqlalchemy_orm.base import Base

# Ensure all SQLAlchemy models import and register with Base.metadata
# e.g., `from models.sqlalchemy import stations, observations` in this module or in models/__init__.py
from models.sqlalchemy_orm import stations, observations  # noqa: F401
from models.sqlalchemy_orm import url_checkpoint  # noqa: F401


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
