from sqlalchemy import Column, String, TIMESTAMP
from sqlalchemy import UniqueConstraint, BigInteger, Identity
from .base import Base


class IngestCheckpoint(Base):
    __tablename__ = "ingest_checkpoint"

    __table_args__ = (
        UniqueConstraint("id", name="uq_ingest_checkpoint_id"),
    )

    id = Column(BigInteger, Identity(start=1), primary_key=True)
    next_url = Column(String)
    table_name = Column(String)
    created = Column(TIMESTAMP(timezone=True))
