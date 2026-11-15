
from sqlalchemy import (
    Column,
    TIMESTAMP,
    Integer,
    String,
    Text,
    func,
)
from .database import Base


class Job(Base):
    __tablename__ = "jobs"

    id = Column(
        Integer,
        primary_key=True,
    )

    task_name = Column(
        String(100),
        nullable=False,
    )

    status = Column(
        String(20),
        nullable=False,
        default="QUEUED",
    )

    priority = Column(
        Integer,
        nullable=False,
        default=0,
    )

    payload = Column(
        Text,
        nullable=True,
    )

    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    started_at = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )

    worker_id = Column(
        String(100),
        nullable=True,
    )


class Worker(Base):
    __tablename__ = "workers"

    id = Column(String(100),primary_key = True,)
    last_heartbeat = Column(TIMESTAMP(timezone = True) , nullable = False , server_default = func.now(),)
    status = Column(String(20) , nullable = False , default = "ACTIVE" , server_default = "ACTIVE",)
