from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import DateTime, Enum as SqlEnum, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class IngestionStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class DataVersion(Base):
    __tablename__ = "data_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    release: Mapped[str] = mapped_column(String(7), unique=True, nullable=False)
    status: Mapped[IngestionStatus] = mapped_column(
        SqlEnum(IngestionStatus), default=IngestionStatus.pending, nullable=False
    )
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    note: Mapped[str | None] = mapped_column(String(255), nullable=True)
