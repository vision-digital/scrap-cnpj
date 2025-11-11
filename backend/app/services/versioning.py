from __future__ import annotations

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from app.db.session import SessionLocal
from app.models import DataVersion


class VersioningService:
    def current_release(self) -> DataVersion | None:
        with SessionLocal() as session:
            stmt = select(DataVersion).order_by(DataVersion.started_at.desc())
            return session.execute(stmt).scalars().first()

    def start_release(self, release: str) -> DataVersion:
        with SessionLocal() as session:
            version = DataVersion(
                release=release,
                status="running",
                started_at=datetime.utcnow(),
            )
            session.add(version)
            session.commit()
            session.refresh(version)
            return version

    def finish_release(self, release: str, success: bool, note: str | None = None) -> None:
        with SessionLocal() as session:
            stmt = select(DataVersion).where(DataVersion.release == release).order_by(
                DataVersion.started_at.desc()
            )
            version = session.execute(stmt).scalars().first()
            if not version:
                version = DataVersion(
                    release=release,
                    started_at=datetime.utcnow(),
                )
                session.add(version)
            version.status = "completed" if success else "failed"
            version.finished_at = datetime.utcnow()
            version.note = note
            session.commit()
