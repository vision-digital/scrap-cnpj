from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from app.db.session import session_scope
from app.models import DataVersion, IngestionStatus


class VersioningService:
    def current_release(self) -> DataVersion | None:
        with session_scope() as session:
            stmt = select(DataVersion).order_by(DataVersion.started_at.desc())
            result = session.execute(stmt).scalars().first()
            if result:
                session.expunge(result)
            return result

    def start_release(self, release: str) -> DataVersion:
        with session_scope() as session:
            stmt = select(DataVersion).where(DataVersion.release == release)
            version = session.execute(stmt).scalars().first()
            if version:
                version.status = IngestionStatus.running
                version.started_at = datetime.utcnow()
                version.finished_at = None
                version.note = None
            else:
                version = DataVersion(
                    release=release,
                    status=IngestionStatus.running,
                    started_at=datetime.utcnow(),
                )
                session.add(version)
            session.flush()
            session.expunge(version)
            return version

    def finish_release(self, release: str, success: bool, note: str | None = None) -> None:
        with session_scope() as session:
            stmt = select(DataVersion).where(DataVersion.release == release)
            version = session.execute(stmt).scalars().first()
            if not version:
                return
            version.status = IngestionStatus.completed if success else IngestionStatus.failed
            version.finished_at = datetime.utcnow()
            version.note = note
