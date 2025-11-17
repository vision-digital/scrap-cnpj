from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Body

from app.core.config import get_settings
from app.services.pipeline import Pipeline
from app.services.versioning import VersioningService
# REMOVED: Loader (old loader replaced by LoaderV2 in pipeline)

router = APIRouter(prefix="/updates", tags=["atualizacao"])
versioning = VersioningService()
settings = get_settings()


def _trigger_update(release: str | None) -> None:
    pipeline = Pipeline()
    pipeline.run(release)


@router.post("/run")
def run_update(
    background_tasks: BackgroundTasks,
    payload: dict | None = Body(default=None),
) -> dict:
    release = payload.get("release") if payload else None
    background_tasks.add_task(_trigger_update, release)
    return {"message": "Atualizacao iniciada", "release": release}


@router.get("/status")
def update_status() -> dict:
    current = versioning.current_release()
    if not current:
        return {"release": None, "status": "unknown"}
    return {
        "release": current.release,
        "status": current.status,
        "started_at": current.started_at,
        "finished_at": current.finished_at,
    }


# REMOVED: /load-tables endpoint - LoaderV2 requires sequential loading with interdependencies
# The denormalized schema needs all datasets to be processed together (estabelecimentos → empresas → simples → merge)
# Partial table loading is no longer supported with the new architecture
