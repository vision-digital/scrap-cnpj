from fastapi import APIRouter

from app.services.versioning import VersioningService

router = APIRouter(prefix="/version", tags=["versao"])
versioning = VersioningService()


@router.get("/latest", summary="Retorna a versao atual da base")
def latest_version() -> dict:
    current = versioning.current_release()
    if not current:
        return {"release": None, "status": "unknown"}
    return {
        "release": current.release,
        "status": current.status,
        "started_at": current.started_at,
        "finished_at": current.finished_at,
    }
