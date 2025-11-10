from fastapi import APIRouter

from app.services.versioning import VersioningService

router = APIRouter(prefix="/version", tags=["versão"])
versioning = VersioningService()


@router.get("/latest", summary="Retorna a versão atual da base")
def latest_version() -> dict:
    current = versioning.current_release()
    if not current:
        return {"release": None, "status": "unknown"}
    return {
        "release": current.release,
        "status": current.status.value,
        "finished_at": current.finished_at,
    }
