from fastapi import APIRouter

router = APIRouter(tags=["infra"])


@router.get("/health", summary="Verifica se o backend está disponível")
def healthcheck() -> dict:
    return {"status": "ok"}
