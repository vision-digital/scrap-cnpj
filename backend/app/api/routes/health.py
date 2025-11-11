from fastapi import APIRouter

router = APIRouter(tags=["infra"])


@router.get("/health", summary="Verifica se o backend esta disponivel")
def healthcheck() -> dict:
    return {"status": "ok"}
