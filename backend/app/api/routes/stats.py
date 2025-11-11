from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models import Empresa, Estabelecimento, Simples, Socio
from app.services.versioning import VersioningService

router = APIRouter(prefix="/stats", tags=["estatisticas"])
versioning = VersioningService()


@router.get("/", summary="Resumo de registros por tabela")
def read_stats(db: Session = Depends(get_db)) -> dict:
    payload = {
        "empresas": db.scalar(select(func.count()).select_from(Empresa)) or 0,
        "estabelecimentos": db.scalar(select(func.count()).select_from(Estabelecimento)) or 0,
        "socios": db.scalar(select(func.count()).select_from(Socio)) or 0,
        "simples": db.scalar(select(func.count()).select_from(Simples)) or 0,
    }
    current = versioning.current_release()
    return {
        "tables": payload,
        "version": {
            "release": current.release if current else None,
            "status": current.status if current else "unknown",
        },
    }
