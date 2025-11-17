"""API routes for auxiliary tables."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.config import get_settings
from app.models.entities import (
    Cnae,
    MotivoSituacaoCadastral,
    Municipio,
    NaturezaJuridica,
    Pais,
    QualificacaoSocio,
)
from app.services.loader_auxiliares import LoaderAuxiliares
from app.services.versioning import VersioningService

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/auxiliares", tags=["auxiliares"])


@router.post("/load")
async def load_auxiliares(release: str | None = None):
    """
    Download and load auxiliary tables from a specific release.
    If no release is specified, uses the latest release.
    """
    try:
        def _run_load():
            loader = LoaderAuxiliares()

            # Get release to use
            if not release:
                versioning = VersioningService()
                latest = versioning.current_release()
                if not latest or latest.status != "completed":
                    raise HTTPException(
                        status_code=400,
                        detail="No completed data version found. Run main data update first."
                    )
                target_release = latest.release
            else:
                target_release = release

            logger.info(f"Downloading and loading auxiliary tables from release: {target_release}")
            loader.download_and_load(target_release)
            return {"status": "completed", "release": target_release}

        result = await anyio.to_thread.run_sync(_run_load)
        return result

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Error loading auxiliary tables")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/paises")
def get_paises(
    q: str | None = Query(None, description="Search term"),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Get list of countries."""
    stmt = select(Pais)
    if q:
        stmt = stmt.where(Pais.descricao.ilike(f"%{q}%"))
    stmt = stmt.limit(limit).order_by(Pais.descricao)

    result = db.execute(stmt)
    rows = result.scalars().all()
    return [{"codigo": r.codigo, "descricao": r.descricao} for r in rows]


@router.get("/municipios")
def get_municipios(
    q: str | None = Query(None, description="Search term"),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Get list of municipalities."""
    stmt = select(Municipio)
    if q:
        stmt = stmt.where(Municipio.descricao.ilike(f"%{q}%"))
    stmt = stmt.limit(limit).order_by(Municipio.descricao)

    result = db.execute(stmt)
    rows = result.scalars().all()
    return [{"codigo": r.codigo, "descricao": r.descricao} for r in rows]


@router.get("/qualificacoes")
def get_qualificacoes(
    q: str | None = Query(None, description="Search term"),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Get list of partner qualifications."""
    stmt = select(QualificacaoSocio)
    if q:
        stmt = stmt.where(QualificacaoSocio.descricao.ilike(f"%{q}%"))
    stmt = stmt.limit(limit).order_by(QualificacaoSocio.codigo)

    result = db.execute(stmt)
    rows = result.scalars().all()
    return [{"codigo": r.codigo, "descricao": r.descricao} for r in rows]


@router.get("/naturezas")
def get_naturezas(
    q: str | None = Query(None, description="Search term"),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Get list of legal nature types."""
    stmt = select(NaturezaJuridica)
    if q:
        stmt = stmt.where(NaturezaJuridica.descricao.ilike(f"%{q}%"))
    stmt = stmt.limit(limit).order_by(NaturezaJuridica.codigo)

    result = db.execute(stmt)
    rows = result.scalars().all()
    return [{"codigo": r.codigo, "descricao": r.descricao} for r in rows]


@router.get("/cnaes")
def get_cnaes(
    q: str | None = Query(None, description="Search term"),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Get list of CNAE codes."""
    stmt = select(Cnae)
    if q:
        stmt = stmt.where(Cnae.descricao.ilike(f"%{q}%"))
    stmt = stmt.limit(limit).order_by(Cnae.codigo)

    result = db.execute(stmt)
    rows = result.scalars().all()
    return [{"codigo": r.codigo, "descricao": r.descricao} for r in rows]


@router.get("/motivos")
def get_motivos(
    q: str | None = Query(None, description="Search term"),
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    """Get list of registration status reasons."""
    stmt = select(MotivoSituacaoCadastral)
    if q:
        stmt = stmt.where(MotivoSituacaoCadastral.descricao.ilike(f"%{q}%"))
    stmt = stmt.limit(limit).order_by(MotivoSituacaoCadastral.codigo)

    result = db.execute(stmt)
    rows = result.scalars().all()
    return [{"codigo": r.codigo, "descricao": r.descricao} for r in rows]


@router.get("/stats")
def get_auxiliares_stats(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Get counts for all auxiliary tables."""
    tables = [
        ("paises", Pais),
        ("municipios", Municipio),
        ("qualificacoes_socios", QualificacaoSocio),
        ("naturezas_juridicas", NaturezaJuridica),
        ("cnaes", Cnae),
        ("motivos_situacao_cadastral", MotivoSituacaoCadastral),
    ]

    stats = {}
    for table_name, model in tables:
        from sqlalchemy import func
        count = db.query(func.count()).select_from(model).scalar()
        stats[table_name] = count or 0

    return stats
