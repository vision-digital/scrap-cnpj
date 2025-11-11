from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Dict, Iterable

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models import Empresa, Estabelecimento, Simples, Socio
from app.schemas.entities import (
    EmpresaSchema,
    EstabelecimentoSchema,
    SimplesSchema,
    SocioSchema,
)

router = APIRouter(prefix="/export", tags=["relatorios"])


def _stream_models(db: Session, stmt, schema) -> Iterable[bytes]:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";")
    columns = list(schema.model_fields.keys())
    writer.writerow(columns)
    yield buffer.getvalue().encode("utf-8")
    buffer.seek(0)
    buffer.truncate(0)
    stream = db.execute(stmt).scalars()
    for obj in stream:
        data = schema.model_validate(obj).model_dump()
        writer.writerow([data.get(col, "") for col in columns])
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.truncate(0)


@router.get("/empresas")
def export_empresas(
    razao_social: str | None = Query(None),
    natureza_juridica: str | None = Query(None),
    porte: str | None = Query(None),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    stmt = select(Empresa)
    if razao_social:
        stmt = stmt.where(Empresa.razao_social.ilike(f"%{razao_social}%"))
    if natureza_juridica:
        stmt = stmt.where(Empresa.natureza_juridica == natureza_juridica)
    if porte:
        stmt = stmt.where(Empresa.porte_empresa == porte)
    filename = f"empresas_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
    return StreamingResponse(
        _stream_models(db, stmt, EmpresaSchema),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/estabelecimentos")
def export_estabelecimentos(
    uf: str | None = Query(None),
    municipio: str | None = Query(None),
    cnae: str | None = Query(None),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    stmt = select(Estabelecimento)
    if uf:
        stmt = stmt.where(Estabelecimento.uf == uf.upper())
    if municipio:
        stmt = stmt.where(Estabelecimento.municipio == municipio)
    if cnae:
        stmt = stmt.where(Estabelecimento.cnae_fiscal_principal == cnae)
    filename = f"estabelecimentos_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
    return StreamingResponse(
        _stream_models(db, stmt, EstabelecimentoSchema),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/socios")
def export_socios(
    cnpj_basico: str | None = Query(None),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    stmt = select(Socio)
    if cnpj_basico:
        stmt = stmt.where(Socio.cnpj_basico == cnpj_basico)
    filename = f"socios_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
    return StreamingResponse(
        _stream_models(db, stmt, SocioSchema),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/simples")
def export_simples(
    opcao_simples: str | None = Query(None),
    opcao_mei: str | None = Query(None),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    stmt = select(Simples)
    if opcao_simples:
        stmt = stmt.where(Simples.opcao_simples == opcao_simples)
    if opcao_mei:
        stmt = stmt.where(Simples.opcao_mei == opcao_mei)
    filename = f"simples_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
    return StreamingResponse(
        _stream_models(db, stmt, SimplesSchema),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
