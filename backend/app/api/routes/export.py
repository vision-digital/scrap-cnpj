from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Iterable, Type

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

router = APIRouter(prefix="/export", tags=["relatórios"])


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
    return _build_csv_response(db, stmt, EmpresaSchema, filename_prefix="empresas")


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
    return _build_csv_response(db, stmt, EstabelecimentoSchema, filename_prefix="estabelecimentos")


@router.get("/socios")
def export_socios(
    cnpj_basico: str | None = Query(None),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    stmt = select(Socio)
    if cnpj_basico:
        stmt = stmt.where(Socio.cnpj_basico == cnpj_basico)
    return _build_csv_response(db, stmt, SocioSchema, filename_prefix="socios")


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
    return _build_csv_response(db, stmt, SimplesSchema, filename_prefix="simples")


def _build_csv_response(db: Session, stmt, schema: Type, filename_prefix: str) -> StreamingResponse:
    filename = f"{filename_prefix}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"

    def streamer() -> Iterable[bytes]:
        header = list(schema.model_fields.keys())
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=header, delimiter=";")
        writer.writeheader()
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.truncate(0)
        result = db.execute(stmt.execution_options(yield_per=5_000))
        for row in result.scalars():
            data = schema.model_validate(row).model_dump()
            writer.writerow(data)
            yield buffer.getvalue().encode("utf-8")
            buffer.seek(0)
            buffer.truncate(0)

    return StreamingResponse(
        streamer(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
