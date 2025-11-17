from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Iterable

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models import Estabelecimento, Socio  # REMOVED: Empresa, Simples (denormalized)
from app.schemas.entities import (
    EstabelecimentoSchema,
    SocioSchema,
)
# REMOVED: EmpresaSchema, SimplesSchema (data now in EstabelecimentoSchema)

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


# REMOVED: /empresas export endpoint - data now in /estabelecimentos (denormalized)
# Use /estabelecimentos with razao_social, natureza_juridica, or porte filters instead


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


# REMOVED: /simples export endpoint - data now in /estabelecimentos (denormalized)
# Use /estabelecimentos with opcao_simples or opcao_mei filters instead
