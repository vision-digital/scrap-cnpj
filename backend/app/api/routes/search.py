from __future__ import annotations

import logging
import time
from typing import Type

from fastapi import APIRouter, Depends, Query
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models import Estabelecimento, Socio  # REMOVED: Empresa, Simples (denormalized)
from app.schemas.entities import (
    EstabelecimentoSchema,
    PaginatedResponse,
    SocioSchema,
)
# REMOVED: EmpresaSchema, SimplesSchema (no longer needed - data in EstabelecimentoSchema)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["busca"])


def _paginate(stmt, schema: Type, db: Session, page: int, page_size: int) -> PaginatedResponse:
    # Skip expensive COUNT for large tables - fetch one extra to detect if there are more pages
    items = (
        db.execute(stmt.offset((page - 1) * page_size).limit(page_size + 1))
        .scalars()
        .all()
    )

    # Check if there are more results
    has_more = len(items) > page_size
    if has_more:
        items = items[:page_size]  # Remove the extra row

    payload = [schema.model_validate(item).model_dump() for item in items]
    # Return -1 as total to indicate "unknown" (avoids expensive COUNT)
    return PaginatedResponse(total=-1, page=page, page_size=page_size, has_more=has_more, items=payload)


# REMOVED: /empresas endpoint - data now in /estabelecimentos (denormalized)
# Use /estabelecimentos with razao_social, natureza_juridica, or porte filters instead

@router.get("/estabelecimentos", response_model=PaginatedResponse)
def search_estabelecimentos(
    cnpj: str | None = Query(None, description="CNPJ completo (14 dígitos)"),
    cnpj_basico: str | None = Query(None, description="CNPJ base (8 dígitos)"),
    nome_fantasia: str | None = Query(None, description="Nome fantasia (busca parcial)"),
    situacao_cadastral: str | None = Query(None, description="Situação cadastral (01, 2, 3, 4, 08)"),
    uf: str | None = Query(None, min_length=2, max_length=2, description="UF (2 letras)"),
    municipio: str | None = Query(None, description="Código do município"),
    bairro: str | None = Query(None, description="Bairro (busca parcial)"),
    logradouro: str | None = Query(None, description="Logradouro (busca parcial)"),
    cep: str | None = Query(None, description="CEP (8 dígitos)"),
    cnae: list[str] | None = Query(None, description="CNAEs principais (múltiplos)"),
    matriz_filial: str | None = Query(None, description="1=Matriz, 2=Filial"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedResponse:
    start_total = time.time()
    print(f"[PERF] Starting search_estabelecimentos: uf={uf}, municipio={municipio}, situacao={situacao_cadastral}, cnae={cnae}", flush=True)

    # DENORMALIZED: Single query, no JOINs needed! razao_social is already in estabelecimentos table
    stmt = select(Estabelecimento)

    if cnpj:
        stmt = stmt.where(Estabelecimento.cnpj14 == cnpj)
    if cnpj_basico:
        stmt = stmt.where(Estabelecimento.cnpj_basico == cnpj_basico)
    if nome_fantasia:
        stmt = stmt.where(Estabelecimento.nome_fantasia.ilike(f"%{nome_fantasia}%"))
    if situacao_cadastral:
        # Pad with leading zeros to match database format (e.g., "2" -> "02")
        situacao_padded = situacao_cadastral.zfill(2)
        stmt = stmt.where(Estabelecimento.situacao_cadastral == situacao_padded)
    if uf:
        stmt = stmt.where(Estabelecimento.uf == uf.upper())
    if municipio:
        stmt = stmt.where(Estabelecimento.municipio == municipio)
    if bairro:
        stmt = stmt.where(Estabelecimento.bairro.ilike(f"%{bairro}%"))
    if logradouro:
        stmt = stmt.where(Estabelecimento.logradouro.ilike(f"%{logradouro}%"))
    if cep:
        stmt = stmt.where(Estabelecimento.cep == cep)
    if cnae and len(cnae) > 0:
        # Search in both principal and secondary CNAE fields
        cnae_pattern = "|".join(cnae)
        stmt = stmt.where(
            or_(
                Estabelecimento.cnae_fiscal_principal.in_(cnae),
                Estabelecimento.cnae_fiscal_secundaria.op("~")(cnae_pattern)
            )
        )
    if matriz_filial:
        stmt = stmt.where(Estabelecimento.matriz_filial == matriz_filial)

    # Order by cnpj14 (indexed, fast)
    stmt = stmt.order_by(Estabelecimento.cnpj14)

    # Get paginated results - fetch one extra to detect if there are more pages
    start_query = time.time()
    results = db.execute(stmt.offset((page - 1) * page_size).limit(page_size + 1)).scalars().all()
    elapsed_query = time.time() - start_query
    print(f"[PERF] Query executed in {elapsed_query:.3f}s, returned {len(results)} rows", flush=True)

    # Check if there are more results
    has_more = len(results) > page_size
    if has_more:
        results = results[:page_size]

    # Serialize directly using Pydantic (NO MERGE NEEDED - all data already in row!)
    start_serialize = time.time()
    items = [EstabelecimentoSchema.model_validate(row).model_dump() for row in results]
    elapsed_serialize = time.time() - start_serialize
    print(f"[PERF] Serialization done in {elapsed_serialize:.3f}s", flush=True)

    elapsed_total = time.time() - start_total
    print(f"[PERF] TOTAL TIME: {elapsed_total:.3f}s", flush=True)

    return PaginatedResponse(total=-1, page=page, page_size=page_size, has_more=has_more, items=items)


@router.get("/socios", response_model=PaginatedResponse)
def search_socios(
    cnpj_basico: str | None = Query(None),
    nome: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedResponse:
    stmt = select(Socio)
    if cnpj_basico:
        stmt = stmt.where(Socio.cnpj_basico == cnpj_basico)
    if nome:
        stmt = stmt.where(Socio.nome_socio.ilike(f"%{nome}%"))
    return _paginate(stmt.order_by(Socio.id), SocioSchema, db, page, page_size)


# REMOVED: /simples endpoint - data now in /estabelecimentos (denormalized)
# Use /estabelecimentos with opcao_simples or opcao_mei filters instead


@router.get("/cnpj/{cnpj_basico}/socios", response_model=PaginatedResponse)
def get_socios_by_cnpj(
    cnpj_basico: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedResponse:
    """Get all partners/shareholders for a specific CNPJ."""
    stmt = select(Socio).where(Socio.cnpj_basico == cnpj_basico)
    return _paginate(stmt.order_by(Socio.nome_socio), SocioSchema, db, page, page_size)


@router.get("/socio/{cpf_cnpj}/empresas", response_model=PaginatedResponse)
def get_empresas_by_socio(
    cpf_cnpj: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedResponse:
    """Get all companies where a person/entity is a partner (by CPF/CNPJ)."""
    start_total = time.time()
    print(f"[PERF] Starting get_empresas_by_socio: cpf_cnpj={cpf_cnpj}", flush=True)

    # Query socios WITHOUT JOIN (much faster)
    stmt = (
        select(Socio)
        .where(Socio.cnpj_cpf_socio.ilike(f"%{cpf_cnpj}%"))
        .order_by(Socio.cnpj_basico)
    )

    # Get paginated results - fetch one extra to detect if there are more pages
    start_query = time.time()
    results = db.execute(stmt.offset((page - 1) * page_size).limit(page_size + 1)).scalars().all()
    elapsed_query = time.time() - start_query
    print(f"[PERF] Socios query executed in {elapsed_query:.3f}s, returned {len(results)} rows", flush=True)

    # Check if there are more results
    has_more = len(results) > page_size
    if has_more:
        results = results[:page_size]

    # DENORMALIZED: Fetch razao_social from estabelecimentos (matriz only)
    start_empresas = time.time()
    cnpj_basicos = [r.cnpj_basico for r in results]
    razao_social_map = {}
    if cnpj_basicos:
        # Query matriz establishments (matriz_filial='1') to get company names
        estabelecimentos = db.execute(
            select(Estabelecimento.cnpj_basico, Estabelecimento.razao_social)
            .where(Estabelecimento.cnpj_basico.in_(cnpj_basicos))
            .where(Estabelecimento.matriz_filial == '1')
        ).all()
        razao_social_map = {e.cnpj_basico: e.razao_social for e in estabelecimentos}
    elapsed_empresas = time.time() - start_empresas
    print(f"[PERF] Estabelecimentos query executed in {elapsed_empresas:.3f}s, fetched {len(razao_social_map)} names", flush=True)

    # Convert rows to dict and merge razao_social
    start_serialize = time.time()
    items = []
    for row in results:
        items.append({
            "cnpj_basico": row.cnpj_basico,
            "razao_social": razao_social_map.get(row.cnpj_basico),
            "identificador_socio": row.identificador_socio,
            "nome_socio": row.nome_socio,
            "cnpj_cpf_socio": row.cnpj_cpf_socio,
            "codigo_qualificacao_socio": row.codigo_qualificacao_socio,
            "percentual_capital_social": row.percentual_capital_social,
            "data_entrada_sociedade": row.data_entrada_sociedade,
        })
    elapsed_serialize = time.time() - start_serialize
    print(f"[PERF] Serialization done in {elapsed_serialize:.3f}s", flush=True)

    elapsed_total = time.time() - start_total
    print(f"[PERF] TOTAL TIME: {elapsed_total:.3f}s", flush=True)

    return PaginatedResponse(total=-1, page=page, page_size=page_size, has_more=has_more, items=items)
