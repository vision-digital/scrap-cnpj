from __future__ import annotations

from typing import Type

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models import Empresa, Estabelecimento, Simples, Socio
from app.schemas.entities import (
    EmpresaSchema,
    EstabelecimentoSchema,
    PaginatedResponse,
    SimplesSchema,
    SocioSchema,
)

router = APIRouter(prefix="/search", tags=["busca"])


def _paginate(stmt, schema: Type, db: Session, page: int, page_size: int) -> PaginatedResponse:
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    items = (
        db.execute(stmt.offset((page - 1) * page_size).limit(page_size))
        .scalars()
        .all()
    )
    payload = [schema.model_validate(item).model_dump() for item in items]
    return PaginatedResponse(total=total, page=page, page_size=page_size, items=payload)


@router.get("/empresas", response_model=PaginatedResponse)
def search_empresas(
    razao_social: str | None = Query(None, description="Filtro por razao social"),
    natureza_juridica: str | None = Query(None),
    porte: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedResponse:
    stmt = select(Empresa)
    if razao_social:
        stmt = stmt.where(Empresa.razao_social.ilike(f"%{razao_social}%"))
    if natureza_juridica:
        stmt = stmt.where(Empresa.natureza_juridica == natureza_juridica)
    if porte:
        stmt = stmt.where(Empresa.porte_empresa == porte)
    return _paginate(stmt.order_by(Empresa.cnpj_basico), EmpresaSchema, db, page, page_size)


@router.get("/estabelecimentos", response_model=PaginatedResponse)
def search_estabelecimentos(
    cnpj: str | None = Query(None),
    nome_fantasia: str | None = Query(None),
    uf: str | None = Query(None, min_length=2, max_length=2),
    municipio: str | None = Query(None),
    cnae: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedResponse:
    stmt = select(Estabelecimento)
    if cnpj:
        stmt = stmt.where(Estabelecimento.cnpj14 == cnpj)
    if nome_fantasia:
        stmt = stmt.where(Estabelecimento.nome_fantasia.ilike(f"%{nome_fantasia}%"))
    if uf:
        stmt = stmt.where(Estabelecimento.uf == uf.upper())
    if municipio:
        stmt = stmt.where(Estabelecimento.municipio == municipio)
    if cnae:
        stmt = stmt.where(Estabelecimento.cnae_fiscal_principal == cnae)
    return _paginate(stmt.order_by(Estabelecimento.cnpj14), EstabelecimentoSchema, db, page, page_size)


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


@router.get("/simples", response_model=PaginatedResponse)
def search_simples(
    cnpj_basico: str | None = Query(None),
    opcao_simples: str | None = Query(None),
    opcao_mei: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedResponse:
    stmt = select(Simples)
    if cnpj_basico:
        stmt = stmt.where(Simples.cnpj_basico == cnpj_basico)
    if opcao_simples:
        stmt = stmt.where(Simples.opcao_simples == opcao_simples)
    if opcao_mei:
        stmt = stmt.where(Simples.opcao_mei == opcao_mei)
    return _paginate(stmt.order_by(Simples.cnpj_basico), SimplesSchema, db, page, page_size)
