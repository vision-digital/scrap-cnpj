from __future__ import annotations

from decimal import Decimal
from typing import List

from pydantic import BaseModel


class EmpresaSchema(BaseModel):
    cnpj_basico: str
    razao_social: str | None = None
    natureza_juridica: str | None = None
    qualificacao_responsavel: str | None = None
    capital_social: Decimal | None = None
    porte_empresa: str | None = None
    ente_federativo: str | None = None

    model_config = {"from_attributes": True}


class EstabelecimentoSchema(BaseModel):
    cnpj14: str
    cnpj_basico: str
    nome_fantasia: str | None = None
    situacao_cadastral: str | None = None
    cnae_fiscal_principal: str | None = None
    cnae_fiscal_secundaria: str | None = None
    uf: str | None = None
    municipio: str | None = None
    cep: str | None = None
    logradouro: str | None = None
    numero: str | None = None
    bairro: str | None = None
    email: str | None = None

    model_config = {"from_attributes": True}


class SocioSchema(BaseModel):
    cnpj_basico: str
    identificador_socio: str | None = None
    nome_socio: str | None = None
    cnpj_cpf_socio: str | None = None
    codigo_qualificacao_socio: str | None = None
    percentual_capital_social: str | None = None
    data_entrada_sociedade: str | None = None

    model_config = {"from_attributes": True}


class SimplesSchema(BaseModel):
    cnpj_basico: str
    opcao_simples: str | None = None
    data_opcao_simples: str | None = None
    data_exclusao_simples: str | None = None
    opcao_mei: str | None = None
    data_opcao_mei: str | None = None
    data_exclusao_mei: str | None = None

    model_config = {"from_attributes": True}


class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[dict]
