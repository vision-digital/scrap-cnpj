from __future__ import annotations

from decimal import Decimal
from typing import List

from pydantic import BaseModel


# REMOVED: EmpresaSchema and SimplesSchema - data now denormalized in EstabelecimentoSchema

class EstabelecimentoSchema(BaseModel):
    """
    SUPER SCHEMA: Contains all establishment, empresa, and simples data
    This reflects the denormalized database structure
    """
    # === Original Estabelecimento fields ===
    cnpj14: str
    cnpj_basico: str
    cnpj_ordem: str | None = None
    cnpj_dv: str | None = None
    matriz_filial: str | None = None
    nome_fantasia: str | None = None
    situacao_cadastral: str | None = None
    data_situacao_cadastral: str | None = None
    motivo_situacao_cadastral: str | None = None
    nome_cidade_exterior: str | None = None
    codigo_pais: str | None = None
    pais: str | None = None
    data_inicio_atividade: str | None = None
    cnae_fiscal_principal: str | None = None
    cnae_fiscal_secundaria: str | None = None
    tipo_logradouro: str | None = None
    logradouro: str | None = None
    numero: str | None = None
    complemento: str | None = None
    bairro: str | None = None
    cep: str | None = None
    uf: str | None = None
    municipio: str | None = None
    ddd1: str | None = None
    telefone1: str | None = None
    ddd2: str | None = None
    telefone2: str | None = None
    ddd_fax: str | None = None
    fax: str | None = None
    email: str | None = None
    situacao_especial: str | None = None
    data_situacao_especial: str | None = None

    # === From Empresa (denormalized) ===
    razao_social: str | None = None
    natureza_juridica: str | None = None
    qualificacao_responsavel: str | None = None
    capital_social: Decimal | None = None
    porte_empresa: str | None = None
    ente_federativo: str | None = None

    # === From Simples (denormalized) ===
    opcao_simples: str | None = None
    data_opcao_simples: str | None = None
    data_exclusao_simples: str | None = None
    opcao_mei: str | None = None
    data_opcao_mei: str | None = None
    data_exclusao_mei: str | None = None

    model_config = {"from_attributes": True}


class SocioSchema(BaseModel):
    cnpj_basico: str
    razao_social: str | None = None  # From empresas table (via JOIN)
    identificador_socio: str | None = None
    nome_socio: str | None = None
    cnpj_cpf_socio: str | None = None
    codigo_qualificacao_socio: str | None = None
    percentual_capital_social: str | None = None
    data_entrada_sociedade: str | None = None

    model_config = {"from_attributes": True}


# REMOVED: SimplesSchema - data now in EstabelecimentoSchema

class PaginatedResponse(BaseModel):
    total: int  # -1 means "unknown" (to avoid expensive COUNT on huge tables)
    page: int
    page_size: int
    has_more: bool | None = None  # Indicates if there are more pages (optional)
    items: List[dict]
