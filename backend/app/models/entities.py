from __future__ import annotations

from sqlalchemy import DateTime, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


# REMOVED: Empresa and Simples tables - data now denormalized into Estabelecimento
# This eliminates JOINs and improves performance significantly

class Estabelecimento(Base):
    """
    SUPER TABLE: Contains establishment data + empresa data + simples data
    This denormalization eliminates 2 JOINs per query, improving performance by ~10x
    """
    __tablename__ = "estabelecimentos"

    # === ESTABELECIMENTO FIELDS (Original) ===
    cnpj14: Mapped[str] = mapped_column(String(14), primary_key=True)
    cnpj_basico: Mapped[str | None] = mapped_column(String(8), index=True)
    cnpj_ordem: Mapped[str | None] = mapped_column(String(4))
    cnpj_dv: Mapped[str | None] = mapped_column(String(2))
    matriz_filial: Mapped[str | None] = mapped_column(String(1))
    nome_fantasia: Mapped[str | None] = mapped_column(Text, index=True)
    situacao_cadastral: Mapped[str | None] = mapped_column(String(2))
    data_situacao_cadastral: Mapped[str | None] = mapped_column(String(8))
    motivo_situacao_cadastral: Mapped[str | None] = mapped_column(String(2))
    nome_cidade_exterior: Mapped[str | None] = mapped_column(Text)
    codigo_pais: Mapped[str | None] = mapped_column(String(3))
    pais: Mapped[str | None] = mapped_column(Text)
    data_inicio_atividade: Mapped[str | None] = mapped_column(String(8))
    cnae_fiscal_principal: Mapped[str | None] = mapped_column(String(7), index=True)
    cnae_fiscal_secundaria: Mapped[str | None] = mapped_column(Text)
    tipo_logradouro: Mapped[str | None] = mapped_column(Text)
    logradouro: Mapped[str | None] = mapped_column(Text)
    numero: Mapped[str | None] = mapped_column(Text)
    complemento: Mapped[str | None] = mapped_column(Text)
    bairro: Mapped[str | None] = mapped_column(Text)
    cep: Mapped[str | None] = mapped_column(String(8))
    uf: Mapped[str | None] = mapped_column(String(2), index=True)
    municipio: Mapped[str | None] = mapped_column(Text, index=True)
    ddd1: Mapped[str | None] = mapped_column(String(4))
    telefone1: Mapped[str | None] = mapped_column(String(9))
    ddd2: Mapped[str | None] = mapped_column(String(4))
    telefone2: Mapped[str | None] = mapped_column(String(9))
    ddd_fax: Mapped[str | None] = mapped_column(String(4))
    fax: Mapped[str | None] = mapped_column(String(9))
    email: Mapped[str | None] = mapped_column(Text)
    situacao_especial: Mapped[str | None] = mapped_column(Text)
    data_situacao_especial: Mapped[str | None] = mapped_column(String(8))

    # === EMPRESA FIELDS (Denormalized from empresas table) ===
    razao_social: Mapped[str | None] = mapped_column(Text, index=True)
    natureza_juridica: Mapped[str | None] = mapped_column(String(4), index=True)
    qualificacao_responsavel: Mapped[str | None] = mapped_column(String(2))
    capital_social: Mapped[Numeric | None] = mapped_column(Numeric(18, 2))
    porte_empresa: Mapped[str | None] = mapped_column(String(2), index=True)
    ente_federativo: Mapped[str | None] = mapped_column(Text)

    # === SIMPLES FIELDS (Denormalized from simples table) ===
    opcao_simples: Mapped[str | None] = mapped_column(String(1), index=True)
    data_opcao_simples: Mapped[str | None] = mapped_column(String(8))
    data_exclusao_simples: Mapped[str | None] = mapped_column(String(8))
    opcao_mei: Mapped[str | None] = mapped_column(String(1), index=True)
    data_opcao_mei: Mapped[str | None] = mapped_column(String(8))
    data_exclusao_mei: Mapped[str | None] = mapped_column(String(8))


class Socio(Base):
    __tablename__ = "socios"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    cnpj_basico: Mapped[str] = mapped_column(String(8), index=True)
    identificador_socio: Mapped[str | None] = mapped_column(String(1))
    nome_socio: Mapped[str | None] = mapped_column(Text, index=True)
    cnpj_cpf_socio: Mapped[str | None] = mapped_column(Text)
    codigo_qualificacao_socio: Mapped[str | None] = mapped_column(String(2))
    percentual_capital_social: Mapped[str | None] = mapped_column(String(6))
    data_entrada_sociedade: Mapped[str | None] = mapped_column(String(8))
    codigo_pais: Mapped[str | None] = mapped_column(String(3))
    cpf_representante_legal: Mapped[str | None] = mapped_column(String(11))
    nome_representante_legal: Mapped[str | None] = mapped_column(Text)
    codigo_qualificacao_representante: Mapped[str | None] = mapped_column(String(2))
    faixa_etaria: Mapped[str | None] = mapped_column(String(2))


# REMOVED: Simples class - data now in Estabelecimento (see above)

class DataVersion(Base):
    __tablename__ = "data_versions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    release: Mapped[str] = mapped_column(String(7), index=True)
    status: Mapped[str] = mapped_column(String(20))
    started_at: Mapped[DateTime] = mapped_column(DateTime, default=func.now())
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime)
    note: Mapped[str | None] = mapped_column(Text)


# Tabelas Auxiliares
class Pais(Base):
    __tablename__ = "paises"

    codigo: Mapped[str] = mapped_column(String(3), primary_key=True)
    descricao: Mapped[str | None] = mapped_column(Text, index=True)


class Municipio(Base):
    __tablename__ = "municipios"

    codigo: Mapped[str] = mapped_column(String(4), primary_key=True)
    descricao: Mapped[str | None] = mapped_column(Text, index=True)


class QualificacaoSocio(Base):
    __tablename__ = "qualificacoes_socios"

    codigo: Mapped[str] = mapped_column(String(2), primary_key=True)
    descricao: Mapped[str | None] = mapped_column(Text, index=True)


class NaturezaJuridica(Base):
    __tablename__ = "naturezas_juridicas"

    codigo: Mapped[str] = mapped_column(String(4), primary_key=True)
    descricao: Mapped[str | None] = mapped_column(Text, index=True)


class Cnae(Base):
    __tablename__ = "cnaes"

    codigo: Mapped[str] = mapped_column(String(7), primary_key=True)
    descricao: Mapped[str | None] = mapped_column(Text, index=True)


class MotivoSituacaoCadastral(Base):
    __tablename__ = "motivos_situacao_cadastral"

    codigo: Mapped[str] = mapped_column(String(2), primary_key=True)
    descricao: Mapped[str | None] = mapped_column(Text, index=True)
