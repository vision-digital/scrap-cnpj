from __future__ import annotations

from sqlalchemy import Date, Index, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Empresa(Base):
    __tablename__ = "empresas"

    cnpj_basico: Mapped[str] = mapped_column(String(8), primary_key=True)
    razao_social: Mapped[str | None] = mapped_column(String(255))
    natureza_juridica: Mapped[str | None] = mapped_column(String(4))
    qualificacao_responsavel: Mapped[str | None] = mapped_column(String(2))
    capital_social: Mapped[float | None] = mapped_column(Numeric(18, 2))
    porte_empresa: Mapped[str | None] = mapped_column(String(1))
    ente_federativo: Mapped[str | None] = mapped_column(String(45))

    __table_args__ = (
        Index("ix_empresas_razao", "razao_social"),
        Index("ix_empresas_natureza", "natureza_juridica"),
    )


class Estabelecimento(Base):
    __tablename__ = "estabelecimentos"

    cnpj: Mapped[str] = mapped_column(String(14), primary_key=True)
    cnpj_basico: Mapped[str] = mapped_column(String(8), index=True)
    cnpj_ordem: Mapped[str] = mapped_column(String(4))
    cnpj_dv: Mapped[str] = mapped_column(String(2))
    matriz_filial: Mapped[str | None] = mapped_column(String(1))
    nome_fantasia: Mapped[str | None] = mapped_column(String(255))
    situacao_cadastral: Mapped[str | None] = mapped_column(String(2))
    data_situacao_cadastral: Mapped[str | None] = mapped_column(String(8))
    motivo_situacao_cadastral: Mapped[str | None] = mapped_column(String(2))
    nome_cidade_exterior: Mapped[str | None] = mapped_column(String(255))
    codigo_pais: Mapped[str | None] = mapped_column(String(3))
    pais: Mapped[str | None] = mapped_column(String(100))
    data_inicio_atividade: Mapped[str | None] = mapped_column(String(8))
    cnae_fiscal_principal: Mapped[str | None] = mapped_column(String(7))
    cnae_fiscal_secundaria: Mapped[str | None] = mapped_column(String(1000))
    tipo_logradouro: Mapped[str | None] = mapped_column(String(20))
    logradouro: Mapped[str | None] = mapped_column(String(255))
    numero: Mapped[str | None] = mapped_column(String(20))
    complemento: Mapped[str | None] = mapped_column(String(100))
    bairro: Mapped[str | None] = mapped_column(String(100))
    cep: Mapped[str | None] = mapped_column(String(8))
    uf: Mapped[str | None] = mapped_column(String(2), index=True)
    municipio: Mapped[str | None] = mapped_column(String(4), index=True)
    ddd1: Mapped[str | None] = mapped_column(String(4))
    telefone1: Mapped[str | None] = mapped_column(String(9))
    ddd2: Mapped[str | None] = mapped_column(String(4))
    telefone2: Mapped[str | None] = mapped_column(String(9))
    ddd_fax: Mapped[str | None] = mapped_column(String(4))
    fax: Mapped[str | None] = mapped_column(String(9))
    email: Mapped[str | None] = mapped_column(String(255))
    situacao_especial: Mapped[str | None] = mapped_column(String(60))
    data_situacao_especial: Mapped[str | None] = mapped_column(String(8))

    __table_args__ = (
        Index("ix_estabelecimentos_nome", "nome_fantasia"),
        Index("ix_estabelecimentos_cnae", "cnae_fiscal_principal"),
    )


class Socio(Base):
    __tablename__ = "socios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cnpj_basico: Mapped[str] = mapped_column(String(8), index=True)
    identificador_socio: Mapped[str | None] = mapped_column(String(1))
    nome_socio: Mapped[str | None] = mapped_column(String(255))
    cnpj_cpf_socio: Mapped[str | None] = mapped_column(String(14))
    codigo_qualificacao_socio: Mapped[str | None] = mapped_column(String(2))
    percentual_capital_social: Mapped[str | None] = mapped_column(String(6))
    data_entrada_sociedade: Mapped[str | None] = mapped_column(String(8))
    codigo_pais: Mapped[str | None] = mapped_column(String(3))
    cpf_representante_legal: Mapped[str | None] = mapped_column(String(11))
    nome_representante_legal: Mapped[str | None] = mapped_column(String(255))
    codigo_qualificacao_representante: Mapped[str | None] = mapped_column(String(2))
    faixa_etaria: Mapped[str | None] = mapped_column(String(2))

    __table_args__ = (Index("ix_socios_nome", "nome_socio"),)


class Simples(Base):
    __tablename__ = "simples"

    cnpj_basico: Mapped[str] = mapped_column(String(8), primary_key=True)
    opcao_simples: Mapped[str | None] = mapped_column(String(1))
    data_opcao_simples: Mapped[str | None] = mapped_column(String(8))
    data_exclusao_simples: Mapped[str | None] = mapped_column(String(8))
    opcao_mei: Mapped[str | None] = mapped_column(String(1))
    data_opcao_mei: Mapped[str | None] = mapped_column(String(8))
    data_exclusao_mei: Mapped[str | None] = mapped_column(String(8))

    __table_args__ = (Index("ix_simples_opcao", "opcao_simples"),)
