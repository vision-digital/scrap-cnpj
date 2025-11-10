from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence, Type

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import session_scope
from app.models import Empresa, Estabelecimento, Simples, Socio

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass(frozen=True)
class DatasetConfig:
    signature: str
    model: Type
    columns: Sequence[str]
    extra_handler: Callable[[dict], dict] | None = None


DATASETS: Dict[str, DatasetConfig] = {
    "EMPRECSV": DatasetConfig(
        signature="EMPRECSV",
        model=Empresa,
        columns=(
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo",
        ),
    ),
    "ESTABELE": DatasetConfig(
        signature="ESTABELE",
        model=Estabelecimento,
        columns=(
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "codigo_pais",
            "pais",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio",
            "ddd1",
            "telefone1",
            "ddd2",
            "telefone2",
            "ddd_fax",
            "fax",
            "email",
            "situacao_especial",
            "data_situacao_especial",
        ),
    ),
    "SOCIOCSV": DatasetConfig(
        signature="SOCIOCSV",
        model=Socio,
        columns=(
            "cnpj_basico",
            "identificador_socio",
            "nome_socio",
            "cnpj_cpf_socio",
            "codigo_qualificacao_socio",
            "percentual_capital_social",
            "data_entrada_sociedade",
            "codigo_pais",
            "cpf_representante_legal",
            "nome_representante_legal",
            "codigo_qualificacao_representante",
            "faixa_etaria",
        ),
    ),
    "SIMECSV": DatasetConfig(
        signature="SIMECSV",
        model=Simples,
        columns=(
            "cnpj_basico",
            "opcao_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
        ),
    ),
}


def identify_dataset(file_path: Path) -> DatasetConfig | None:
    name = file_path.name.upper()
    for key, config in DATASETS.items():
        if key in name:
            return config
    return None


class Loader:
    """Loads extracted CSV/TXT files into MySQL in batches."""

    def __init__(self, batch_size: int | None = None) -> None:
        self.batch_size = batch_size or settings.batch_size

    def load_files(self, files: Iterable[Path]) -> None:
        for file_path in files:
            dataset = identify_dataset(file_path)
            if not dataset:
                logger.info("Arquivo %s ignorado (dataset desconhecido)", file_path.name)
                continue
            logger.info("Iniciando carga de %s", file_path.name)
            self._load_file(file_path, dataset)

    def _load_file(self, file_path: Path, dataset: DatasetConfig) -> None:
        with session_scope() as session:
            self._stream_insert(session, file_path, dataset)

    def _stream_insert(self, session: Session, file_path: Path, dataset: DatasetConfig) -> None:
        rows_buffer: List[dict] = []
        with open(file_path, encoding="latin-1") as handle:
            reader = csv.reader(handle, delimiter=";", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                prepared = self._prepare_row(row, dataset)
                rows_buffer.append(prepared)
                if len(rows_buffer) >= self.batch_size:
                    self._flush(session, dataset, rows_buffer)
                    rows_buffer.clear()
        if rows_buffer:
            self._flush(session, dataset, rows_buffer)

    def _prepare_row(self, row: List[str], dataset: DatasetConfig) -> dict:
        values = [value.strip() or None for value in row]
        mapped = dict(zip(dataset.columns, values))
        if dataset.model is Estabelecimento:
            mapped["cnpj"] = (
                (mapped.get("cnpj_basico") or "")
                + (mapped.get("cnpj_ordem") or "")
                + (mapped.get("cnpj_dv") or "")
            )
        if dataset.model is Empresa and mapped.get("capital_social"):
            mapped["capital_social"] = (
                mapped["capital_social"].replace(".", "").replace(",", ".")
            )
        if dataset.extra_handler:
            mapped = dataset.extra_handler(mapped)
        return mapped

    def _flush(self, session: Session, dataset: DatasetConfig, buffer: List[dict]) -> None:
        stmt = mysql_insert(dataset.model).prefix_with("IGNORE").values(buffer)
        session.execute(stmt)
        logger.info("Inseridos %s registros em %s", len(buffer), dataset.model.__tablename__)
