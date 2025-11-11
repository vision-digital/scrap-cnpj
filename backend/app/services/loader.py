from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence

from tqdm import tqdm

from app.db.postgres import get_connection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetConfig:
    signature: str
    table: str
    columns: Sequence[str]
    builder: Callable[[List[str]], List[str]]


def _pad(value: str, size: int) -> str:
    cleaned = "".join(ch for ch in value if ch.isdigit())
    return cleaned[:size].zfill(size)


def _build_empresas(row: List[str]) -> List[str]:
    values = [value.strip() for value in row]
    while len(values) < 7:
        values.append("")
    values[0] = _pad(values[0], 8)
    values[4] = values[4].replace(".", "").replace(",", ".") or "0"
    return values[:7]


def _build_estabelecimentos(row: List[str]) -> List[str]:
    values = [value.strip() for value in row]
    while len(values) < 31:
        values.append("")
    values[0] = _pad(values[0], 8)
    values[1] = _pad(values[1], 4)
    values[2] = _pad(values[2], 2)
    cnpj14 = f"{values[0]}{values[1]}{values[2]}"
    return [cnpj14, *values[:31]]


def _build_socios(row: List[str]) -> List[str]:
    values = [value.strip() for value in row]
    while len(values) < 12:
        values.append("")
    values[0] = _pad(values[0], 8)
    return values[:12]


def _build_simples(row: List[str]) -> List[str]:
    values = [value.strip() for value in row]
    while len(values) < 7:
        values.append("")
    values[0] = _pad(values[0], 8)
    return values[:7]


DATASETS: Dict[str, DatasetConfig] = {
    "EMPRECSV": DatasetConfig(
        signature="EMPRECSV",
        table="empresas",
        columns=(
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo",
        ),
        builder=_build_empresas,
    ),
    "ESTABELE": DatasetConfig(
        signature="ESTABELE",
        table="estabelecimentos",
        columns=(
            "cnpj14",
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
        builder=_build_estabelecimentos,
    ),
    "SOCIOCSV": DatasetConfig(
        signature="SOCIOCSV",
        table="socios",
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
        builder=_build_socios,
    ),
    "SIMECSV": DatasetConfig(
        signature="SIMECSV",
        table="simples",
        columns=(
            "cnpj_basico",
            "opcao_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
        ),
        builder=_build_simples,
    ),
}


def identify_dataset(file_path: Path) -> DatasetConfig | None:
    name = file_path.name.upper()
    for key, config in DATASETS.items():
        if key in name:
            return config
    return None


class Loader:
    def load_files(self, files: Iterable[Path]) -> None:
        for file_path in files:
            dataset = identify_dataset(file_path)
            if not dataset:
                logger.info("File %s ignored (unknown dataset)", file_path.name)
                continue
            logger.info("Copying %s into %s", file_path.name, dataset.table)
            self._copy_file(file_path, dataset)

    def _copy_file(self, file_path: Path, dataset: DatasetConfig) -> None:
        with get_connection() as conn, conn.cursor() as cur, open(
            file_path, encoding="latin-1", newline=""
        ) as handle:
            reader = csv.reader(handle, delimiter=";", quotechar='"')
            copy_sql = (
                f"COPY {dataset.table} ({', '.join(dataset.columns)}) "
                "FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', NULL '')"
            )

            with cur.copy(copy_sql) as copy, tqdm(
                desc=f"{dataset.table}:{file_path.name}", unit="rows"
            ) as progress:
                buffer = io.StringIO()
                writer = csv.writer(buffer, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

                batch = []
                for row in reader:
                    if not row or all(not field.strip() for field in row):
                        continue

                    if len(row) == 1 and "\t" in row[0]:
                        row = row[0].split("\t")

                    try:
                        built = dataset.builder(row)
                    except Exception:
                        logger.exception("Failed to parse row in %s: %s", file_path.name, row[:3])
                        continue

                    batch.append(built)
                    progress.update()

                    if len(batch) >= 5000:
                        self._write_batch(writer, buffer, copy, batch)
                        batch = []

                # Write remaining rows
                if batch:
                    self._write_batch(writer, buffer, copy, batch)

            conn.commit()
            logger.info("Finished loading %s", file_path.name)

    @staticmethod
    def _write_batch(writer: csv.writer, buffer: io.StringIO, copy, batch: list) -> None:
        buffer.seek(0)
        buffer.truncate()
        for row in batch:
            writer.writerow(row)
        buffer.seek(0)
        copy.write(buffer.read())
