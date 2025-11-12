from __future__ import annotations

import csv
import io
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence, Set

from tqdm import tqdm

from app.core.config import get_settings
from app.db.postgres import get_connection

logger = logging.getLogger(__name__)
settings = get_settings()


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

    # Pad and truncate to match database schema
    values[0] = _pad(values[0], 8)                              # cnpj_basico VARCHAR(8)
    values[2] = values[2][:4] if len(values[2]) > 0 else ""    # natureza_juridica VARCHAR(4)
    values[3] = values[3][:2] if len(values[3]) > 0 else ""    # qualificacao_responsavel VARCHAR(2)
    values[4] = values[4].replace(".", "").replace(",", ".") or "0"  # capital_social NUMERIC(18,2)
    values[5] = values[5][:2] if len(values[5]) > 0 else ""    # porte_empresa VARCHAR(2)

    return values[:7]


def _build_estabelecimentos(row: List[str]) -> List[str]:
    values = [value.strip() for value in row]

    # CSV has 30 columns, not 31 (missing data_situacao_especial)
    while len(values) < 30:
        values.append("")

    # Pad CNPJ components
    values[0] = _pad(values[0], 8)   # cnpj_basico
    values[1] = _pad(values[1], 4)   # cnpj_ordem
    values[2] = _pad(values[2], 2)   # cnpj_dv
    cnpj14 = f"{values[0]}{values[1]}{values[2]}"

    # Truncate fields to match database column sizes
    values[3] = values[3][:1] if len(values[3]) > 0 else ""     # matriz_filial VARCHAR(1)
    values[5] = values[5][:2] if len(values[5]) > 0 else ""     # situacao_cadastral VARCHAR(2)
    values[6] = values[6][:8] if len(values[6]) > 0 else ""     # data_situacao_cadastral VARCHAR(8)
    values[7] = values[7][:2] if len(values[7]) > 0 else ""     # motivo_situacao_cadastral VARCHAR(2)
    values[9] = values[9][:3] if len(values[9]) > 0 else ""     # codigo_pais VARCHAR(3)
    values[11] = values[11][:8] if len(values[11]) > 0 else ""  # data_inicio_atividade VARCHAR(8)
    values[12] = values[12][:7] if len(values[12]) > 0 else ""  # cnae_fiscal_principal VARCHAR(7)
    values[19] = values[19][:8] if len(values[19]) > 0 else ""  # cep VARCHAR(8)
    values[20] = values[20][:2] if len(values[20]) > 0 else ""  # uf VARCHAR(2)
    values[22] = values[22][:4] if len(values[22]) > 0 else ""  # ddd1 VARCHAR(4)
    values[23] = values[23][:9] if len(values[23]) > 0 else ""  # telefone1 VARCHAR(9)
    values[24] = values[24][:4] if len(values[24]) > 0 else ""  # ddd2 VARCHAR(4)
    values[25] = values[25][:9] if len(values[25]) > 0 else ""  # telefone2 VARCHAR(9)
    values[26] = values[26][:4] if len(values[26]) > 0 else ""  # ddd_fax VARCHAR(4)
    values[27] = values[27][:9] if len(values[27]) > 0 else ""  # fax VARCHAR(9)

    # Return: cnpj14 + 30 CSV columns + empty data_situacao_especial = 32 total
    return [cnpj14, *values[:30], ""]


def _build_socios(row: List[str]) -> List[str]:
    values = [value.strip() for value in row]

    # CSV has 11 columns, missing percentual_capital_social at position 5
    while len(values) < 11:
        values.append("")

    # Pad and truncate to match database schema
    values[0] = _pad(values[0], 8)                             # cnpj_basico VARCHAR(8)
    values[1] = values[1][:1] if len(values[1]) > 0 else ""   # identificador_socio VARCHAR(1)
    values[4] = values[4][:2] if len(values[4]) > 0 else ""   # codigo_qualificacao_socio VARCHAR(2)
    values[5] = values[5][:8] if len(values[5]) > 0 else ""   # data_entrada_sociedade VARCHAR(8)
    values[6] = values[6][:3] if len(values[6]) > 0 else ""   # codigo_pais VARCHAR(3)
    values[7] = values[7][:11] if len(values[7]) > 0 else ""  # cpf_representante_legal VARCHAR(11)
    values[9] = values[9][:2] if len(values[9]) > 0 else ""   # codigo_qualificacao_representante VARCHAR(2)
    values[10] = values[10][:2] if len(values[10]) > 0 else "" # faixa_etaria VARCHAR(2)

    # Insert missing percentual_capital_social at position 5
    # Return: cols 0-4, empty percentual, cols 5-10 = 12 total
    return values[:5] + [""] + values[5:11]


def _build_simples(row: List[str]) -> List[str]:
    values = [value.strip() for value in row]
    while len(values) < 7:
        values.append("")

    # Pad and truncate to match database schema
    values[0] = _pad(values[0], 8)                             # cnpj_basico VARCHAR(8)
    values[1] = values[1][:1] if len(values[1]) > 0 else ""   # opcao_simples VARCHAR(1)
    values[2] = values[2][:8] if len(values[2]) > 0 else ""   # data_opcao_simples VARCHAR(8)
    values[3] = values[3][:8] if len(values[3]) > 0 else ""   # data_exclusao_simples VARCHAR(8)
    values[4] = values[4][:1] if len(values[4]) > 0 else ""   # opcao_mei VARCHAR(1)
    values[5] = values[5][:8] if len(values[5]) > 0 else ""   # data_opcao_mei VARCHAR(8)
    values[6] = values[6][:8] if len(values[6]) > 0 else ""   # data_exclusao_mei VARCHAR(8)

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
    def __init__(self):
        self._truncated_tables: Set[str] = set()
        self._dropped_indexes: Dict[str, List[str]] = {}
        self._dropped_pks: Dict[str, tuple] = {}  # table -> (constraint_name, columns)

    def load_files(self, files: Iterable[Path]) -> None:
        # Convert to list to get count and iterate
        file_list = list(files)

        start_time = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'='*80}", flush=True)
        print(f"[{start_time}] 🚀 INICIANDO CARGA DE DADOS", flush=True)
        print(f"{'='*80}", flush=True)
        print(f"Total de arquivos: {len(file_list)}", flush=True)
        print(f"\n⚡ ESTRATÉGIA DE OTIMIZAÇÃO:", flush=True)
        print(f"  1. DROP todos os indexes (exceto PK) antes da carga", flush=True)
        print(f"  2. Carregamento rápido SEM atualizar indexes", flush=True)
        print(f"  3. Recreate indexes em batch no final (muito mais eficiente)", flush=True)
        print(f"{'='*80}\n", flush=True)

        for file_path in file_list:
            dataset = identify_dataset(file_path)
            if not dataset:
                logger.info("File %s ignored (unknown dataset)", file_path.name)
                continue

            # Truncate table and drop indexes before first file
            if dataset.table not in self._truncated_tables:
                self._drop_indexes(dataset.table)
                self._truncate_table(dataset.table)
                self._truncated_tables.add(dataset.table)

            logger.info("Copying %s into %s", file_path.name, dataset.table)
            self._copy_file(file_path, dataset)

        # Recreate indexes after all files loaded
        self._recreate_indexes()

    def _truncate_table(self, table_name: str) -> None:
        """Truncate table before loading to avoid duplicates."""
        start_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{start_time}] 🗑️  TRUNCANDO TABELA: {table_name}", flush=True)

        truncate_start = time.time()
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
            conn.commit()
        truncate_elapsed = time.time() - truncate_start

        end_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{end_time}] ✅ TABELA TRUNCADA: {table_name} (tempo: {truncate_elapsed:.1f}s)\n", flush=True)

    def _copy_file(self, file_path: Path, dataset: DatasetConfig) -> None:
        """Load file using periodic commits - close/reopen COPY properly."""
        start_timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'='*80}", flush=True)
        print(f"[{start_timestamp}] 📂 INICIANDO PROCESSAMENTO: {file_path.name} → {dataset.table}", flush=True)
        print(f"{'='*80}\n", flush=True)

        global_start = time.time()
        total_rows = 0

        with get_connection() as conn, conn.cursor() as cur, open(
            file_path, encoding="latin-1", newline=""
        ) as handle:
            reader = csv.reader(handle, delimiter=";", quotechar='"')
            copy_sql = (
                f"COPY {dataset.table} ({', '.join(dataset.columns)}) "
                "FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', NULL '')"
            )

            batch_data = []

            commit_count = 0
            last_progress_log = 0
            with tqdm(desc=f"{dataset.table}:{file_path.name}", unit="rows", mininterval=0.5) as progress:
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

                    batch_data.append(built)
                    total_rows += 1
                    progress.update()

                    # Log progress every 100k rows (besides commits)
                    if total_rows - last_progress_log >= 100_000 and len(batch_data) < settings.commit_batch_size:
                        elapsed = time.time() - global_start
                        rate = total_rows / elapsed if elapsed > 0 else 0
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        msg = f"[{timestamp}] 📊 Processando... {total_rows:,} linhas lidas | Velocidade média: {rate/1000:.0f}k rows/sec"
                        progress.write(msg)
                        print(msg, flush=True)
                        last_progress_log = total_rows

                    # When batch is full, write and commit
                    if len(batch_data) >= settings.commit_batch_size:
                        commit_count += 1
                        start_time = datetime.now().strftime("%H:%M:%S")

                        # Log ANTES do commit
                        start_msg = f"[{start_time}] 🔄 INICIANDO COMMIT #{commit_count} | {len(batch_data):,} rows | Total: {total_rows:,}"
                        progress.write(start_msg)
                        print(start_msg, flush=True)

                        batch_start = time.time()
                        timings = self._write_and_commit(cur, conn, copy_sql, batch_data)
                        batch_elapsed = time.time() - batch_start

                        end_time = datetime.now().strftime("%H:%M:%S")

                        rate = len(batch_data) / batch_elapsed if batch_elapsed > 0 else 0
                        msg = (
                            f"[{end_time}] ✅ COMMIT #{commit_count} COMPLETO | {len(batch_data):,} rows → {dataset.table} | "
                            f"Total: {total_rows:,} | Velocidade: {rate/1000:.0f}k rows/sec | "
                            f"Tempo: {batch_elapsed:.1f}s (CSV: {timings['build_csv']:.1f}s, COPY: {timings['copy']:.1f}s, COMMIT: {timings['commit']:.1f}s)"
                        )
                        progress.write(msg)
                        print(msg, flush=True)
                        batch_data = []

                # Write remaining rows
                if batch_data:
                    commit_count += 1
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    batch_start = time.time()
                    timings = self._write_and_commit(cur, conn, copy_sql, batch_data)
                    batch_elapsed = time.time() - batch_start

                    rate = len(batch_data) / batch_elapsed if batch_elapsed > 0 else 0
                    msg = (
                        f"[{timestamp}] ✅ COMMIT FINAL #{commit_count} | {len(batch_data):,} rows → {dataset.table} | "
                        f"Total: {total_rows:,} | Velocidade: {rate/1000:.0f}k rows/sec | "
                        f"Tempo: {batch_elapsed:.1f}s (CSV: {timings['build_csv']:.1f}s, COPY: {timings['copy']:.1f}s, COMMIT: {timings['commit']:.1f}s)"
                    )
                    progress.write(msg)
                    print(msg, flush=True)

            total_elapsed = time.time() - global_start
            overall_rate = total_rows / total_elapsed if total_elapsed > 0 else 0
            logger.info(
                "✅ Finished %s: %s rows in %.1f sec (%.0fk rows/sec overall)",
                file_path.name, f"{total_rows:,}", total_elapsed, overall_rate / 1000
            )
            sys.stdout.flush()

    def _drop_indexes(self, table_name: str) -> None:
        """Drop ALL indexes INCLUDING PRIMARY KEY before loading."""
        start_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{start_time}] 🔨 DROPANDO INDEXES E PRIMARY KEY: {table_name}", flush=True)

        drop_start = time.time()
        dropped = []

        with get_connection() as conn, conn.cursor() as cur:
            # Drop PRIMARY KEY constraint first
            cur.execute("""
                SELECT conname, pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = %s::regclass
                AND contype = 'p'
            """, (table_name,))

            pk_info = cur.fetchone()
            if pk_info:
                pk_name, pk_def = pk_info
                print(f"  - Dropping PRIMARY KEY {pk_name}...", flush=True)
                cur.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {pk_name}")
                self._dropped_pks[table_name] = (pk_name, pk_def)
                conn.commit()

            # Get all remaining indexes (secondary indexes)
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s
                ORDER BY indexname
            """, (table_name,))

            indexes = cur.fetchall()

            for index_name, index_def in indexes:
                print(f"  - Dropping index {index_name}...", flush=True)
                cur.execute(f"DROP INDEX IF EXISTS {index_name}")
                dropped.append((index_name, index_def))

            conn.commit()

        self._dropped_indexes[table_name] = dropped
        drop_elapsed = time.time() - drop_start

        end_time = datetime.now().strftime("%H:%M:%S")
        pk_msg = " + PK" if table_name in self._dropped_pks else ""
        print(f"[{end_time}] ✅ {len(dropped)} INDEXES{pk_msg} DROPADOS de {table_name} (tempo: {drop_elapsed:.1f}s)\n", flush=True)

    def _recreate_indexes(self) -> None:
        """Recreate all dropped PRIMARY KEYS and indexes after loading."""
        if not self._dropped_indexes and not self._dropped_pks:
            return

        start_time = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'='*80}", flush=True)
        print(f"[{start_time}] 🔧 RECRIANDO PRIMARY KEYS E INDEXES...", flush=True)
        print(f"{'='*80}\n", flush=True)

        total_start = time.time()

        with get_connection() as conn, conn.cursor() as cur:
            # Recreate PRIMARY KEYs first (more important for data integrity)
            for table_name, (pk_name, pk_def) in self._dropped_pks.items():
                pk_start_time = datetime.now().strftime("%H:%M:%S")
                print(f"[{pk_start_time}] 🔑 Recriando PRIMARY KEY em {table_name}...", flush=True)

                pk_start = time.time()
                cur.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT {pk_name} {pk_def}")
                conn.commit()
                pk_elapsed = time.time() - pk_start

                pk_end_time = datetime.now().strftime("%H:%M:%S")
                print(f"[{pk_end_time}] ✅ PRIMARY KEY {pk_name} criado (tempo: {pk_elapsed:.1f}s)\n", flush=True)

            # Recreate secondary indexes
            for table_name, indexes in self._dropped_indexes.items():
                for index_name, index_def in indexes:
                    idx_start_time = datetime.now().strftime("%H:%M:%S")
                    print(f"[{idx_start_time}] 🔨 Recriando {index_name}...", flush=True)

                    idx_start = time.time()
                    cur.execute(index_def)
                    conn.commit()
                    idx_elapsed = time.time() - idx_start

                    idx_end_time = datetime.now().strftime("%H:%M:%S")
                    print(f"[{idx_end_time}] ✅ {index_name} criado (tempo: {idx_elapsed:.1f}s)", flush=True)

        total_elapsed = time.time() - total_start
        end_time = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{end_time}] ✅ TODAS AS PRIMARY KEYS E INDEXES RECRIADOS (tempo total: {total_elapsed:.1f}s)\n", flush=True)

    @staticmethod
    def _clean_null_bytes(value: str) -> str:
        """Remove null bytes from string to prevent UTF-8 encoding errors."""
        if isinstance(value, str):
            return value.replace('\x00', '')
        return value

    @staticmethod
    def _write_and_commit(cur, conn, copy_sql: str, rows: list) -> dict:
        """Write batch using COPY and commit - atomic operation."""
        timings = {}

        # Build CSV buffer
        t0 = time.time()
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            # Clean null bytes from each cell to prevent encoding errors
            cleaned_row = [Loader._clean_null_bytes(cell) for cell in row]
            writer.writerow(cleaned_row)
        buffer.seek(0)
        timings['build_csv'] = time.time() - t0

        # Execute COPY
        t0 = time.time()
        with cur.copy(copy_sql) as copy:
            copy.write(buffer.read())
        timings['copy'] = time.time() - t0

        # Commit transaction
        t0 = time.time()
        conn.commit()
        timings['commit'] = time.time() - t0

        return timings
