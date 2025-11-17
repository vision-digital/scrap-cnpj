"""Loader for auxiliary tables (CNAEs, Municipios, Paises, etc)."""
from __future__ import annotations

import csv
import io
import logging
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Sequence
from urllib.parse import urljoin

import httpx
from tqdm import tqdm

from app.core.config import get_settings
from app.db.postgres import get_connection
from app.services.receita_client import ReceitaFederalClient

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass(frozen=True)
class AuxiliarConfig:
    """Configuration for auxiliary dataset."""
    signature: str  # Filename pattern to match
    table: str
    columns: Sequence[str]
    builder: Callable[[List[str]], List[str]]


def _clean_null_bytes(value: str) -> str:
    """Remove null bytes from string."""
    if isinstance(value, str):
        return value.replace('\x00', '').strip()
    return value


def _build_simple_pair(row: List[str]) -> List[str]:
    """Build simple codigo/descricao pair."""
    values = [_clean_null_bytes(v) for v in row]
    if len(values) < 2:
        values.extend([''] * (2 - len(values)))
    return values[:2]


# Map of auxiliary datasets
AUXILIARES: Dict[str, AuxiliarConfig] = {
    "Paises": AuxiliarConfig(
        signature="Paises",
        table="paises",
        columns=("codigo", "descricao"),
        builder=_build_simple_pair,
    ),
    "Municipios": AuxiliarConfig(
        signature="Municipios",
        table="municipios",
        columns=("codigo", "descricao"),
        builder=_build_simple_pair,
    ),
    "Qualificacoes": AuxiliarConfig(
        signature="Qualificacoes",
        table="qualificacoes_socios",
        columns=("codigo", "descricao"),
        builder=_build_simple_pair,
    ),
    "Naturezas": AuxiliarConfig(
        signature="Naturezas",
        table="naturezas_juridicas",
        columns=("codigo", "descricao"),
        builder=_build_simple_pair,
    ),
    "Cnaes": AuxiliarConfig(
        signature="Cnaes",
        table="cnaes",
        columns=("codigo", "descricao"),
        builder=_build_simple_pair,
    ),
    "Motivos": AuxiliarConfig(
        signature="Motivos",
        table="motivos_situacao_cadastral",
        columns=("codigo", "descricao"),
        builder=_build_simple_pair,
    ),
}


def identify_auxiliar(file_path: Path) -> AuxiliarConfig | None:
    """Identify which auxiliary dataset a file belongs to."""
    name = file_path.name
    for key, config in AUXILIARES.items():
        if key in name:
            return config
    return None


class LoaderAuxiliares:
    """Loader for auxiliary tables."""

    # Expected auxiliary file names
    AUXILIARY_FILES = [
        "Paises.zip",
        "Municipios.zip",
        "Qualificacoes.zip",
        "Naturezas.zip",
        "Cnaes.zip",
        "Motivos.zip",
    ]

    def download_and_load(self, release: str) -> None:
        """Download auxiliary files and load them into database."""
        print(f"\n{'='*80}", flush=True)
        print(f"📥 BAIXANDO TABELAS AUXILIARES - Release: {release}", flush=True)
        print(f"{'='*80}\n", flush=True)

        # Download auxiliary files
        raw_dir = Path(settings.data_dir) / "raw" / release
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Get base URL for release
        client = ReceitaFederalClient()
        release_url = urljoin(client.base_url, f"{release}/")

        downloaded_count = 0
        with httpx.Client(timeout=settings.http_timeout) as http_client:
            for filename in self.AUXILIARY_FILES:
                file_url = urljoin(release_url, filename)
                target_path = raw_dir / filename

                # Skip if already exists and has content
                if target_path.exists() and target_path.stat().st_size > 0:
                    print(f"✅ {filename} já existe, pulando download", flush=True)
                    downloaded_count += 1
                    continue

                try:
                    print(f"📥 Baixando {filename}...", flush=True)

                    # Retry up to 2 times for connection errors
                    for attempt in range(2):
                        try:
                            response = http_client.get(file_url, follow_redirects=True)
                            response.raise_for_status()

                            with open(target_path, 'wb') as f:
                                f.write(response.content)

                            size_kb = target_path.stat().st_size / 1024
                            print(f"✅ {filename} baixado ({size_kb:.1f} KB)", flush=True)
                            downloaded_count += 1
                            break

                        except (httpx.RemoteProtocolError, httpx.ReadTimeout) as e:
                            if attempt < 1:
                                print(f"⚠️  Tentando novamente {filename}...", flush=True)
                                continue
                            raise

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        logger.warning(f"⚠️  {filename} não encontrado na release {release}")
                    else:
                        logger.error(f"❌ Erro ao baixar {filename}: {e}")
                except Exception as e:
                    logger.error(f"❌ Erro ao baixar {filename}: {e}")

        client.close()

        print(f"\n📦 {downloaded_count}/{len(self.AUXILIARY_FILES)} arquivos prontos\n", flush=True)

        # Now load the data
        self.load_from_release(release)

    def load_from_release(self, release: str) -> None:
        """Load all auxiliary files from a release."""
        raw_dir = Path(settings.data_dir) / "raw" / release
        if not raw_dir.exists():
            raise FileNotFoundError(f"Release directory not found: {raw_dir}")

        print(f"\n{'='*80}", flush=True)
        print(f"🗂️  CARREGANDO TABELAS AUXILIARES - Release: {release}", flush=True)
        print(f"{'='*80}\n", flush=True)

        # Find all auxiliary ZIPs
        zip_files = []
        for config in AUXILIARES.values():
            pattern = f"{config.signature}*.zip"
            found = list(raw_dir.glob(pattern))
            if found:
                zip_files.extend(found)
            else:
                logger.warning(f"⚠️  Arquivo {pattern} não encontrado em {raw_dir}")

        if not zip_files:
            raise FileNotFoundError(f"Nenhum arquivo auxiliar encontrado em {raw_dir}")

        print(f"📦 Encontrados {len(zip_files)} arquivos auxiliares\n", flush=True)

        # Process each ZIP
        for zip_path in zip_files:
            config = identify_auxiliar(zip_path)
            if not config:
                logger.warning(f"Arquivo ignorado (desconhecido): {zip_path.name}")
                continue

            self._load_zip(zip_path, config)

        print(f"\n{'='*80}", flush=True)
        print("✅ CARGA DE TABELAS AUXILIARES CONCLUÍDA", flush=True)
        print(f"{'='*80}\n", flush=True)

    def _load_zip(self, zip_path: Path, config: AuxiliarConfig) -> None:
        """Load data from a ZIP file."""
        start_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{start_time}] 📂 Processando {zip_path.name} → {config.table}", flush=True)

        global_start = time.time()

        # Truncate table first
        self._truncate_table(config.table)

        total_rows = 0

        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Get first CSV file in ZIP (files end with CSV, not .CSV)
            csv_files = [name for name in zf.namelist() if name.upper().endswith('CSV')]
            if not csv_files:
                logger.warning(f"Nenhum CSV encontrado em {zip_path.name}")
                return

            csv_name = csv_files[0]
            logger.info(f"Extraindo {csv_name} de {zip_path.name}")

            with zf.open(csv_name) as csv_file:
                # Read with latin-1 encoding
                text_file = io.TextIOWrapper(csv_file, encoding='latin-1', newline='')
                reader = csv.reader(text_file, delimiter=';', quotechar='"')

                with get_connection() as conn, conn.cursor() as cur:
                    copy_sql = (
                        f"COPY {config.table} ({', '.join(config.columns)}) "
                        "FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', NULL '')"
                    )

                    batch_data = []
                    batch_size = 1000  # Smaller batch for auxiliary tables

                    with tqdm(desc=f"{config.table}", unit="rows", mininterval=0.5) as progress:
                        for row in reader:
                            if not row or all(not field.strip() for field in row):
                                continue

                            try:
                                built = config.builder(row)
                            except Exception:
                                logger.exception("Failed to parse row: %s", row[:2])
                                continue

                            batch_data.append(built)
                            total_rows += 1
                            progress.update()

                            # Write batch when full
                            if len(batch_data) >= batch_size:
                                self._write_batch(cur, copy_sql, batch_data)
                                batch_data = []

                        # Write remaining rows
                        if batch_data:
                            self._write_batch(cur, copy_sql, batch_data)

                    conn.commit()

        elapsed = time.time() - global_start
        end_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{end_time}] ✅ {config.table}: {total_rows:,} linhas em {elapsed:.1f}s\n", flush=True)

    def _truncate_table(self, table_name: str) -> None:
        """Truncate table before loading."""
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
            conn.commit()

    @staticmethod
    def _write_batch(cur, copy_sql: str, rows: list) -> None:
        """Write batch using COPY."""
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(row)
        buffer.seek(0)

        with cur.copy(copy_sql) as copy:
            copy.write(buffer.read())
