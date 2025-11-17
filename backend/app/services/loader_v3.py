"""
LOADER V3: STREAMING DENORMALIZED IMPORT WITH CHECKPOINTS

Efficient streaming loader that:
1. Uses TEMP staging tables (cleared on each run)
2. Streams data via COPY FROM STDIN (memory efficient)
3. Tracks progress with checkpoints (4 phases)
4. Properly commits after each phase
5. Batch size: 5000 (configurable)

PHASES:
- FASE 1: Import EMPRESAS to staging
- FASE 2: Import SIMPLES to staging
- FASE 3: Import ESTABELECIMENTOS to staging
- FASE 4: Import SOCIOS to staging
- Final: Merge staging -> production tables
"""
from __future__ import annotations

import csv
import io
import logging
import shutil
from pathlib import Path
from typing import List

import psycopg
from sqlalchemy import text
from tqdm import tqdm

from app.core.config import get_settings
from app.db.session import engine

logger = logging.getLogger(__name__)
settings = get_settings()


class LoaderV3:
    def __init__(self):
        self.batch_size = 5000
        self.consolidation_batch_size = 10000  # CNPJs por lote na PARTE 2
        self._ensure_checkpoint_table()

    def _ensure_checkpoint_table(self) -> None:
        """Create checkpoint tracking table if not exists"""
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS import_checkpoints (
                    release VARCHAR(7) PRIMARY KEY,
                    fase1_empresas BOOLEAN DEFAULT FALSE,
                    fase2_simples BOOLEAN DEFAULT FALSE,
                    fase3_estabelecimentos BOOLEAN DEFAULT FALSE,
                    fase4_socios BOOLEAN DEFAULT FALSE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            # Table to track individual files processed
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS import_files_processed (
                    id SERIAL PRIMARY KEY,
                    release VARCHAR(7),
                    fase VARCHAR(50),
                    filename VARCHAR(255),
                    rows_imported BIGINT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(release, fase, filename)
                )
            """))

    def _get_checkpoint(self, release: str) -> dict:
        """Get current checkpoint status for release"""
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT * FROM import_checkpoints WHERE release = :release"),
                {"release": release}
            ).fetchone()

            if result is None:
                # Initialize checkpoint
                conn.execute(
                    text("""
                        INSERT INTO import_checkpoints (release)
                        VALUES (:release)
                        ON CONFLICT (release) DO NOTHING
                    """),
                    {"release": release}
                )
                return {
                    "fase1_empresas": False,
                    "fase2_simples": False,
                    "fase3_estabelecimentos": False,
                    "fase4_socios": False
                }

            return {
                "fase1_empresas": result[1],
                "fase2_simples": result[2],
                "fase3_estabelecimentos": result[3],
                "fase4_socios": result[4]
            }

    def _mark_phase_complete(self, release: str, phase: str) -> None:
        """Mark a phase as completed"""
        with engine.begin() as conn:
            conn.execute(
                text(f"UPDATE import_checkpoints SET {phase} = TRUE, updated_at = CURRENT_TIMESTAMP WHERE release = :release"),
                {"release": release}
            )

    def _is_file_processed(self, release: str, fase: str, filename: str) -> bool:
        """Check if a file was already processed"""
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM import_files_processed WHERE release = :release AND fase = :fase AND filename = :filename"),
                {"release": release, "fase": fase, "filename": filename}
            ).scalar()
            return result > 0

    def _mark_file_processed(self, release: str, fase: str, filename: str, rows_imported: int) -> None:
        """Mark a file as processed"""
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO import_files_processed (release, fase, filename, rows_imported)
                    VALUES (:release, :fase, :filename, :rows_imported)
                    ON CONFLICT (release, fase, filename) DO UPDATE SET
                        rows_imported = :rows_imported,
                        processed_at = CURRENT_TIMESTAMP
                """),
                {"release": release, "fase": fase, "filename": filename, "rows_imported": rows_imported}
            )

    def _execute_copy(self, sql: str, buffer: io.StringIO) -> None:
        """Execute COPY command using raw psycopg connection"""
        buffer.seek(0)
        with psycopg.connect(settings.psycopg_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.copy_expert(sql, buffer)
            conn.commit()  # CRITICAL: Commit the transaction

    def load_files(self, files: List[Path]) -> None:
        """Load all CSV files in 4 phases with checkpoints"""
        if not files:
            logger.warning("No files to load")
            return

        # Determine release from first file path
        release = files[0].parent.name
        checkpoint = self._get_checkpoint(release)

        logger.info("")
        logger.info("=" * 80)
        logger.info("🚀 LOADER V3: STREAMING DENORMALIZED IMPORT (WITH CHECKPOINTS)")
        logger.info("=" * 80)
        logger.info("")
        logger.info(f"📦 Release: {release}")
        logger.info("🔍 Checkpoint status:")
        logger.info(f"  FASE 1 (Empresas): {'✓ CONCLUÍDA' if checkpoint['fase1_empresas'] else '⏳ PENDENTE'}")
        logger.info(f"  FASE 2 (Simples): {'✓ CONCLUÍDA' if checkpoint['fase2_simples'] else '⏳ PENDENTE'}")
        logger.info(f"  FASE 3 (Estabelecimentos): {'✓ CONCLUÍDA' if checkpoint['fase3_estabelecimentos'] else '⏳ PENDENTE'}")
        logger.info(f"  FASE 4 (Sócios): {'✓ CONCLUÍDA' if checkpoint['fase4_socios'] else '⏳ PENDENTE'}")

        # Categorize files
        empresas_files = [f for f in files if "EMPRECSV" in f.name]
        simples_files = [f for f in files if "SIMPLES" in f.name or "SIMECSV" in f.name]
        estabelecimentos_files = [f for f in files if "ESTABELE" in f.name]
        socios_files = [f for f in files if "SOCIO" in f.name]

        logger.info("")
        logger.info("📁 Arquivos identificados:")
        logger.info(f"  Estabelecimentos: {len(estabelecimentos_files)}")
        logger.info(f"  Empresas: {len(empresas_files)}")
        logger.info(f"  Simples: {len(simples_files)}")
        logger.info(f"  Sócios: {len(socios_files)}")
        logger.info("")

        # FASE 1: EMPRESAS
        if not checkpoint["fase1_empresas"]:
            self._load_empresas_phase(empresas_files, release)
            self._mark_phase_complete(release, "fase1_empresas")
        else:
            logger.info("⏭️  Pulando FASE 1 (Empresas) - já concluída")

        # FASE 2: SIMPLES
        if not checkpoint["fase2_simples"]:
            self._load_simples_phase(simples_files, release)
            self._mark_phase_complete(release, "fase2_simples")
        else:
            logger.info("⏭️  Pulando FASE 2 (Simples) - já concluída")

        # VALIDAÇÃO: Verify staging tables exist before FASE 3
        if not checkpoint["fase3_estabelecimentos"]:
            if checkpoint["fase1_empresas"] or checkpoint["fase2_simples"]:
                logger.info("🔍 Validando staging tables antes da FASE 3...")
                with psycopg.connect(settings.psycopg_dsn) as conn:
                    with conn.cursor() as cursor:
                        # Check if staging tables exist and have data
                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_name = 'staging_empresas'
                            )
                        """)
                        empresas_exists = cursor.fetchone()[0]

                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_name = 'staging_simples'
                            )
                        """)
                        simples_exists = cursor.fetchone()[0]

                        if not empresas_exists or not simples_exists:
                            logger.warning("⚠️  Staging tables não existem! Invalidando checkpoints e reexecutando FASES 1 e 2...")
                            cursor.execute("DELETE FROM import_files_processed WHERE release = %s AND fase IN ('fase1_empresas', 'fase2_simples')", (release,))
                            conn.commit()
                            # Re-execute phases
                            logger.info("🔄 Reexecutando FASE 1...")
                            self._load_empresas_phase(empresas_files, release)
                            self._mark_phase_complete(release, "fase1_empresas")
                            logger.info("🔄 Reexecutando FASE 2...")
                            self._load_simples_phase(simples_files, release)
                            self._mark_phase_complete(release, "fase2_simples")

        # FASE 3: ESTABELECIMENTOS
        if not checkpoint["fase3_estabelecimentos"]:
            self._load_estabelecimentos_phase(estabelecimentos_files, release)
            self._mark_phase_complete(release, "fase3_estabelecimentos")
        else:
            logger.info("⏭️  Pulando FASE 3 (Estabelecimentos) - já concluída")

        # FASE 4: SOCIOS
        if not checkpoint["fase4_socios"]:
            self._load_socios_phase(socios_files, release)
            self._mark_phase_complete(release, "fase4_socios")
        else:
            logger.info("⏭️  Pulando FASE 4 (Sócios) - já concluída")

        # ========== MELHORIA 4: LIMPEZA FINAL E MENSAGEM DE SUCESSO ==========
        logger.info("")
        logger.info("=" * 80)
        logger.info("🧹 LIMPEZA FINAL")
        logger.info("=" * 80)
        logger.info("")

        with psycopg.connect(settings.psycopg_dsn) as conn:
            with conn.cursor() as cursor:
                # Drop staging tables
                logger.info("🗑️  Removendo tabelas staging...")
                cursor.execute("DROP TABLE IF EXISTS staging_empresas CASCADE")
                cursor.execute("DROP TABLE IF EXISTS staging_simples CASCADE")
                cursor.execute("DROP TABLE IF EXISTS staging_estabelecimentos CASCADE")
                logger.info("  ✓ Tabelas staging removidas")

                # Drop control tables
                logger.info("🗑️  Removendo tabelas de controle...")
                cursor.execute("DROP TABLE IF EXISTS import_checkpoints CASCADE")
                cursor.execute("DROP TABLE IF EXISTS import_files_processed CASCADE")
                logger.info("  ✓ Tabelas de controle removidas")

            conn.commit()

        # ========== MELHORIA 5: REMOVER ARQUIVOS BAIXADOS ==========
        logger.info("")
        logger.info("🗑️  Removendo arquivos baixados...")
        raw_dir = Path(settings.data_dir) / "raw" / release
        staging_dir = Path(settings.data_dir) / "staging" / release

        if raw_dir.exists():
            shutil.rmtree(raw_dir)
            logger.info(f"  ✓ Removido: {raw_dir}")

        if staging_dir.exists():
            shutil.rmtree(staging_dir)
            logger.info(f"  ✓ Removido: {staging_dir}")

        logger.info("✅ Arquivos removidos")

        # ========== MENSAGEM DE SUCESSO FINAL ==========
        logger.info("")
        logger.info("=" * 80)
        logger.info("🎉 IMPORTAÇÃO CONCLUÍDA COM SUCESSO!")
        logger.info("=" * 80)
        logger.info(f"📊 Release: {release}")
        logger.info("✅ Todas as fases completadas")
        logger.info("✅ Índices criados")
        logger.info("✅ Limpeza realizada")
        logger.info("=" * 80)

    def _load_empresas_phase(self, files: List[Path], release: str) -> None:
        """FASE 1: Load empresas to staging table"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📋 FASE 1: IMPORTANDO EMPRESAS (STAGING)")
        logger.info("=" * 80)
        logger.info("")

        # Create/truncate staging table - NORMAL TABLE (not TEMP!)
        with psycopg.connect(settings.psycopg_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS staging_empresas (
                        cnpj_basico VARCHAR(8) PRIMARY KEY,
                        razao_social VARCHAR(255),
                        natureza_juridica VARCHAR(4),
                        qualificacao_responsavel VARCHAR(2),
                        capital_social DECIMAL(20,2),
                        porte_empresa VARCHAR(2),
                        ente_federativo VARCHAR(100)
                    )
                """)

                # TRUNCATE only if no files processed yet for this release (version change)
                cursor.execute(
                    "SELECT COUNT(*) FROM import_files_processed WHERE release = %s AND fase = 'fase1_empresas'",
                    (release,)
                )
                processed_count = cursor.fetchone()[0]

                if processed_count == 0:
                    logger.info("🗑️  TRUNCATE staging_empresas (nova versão)")
                    cursor.execute("TRUNCATE TABLE staging_empresas")
                else:
                    logger.info(f"⏭️  Mantendo dados existentes ({processed_count} arquivos já processados)")

            conn.commit()

            logger.info("✓ Staging table criada")

            # Filter already processed files
            pending_files = [f for f in files if not self._is_file_processed(release, "fase1_empresas", f.name)]

            if len(pending_files) < len(files):
                logger.info(f"⏭️  Pulando {len(files) - len(pending_files)} arquivos já processados")

            # Load files using SAME connection, commit after each
            for file_path in pending_files:
                rows_imported = self._stream_empresas_file_same_conn(file_path, conn)
                conn.commit()  # COMMIT AFTER EACH FILE
                self._mark_file_processed(release, "fase1_empresas", file_path.name, rows_imported)
                logger.info(f"  ✓ Arquivo {file_path.name} commitado ({rows_imported:,} linhas)")

        logger.info("✓ FASE 1 CONCLUÍDA")

    def _clean_null_bytes(self, value: str) -> str:
        """Remove NULL bytes (0x00) from string - invalid in UTF-8/PostgreSQL"""
        if not value:
            return ""
        return value.replace("\x00", "")

    def _escape_pg_copy(self, value: str) -> str:
        """Escape string for PostgreSQL COPY TEXT format"""
        if not value:
            return ""
        # Remove NULL bytes (0x00) - invalid in UTF-8/PostgreSQL
        value = value.replace("\x00", "")
        # Escape backslash, tab, newline, carriage return
        value = value.replace("\\", "\\\\")
        value = value.replace("\t", "\\t")
        value = value.replace("\n", "\\n")
        value = value.replace("\r", "\\r")
        return value

    def _stream_empresas_file_same_conn(self, file_path: Path, conn: psycopg.Connection) -> int:
        """Stream empresas file using COPY FROM STDIN on same connection. Returns number of rows imported."""
        logger.info(f"📂 Importando: {file_path.name}")

        total_rows = 0
        last_rows = []  # Keep last 10 rows for debugging

        with open(file_path, "r", encoding="latin1") as f:
            reader = csv.reader(f, delimiter=";", quotechar='"')

            pbar = tqdm(desc=f"  {file_path.name}", unit="rows", position=0, leave=True)

            buffer = io.StringIO()
            batch_count = 0

            for row in reader:
                if len(row) < 7:
                    continue

                # Keep last 10 rows for debugging
                last_rows.append(row[:])
                if len(last_rows) > 10:
                    last_rows.pop(0)

                cnpj_basico = self._clean_null_bytes(row[0].strip().zfill(8))
                razao_social = self._escape_pg_copy(row[1][:255] if row[1] else "")
                natureza_juridica = self._clean_null_bytes(row[2][:4] if row[2] else "")

                # Parse qualificacao_responsavel - remove decimal part if present
                qualificacao_raw = row[3].strip().split(".")[0] if row[3] else ""
                qualificacao = self._clean_null_bytes(qualificacao_raw[:2] if qualificacao_raw else "")

                # Parse capital_social
                capital_str = self._clean_null_bytes(row[4].replace(",", ".") if row[4] else "0")
                try:
                    capital_social = float(capital_str)
                except ValueError:
                    capital_social = 0.0

                # Parse porte_empresa - remove decimal part if present
                porte_raw = row[5].strip().split(".")[0] if row[5] else ""
                porte = self._clean_null_bytes(porte_raw[:2] if porte_raw else "")

                ente = self._escape_pg_copy(row[6][:100] if row[6] else "")

                # Write directly as tab-separated line (no csv.writer!)
                line = f"{cnpj_basico}\t{razao_social}\t{natureza_juridica}\t{qualificacao}\t{capital_social}\t{porte}\t{ente}\n"
                buffer.write(line)

                batch_count += 1
                total_rows += 1
                pbar.update(1)

                if batch_count >= self.batch_size:
                    buffer.seek(0)
                    try:
                        with conn.cursor() as cursor:
                            with cursor.copy("COPY staging_empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo) FROM STDIN") as copy:
                                copy.write(buffer.read())
                    except Exception as e:
                        logger.error("=" * 80)
                        logger.error(f"❌ ERRO NO COPY - Total rows: {total_rows}")
                        logger.error(f"❌ ÚLTIMAS 10 LINHAS PROCESSADAS (RAW CSV):")
                        for i, debug_row in enumerate(last_rows, 1):
                            logger.error(f"   Linha {i}: {debug_row}")
                        logger.error("=" * 80)
                        raise
                    buffer = io.StringIO()
                    batch_count = 0

            # Final batch
            if batch_count > 0:
                buffer.seek(0)
                try:
                    with conn.cursor() as cursor:
                        with cursor.copy("COPY staging_empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo) FROM STDIN") as copy:
                            copy.write(buffer.read())
                except Exception as e:
                    logger.error("=" * 80)
                    logger.error(f"❌ ERRO NO COPY FINAL - Total rows: {total_rows}")
                    logger.error(f"❌ ÚLTIMAS 10 LINHAS PROCESSADAS (RAW CSV):")
                    for i, debug_row in enumerate(last_rows, 1):
                        logger.error(f"   Linha {i}: {debug_row}")
                    logger.error("=" * 80)
                    raise

            pbar.close()

        return total_rows

    def _load_simples_phase(self, files: List[Path], release: str) -> None:
        """FASE 2: Load simples to staging table"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📋 FASE 2: IMPORTANDO SIMPLES (STAGING)")
        logger.info("=" * 80)
        logger.info("")

        # Create/truncate staging table - NORMAL TABLE (not TEMP!)
        with psycopg.connect(settings.psycopg_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS staging_simples (
                        cnpj_basico VARCHAR(8) PRIMARY KEY,
                        opcao_simples VARCHAR(1),
                        data_opcao_simples VARCHAR(8),
                        data_exclusao_simples VARCHAR(8),
                        opcao_mei VARCHAR(1),
                        data_opcao_mei VARCHAR(8),
                        data_exclusao_mei VARCHAR(8)
                    )
                """)

                # TRUNCATE only if no files processed yet for this release (version change)
                cursor.execute(
                    "SELECT COUNT(*) FROM import_files_processed WHERE release = %s AND fase = 'fase2_simples'",
                    (release,)
                )
                processed_count = cursor.fetchone()[0]

                if processed_count == 0:
                    logger.info("🗑️  TRUNCATE staging_simples (nova versão)")
                    cursor.execute("TRUNCATE TABLE staging_simples")
                else:
                    logger.info(f"⏭️  Mantendo dados existentes ({processed_count} arquivos já processados)")

            conn.commit()

            logger.info("✓ Staging table criada")

            # Filter already processed files
            pending_files = [f for f in files if not self._is_file_processed(release, "fase2_simples", f.name)]

            if len(pending_files) < len(files):
                logger.info(f"⏭️  Pulando {len(files) - len(pending_files)} arquivos já processados")

            # Load files using SAME connection, commit after each
            for file_path in pending_files:
                rows_imported = self._stream_simples_file_same_conn(file_path, conn)
                conn.commit()  # COMMIT AFTER EACH FILE
                self._mark_file_processed(release, "fase2_simples", file_path.name, rows_imported)
                logger.info(f"  ✓ Arquivo {file_path.name} commitado ({rows_imported:,} linhas)")

        logger.info("✓ FASE 2 CONCLUÍDA")

    def _stream_simples_file_same_conn(self, file_path: Path, conn: psycopg.Connection) -> int:
        """Stream simples file using COPY FROM STDIN on same connection. Returns number of rows imported."""
        logger.info(f"📂 Importando: {file_path.name}")

        total_rows = 0
        with open(file_path, "r", encoding="latin1") as f:
            reader = csv.reader(f, delimiter=";", quotechar='"')

            pbar = tqdm(desc=f"  {file_path.name}", unit="rows", position=0, leave=True)

            buffer = io.StringIO()
            batch_count = 0

            for row in reader:
                if len(row) < 7:
                    continue

                cnpj_basico = self._clean_null_bytes(row[0].strip().zfill(8))

                # Clean all fields
                opcao_simples = self._clean_null_bytes(row[1][:1] if row[1] else '')
                data_opcao_simples = self._clean_null_bytes(row[2][:8] if row[2] else '')
                data_exclusao_simples = self._clean_null_bytes(row[3][:8] if row[3] else '')
                opcao_mei = self._clean_null_bytes(row[4][:1] if row[4] else '')
                data_opcao_mei = self._clean_null_bytes(row[5][:8] if row[5] else '')
                data_exclusao_mei = self._clean_null_bytes(row[6][:8] if row[6] else '')

                # Write directly as tab-separated line (no csv.writer!)
                line = f"{cnpj_basico}\t{opcao_simples}\t{data_opcao_simples}\t{data_exclusao_simples}\t{opcao_mei}\t{data_opcao_mei}\t{data_exclusao_mei}\n"
                buffer.write(line)

                batch_count += 1
                total_rows += 1
                pbar.update(1)

                if batch_count >= self.batch_size:
                    buffer.seek(0)
                    with conn.cursor() as cursor:
                        with cursor.copy("COPY staging_simples FROM STDIN") as copy:
                            copy.write(buffer.read())
                    buffer = io.StringIO()
                    batch_count = 0

            # Final batch
            if batch_count > 0:
                buffer.seek(0)
                with conn.cursor() as cursor:
                    with cursor.copy("COPY staging_simples FROM STDIN") as copy:
                        copy.write(buffer.read())

            pbar.close()

        return total_rows

    def _drop_all_indexes_except_pk(self, conn: psycopg.Connection, table_name: str) -> List[str]:
        """Drop all indexes except PRIMARY KEY. Returns list of dropped index DDL for recreation."""
        logger.info(f"🗑️  Dropando índices da tabela {table_name} para otimizar carga...")

        dropped_indexes = []
        with conn.cursor() as cursor:
            # Get all indexes except PRIMARY KEY
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s
                AND indexname NOT LIKE '%%_pkey'
                ORDER BY indexname
            """, (table_name,))

            indexes = cursor.fetchall()
            logger.info(f"  Encontrados {len(indexes)} índices para dropar")

            for idx_name, idx_def in indexes:
                cursor.execute(f"DROP INDEX IF EXISTS {idx_name}")
                dropped_indexes.append(idx_def)
                logger.info(f"  ✓ Dropado: {idx_name}")

        return dropped_indexes

    def _recreate_indexes(self, conn: psycopg.Connection, index_ddls: List[str]) -> None:
        """Recreate indexes from DDL list"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("🔨 RECRIANDO ÍNDICES (isso pode demorar alguns minutos...)")
        logger.info("=" * 80)

        with conn.cursor() as cursor:
            for idx_ddl in index_ddls:
                logger.info(f"  Criando: {idx_ddl[:80]}...")
                cursor.execute(idx_ddl)
                logger.info(f"  ✓ Criado")

        logger.info("✓ Todos os índices foram recriados")

    def _load_estabelecimentos_phase(self, files: List[Path], release: str) -> None:
        """
        FASE 3: Load estabelecimentos com nova estratégia CREATE TABLE AS SELECT

        PARTE 1: INSERT em staging_estabelecimentos (sem dados de empresas/simples)
        PARTE 2: CREATE TABLE estabelecimentos AS SELECT com LEFT JOIN (1 query!)
        PARTE 3: Criar índices
        PARTE 4: DROP staging tables
        """

        with psycopg.connect(settings.psycopg_dsn) as conn:

            # ========== PARTE 1: INSERIR EM STAGING_ESTABELECIMENTOS ==========
            logger.info("")
            logger.info("=" * 80)
            logger.info("📋 FASE 3 - PARTE 1: INSERINDO ESTABELECIMENTOS (STAGING)")
            logger.info("=" * 80)
            logger.info("")

            with conn.cursor() as cursor:
                # Create staging_estabelecimentos table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS staging_estabelecimentos (
                        cnpj14 VARCHAR(14) PRIMARY KEY,
                        cnpj_basico VARCHAR(8),
                        cnpj_ordem VARCHAR(4),
                        cnpj_dv VARCHAR(2),
                        matriz_filial VARCHAR(1),
                        nome_fantasia TEXT,
                        situacao_cadastral VARCHAR(2),
                        data_situacao_cadastral VARCHAR(8),
                        motivo_situacao_cadastral VARCHAR(2),
                        nome_cidade_exterior TEXT,
                        codigo_pais VARCHAR(3),
                        pais TEXT,
                        data_inicio_atividade VARCHAR(8),
                        cnae_fiscal_principal VARCHAR(7),
                        cnae_fiscal_secundaria TEXT,
                        tipo_logradouro TEXT,
                        logradouro TEXT,
                        numero TEXT,
                        complemento TEXT,
                        bairro TEXT,
                        cep VARCHAR(8),
                        uf VARCHAR(2),
                        municipio TEXT,
                        ddd1 VARCHAR(4),
                        telefone1 VARCHAR(9),
                        ddd2 VARCHAR(4),
                        telefone2 VARCHAR(9),
                        ddd_fax VARCHAR(4),
                        fax VARCHAR(9),
                        email TEXT,
                        situacao_especial TEXT,
                        data_situacao_especial VARCHAR(8)
                    )
                """)

                # Check if fresh import
                cursor.execute(
                    "SELECT COUNT(*) FROM import_files_processed WHERE release = %s AND fase = 'fase3_parte1_staging'",
                    (release,)
                )
                processed_count = cursor.fetchone()[0]

                if processed_count == 0:
                    logger.info("🗑️  TRUNCATE staging_estabelecimentos (nova versão)")
                    cursor.execute("TRUNCATE TABLE staging_estabelecimentos")

            conn.commit()

            # Process files
            pending_files = [f for f in files if not self._is_file_processed(release, "fase3_parte1_staging", f.name)]

            if len(pending_files) < len(files):
                logger.info(f"⏭️  Pulando {len(files) - len(pending_files)} arquivos já processados")

            for file_path in pending_files:
                rows_imported = self._stream_estabelecimentos_to_staging(file_path, conn)
                conn.commit()
                self._mark_file_processed(release, "fase3_parte1_staging", file_path.name, rows_imported)
                logger.info(f"  ✓ Arquivo {file_path.name} commitado ({rows_imported:,} linhas)")

            logger.info("✅ PARTE 1 CONCLUÍDA - Staging table populada")


            # ========== PARTE 2: CREATE TABLE AS SELECT (CONSOLIDACAO) ==========
            logger.info("")
            logger.info("=" * 80)
            logger.info(">> FASE 3 - PARTE 2: CONSOLIDACAO (CREATE TABLE AS SELECT)")
            logger.info("=" * 80)
            logger.info("")

            chunk_fase = "fase3_parte2_chunks"
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM import_files_processed WHERE release = %s AND fase = 'fase3_parte2_create_table'",
                    (release,)
                )
                parte2_done = cursor.fetchone()[0] > 0

                cursor.execute(
                    """
                    SELECT filename, rows_imported
                    FROM import_files_processed
                    WHERE release = %s AND fase = %s
                    ORDER BY filename
                    """,
                    (release, chunk_fase)
                )
                processed_chunks = {row[0]: row[1] for row in cursor.fetchall()}

                cursor.execute("SELECT to_regclass('public.estabelecimentos') IS NOT NULL")
                table_exists = cursor.fetchone()[0]

                cursor.execute("SELECT to_regclass('public.staging_estabelecimentos') IS NOT NULL")
                staging_exists = cursor.fetchone()[0]

            if not processed_chunks and table_exists and staging_exists:
                logger.info(">> Nenhum checkpoint encontrado para a PARTE 2. Tentando recuperar progresso pre-existente...")
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        WITH final_counts AS (
                            SELECT (cnpj_basico::bigint / 1000000)::int AS chunk_num,
                                   COUNT(*) AS rows_imported
                            FROM estabelecimentos
                            GROUP BY 1
                        ),
                        staging_counts AS (
                            SELECT (cnpj_basico::bigint / 1000000)::int AS chunk_num,
                                   COUNT(*) AS rows_expected
                            FROM staging_estabelecimentos
                            GROUP BY 1
                        )
                        SELECT f.chunk_num, f.rows_imported, s.rows_expected
                        FROM final_counts f
                        JOIN staging_counts s ON s.chunk_num = f.chunk_num
                        ORDER BY f.chunk_num
                        """
                    )
                    recovered = cursor.fetchall()

                recovered_chunks = {}
                for chunk_num, rows_imported, rows_expected in recovered:
                    if rows_expected and rows_imported == rows_expected:
                        chunk_label = f"chunk_{int(chunk_num):03d}"
                        self._mark_file_processed(release, chunk_fase, chunk_label, rows_imported)
                        recovered_chunks[chunk_label] = rows_imported

                if recovered_chunks:
                    processed_chunks.update(recovered_chunks)
                    logger.info(f">> Recuperamos {len(recovered_chunks)} chunks consolidados antes da falha.")

            if not parte2_done:
                import time

                chunks_done = len(processed_chunks)
                if chunks_done == 100:
                    total_rows_inserted = sum(processed_chunks.values())
                    if total_rows_inserted == 0:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT COUNT(*) FROM estabelecimentos")
                            total_rows_inserted = cursor.fetchone()[0]
                    self._mark_file_processed(release, "fase3_parte2_create_table", "CONSOLIDATED", total_rows_inserted)
                    logger.info(">> PARTE 2 ja estava concluida - pulando")
                else:
                    logger.info(">> Verificando tabelas auxiliares...")
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT COUNT(*) FROM staging_empresas")
                        empresas_count = cursor.fetchone()[0]

                        cursor.execute("SELECT COUNT(*) FROM staging_simples")
                        simples_count = cursor.fetchone()[0]

                    logger.info(f"  staging_empresas: {empresas_count:,} linhas")
                    logger.info(f"  staging_simples: {simples_count:,} linhas")

                    if empresas_count == 0:
                        raise RuntimeError("FATAL: staging_empresas esta vazia! Execute a FASE 1 antes da FASE 3.")

                    if simples_count == 0:
                        raise RuntimeError("FATAL: staging_simples esta vazia! Execute a FASE 2 antes da FASE 3.")

                    if chunks_done == 0 or not table_exists:
                        with conn.cursor() as cursor:
                            logger.info(">> DROP TABLE estabelecimentos (se existir)")
                            cursor.execute("DROP TABLE IF EXISTS estabelecimentos CASCADE")

                            logger.info(">> Criando tabela estabelecimentos vazia...")
                            cursor.execute("""
                                CREATE TABLE estabelecimentos (
                                    cnpj14 VARCHAR(14) PRIMARY KEY,
                                    cnpj_basico VARCHAR(8),
                                    cnpj_ordem VARCHAR(4),
                                    cnpj_dv VARCHAR(2),
                                    matriz_filial VARCHAR(1),
                                    nome_fantasia TEXT,
                                    situacao_cadastral VARCHAR(2),
                                    data_situacao_cadastral VARCHAR(8),
                                    motivo_situacao_cadastral VARCHAR(2),
                                    nome_cidade_exterior TEXT,
                                    codigo_pais VARCHAR(3),
                                    pais TEXT,
                                    data_inicio_atividade VARCHAR(8),
                                    cnae_fiscal_principal VARCHAR(7),
                                    cnae_fiscal_secundaria TEXT,
                                    tipo_logradouro TEXT,
                                    logradouro TEXT,
                                    numero TEXT,
                                    complemento TEXT,
                                    bairro TEXT,
                                    cep VARCHAR(8),
                                    uf VARCHAR(2),
                                    municipio TEXT,
                                    ddd1 VARCHAR(4),
                                    telefone1 VARCHAR(9),
                                    ddd2 VARCHAR(4),
                                    telefone2 VARCHAR(9),
                                    ddd_fax VARCHAR(4),
                                    fax VARCHAR(9),
                                    email TEXT,
                                    situacao_especial TEXT,
                                    data_situacao_especial VARCHAR(8),
                                    razao_social TEXT,
                                    natureza_juridica VARCHAR(4),
                                    qualificacao_responsavel VARCHAR(2),
                                    capital_social DECIMAL(18,2),
                                    porte_empresa VARCHAR(2),
                                    ente_federativo TEXT,
                                    opcao_simples VARCHAR(1),
                                    data_opcao_simples VARCHAR(8),
                                    data_exclusao_simples VARCHAR(8),
                                    opcao_mei VARCHAR(1),
                                    data_opcao_mei VARCHAR(8),
                                    data_exclusao_mei VARCHAR(8)
                                )
                            """)
                            logger.info("  Tabela criada")

                        conn.commit()
                        processed_chunks = {}
                        chunks_done = 0

                    else:
                        logger.info(f">> Detectados {chunks_done} chunks ja consolidados. Retomando do proximo chunk.")

                    logger.info("")
                    logger.info(">> Processando 100 chunks (ranges de 1M CNPJs)...")
                    logger.info("")

                    total_rows_inserted = sum(processed_chunks.values())
                    initial_chunks_done = len(processed_chunks)
                    needs_cleanup = initial_chunks_done > 0
                    global_start = time.time()

                    for chunk_num in range(100):
                        chunk_label = f"chunk_{chunk_num:03d}"
                        if chunk_label in processed_chunks:
                            logger.info(f">> Chunk {chunk_num + 1:3d}/100 ja consolidado - pulando")
                            continue

                        chunk_start = time.time()
                        range_start = f"{chunk_num * 1000000:08d}"

                        if chunk_num == 99:
                            range_end = "99999999"
                            where_clause = "WHERE e.cnpj_basico >= %s AND e.cnpj_basico <= %s"
                            delete_clause = "cnpj_basico >= %s AND cnpj_basico <= %s"
                        else:
                            range_end = f"{(chunk_num + 1) * 1000000:08d}"
                            where_clause = "WHERE e.cnpj_basico >= %s AND e.cnpj_basico < %s"
                            delete_clause = "cnpj_basico >= %s AND cnpj_basico < %s"

                        with conn.cursor() as cursor:
                            if needs_cleanup:
                                cursor.execute(
                                    f"DELETE FROM estabelecimentos WHERE {delete_clause}",
                                    (range_start, range_end)
                                )
                                needs_cleanup = False

                            query = f"""
                                INSERT INTO estabelecimentos
                                SELECT
                                  e.cnpj14,
                                  e.cnpj_basico,
                                  e.cnpj_ordem,
                                  e.cnpj_dv,
                                  e.matriz_filial,
                                  e.nome_fantasia,
                                  e.situacao_cadastral,
                                  e.data_situacao_cadastral,
                                  e.motivo_situacao_cadastral,
                                  e.nome_cidade_exterior,
                                  e.codigo_pais,
                                  e.pais,
                                  e.data_inicio_atividade,
                                  e.cnae_fiscal_principal,
                                  e.cnae_fiscal_secundaria,
                                  e.tipo_logradouro,
                                  e.logradouro,
                                  e.numero,
                                  e.complemento,
                                  e.bairro,
                                  e.cep,
                                  e.uf,
                                  e.municipio,
                                  e.ddd1,
                                  e.telefone1,
                                  e.ddd2,
                                  e.telefone2,
                                  e.ddd_fax,
                                  e.fax,
                                  e.email,
                                  e.situacao_especial,
                                  e.data_situacao_especial,
                                  emp.razao_social,
                                  emp.natureza_juridica,
                                  emp.qualificacao_responsavel,
                                  emp.capital_social,
                                  emp.porte_empresa,
                                  emp.ente_federativo,
                                  s.opcao_simples,
                                  s.data_opcao_simples,
                                  s.data_exclusao_simples,
                                  s.opcao_mei,
                                  s.data_opcao_mei,
                                  s.data_exclusao_mei
                                FROM staging_estabelecimentos e
                                LEFT JOIN staging_empresas emp ON emp.cnpj_basico = e.cnpj_basico
                                LEFT JOIN staging_simples s ON s.cnpj_basico = e.cnpj_basico
                                {where_clause}
                            """
                            cursor.execute(query, (range_start, range_end))
                            rows_inserted = cursor.rowcount

                        conn.commit()

                        processed_chunks[chunk_label] = rows_inserted
                        chunks_done += 1
                        total_rows_inserted += rows_inserted
                        self._mark_file_processed(release, chunk_fase, chunk_label, rows_inserted)

                        chunk_elapsed = time.time() - chunk_start
                        processed_in_this_run = chunks_done - initial_chunks_done
                        global_elapsed = time.time() - global_start
                        avg_time_per_chunk = (global_elapsed / processed_in_this_run) if processed_in_this_run else 0
                        remaining_chunks = 100 - chunks_done
                        eta_minutes = ((avg_time_per_chunk * remaining_chunks) / 60) if avg_time_per_chunk and remaining_chunks else 0
                        progress_pct = (chunks_done / 100) * 100

                        logger.info(
                            f"-- Chunk {chunk_num + 1:3d}/100 | Range {range_start}-{range_end} | "
                            f"{rows_inserted:7,} linhas | {chunk_elapsed:5.1f}s | "
                            f"Total: {total_rows_inserted:10,} | Progresso: {progress_pct:5.1f}% | "
                            f"ETA: {eta_minutes:5.1f}min"
                        )

                    logger.info("")
                    logger.info(f"  Total consolidado: {total_rows_inserted:,} linhas")

                    self._mark_file_processed(release, "fase3_parte2_create_table", "CONSOLIDATED", total_rows_inserted)
                    logger.info(">> PARTE 2 CONCLUIDA - tabela estabelecimentos pronta")
            else:
                logger.info(">> PARTE 2 ja concluida - pulando")


            # ========== PARTE 3: CRIAR ÍNDICES ==========
            logger.info("")
            logger.info("=" * 80)
            logger.info("📋 FASE 3 - PARTE 3: CRIANDO ÍNDICES")
            logger.info("=" * 80)
            logger.info("")

            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM import_files_processed WHERE release = %s AND fase = 'fase3_parte3_indexes'",
                    (release,)
                )
                parte3_done = cursor.fetchone()[0] > 0

            if not parte3_done:
                self._create_estabelecimentos_indexes(conn)
                self._mark_file_processed(release, "fase3_parte3_indexes", "INDEXES_CREATED", 0)
                logger.info("✅ PARTE 3 CONCLUÍDA - Índices criados")
            else:
                logger.info("⏭️  PARTE 3 já concluída - pulando")


            # ========== PARTE 4: LIMPAR STAGING TABLES ==========
            logger.info("")
            logger.info("=" * 80)
            logger.info("📋 FASE 3 - PARTE 4: LIMPANDO STAGING TABLES")
            logger.info("=" * 80)
            logger.info("")

            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM import_files_processed WHERE release = %s AND fase = 'fase3_parte4_cleanup'",
                    (release,)
                )
                parte4_done = cursor.fetchone()[0] > 0

            if not parte4_done:
                with conn.cursor() as cursor:
                    logger.info("🗑️  DROP TABLE staging_estabelecimentos")
                    cursor.execute("DROP TABLE IF EXISTS staging_estabelecimentos CASCADE")

                    logger.info("🗑️  DROP TABLE staging_empresas")
                    cursor.execute("DROP TABLE IF EXISTS staging_empresas CASCADE")

                    logger.info("🗑️  DROP TABLE staging_simples")
                    cursor.execute("DROP TABLE IF EXISTS staging_simples CASCADE")

                conn.commit()
                self._mark_file_processed(release, "fase3_parte4_cleanup", "STAGING_DROPPED", 0)
                logger.info("✅ PARTE 4 CONCLUÍDA - Staging tables removidas")
            else:
                logger.info("⏭️  PARTE 4 já concluída - pulando")

            logger.info("")
            logger.info("=" * 80)
            logger.info("🎉 FASE 3 COMPLETA!")
            logger.info("=" * 80)

    def _stream_estabelecimentos_to_staging(self, file_path: Path, conn: psycopg.Connection) -> int:
        """Stream estabelecimentos to staging_estabelecimentos (without empresa/simples data)"""
        logger.info(f"📂 Importando: {file_path.name}")

        total_rows = 0

        with open(file_path, "r", encoding="latin1") as f:
            reader = csv.reader(f, delimiter=";", quotechar='"')
            pbar = tqdm(desc=f"  {file_path.name}", unit="rows", position=0, leave=True)

            buffer = io.StringIO()
            batch_count = 0

            for row in reader:
                if len(row) < 30:
                    continue

                cnpj_basico = row[0].strip().zfill(8)
                cnpj_ordem = row[1].strip().zfill(4)
                cnpj_dv = row[2].strip().zfill(2)
                cnpj14 = cnpj_basico + cnpj_ordem + cnpj_dv

                # Parse situacao_cadastral
                situacao_cadastral_raw = row[5].strip().split(".")[0] if row[5] else ""
                situacao_cadastral = situacao_cadastral_raw[:2] if situacao_cadastral_raw else ""

                # FILTRO: PULA EMPRESAS INATIVAS (situacao_cadastral == '08')
                if situacao_cadastral == '08':
                    continue

                motivo_situacao_raw = row[7].strip().split(".")[0] if row[7] else ""
                motivo_situacao = motivo_situacao_raw[:2] if motivo_situacao_raw else ""

                # Escape text fields
                nome_fantasia = self._escape_pg_copy(row[4][:255] if row[4] else "")
                nome_cidade_exterior = self._escape_pg_copy(row[8][:100] if row[8] else "")
                pais = self._escape_pg_copy(row[10][:100] if row[10] else "")
                cnae_secundaria = self._escape_pg_copy(row[12] if row[12] else "")
                tipo_logradouro = self._escape_pg_copy(row[13][:50] if row[13] else "")
                logradouro = self._escape_pg_copy(row[14][:255] if row[14] else "")
                numero = self._escape_pg_copy(row[15][:20] if row[15] else "")
                complemento = self._escape_pg_copy(row[16][:255] if row[16] else "")
                bairro = self._escape_pg_copy(row[17][:100] if row[17] else "")
                municipio = self._escape_pg_copy(row[20][:100] if row[20] else "")
                telefone1 = self._escape_pg_copy(row[22][:20] if row[22] else "")
                telefone2 = self._escape_pg_copy(row[24][:20] if row[24] else "")
                fax = self._escape_pg_copy(row[26][:20] if row[26] else "")
                correio_eletronico = self._escape_pg_copy(row[27][:255] if row[27] else "")
                situacao_especial = self._escape_pg_copy(row[28][:100] if row[28] else "")

                # Clean remaining fields
                matriz_filial = self._clean_null_bytes(row[3][:1] if row[3] else '')
                data_situacao = self._clean_null_bytes(row[6][:8] if row[6] else '')
                codigo_pais = self._clean_null_bytes(row[9][:3] if row[9] else '')
                data_inicio = self._clean_null_bytes(row[11][:8] if row[11] else '')
                cnae_principal = self._clean_null_bytes(row[12][:7] if row[12] else '')
                cep = self._clean_null_bytes(row[18][:8] if row[18] else '')
                uf = self._clean_null_bytes(row[19][:2] if row[19] else '')
                ddd1 = self._clean_null_bytes(row[21][:4] if row[21] else '')
                ddd2 = self._clean_null_bytes(row[23][:4] if row[23] else '')
                ddd_fax = self._clean_null_bytes(row[25][:4] if row[25] else '')
                data_sit_especial = self._clean_null_bytes(row[29][:8] if row[29] else '')

                # Write tab-separated line
                line = f"{cnpj14}\t{cnpj_basico}\t{cnpj_ordem}\t{cnpj_dv}\t{matriz_filial}\t{nome_fantasia}\t{situacao_cadastral}\t{data_situacao}\t{motivo_situacao}\t{nome_cidade_exterior}\t{codigo_pais}\t{pais}\t{data_inicio}\t{cnae_principal}\t{cnae_secundaria}\t{tipo_logradouro}\t{logradouro}\t{numero}\t{complemento}\t{bairro}\t{cep}\t{uf}\t{municipio}\t{ddd1}\t{telefone1}\t{ddd2}\t{telefone2}\t{ddd_fax}\t{fax}\t{correio_eletronico}\t{situacao_especial}\t{data_sit_especial}\n"
                buffer.write(line)

                batch_count += 1
                total_rows += 1
                pbar.update(1)

                if batch_count >= self.batch_size:
                    buffer.seek(0)
                    with conn.cursor() as cursor:
                        with cursor.copy("COPY staging_estabelecimentos FROM STDIN") as copy:
                            copy.write(buffer.read())
                    buffer = io.StringIO()
                    batch_count = 0

            # Final batch
            if batch_count > 0:
                buffer.seek(0)
                with conn.cursor() as cursor:
                    with cursor.copy("COPY staging_estabelecimentos FROM STDIN") as copy:
                        copy.write(buffer.read())

            pbar.close()

        return total_rows

    def _create_estabelecimentos_indexes(self, conn: psycopg.Connection) -> None:
        """Create all indexes for estabelecimentos table"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnpj_basico ON estabelecimentos (cnpj_basico)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos (uf)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_municipio ON estabelecimentos (municipio)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae ON estabelecimentos (cnae_fiscal_principal)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_nome_trgm ON estabelecimentos USING GIN (nome_fantasia gin_trgm_ops)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_razao_trgm ON estabelecimentos USING GIN (razao_social gin_trgm_ops)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_natureza ON estabelecimentos (natureza_juridica)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_porte ON estabelecimentos (porte_empresa)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_opcao_simples ON estabelecimentos (opcao_simples)",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_opcao_mei ON estabelecimentos (opcao_mei)",
        ]

        with conn.cursor() as cursor:
            for idx_sql in indexes:
                idx_name = idx_sql.split()[3]  # Extract index name (IF NOT EXISTS shifts position)
                logger.info(f"  🔨 Criando {idx_name}...")
                cursor.execute(idx_sql)
                logger.info(f"  ✓ {idx_name} criado")

        conn.commit()

    def _create_socios_indexes(self, conn: psycopg.Connection) -> None:
        """Create all indexes for socios table"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico)",
            "CREATE INDEX IF NOT EXISTS idx_socios_nome_trgm ON socios USING GIN (nome_socio gin_trgm_ops)",
            "CREATE INDEX IF NOT EXISTS idx_socios_cpf_trgm ON socios USING GIN (cnpj_cpf_socio gin_trgm_ops)",
        ]

        with conn.cursor() as cursor:
            for idx_sql in indexes:
                idx_name = idx_sql.split()[3]  # Extract index name (IF NOT EXISTS shifts position)
                logger.info(f"  🔨 Criando {idx_name}...")
                cursor.execute(idx_sql)
                logger.info(f"  ✓ {idx_name} criado")

        conn.commit()

    def _load_socios_phase(self, files: List[Path], release: str) -> None:
        """FASE 4: Load socios to final table"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📋 FASE 4: IMPORTANDO SÓCIOS (TABELA FINAL)")
        logger.info("=" * 80)
        logger.info("")

        # Create/truncate final table
        with psycopg.connect(settings.psycopg_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS socios (
                        id SERIAL PRIMARY KEY,
                        cnpj_basico VARCHAR(8),
                        identificador_socio VARCHAR(1),
                        nome_socio TEXT,
                        cnpj_cpf_socio TEXT,
                        codigo_qualificacao_socio VARCHAR(2),
                        percentual_capital_social VARCHAR(6),
                        data_entrada_sociedade VARCHAR(8),
                        codigo_pais VARCHAR(3),
                        cpf_representante_legal VARCHAR(11),
                        nome_representante_legal TEXT,
                        codigo_qualificacao_representante VARCHAR(2),
                        faixa_etaria VARCHAR(2)
                    )
                """)

                # TRUNCATE only if no files processed yet for this release (version change)
                cursor.execute(
                    "SELECT COUNT(*) FROM import_files_processed WHERE release = %s AND fase = 'fase4_socios'",
                    (release,)
                )
                processed_count = cursor.fetchone()[0]

                if processed_count == 0:
                    logger.info("🗑️  TRUNCATE socios (nova versão)")
                    cursor.execute("TRUNCATE TABLE socios")
                else:
                    logger.info(f"⏭️  Mantendo dados existentes ({processed_count} arquivos já processados)")

            conn.commit()

            logger.info("✓ Tabela socios criada")
            pending_files = [f for f in files if not self._is_file_processed(release, "fase4_socios", f.name)]
            if len(pending_files) < len(files):
                logger.info(f"⏭️  Pulando {len(files) - len(pending_files)} arquivos já processados")
            for file_path in pending_files:
                rows_imported = self._stream_socios_file_same_conn(file_path, conn)
                conn.commit()
                self._mark_file_processed(release, "fase4_socios", file_path.name, rows_imported)
                logger.info(f"  ✓ Arquivo {file_path.name} commitado ({rows_imported:,} linhas)")

            # Create indexes for socios
            logger.info("")
            logger.info("🔨 Criando índices para sócios...")
            self._create_socios_indexes(conn)
            logger.info("✅ Índices de sócios criados")

        logger.info("✅ FASE 4 CONCLUÍDA")

    def _stream_socios_file_same_conn(self, file_path: Path, conn: psycopg.Connection) -> int:
        """Stream socios file using COPY FROM STDIN on same connection. Returns number of rows imported."""
        logger.info(f"📂 Importando: {file_path.name}")

        total_rows = 0
        with open(file_path, "r", encoding="latin1") as f:
            reader = csv.reader(f, delimiter=";", quotechar='"')

            pbar = tqdm(desc=f"  {file_path.name}", unit="rows", position=0, leave=True)

            buffer = io.StringIO()
            batch_count = 0

            for row in reader:
                if not row or len(row) < 5:
                    continue

                def _get(idx: int) -> str:
                    return row[idx].strip() if len(row) > idx and row[idx] else ""

                cnpj_basico = self._clean_null_bytes(_get(0).zfill(8))

                # Parse numeric codes - remove decimal part if present
                qualificacao_socio_raw = _get(4).split(".")[0]
                qualificacao_socio = self._clean_null_bytes(qualificacao_socio_raw[:2] if qualificacao_socio_raw else "")

                has_percentual = len(row) >= 12
                percentual_capital = self._clean_null_bytes(_get(5).split(".")[0]) if has_percentual else ""

                data_idx = 6 if has_percentual else 5
                pais_idx = 7 if has_percentual else 6
                rep_idx = 8 if has_percentual else 7
                nome_rep_idx = 9 if has_percentual else 8
                qual_rep_idx = 10 if has_percentual else 9
                faixa_idx = 11 if has_percentual else 10

                data_entrada = self._clean_null_bytes(_get(data_idx)[:8])

                pais = self._clean_null_bytes(_get(pais_idx)[:3])

                cpf_rep_legal = self._clean_null_bytes(_get(rep_idx).zfill(11))

                qualificacao_rep_raw = _get(qual_rep_idx).split(".")[0]
                qualificacao_rep = self._clean_null_bytes(qualificacao_rep_raw[:2] if qualificacao_rep_raw else "")

                # Escape text fields
                nome_socio = self._escape_pg_copy(_get(2)[:255])
                nome_representante = self._escape_pg_copy(_get(nome_rep_idx)[:255])

                # Clean remaining fields
                identificador = self._clean_null_bytes(_get(1)[:1])
                cpf_cnpj = self._clean_null_bytes(_get(3).zfill(14))
                faixa_etaria = self._clean_null_bytes(_get(faixa_idx)[:2])

                # Write directly as tab-separated line (no csv.writer!)
                line = (
                    f"{cnpj_basico}\t{identificador}\t{nome_socio}\t{cpf_cnpj}\t"
                    f"{qualificacao_socio}\t{percentual_capital}\t{data_entrada}\t{pais}\t"
                    f"{cpf_rep_legal}\t{nome_representante}\t{qualificacao_rep}\t{faixa_etaria}\n"
                )
                buffer.write(line)

                batch_count += 1
                total_rows += 1
                pbar.update(1)

                if batch_count >= self.batch_size:
                    buffer.seek(0)
                    with conn.cursor() as cursor:
                        with cursor.copy(
                            "COPY socios (cnpj_basico, identificador_socio, nome_socio, cnpj_cpf_socio, "
                            "codigo_qualificacao_socio, percentual_capital_social, data_entrada_sociedade, "
                            "codigo_pais, cpf_representante_legal, nome_representante_legal, "
                            "codigo_qualificacao_representante, faixa_etaria) FROM STDIN"
                        ) as copy:
                            copy.write(buffer.read())
                    buffer = io.StringIO()
                    batch_count = 0

            # Final batch
            if batch_count > 0:
                buffer.seek(0)
                with conn.cursor() as cursor:
                    with cursor.copy(
                        "COPY socios (cnpj_basico, identificador_socio, nome_socio, cnpj_cpf_socio, "
                        "codigo_qualificacao_socio, percentual_capital_social, data_entrada_sociedade, "
                        "codigo_pais, cpf_representante_legal, nome_representante_legal, "
                        "codigo_qualificacao_representante, faixa_etaria) FROM STDIN"
                    ) as copy:
                        copy.write(buffer.read())

            pbar.close()

        return total_rows
