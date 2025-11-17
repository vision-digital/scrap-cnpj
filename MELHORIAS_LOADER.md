# Melhorias do Loader V3

## 1. Verificar e reimportar tabelas auxiliares se vazias

**Localização**: FASE 3 PARTE 2, antes dos 100 chunks
**Mudança**: Adicionar verificação se `staging_empresas` e `staging_simples` estão vazias. Se sim, reimportar.

```python
# Antes dos 100 chunks, adicionar:
with conn.cursor() as cursor:
    # Check if staging tables are empty
    cursor.execute("SELECT COUNT(*) FROM staging_empresas")
    empresas_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM staging_simples")
    simples_count = cursor.fetchone()[0]

if empresas_count == 0:
    logger.warning("⚠️  staging_empresas vazia! Reimportando...")
    # Clear checkpoint to force reimport
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM import_files_processed WHERE release = %s AND fase = 'fase1_empresas'", (release,))
    conn.commit()
    # Reimport
    self._load_empresas_phase(empresas_files, release)

if simples_count == 0:
    logger.warning("⚠️  staging_simples vazia! Reimportando...")
    # Clear checkpoint to force reimport
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM import_files_processed WHERE release = %s AND fase = 'fase2_simples'", (release,))
    conn.commit()
    # Reimport
    self._load_simples_phase(simples_files, release)
```

---

## 2. Sócios: importar diretamente para tabela final

**Localização**: `_load_socios_phase`
**Mudança**: Mudar de `staging_socios` para `socios`

```python
# Linha ~910: Mudar
CREATE TABLE IF NOT EXISTS staging_socios (
# Para:
CREATE TABLE IF NOT EXISTS socios (

# Linha ~1001: Mudar
COPY staging_socios (...)
# Para:
COPY socios (...)
```

---

## 3. Criar índices para sócios

**Localização**: Final da `_load_socios_phase`
**Mudança**: Adicionar criação de índices

```python
def _create_socios_indexes(self, conn: psycopg.Connection) -> None:
    """Create all indexes for socios table"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico)",
        "CREATE INDEX IF NOT EXISTS idx_socios_nome_trgm ON socios USING GIN (nome_socio gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_socios_cpf_trgm ON socios USING GIN (cpf_cnpj_socio gin_trgm_ops)",
    ]

    with conn.cursor() as cursor:
        for idx_sql in indexes:
            idx_name = idx_sql.split()[3]
            logger.info(f"  🔨 Criando {idx_name}...")
            cursor.execute(idx_sql)
            logger.info(f"  ✓ {idx_name} criado")

    conn.commit()
```

Chamar no final de `_load_socios_phase`:
```python
# Após processar todos os arquivos
logger.info("🔨 Criando índices para sócios...")
self._create_socios_indexes(conn)
logger.info("✅ Índices de sócios criados")
```

---

## 4. Mensagem de sucesso e cleanup de tabelas de controle

**Localização**: Final de `load_files`
**Mudança**: Adicionar após FASE 4

```python
# No final de load_files(), após "✅ IMPORT COMPLETO!"
logger.info("")
logger.info("=" * 80)
logger.info("🧹 LIMPEZA FINAL")
logger.info("=" * 80)
logger.info("")

with engine.begin() as conn:
    # Drop staging tables
    logger.info("🗑️  Removendo tabelas staging...")
    conn.execute(text("DROP TABLE IF EXISTS staging_empresas CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS staging_simples CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS staging_estabelecimentos CASCADE"))
    logger.info("  ✓ Tabelas staging removidas")

    # Drop control tables
    logger.info("🗑️  Removendo tabelas de controle...")
    conn.execute(text("DROP TABLE IF EXISTS import_checkpoints CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS import_files_processed CASCADE"))
    logger.info("  ✓ Tabelas de controle removidas")

logger.info("")
logger.info("=" * 80)
logger.info("🎉 IMPORTAÇÃO CONCLUÍDA COM SUCESSO!")
logger.info("=" * 80)
logger.info(f"📊 Release: {release}")
logger.info("✅ Todas as fases completadas")
logger.info("✅ Índices criados")
logger.info("✅ Limpeza realizada")
logger.info("=" * 80)
```

---

## 5. Remover arquivos raw e staging

**Localização**: Final de `load_files`, após cleanup de tabelas
**Mudança**: Adicionar remoção de arquivos

```python
import shutil

# Após cleanup de tabelas
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
```

---

## Resumo das Mudanças

| # | Melhoria | Arquivos | Linhas Aprox |
|---|----------|----------|--------------|
| 1 | Verificar/reimportar auxiliares | loader_v3.py | ~621 (antes 100 chunks) |
| 2 | Sócios → tabela final | loader_v3.py | ~910, ~1001 |
| 3 | Índices sócios | loader_v3.py | ~1015 (novo método) |
| 4 | Mensagem sucesso + cleanup tabelas | loader_v3.py | ~207 (final load_files) |
| 5 | Remover arquivos | loader_v3.py | ~207 (após cleanup) |

## Impactos

- ✅ Mais robusto: reimporta auxiliares se necessário
- ✅ Mais simples: sócios direto na tabela final
- ✅ Mais completo: índices de sócios
- ✅ Mais limpo: remove tudo no final
- ⚠️ Arquivos removidos: não pode reimportar sem baixar novamente
