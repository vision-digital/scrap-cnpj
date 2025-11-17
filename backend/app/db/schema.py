from __future__ import annotations

from sqlalchemy import text

from app.db.session import engine
from app.models import Base


def ensure_tables() -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
    Base.metadata.create_all(bind=engine)
    # DISABLED: Don't create indexes on startup (too slow with 36M rows)
    # Indexes will be created by FASE 3 - PARTE 3 after consolidation
    # _ensure_indexes()


def _ensure_indexes() -> None:
    """
    Create indexes for the denormalized schema.
    NOTE: empresas and simples tables removed - data now in estabelecimentos.
    """
    statements = [
        # === ESTABELECIMENTOS INDEXES ===
        # Original estabelecimentos indexes
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos (uf)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_municipio ON estabelecimentos (municipio)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae ON estabelecimentos (cnae_fiscal_principal)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_nome_trgm ON estabelecimentos USING GIN (nome_fantasia gin_trgm_ops)",

        # From empresas (now in estabelecimentos)
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_razao_trgm ON estabelecimentos USING GIN (razao_social gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_natureza ON estabelecimentos (natureza_juridica)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_porte ON estabelecimentos (porte_empresa)",

        # From simples (now in estabelecimentos)
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_opcao_simples ON estabelecimentos (opcao_simples)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_opcao_mei ON estabelecimentos (opcao_mei)",

        # === SOCIOS INDEXES ===
        "CREATE INDEX IF NOT EXISTS idx_socios_nome_trgm ON socios USING GIN (nome_socio gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_socios_cpf_trgm ON socios USING GIN (cnpj_cpf_socio gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios (cnpj_basico)",
    ]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
