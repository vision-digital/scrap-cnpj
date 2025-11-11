from __future__ import annotations

from textwrap import dedent

from sqlalchemy import text

from app.db.session import engine
from app.models import Base


def ensure_tables() -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
    Base.metadata.create_all(bind=engine)
    _ensure_indexes()


def _ensure_indexes() -> None:
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_empresas_razao_trgm ON empresas USING GIN (razao_social gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_empresas_natureza ON empresas (natureza_juridica)",
        "CREATE INDEX IF NOT EXISTS idx_empresas_porte ON empresas (porte_empresa)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos (uf)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_municipio ON estabelecimentos (municipio)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae ON estabelecimentos (cnae_fiscal_principal)",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimentos_nome_trgm ON estabelecimentos USING GIN (nome_fantasia gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_socios_nome_trgm ON socios USING GIN (nome_socio gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios (cnpj_basico)",
        "CREATE INDEX IF NOT EXISTS idx_simples_opcao ON simples (opcao_simples)",
        "CREATE INDEX IF NOT EXISTS idx_simples_mei ON simples (opcao_mei)",
    ]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
