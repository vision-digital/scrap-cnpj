from __future__ import annotations

import psycopg

from app.core.config import get_settings

settings = get_settings()


def get_connection() -> psycopg.Connection:
    return psycopg.connect(settings.psycopg_dsn, autocommit=False)
