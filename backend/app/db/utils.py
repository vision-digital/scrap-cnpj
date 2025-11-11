from __future__ import annotations

import logging
import time

import psycopg

from app.core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


def wait_for_postgres(max_attempts: int = 20, delay: float = 3.0) -> None:
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            with psycopg.connect(settings.psycopg_dsn) as conn:
                conn.execute("SELECT 1")
            logger.info("Postgres connected on attempt %s", attempt)
            return
        except psycopg.Error as exc:
            logger.warning(
                "Waiting for Postgres (attempt %s/%s): %s",
                attempt,
                max_attempts,
                exc,
            )
            time.sleep(delay)
    raise RuntimeError("Postgres did not become available in time")
