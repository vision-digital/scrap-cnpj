from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import auxiliares, export, health, search, stats, update, version
# REMOVED: cleanup (no longer needed - filtering during import instead)
from app.core.config import get_settings
from app.db.schema import ensure_tables
from app.db.utils import wait_for_postgres

settings = get_settings()

app = FastAPI(title=settings.app_name)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(version.router, prefix=settings.api_prefix)
app.include_router(update.router, prefix=settings.api_prefix)
app.include_router(auxiliares.router, prefix=settings.api_prefix)
# REMOVED: cleanup router (no longer needed - filtering during import)
app.include_router(search.router, prefix=settings.api_prefix)
app.include_router(stats.router, prefix=settings.api_prefix)
app.include_router(export.router, prefix=settings.api_prefix)


@app.get("/", tags=["infra"])
def root() -> dict:
    return {"message": "Scrap CNPJ API", "version": settings.app_name}


@app.on_event("startup")
def init_db() -> None:
    wait_for_postgres()
    ensure_tables()
