from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import export, health, search, update, version
from app.core.config import get_settings
from app.db.session import engine
from app.models import Base

settings = get_settings()

app = FastAPI(title=settings.app_name)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"]
    ,
    allow_headers=["*"],
)

app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(version.router, prefix=settings.api_prefix)
app.include_router(update.router, prefix=settings.api_prefix)
app.include_router(search.router, prefix=settings.api_prefix)
app.include_router(export.router, prefix=settings.api_prefix)


@app.get("/", tags=["infra"])
def root() -> dict:
    return {"message": "Scrap CNPJ API", "version": settings.app_name}


@app.on_event("startup")
def init_db() -> None:
    Base.metadata.create_all(bind=engine)
