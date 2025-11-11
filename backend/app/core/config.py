from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralized application settings loaded from environment variables."""

    app_name: str = Field(default="Scrap CNPJ")
    api_prefix: str = Field(default="/api")
    pg_host: str = Field(default="postgres")
    pg_port: int = Field(default=5432)
    pg_user: str = Field(default="cnpj")
    pg_password: str = Field(default="cnpj")
    pg_database: str = Field(default="cnpj")
    download_base_url: str = Field(
        default="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    )
    data_dir: Path = Field(default=Path("/data"))
    raw_subdir: str = Field(default="raw")
    staging_subdir: str = Field(default="staging")
    batch_size: int = Field(default=50_000)
    commit_batch_size: int = Field(default=50_000)
    http_timeout: int = Field(default=120)
    max_parallel_downloads: int = Field(default=2)
    reuse_downloads: bool = Field(default=True)
    reuse_extractions: bool = Field(default=True)
    cleanup_raw_after_load: bool = Field(default=False)
    cleanup_staging_after_load: bool = Field(default=False)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="SCRAP_",
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

    @property
    def psycopg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / self.raw_subdir

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / self.staging_subdir


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
