from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralized application settings loaded from environment variables."""

    app_name: str = Field(default="Scrap CNPJ")
    api_prefix: str = Field(default="/api")
    mysql_user: str = Field(default="cnpj_user")
    mysql_password: str = Field(default="cnpj_pass")
    mysql_host: str = Field(default="mysql")
    mysql_port: int = Field(default=3306)
    mysql_db: str = Field(default="cnpj")
    download_base_url: str = Field(
        default="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    )
    data_dir: Path = Field(default=Path("/data"))
    raw_subdir: str = Field(default="raw")
    staging_subdir: str = Field(default="staging")
    batch_size: int = Field(default=5_000)
    http_timeout: int = Field(default=120)
    max_parallel_downloads: int = Field(default=2)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="SCRAP_",
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        return (
            "mysql+mysqlconnector://"
            f"{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"
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
