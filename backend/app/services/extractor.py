from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List
from zipfile import ZipFile

from app.core.config import get_settings
from app.utils.paths import ensure_dir

logger = logging.getLogger(__name__)
settings = get_settings()


class Extractor:
    """Unzips Receita archives into a staging directory."""

    def __init__(self, staging_dir: Path | None = None) -> None:
        self.staging_dir = ensure_dir(staging_dir or settings.staging_dir)

    def extract_release(self, release: str, archives: Iterable[Path]) -> List[Path]:
        release_dir = ensure_dir(self.staging_dir / release)
        extracted: List[Path] = []
        for archive in archives:
            if not archive.exists():
                logger.warning("Arquivo %s não encontrado para extração", archive)
                continue
            with ZipFile(archive) as zip_handle:
                for member in zip_handle.namelist():
                    target_path = Path(zip_handle.extract(member, path=release_dir))
                    extracted.append(target_path)
                    logger.info("Extraído %s", target_path.name)
        return extracted
