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

    def extract_release(
        self, release: str, archives: Iterable[Path], reuse_existing: bool = True
    ) -> List[Path]:
        release_dir = ensure_dir(self.staging_dir / release)
        existing_files = sorted(p for p in release_dir.rglob("*") if p.is_file())
        if reuse_existing and existing_files:
            logger.info("Reusing previously extracted files in %s", release_dir)
            return existing_files
        extracted: List[Path] = []
        for archive in archives:
            if not archive.exists():
                logger.warning("Archive %s not found for extraction", archive)
                continue
            with ZipFile(archive) as zip_handle:
                for member in zip_handle.namelist():
                    extracted_path = Path(zip_handle.extract(member, path=release_dir))
                    if extracted_path.is_file():
                        extracted.append(extracted_path)
                        logger.info("Extracted %s", extracted_path.name)
        return extracted
