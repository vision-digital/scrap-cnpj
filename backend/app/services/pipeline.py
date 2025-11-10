from __future__ import annotations

import logging
import shutil
from pathlib import Path

from app.core.config import get_settings
from app.models import IngestionStatus
from app.services.downloader import DownloadManager
from app.services.extractor import Extractor
from app.services.loader import Loader
from app.services.receita_client import ReceitaFederalClient
from app.services.versioning import VersioningService

logger = logging.getLogger(__name__)
settings = get_settings()


class Pipeline:
    def __init__(self) -> None:
        self.versioning = VersioningService()
        self.downloader = DownloadManager()
        self.extractor = Extractor()
        self.loader = Loader()

    def run(self, release: str | None = None) -> str:
        logger.info("Iniciando pipeline para release %s", release or "mais recente")
        with ReceitaFederalClient() as client:
            target_release = release or client.latest_release()
        current = self.versioning.current_release()
        if (
            current
            and current.release == target_release
            and current.status == IngestionStatus.completed
        ):
            logger.info("Base já está na versão %s", target_release)
            return target_release
        self.versioning.start_release(target_release)
        try:
            archives = self.downloader.download_release(target_release)
            extracted = self.extractor.extract_release(target_release, archives)
            self.loader.load_files(extracted)
            self._cleanup(target_release)
            self.versioning.finish_release(target_release, success=True)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Falha na atualização da versão %s", target_release)
            self.versioning.finish_release(target_release, success=False, note=str(exc))
            raise
        return target_release

    def _cleanup(self, release: str) -> None:
        for directory in (
            self.downloader.data_dir / release,
            self.extractor.staging_dir / release,
        ):
            self._safe_rmtree(directory)

    @staticmethod
    def _safe_rmtree(path: Path) -> None:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
