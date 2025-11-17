from __future__ import annotations

import logging
import shutil
from pathlib import Path

from app.core.config import get_settings
from app.services.downloader import DownloadManager
from app.services.extractor import Extractor
from app.services.loader_v3 import LoaderV3  # NEW: Streaming denormalized loader (memory efficient)
from app.services.receita_client import ReceitaFederalClient
from app.services.versioning import VersioningService

logger = logging.getLogger(__name__)
settings = get_settings()


class Pipeline:
    def __init__(self) -> None:
        self.versioning = VersioningService()
        self.downloader = DownloadManager()
        self.extractor = Extractor()
        self.loader = LoaderV3()  # NEW: Streaming denormalized loader (memory efficient)

    def run(self, release: str | None = None) -> str:
        logger.info("Starting pipeline for release %s", release or "latest")

        # Discover target release
        if release:
            target_release = release
        else:
            # Try to find existing data in staging first
            target_release = self._find_existing_release()
            if not target_release:
                # Fall back to fetching from Receita Federal
                try:
                    with ReceitaFederalClient() as client:
                        target_release = client.latest_release()
                except Exception as e:
                    logger.error("Failed to fetch latest release: %s", e)
                    raise RuntimeError("No release specified and cannot fetch from Receita Federal. Check network connection.") from e

        logger.info("Target release: %s", target_release)
        current = self.versioning.current_release()
        if current and current.release == target_release and current.status == "completed":
            logger.info("Database already at version %s", target_release)
            return target_release
        self.versioning.start_release(target_release)
        try:
            archives = self.downloader.download_release(
                target_release, reuse_existing=settings.reuse_downloads
            )
            logger.info("%s archives ready for extraction", len(archives))
            extracted = self.extractor.extract_release(
                target_release, archives, reuse_existing=settings.reuse_extractions
            )
            logger.info("%s files ready for loading", len(extracted))
            self.loader.load_files(extracted)
            self._cleanup(target_release)
            self.versioning.finish_release(target_release, success=True)
        except Exception as exc:  # pragma: no cover
            logger.exception("Pipeline failed for version %s", target_release)
            self.versioning.finish_release(target_release, success=False, note=str(exc))
            raise
        return target_release

    def _cleanup(self, release: str) -> None:
        targets: list[tuple[Path, bool]] = [
            (self.downloader.data_dir / release, settings.cleanup_raw_after_load),
            (self.extractor.staging_dir / release, settings.cleanup_staging_after_load),
        ]
        for path, should_remove in targets:
            if should_remove:
                self._safe_rmtree(path)

    def _find_existing_release(self) -> str | None:
        """Find most recent release from existing staging data."""
        staging_dir = self.extractor.staging_dir
        if not staging_dir.exists():
            return None

        # List all release directories (format: YYYY-MM)
        releases = [
            d.name for d in staging_dir.iterdir()
            if d.is_dir() and len(d.name) == 7 and d.name[4] == '-'
        ]

        if not releases:
            return None

        # Return most recent
        releases.sort(reverse=True)
        logger.info("Found existing releases in staging: %s", releases)
        return releases[0]

    @staticmethod
    def _safe_rmtree(path: Path) -> None:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
