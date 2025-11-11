from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import httpx
from tqdm import tqdm

from app.core.config import get_settings
from app.services.receita_client import ReceitaFederalClient, RemoteFile
from app.utils.paths import ensure_dir

logger = logging.getLogger(__name__)
settings = get_settings()


class DownloadManager:
    """Handles download of release archives to the local filesystem."""

    def __init__(
        self,
        data_dir: Path | None = None,
        client: ReceitaFederalClient | None = None,
    ) -> None:
        self.data_dir = data_dir or settings.raw_dir
        self.client = client or ReceitaFederalClient()
        ensure_dir(self.data_dir)
        self._http = httpx.Client(timeout=settings.http_timeout)

    def download_release(self, release: str, reuse_existing: bool = True) -> List[Path]:
        target_dir = ensure_dir(self.data_dir / release)
        if reuse_existing and any(target_dir.glob("*.zip")):
            logger.info("Reusing previously downloaded archives in %s", target_dir)
            return sorted(target_dir.glob("*.zip"))
        files = self.client.list_files(release)
        downloaded: List[Path] = []
        for remote in files:
            local_path = target_dir / remote.name
            if local_path.exists() and local_path.stat().st_size > 0:
                logger.info("File %s already exists, skipping download", local_path.name)
                downloaded.append(local_path)
                continue
            self._download_file(remote, local_path)
            downloaded.append(local_path)
        return downloaded

    def _download_file(self, remote: RemoteFile, target_path: Path) -> None:
        logger.info("Baixando %s", remote.url)
        with self._http.stream("GET", remote.url, follow_redirects=True) as response:
            response.raise_for_status()
            total = int(response.headers.get("Content-Length", 0))
            with open(target_path, "wb") as file_handle:
                progress = (
                    tqdm(total=total, unit="B", unit_scale=True, desc=remote.name)
                    if total
                    else None
                )
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    file_handle.write(chunk)
                    if progress:
                        progress.update(len(chunk))
                if progress:
                    progress.close()

    def close(self) -> None:
        self._http.close()
        self.client.close()

    def __enter__(self) -> "DownloadManager":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
