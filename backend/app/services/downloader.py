from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    def download_release(self, release: str, reuse_existing: bool = True) -> List[Path]:
        target_dir = ensure_dir(self.data_dir / release)
        if reuse_existing and any(target_dir.glob("*.zip")):
            logger.info("Reusing previously downloaded archives in %s", target_dir)
            return sorted(target_dir.glob("*.zip"))
        files = self.client.list_files(release)

        # Separate already downloaded from pending
        downloaded: List[Path] = []
        pending: List[tuple[RemoteFile, Path]] = []

        for remote in files:
            local_path = target_dir / remote.name
            if local_path.exists() and local_path.stat().st_size > 0:
                logger.info("File %s already exists, skipping download", local_path.name)
                downloaded.append(local_path)
            else:
                pending.append((remote, local_path))

        # Download pending files in parallel with staggered start
        if pending:
            max_workers = min(settings.max_parallel_downloads, len(pending))
            logger.info("Downloading %d files with %d parallel workers (delay: %ds between starts)",
                       len(pending), max_workers, settings.download_start_delay)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_path = {}

                # Submit tasks with delay between each one to avoid overwhelming the server
                for i, (remote, path) in enumerate(pending):
                    if i > 0:
                        logger.info("Aguardando %ds antes de iniciar próximo download...", settings.download_start_delay)
                        time.sleep(settings.download_start_delay)

                    future = executor.submit(self._download_file, remote, path)
                    future_to_path[future] = path
                    logger.info("Download #%d iniciado: %s", i + 1, remote.name)

                # Wait for all downloads to complete
                for future in as_completed(future_to_path):
                    path = future_to_path[future]
                    try:
                        future.result()
                        downloaded.append(path)
                    except Exception as exc:
                        logger.error("Failed to download %s: %s", path.name, exc)
                        raise

        return sorted(downloaded)

    def _download_file(self, remote: RemoteFile, target_path: Path) -> None:
        logger.info("Baixando %s", remote.url)

        max_retries = 3
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                # Create a separate HTTP client for each thread to enable true parallelism
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                }

                with httpx.Client(timeout=settings.http_timeout, headers=headers, follow_redirects=True) as http_client:
                    with http_client.stream("GET", remote.url) as response:
                        response.raise_for_status()
                        total = int(response.headers.get("Content-Length", 0))

                        with open(target_path, "wb") as file_handle:
                            progress = (
                                tqdm(total=total, unit="B", unit_scale=True, desc=remote.name, position=None, leave=True)
                                if total
                                else None
                            )

                            downloaded = 0
                            for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):  # 8MB chunks
                                file_handle.write(chunk)
                                downloaded += len(chunk)
                                if progress:
                                    progress.update(len(chunk))

                            if progress:
                                progress.close()

                            # Success!
                            logger.info("✓ Download concluído: %s (%d bytes)", remote.name, downloaded)
                            return

            except Exception as exc:
                if attempt < max_retries - 1:
                    logger.warning("Tentativa %d/%d falhou para %s: %s. Aguardando %ds...",
                                 attempt + 1, max_retries, remote.name, exc, retry_delay)
                    time.sleep(retry_delay)
                else:
                    logger.error("Falha após %d tentativas para %s: %s", max_retries, remote.name, exc)
                    raise

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "DownloadManager":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
