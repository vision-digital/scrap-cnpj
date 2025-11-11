from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from app.core.config import get_settings

settings = get_settings()


@dataclass(slots=True)
class RemoteFile:
    name: str
    url: str
    size: str | None = None
    last_modified: str | None = None


class ReceitaFederalClient:
    """Scrapes the Receita Federal open-data directory listings."""

    release_pattern = re.compile(r"^(?P<release>\d{4}-\d{2})/\Z")

    def __init__(self, base_url: str | None = None, timeout: int | None = None) -> None:
        self.base_url = base_url or settings.download_base_url
        self.timeout = timeout or settings.http_timeout
        self._client = httpx.Client(timeout=self.timeout)

    def list_releases(self) -> List[str]:
        response = self._client.get(self.base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        releases: List[str] = []
        for link in soup.select("table a"):
            href = link.get("href", "")
            match = self.release_pattern.match(href)
            if match:
                releases.append(match.group("release"))
        releases.sort()
        return releases

    def latest_release(self) -> str:
        releases = self.list_releases()
        if not releases:
            raise RuntimeError("Nenhuma versao disponivel encontrada")
        return releases[-1]

    def list_files(self, release: str) -> List[RemoteFile]:
        release_url = urljoin(self.base_url, f"{release}/")
        response = self._client.get(release_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        files: List[RemoteFile] = []
        for row in soup.select("table tr"):
            link = row.find("a")
            if not link:
                continue
            href = link.get("href", "")
            if not href.lower().endswith(".zip"):
                continue
            cols = row.find_all("td")
            size = cols[3].get_text(strip=True) if len(cols) >= 4 else None
            last_modified = cols[2].get_text(strip=True) if len(cols) >= 3 else None
            files.append(
                RemoteFile(
                    name=href,
                    url=urljoin(release_url, href),
                    size=size,
                    last_modified=last_modified,
                )
            )
        if not files:
            raise RuntimeError(f"Nenhum arquivo encontrado para a versao {release}")
        return files

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ReceitaFederalClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
