# -*- coding: utf-8 -*-
from typing import AsyncIterable, Mapping, Optional, Tuple

from cycurl.requests import AsyncSession, Response  # type: ignore

from pygetex.config import Config
from pygetex.downloader import AsyncReader, HTTPDownloaderBase


class CURLBodyReader(AsyncReader):
    def __init__(
        self,
        resp: Response,
        config: Config,
    ):
        self._resp = resp
        self.config = config

    async def __aiter__(self):
        async for chunk in self._resp.aiter_content(self.config.chunk_size):
            yield chunk

    async def close(self):
        await self._resp.aclose()


class CURLDownloader(HTTPDownloaderBase):
    def __init__(self, config: Config):
        self.config = config
        self.session = AsyncSession(
            headers=getattr(config, "headers", None),
            allow_redirects=True,
            impersonate=getattr(config, "impersonate", None),
        )

    async def download(
        self,
        uri,
        method="GET",
        headers: Optional[Mapping] = None,
        payload: Optional[bytes] = None,
    ) -> Tuple[int, Mapping, CURLBodyReader]:
        response: Response = await self.session.request(
            method, uri, headers=headers, data=payload, stream=True
        )

        return (
            response.status_code,
            response.headers,
            CURLBodyReader(response, self.config),
        )

    async def close(self) -> None:
        await self.session.close()
