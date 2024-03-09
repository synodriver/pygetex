# -*- coding: utf-8 -*-
from typing import AsyncIterable, Mapping, Optional, Tuple

import httpx

from pygetex.config import Config
from pygetex.downloader import AsyncReader, HTTPDownloaderBase


class HTTPXBodyReader(AsyncReader):
    def __init__(
        self,
        response: httpx.Response,
        config: Config,
    ):
        self.response = response
        self.config = config
        self._iter = None

    async def __aiter__(self):
        async for chunk in self.response.aiter_raw(self.config.chunk_size):
            yield chunk

    async def close(self):
        await self.response.aclose()


class HTTPXDownloader(HTTPDownloaderBase):
    def __init__(self, config: Config):
        self.config = config
        self.client = httpx.AsyncClient(
            headers=getattr(config, "headers", None),
            follow_redirects=True,
            http2=getattr(config, "http2", True),
        )

    async def download(
        self,
        uri,
        method="GET",
        headers: Optional[Mapping] = None,
        payload: Optional[bytes] = None,
    ) -> Tuple[int, Mapping, HTTPXBodyReader]:
        request = self.client.build_request(
            method=method, url=uri, content=payload, headers=headers
        )

        response = await self.client.send(request, stream=True)
        status = response.status_code
        headers = response.headers
        body = HTTPXBodyReader(response, self.config)
        return status, headers, body

    async def close(self) -> None:
        await self.client.aclose()
