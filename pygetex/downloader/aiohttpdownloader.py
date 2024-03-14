# -*- coding: utf-8 -*-
from typing import AsyncIterable, Mapping, Optional, Tuple

import aiohttp

from pygetex.config import Config
from pygetex.downloader import AsyncReader, HTTPDownloaderBase


class AIOHTTPBodyReader(AsyncReader):
    def __init__(
        self,
        context: aiohttp.client._RequestContextManager,
        resp: aiohttp.ClientResponse,
        config: Config,
    ):
        self.context = context
        self._resp = resp
        self.config = config

    def __aiter__(self):
        return self

    async def __anext__(self):
        if chunk := await self._resp.content.read(self.config.chunk_size):
            return chunk
        else:
            raise StopAsyncIteration

    async def close(self):
        await self.context.__aexit__(None, None, None)


class AIOHTTPDownloader(HTTPDownloaderBase):
    def __init__(self, config: Config):
        self.config = config
        self.session = aiohttp.ClientSession(headers=getattr(config, "headers", None))

    async def download(
        self,
        uri,
        method="GET",
        headers: Optional[Mapping] = None,
        payload: Optional[bytes] = None,
    ) -> Tuple[int, Mapping, AIOHTTPBodyReader]:
        context = self.session.request(method, uri, headers=headers, data=payload)
        resp = await context.__aenter__()
        status = resp.status
        headers = resp.headers
        body = AIOHTTPBodyReader(context, resp, self.config)

        return status, headers, body

    async def close(self) -> None:
        await self.session.close()
