# -*- coding: utf-8 -*-
import os
from typing import Optional, Tuple
from urllib.parse import urlparse

from aioftp import DataConnectionThrottleStreamIO  # type: ignore
from aioftp import Client, StatusCodeError
from aioftp.common import DEFAULT_PASSWORD, DEFAULT_PORT, DEFAULT_USER  # type: ignore

from pygetex.config import Config
from pygetex.downloader import AsyncReader, FTPDownloaderBase


class AIOFTPBodyReader(AsyncReader):
    def __init__(
        self,
        stream: DataConnectionThrottleStreamIO,
        client: Client,
        count: int,
        ctx,
        config: Config,
    ):
        self.stream = stream
        self.client = client
        self.count = count
        self.ctx = ctx
        self.config = config

    async def __aiter__(self):
        while self.count > 0:
            chunk = await self.stream.read(self.config.chunk_size)
            self.count -= len(chunk)
            if self.count >= 0:
                yield chunk
            else:
                yield chunk[: self.count]

    async def close(self) -> None:
        await self.client.abort()
        self.stream.close()
        await self.ctx.__aexit__(None, None, None)


class AIOFTPDownloader(FTPDownloaderBase):
    def __init__(self, config: Config):
        self.config = config
        self.username = getattr(config, "username", DEFAULT_USER)
        self.password = getattr(config, "password", DEFAULT_PASSWORD)

    async def guess_file_metadata(self, uri: str) -> Tuple[Optional[int], str, bool]:
        parsed = urlparse(uri)
        host = parsed.netloc
        support_range = True
        if ":" in host:
            host, portstr = host.split(":")
            port = int(portstr)
        else:
            port = DEFAULT_PORT
        async with Client.context(
            host,
            port,
            self.username,
            self.password,
            ssl=getattr(self.config, "ssl", None),
            encoding=getattr(self.config, "encoding", "utf-8"),
        ) as client:  # type: Client
            code, info = await client.command(f"SIZE {parsed.path}", "213")
            try:
                await client.command(
                    f"REST 0", "350"
                )  # todo 看看gopeed怎么知道ftp服务器是否支持断点续传的
            except StatusCodeError:
                support_range = False
            return int(info[-1].strip()), os.path.split(parsed.path)[-1], support_range

    async def download(self, uri: str, offset: int, count: int) -> AIOFTPBodyReader:
        parsed = urlparse(uri)
        host = parsed.netloc
        if ":" in host:
            host, portstr = host.split(":")
            port = int(portstr)
        else:
            port = DEFAULT_PORT
        ctx = Client.context(
            host,
            port,
            self.username,
            self.password,
            ssl=getattr(self.config, "ssl", None),
            encoding=getattr(self.config, "encoding", "utf-8"),
        )
        client = await ctx.__aenter__()  # type: Client
        stream = await client.download_stream(parsed.path, offset=offset)
        return AIOFTPBodyReader(stream, client, count, ctx, self.config)

    async def close(self) -> None:
        pass
