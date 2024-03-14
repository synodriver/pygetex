# -*- coding: utf-8 -*-
import os
from typing import Optional, Tuple
from urllib.parse import urlparse

import asyncssh

from pygetex.config import Config
from pygetex.downloader import AsyncReader, FTPDownloaderBase


class SFTPBodyReader(AsyncReader):
    def __init__(
        self,
        ctx,
        sftpctx,
        file: asyncssh.SFTPClientFile,
        offset: int,
        count: int,
        config: Config,
    ):
        self.ctx = ctx
        self.sftpctx = sftpctx
        self.file = file
        self.offset = offset
        self.count = count  # remain
        self.config = config

    async def readexactly(self, count: int) -> bytes:
        ret = bytearray()
        while count:
            chunk: bytes = await self.file.read(count, self.offset)
            ret.extend(chunk)
            self.offset += len(chunk)
            count -= len(chunk)
        return bytes(ret)

    async def __aiter__(self):
        while True:
            if self.count <= self.config.chunk_size:
                data = await self.readexactly(self.count)
                # print(f"Read {len(data)} bytes from sftp")
                self.count -= len(data)
                yield data
                break
            data = await self.readexactly(self.config.chunk_size)
            self.count -= len(data)
            yield data

    async def close(self) -> None:
        await self.file.close()
        await self.ctx.__aexit__(None, None, None)
        await self.sftpctx.__aexit__(None, None, None)


class SFTPDownloader(FTPDownloaderBase):
    def __init__(self, config: Config):
        self.config = config

    async def guess_file_metadata(self, uri: str) -> Tuple[Optional[int], str, bool]:
        parsed = urlparse(uri)
        host = parsed.netloc
        support_range = True
        if ":" in host:
            host, portstr = host.split(":")
            port = int(portstr)
        else:
            port = asyncssh.DEFAULT_PORT
        async with asyncssh.connect(
            host,
            port,
            options=asyncssh.SSHClientConnectionOptions(
                username=getattr(self.config, "username", None),
                password=getattr(self.config, "password", None),
            ),
        ) as conn:
            async with conn.start_sftp_client() as sftp:
                stat = await sftp.stat(parsed.path)
                return stat.size, os.path.split(parsed.path)[-1], support_range
                # async with sftp.open(parsed.path) as f:
                #     await f.read()

    async def download(self, uri: str, offset: int, count: int) -> SFTPBodyReader:
        parsed = urlparse(uri)
        host = parsed.netloc
        if ":" in host:
            host, portstr = host.split(":")
            port = int(portstr)
        else:
            port = asyncssh.DEFAULT_PORT
        ctx = asyncssh.connect(
            host,
            port,
            options=asyncssh.SSHClientConnectionOptions(
                username=getattr(self.config, "username", None),
                password=getattr(self.config, "password", None),
            ),
        )
        conn = await ctx.__aenter__()
        sftpctx = conn.start_sftp_client()
        sftp = await sftpctx.__aenter__()
        f = await sftp.open(
            parsed.path, "rb", block_size=self.config.chunk_size
        )  # type: asyncssh.SFTPClientFile
        return SFTPBodyReader(ctx, sftpctx, f, offset, count, self.config)

    async def close(self) -> None:
        pass
