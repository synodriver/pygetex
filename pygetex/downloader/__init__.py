# -*- coding: utf-8 -*-
"""
Copyright (c) 2008-2024 synodriver <diguohuangjiajinweijun@gmail.com>
"""
from typing import AsyncIterable, Mapping, Optional, Tuple

from pygetex.config import Config


class AsyncReader:
    def __aiter__(self):
        ...

    async def close(self) -> None:
        ...


class DownloaderBase:
    async def download(self, *args, **kwargs):
        ...


class HTTPDownloaderBase(DownloaderBase):
    async def download(self, uri, method="GET", headers: Optional[Mapping] = None, payload: Optional[bytes] = None) -> Tuple[int, Mapping, AsyncReader]:  # type: ignore
        """

        :param uri:
        :param method:
        :param headers:
        :param payload:
        :return: status headers bodyiter
        """
        ...

    async def close(self):
        ...
