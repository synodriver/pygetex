# -*- coding: utf-8 -*-
"""
Copyright (c) 2008-2024 synodriver <diguohuangjiajinweijun@gmail.com>
"""
import asyncio
from unittest import IsolatedAsyncioTestCase

from loguru import logger

from pygetex.config import Config
from pygetex.core import CoreProcess
from pygetex.plugin import PluginBase


class LogPlugin(PluginBase):
    enabled: bool = True
    name: str = "log"
    description: str = "luguru"

    def __init__(self, core: "CoreProcess"):
        self.process = core

    async def on_startup(self):
        logger.info("Log plugin on_startup")

    async def on_shutdown(self):
        logger.info("Log plugin on_shutdown")

    async def on_add_uri(self, uri: str, **options):
        logger.info(f"Log plugin on_add_uri {uri} {options}")
        return []

    async def on_download_start(self, taskid: int):
        logger.info(f"Log plugin on_download_start {taskid}")

    async def on_download_error(self, id, e, tb):
        logger.catch(e)


headers = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6
Cache-Control: no-cache
Pragma: no-cache
Referer: https://sakustar.moe/
Sec-Ch-Ua: "Not A(Brand";v="99", "Microsoft Edge";v="121", "Chromium";v="121"
Sec-Ch-Ua-Mobile: ?0
Sec-Ch-Ua-Platform: "Windows"
Sec-Fetch-Dest: document
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: cross-site
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0
"""
headers_dict = {}
for i in headers.splitlines():
    k, v = i.split(": ", maxsplit=1)
    headers_dict[k] = v
print(headers_dict)


class TestDownloader(IsolatedAsyncioTestCase):
    async def test_download(self):
        async with CoreProcess(
            Config(
                database="sqlite+aiosqlite:///E:\pyproject\pyget\pygetex\pyget.db",
                dir=r"E:\pyproject\pyget\unitest\download",
            )
        ) as process:
            await process.add_uri(
                "https://images.sampletemplates.com/wp-content/uploads/2016/04/14141613/Sample-Downloadable-Release-Notes-Template-.jpeg",
                headers=headers_dict,
            )
            await asyncio.sleep(1000)
