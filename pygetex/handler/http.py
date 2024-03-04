# -*- coding: utf-8 -*-
import asyncio
import os
import pickle
import re
import traceback
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, cast

from pygetex.config import Config, update_config
from pygetex.downloader.aiohttpdownloader import AIOHTTPDownloader, HTTPDownloaderBase
from pygetex.fileio import pwrite, pwrite_async
from pygetex.fileio.utils import open_fd_with_config, pre_alloc_file
from pygetex.handler import HandlerBase
from pygetex.task import DownloadTask
from pygetex.utils.http import guess_file_metadata
from pygetex.utils.misc import get_divisional_range, load_object

if TYPE_CHECKING:
    from pygetex.core import CoreProcess


class HttpHandler(HandlerBase):
    def __init__(self, process: "CoreProcess"):
        super().__init__(process)
        self.scope = re.compile(r"^https??://\S+")

    async def check_scope(self, uri: str) -> bool:
        if self.scope.match(uri):
            return True
        return False

    async def handle(self, task: DownloadTask, resume: Optional[bool] = False):
        """

        :param task:
        :param resume: 是否为断点续传后添加的task
        :return:
        """
        self.process.collector.task_add(task.id, [[0, -1]])  # type: ignore # 占坑，防止后面报错时删除无门
        temp_config = update_config(self.config, **cast(dict, task.options))
        downloader_cls: Type = load_object(temp_config.downloader)  # type: ignore
        downloader = downloader_cls(temp_config)
        path = task.path
        # if not resume:
        #     while os.path.exists(path):
        #         dir_, ext = os.path.splitext(path)
        #         path = dir_ + "(1)" + ext
        if task.filesize and not resume:
            pre_alloc_file(path, task.filesize)
        raw_fd, wrapped_fd = open_fd_with_config(path, temp_config)
        try:
            if not task.support_range:
                status, headers, body_iter = await downloader.download(
                    task.uri,
                    getattr(temp_config, "method", "GET"),
                    getattr(temp_config, "headers", None),
                    getattr(temp_config, "payload", None),
                )
                split_result = [[0, task.filesize or -1]]
                # 这里不支持多线程下载，因此只用划分一个block，-1表示不知道具体文件大小，假设无限大
                self.process.collector.task_add(task.id, split_result)  # type: ignore
                # collector和自己引用同一个split_result对象，利用下浅拷贝的性质
                try:
                    async for chunk in body_iter:
                        if temp_config.fileio_async:
                            await pwrite_async(
                                temp_config, wrapped_fd, chunk, split_result[0][0]
                            )
                        else:
                            pwrite(temp_config, wrapped_fd, chunk, split_result[0][0])
                        split_result[0][0] += len(chunk)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    await self.process.collector.task_error(task.id)  # type: ignore
                    self.process.dispatch_nowait(
                        "on_download_error", task.id, e, traceback.format_exc()
                    )
                    raise e  # 重新抛出异常 这一步很重要， 这样task.exception()为True _on_download_task_complete就可以知道
                finally:
                    await body_iter.close()
            else:
                assert task.filesize is not None
                tempfile = task.path + self.config.tempfile_suffix  # type: ignore
                if resume and os.path.exists(tempfile):
                    with open(tempfile, "rb") as f:
                        try:
                            split_result = pickle.load(f)
                        except pickle.UnpicklingError:
                            split_result = get_divisional_range(
                                task.filesize, temp_config.split
                            )  # 交给statcollector处理
                else:
                    split_result = get_divisional_range(
                        task.filesize, temp_config.split
                    )  # 交给statcollector处理
                self.process.collector.task_add(task.id, split_result)  # type: ignore
                tasks = []
                for block_index in range(len(split_result)):
                    tasks.append(
                        asyncio.create_task(
                            self.block_download(
                                task,
                                wrapped_fd,
                                split_result,
                                block_index,
                                downloader,
                                temp_config,
                            )
                        )
                    )
                try:
                    await asyncio.gather(*tasks)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    await self.process.collector.task_error(task.id)  # type: ignore
                    self.process.dispatch_nowait(
                        "on_download_error", task.id, e, traceback.format_exc()
                    )
                    raise e
        finally:
            await downloader.close()
            os.close(raw_fd)

    async def get_file_metadata(
        self, uri: str, **options
    ) -> Tuple[Optional[int], str, bool]:
        temp_config = update_config(self.config, **options)

        downloader = AIOHTTPDownloader(temp_config)
        try:
            filesize, filename, support_range = await guess_file_metadata(
                downloader, uri, temp_config
            )
            filename = temp_config.out or filename
            return filesize, filename, support_range
        finally:
            await downloader.close()

    async def block_download(
        self,
        task: DownloadTask,
        file: Any,
        ranges: List[List[int]],
        block_index: int,
        downloader: HTTPDownloaderBase,
        config: Config,
    ):
        """

        :param task:
        :param file: 文件句柄，特定于系统
        :param ranges: 分块的结果
        :param block_index: 这个是第几块
        :param downloader:
        :param config:
        :return:
        """
        headers = getattr(config, "headers", None)
        if headers is None:
            headers = {
                "Range": f"bytes={ranges[block_index][0]}-{ranges[block_index][1]}"
            }
        else:
            headers.update(
                {"Range": f"bytes={ranges[block_index][0]}-{ranges[block_index][1]}"}
            )
        status, headers, body_iter = await downloader.download(
            task.uri,
            method=getattr(config, "method", "GET"),
            headers=headers,
            payload=getattr(config, "payload", None),
        )
        try:
            async for chunk in body_iter:
                if config.fileio_async:
                    await pwrite_async(config, file, chunk, ranges[block_index][0])
                else:
                    pwrite(config, file, chunk, ranges[block_index][0])
                ranges[block_index][0] += len(chunk)
            assert ranges[block_index][0] == ranges[block_index][1] + 1  # 确保这个block是完整的
        finally:
            await body_iter.close()
