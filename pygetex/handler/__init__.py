# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type

from pygetex.task import DownloadTask

if TYPE_CHECKING:
    from pygetex.core import CoreProcess


class HandlerMeta(type):
    handlers = {}  # type: Dict[str, Type[HandlerBase]]

    def __new__(cls, name, bases, attrs, **kwargs):
        tp = super().__new__(cls, name, bases, attrs)
        name = getattr(tp, "name", None) or tp.__name__
        if name not in cls.handlers and name != "HandlerBase":
            cls.handlers[name] = tp
        return tp


class HandlerBase(metaclass=HandlerMeta):
    """
    建议只识别http ftp之类的基础协议，其余的给plugin完成
    """

    def __init__(self, process: "CoreProcess"):
        self.process = process
        self.config = process.config

    async def check_scope(self, uri: str) -> bool:
        ...

    async def handle(self, task: DownloadTask, resume: Optional[bool] = False) -> None:
        """
        处理一个新的下载任务
        :param task:
        :param resume: 是否属于断点续传
        :return:
        """
        ...

    async def get_file_metadata(
        self, uri: str, **options
    ) -> Tuple[Optional[int], str, bool]:
        """
        获取文件的元数据 filesize in bytes or None, filename, support_range
        :param uri:
        :return:
        """
        return None, "", False  # make mypy happy
