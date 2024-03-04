# -*- coding: utf-8 -*-
from copy import deepcopy
from pathlib import Path
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

envfile = str(Path(__file__).parent.parent.resolve() / ".env")


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="allow"
    )

    debug: bool = False
    timezone_offset: Optional[int] = Field(8, description="UTC time zone")
    fileio: Literal["mmapio", "sysio", "generalio"] = Field(
        "mmapio", description="File IO mode"
    )
    fileio_async: Optional[bool] = Field(False, description="file io in thread pool")
    database: Optional[str] = Field(
        "sqlite+aiosqlite:///pyget.db", description="must be an async driver"
    )
    tempfile_suffix: Optional[str] = Field(
        ".pyget", description="cache file suffix"
    )  # 存储没下载完成的文件的分块结果
    update_interval: Optional[float] = Field(
        5.0, description="update interval in seconds"
    )  # statscollector更新间隔
    split: int = Field(
        16, description="block count for large file downloading"
    )  # 默认下载线程数
    chunk_size: Optional[int] = Field(64 * 1024 * 1024, description="stream read size")
    dir: str = Field("/download", description="default download path")
    out: Optional[str] = Field(None, description="default download file name")

    downloader: Optional[str] = Field(
        "pygetex.downloader.aiohttpdownloader.AIOHTTPDownloader",
        description="default download path",
    )


def update_config(config: Config, **options) -> Config:
    temp_config = deepcopy(config)
    for name, value in options.items():
        setattr(temp_config, name, value)
    return temp_config
