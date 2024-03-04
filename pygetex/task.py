# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any, Literal, Optional

import orjson
import sqlalchemy as sa
from sqlalchemy.ext.mutable import Mutable
from sqlmodel import Field, SQLModel


class MutableDict(Mutable, dict):
    @classmethod
    def coerce(cls, key, value):
        """Convert plain dictionaries to MutableDict."""

        if not isinstance(value, MutableDict):
            if isinstance(value, dict):
                return MutableDict(value)

            # this call will raise ValueError
            return Mutable.coerce(key, value)
        else:
            return value

    def __setitem__(self, key, value):
        """Detect dictionary set events and emit change events."""

        dict.__setitem__(self, key, value)
        self.changed()

    def __delitem__(self, key):
        """Detect dictionary del events and emit change events."""

        dict.__delitem__(self, key)
        self.changed()


class JSONEncodedDict(sa.types.TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Usage:

        JSONEncodedDict(255)

    """

    impl = sa.types.UnicodeText

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = orjson.dumps(value).decode("utf8")

        return value

    def process_result_value(self, value: Optional[str], dialect):
        if value is not None:
            value = orjson.loads(value)
        return value


json_type = MutableDict.as_mutable(JSONEncodedDict)


class DownloadTask(SQLModel, table=True):
    # 不会实时更新 启动的时候从数据库读取之前的剩余任务 任务结束了更新
    __tablename__ = "download_task"
    id: Optional[int] = Field(None, primary_key=True)
    uri: str = Field(..., description="download uri")
    filesize: Optional[int] = Field(None, description="file size")
    path: str = Field(
        ..., description="download file path include name, e.g. /path/to/file/a.zip"
    )
    support_range: bool = Field(...)
    options: Optional[Any] = Field(sa_column=sa.Column(json_type), description="")
    start_time: Optional[datetime] = Field(None)
    end_time: Optional[datetime] = Field(None)
    status: str = Field(
        "downloading", description="download status"
    )  # Literal["downloading", "paused", "stopped", "complete", "error"]
    speed: Optional[float] = Field(0.0, description="download speed")
