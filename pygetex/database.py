# -*- coding: utf-8 -*-
"""
初始化db
"""
import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, and_, func, or_, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pygetex.task import DownloadTask


async def init_db():
    sqlite_file_name = "data.db"
    sqlite_url = "sqlite+aiosqlite:///pyget.db"
    engine = create_async_engine(sqlite_url, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)
        await conn.run_sync(SQLModel.metadata.create_all)


async def add_db():
    sqlite_url = "sqlite+aiosqlite:///pyget.db"
    engine = create_async_engine(sqlite_url, echo=True)
    async with AsyncSession(engine) as session:
        session.add(
            DownloadTask(
                uri="https://alpha.zrflie1.pw/PC-2/%E4%BD%8F%E5%9C%A8%E4%B8%8B%E4%BD%93%E5%8D%87%E7%BA%A7%E5%B2%9B%E4%B8%8A%E7%9A%84%E8%B4%AB%E4%B9%B3%E8%AF%A5%E5%A6%82%E4%BD%95%E6%98%AF%E5%A5%BD2(%E5%AE%98%E4%B8%AD).rar",
                filesize=None,
                path="out.rar",
                support_range=True,
                options={},
                start_time=datetime.now(
                    tz=timezone(timedelta(hours=8))  # type: ignore
                ),
                status="downloading",
            )
        )
        await session.commit()


async def get_db():
    sqlite_url = "sqlite+aiosqlite:///pyget.db"
    engine = create_async_engine(sqlite_url, echo=True)
    async with AsyncSession(engine) as session:
        tasks = (await session.exec(select(DownloadTask))).all()
        pass


if __name__ == "__main__":
    asyncio.run(init_db())
