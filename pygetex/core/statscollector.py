# -*- coding: utf-8 -*-
import asyncio
import os
import pickle
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Dict, List, Union

from sqlalchemy.exc import NoResultFound
from sqlmodel import and_, func, or_, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pygetex.config import Config
from pygetex.task import DownloadTask
from pygetex.utils.misc import get_remain_bytes, get_unfinished_range

if TYPE_CHECKING:
    from pygetex.core import CoreProcess


class StatsCollector:
    def __init__(self, process: "CoreProcess"):
        self.config = process.config  # type: Config
        self.db = process.db
        self.speed = {}  # type: Dict[int, float]
        # task_id, speed bytes/second
        self._active_tasks = {}  # type: Dict[int, List[List[int]]]
        # task_id, 多线程的任务就是fileblocks，单线程的就是[[xxx, None]]只有一块，和handler引用同一个list对象
        self._background_task = asyncio.create_task(self._updating_task())
        # status是downloading的task Dict[int, int] id, received_bytes

    def task_add(self, taskid: int, split_result: List[List[int]]):
        """
        handler开始handle之后由对应的handler调用这个函数
        :param taskid:
        :return:
        """
        # print(f"task_add {taskid}") 调用2次很正常 一次占坑
        self._active_tasks[taskid] = split_result
        # todo 重启进程后 CoreProcess负责查数据库，dispatch消息，调用handler.handle,传入resume=True参数，handler自己会调用task_add

    async def task_complete(self, taskid: int):
        """下载完成之后触发core，core会在之后调用这个"""
        print(f"task_complete {taskid}")
        if taskid not in self._active_tasks:
            raise ValueError(f"no active task with id {taskid}")
        # assert isinstance(task, DownloadTask)
        async with AsyncSession(self.db) as session:
            task = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.id == taskid)
                )
            ).one()
            tempfile = task.path + self.config.tempfile_suffix  # type: ignore
            if os.path.exists(tempfile):
                os.remove(tempfile)  # 删除断点续传临时文件
            task.status = "complete"
            task.end_time = datetime.now(
                tz=timezone(timedelta(hours=self.config.timezone_offset))  # type: ignore
            )
            # await session.merge(task)
            session.add(
                task
            )  # 这个task对象可能并不是这个session查出来的，这样可以算update吗？还是变成insert然后说主键冲突？
            await session.commit()  # db层标识任务已完成
        del self._active_tasks[taskid]
        self.speed.pop(taskid, None)  # 删除speed都用pop 因为可能更新不及时 防止KeyError

    async def task_pause(self, taskid: int):
        """
        和task_complete一样，但是不能删除断点续传临时文件，还要写入当前的进度
        :param task:
        :return:
        """
        print(f"task_pause {taskid}")
        if taskid not in self._active_tasks:
            raise ValueError(f"no active task with id {taskid}")
        async with AsyncSession(self.db) as session:
            task = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.id == taskid)
                )
            ).one()
            task.status = "paused"
            self.save_one(task)  # 写入当前的进度
            session.add(task)
            await session.commit()  # db层标识任务已暂停
        del self._active_tasks[taskid]
        self.speed.pop(taskid, None)

    async def task_stop(
        self, taskid: int
    ):  # todo 这个貌似可以直接约等于task_complete，但是不仅仅从self._activa_tasks里面寻找，还要找数据库，因为可以stop掉pause的任务，
        # 不过因为外面是cancel掉handler.handle的，on_download_task_complete不会触发调用collector.task_complete，
        # 因此cancel后需要coreprocess调用这个取消collector的追踪。stop已经stop的任务是可以的，具有幂等性
        print(f"task_stop {taskid}")
        async with AsyncSession(self.db) as session:
            try:
                task = (
                    await session.exec(
                        select(DownloadTask).where(DownloadTask.id == taskid)
                    )
                ).one()
            except NoResultFound:
                raise ValueError(f"no task with id {task}")
            tempfile = task.path + self.config.tempfile_suffix  # type: ignore
            if os.path.exists(tempfile):
                os.remove(tempfile)  # 删除断点续传临时文件
            task.status = "stopped"  # type: ignore
            task.end_time = datetime.now(  # type: ignore
                tz=timezone(timedelta(hours=self.config.timezone_offset))  # type: ignore
            )
            session.add(task)
            await session.commit()
        self._active_tasks.pop(taskid, None)  # type: ignore
        self.speed.pop(taskid, None)

    async def task_error(self, taskid: int):
        # todo 只有handler知道何时下载出错，这个只能handler.process.collector.task_error这样调用，
        #  handler只可能知道正在下载的活跃任务有没有出错，一定是活跃的任务
        print(f"task_error {taskid}")
        async with AsyncSession(self.db) as session:
            task = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.id == taskid)
                )
            ).one()
            # tempfile = task.path + self.config.tempfile_suffix  # type: ignore
            # if os.path.exists(tempfile):
            #     os.remove(tempfile)  todo 下载出错的前提下需要删除断点续传临时文件吗？
            task.status = "error"
            task.end_time = datetime.now(
                tz=timezone(timedelta(hours=self.config.timezone_offset))  # type: ignore
            )
            session.add(task)
            await session.commit()
        del self._active_tasks[taskid]
        self.speed.pop(taskid, None)

    # 启动进程的时候先读sql，status是downloading的全部调用task_add 恢复关闭进程之前的状态
    async def _updating_task(self) -> None:
        after_remains = None
        while True:
            if after_remains is None:
                previous_remains = {}  # type: Dict[int, int]
                for download_taskid, split_result in self._active_tasks.items():
                    previous_remains[download_taskid] = get_remain_bytes(split_result)  # type: ignore
            else:
                previous_remains = after_remains
            await asyncio.sleep(self.config.update_interval)  # type: ignore
            after_remains = {}  # type: Dict[int, int]
            for download_taskid, split_result in self._active_tasks.items():
                after_remains[download_taskid] = get_remain_bytes(split_result)  # type: ignore
            for taskid in after_remains:  # type: ignore
                if taskid in previous_remains:
                    self.speed[taskid] = previous_remains[taskid] - after_remains[taskid]  # type: ignore

    # 完成的块就别写入了
    def save_one(self, task: DownloadTask):
        split_result = self._active_tasks[task.id]  # type: ignore
        tempfile = task.path + self.config.tempfile_suffix  # type: ignore
        with open(tempfile, "wb") as f:
            pickle.dump(get_unfinished_range(split_result), f)

    async def save_all(self):
        """
        把分块下载的进度结果dump进二进制文件，重启进程后方便读取了断点续传
        :return:
        """
        async with AsyncSession(self.db) as session:
            for taskid, split_result in self._active_tasks.items():
                task = (
                    await session.exec(
                        select(DownloadTask).where(DownloadTask.id == taskid)
                    )
                ).one()
                tempfile = task.path + self.config.tempfile_suffix
                with open(tempfile, "wb") as f:
                    pickle.dump(get_unfinished_range(split_result), f)

    async def close(self):
        await self.save_all()
        self._background_task.cancel()
        try:
            await self._background_task
        except asyncio.CancelledError:
            pass
        self._background_task = None

    # def data_received(self, taskid: int, size: int):
    #     """
    #     handler从server那收到了size个字节，调用这个函数告知StatsCollector，以便统计下载速度
    #     :param taskid:
    #     :param size:
    #     :return:
    #     """
    #     self._total_received += size
    #     if taskid in self._activa_tasks:
    #         self._activa_tasks[taskid] += size
    #     else:
    #         self._activa_tasks[taskid] = size
    #
    # def add_file_blocks(self, taskid: int, file_blocks: List[List[int]]):
    #     assert taskid not in self._activa_tasks
    #     self._activa_tasks[taskid] = file_blocks
