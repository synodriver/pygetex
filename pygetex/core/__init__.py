# -*- coding: utf-8 -*-
import asyncio
import os
from datetime import datetime, timedelta, timezone
from functools import partial
from itertools import chain
from typing import Any, Dict, List, Set

from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import and_, func, or_, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pygetex import __version__
from pygetex.config import Config
from pygetex.core.statscollector import StatsCollector
from pygetex.handler import HandlerBase, HandlerMeta
from pygetex.handler.http import HttpHandler  # load this handler
from pygetex.plugin import PluginBase, PluginMeta
from pygetex.task import DownloadTask


class CoreProcess:
    def __init__(self, config: Config):
        self.config = config
        self.db = create_async_engine(config.database, echo=config.debug)
        self.handlers = {}  # type: Dict[str, HandlerBase]
        self.plugins = {}  # type: Dict[str, PluginBase]
        self.collector = StatsCollector(self)  # type: ignore
        for name, plugin_tp in PluginMeta.plugins.items():
            self.plugins[name] = plugin_tp(self)  # type: ignore
        for name, handler_tp in HandlerMeta.handlers.items():
            self.handlers[name] = handler_tp(self)  # type: ignore
        self._pending_tasks = {}  # type: Dict[int, asyncio.Task]
        self._dispatch_tasks = set()  # type: Set[asyncio.Task]
        self._complete_event = asyncio.Event()
        self._complete_event.set()

    def enable_plugin(self, name: str):
        if name in self.plugins:
            self.plugins[name].enabled = True

    def disable_plugin(self, name: str):
        if name in self.plugins:
            self.plugins[name].enabled = False

    def get_plugins(self) -> List[str]:
        return list(self.plugins.keys())

    async def _check_handler(self, uri: str) -> List[HandlerBase]:
        """
        哪些handler可以处理uri
        :param uri:
        :return:
        """
        tasks = []
        all_handlers = []
        for handler in self.handlers.values():
            all_handlers.append(handler)
            tasks.append(handler.check_scope(uri))
        results = await asyncio.gather(*tasks)
        runners = filter(
            lambda x: x[0], zip(results, all_handlers)
        )  # [(bool, HandlerBase)]
        return [i[1] for i in runners]

    async def add_uri(self, uri: str, **options) -> List[DownloadTask]:
        """
        解析完文件的名字 大小等元数据之后就会返回 不会真的等下载完成
        :param uri:
        :param options:
        :return:
        """
        results = await self.dispatch(
            "on_add_uri", uri, **options
        )  # plugin handle this
        results_filtered = set(
            filter(lambda x: x is not None, chain(*results.values()))
        )  # type: Set[str]
        uris = list(results_filtered) if results_filtered else [uri]

        download_tasks = []  # type: List[DownloadTask]
        async with AsyncSession(self.db) as session:
            for uri in uris:
                handlers = await self._check_handler(uri)
                if handlers:
                    filesize, filename, support_range = await handlers[
                        0
                    ].get_file_metadata(uri, **options)
                    path = os.path.join(
                        options.get("dir", None) or self.config.dir, filename
                    )
                    while os.path.exists(path):
                        dir_, ext = os.path.splitext(path)
                        path = dir_ + "(1)" + ext
                    download_task = DownloadTask(
                        uri=uri,
                        filesize=filesize,
                        path=path,
                        support_range=support_range,
                        options=options,
                        start_time=datetime.now(
                            tz=timezone(timedelta(hours=self.config.timezone_offset))  # type: ignore
                        ),
                        status="downloading",
                    )

                    session.add(download_task)
                    await session.commit()
                    await session.refresh(download_task)

                    aiotask = asyncio.create_task(handlers[0].handle(download_task))
                    self._pending_tasks[download_task.id] = aiotask  # type: ignore
                    aiotask.add_done_callback(
                        partial(
                            self._on_download_task_complete, taskid=download_task.id
                        )
                    )
                    download_tasks.append(download_task)
                    self.dispatch_nowait("on_download_start", download_task.id)
                    self._complete_event.clear()  # 现在不是处于完成状态了
        return download_tasks  # 下载results_new的东西

    def _on_download_task_complete(self, aiotask: asyncio.Task, taskid: int) -> None:
        if (
            aiotask.done() and not aiotask.cancelled() and not aiotask.exception()
        ):  # 其他情况是被暂停或者中止了或者出错了，这些在别的地方判断定
            tmp_task = asyncio.create_task(
                self.collector.task_complete(taskid)
            )  # 通知plugin和statcollector
            self._dispatch_tasks.add(tmp_task)
            tmp_task.add_done_callback(self._dispatch_tasks.discard)
            self.dispatch_nowait("on_download_complete", taskid)
        self._pending_tasks.pop(taskid)  # type: ignore
        if not self._pending_tasks:
            self._complete_event.set()  # 现在处于完成状态

    # todo 增加exception子模块，抛出合适的异常
    async def stop(self, taskid: int):
        if taskid in self._pending_tasks:
            aiotask = self._pending_tasks.pop(taskid)
            aiotask.cancel()
            try:
                await aiotask
            except asyncio.CancelledError:
                pass
        await self.collector.task_stop(taskid)
        self.dispatch_nowait("on_download_stop", taskid)

    async def remove(self, taskid: int):
        await self.stop(taskid)
        async with AsyncSession(self.db) as session:
            download_task = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.id == taskid)
                )
            ).one()  # 反正都删除了，就不设置status为stopped了
            await session.delete(download_task)
            await session.commit()

    async def pause(self, taskid: int):
        if taskid not in self._pending_tasks:
            raise ValueError(f"no active task with id {taskid}")
        aiotask = self._pending_tasks[taskid]
        aiotask.cancel()  # callback会删除_pending_tasks的
        try:
            await aiotask
        except asyncio.CancelledError:
            pass
        await self.collector.task_pause(taskid)
        self.dispatch_nowait("on_download_pause", taskid)

    async def pause_all(self):
        aiotasks = []
        # taskids = list(self._pending_tasks.keys()) # 拷贝一次key，免得并发执行的时候_pending_task变了：边迭代边修改dict是ub
        for taskid in self._pending_tasks:
            aiotasks.append(asyncio.create_task(self.pause(taskid)))
        await asyncio.gather(*aiotasks)

    async def unpause(self, taskid: int):
        if taskid in self._pending_tasks:
            raise ValueError(f"task {taskid} is already running")
        async with AsyncSession(self.db) as session:
            download_task = (
                await session.exec(
                    select(DownloadTask).where(
                        and_(
                            DownloadTask.id == taskid,
                            or_(
                                DownloadTask.status == "paused",
                                DownloadTask.status == "error",
                            ),
                        )
                    )
                )
            ).one()
            await self._resume_one(download_task)

    async def unpause_all(self):
        aiotasks = []
        async with AsyncSession(self.db) as session:
            download_tasks = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.status == "paused")
                )
            ).all()
            for download_task in download_tasks:
                aiotasks.append(self._resume_one(download_task))
            await asyncio.gather(*aiotasks)

    async def tell_status(self, taskid: int) -> DownloadTask:
        async with AsyncSession(self.db) as session:
            download_task = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.id == taskid)
                )
            ).one()
            if taskid in self.collector.speed:
                download_task.speed = self.collector.speed[taskid]  # todo 是否需要返回下载进度？
            return download_task

    async def tell_active(self) -> List[int]:
        return list(self._pending_tasks.keys())

    async def tell_paused(self, offset: int, count: int) -> List[int]:
        async with AsyncSession(self.db) as session:
            download_tasks = (
                await session.exec(
                    select(DownloadTask)
                    .where(DownloadTask.status == "paused")
                    .offset(offset)
                    .limit(count)
                )
            ).all()
            return list(map(lambda x: x.id, download_tasks))

    async def tell_stopped(self, offset: int, count: int) -> List[int]:
        async with AsyncSession(self.db) as session:
            download_tasks = (
                await session.exec(
                    select(DownloadTask)
                    .where(DownloadTask.status == "stopped")
                    .offset(offset)
                    .limit(count)
                )
            ).all()
            return list(map(lambda x: x.id, download_tasks))

    async def get_option(self, taskid: int) -> dict:
        async with AsyncSession(self.db) as session:
            download_task = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.id == taskid)
                )
            ).one()
            return download_task.options

    async def change_option(self, taskid: int, **options):
        async with AsyncSession(self.db) as session:
            download_task = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.id == taskid)
                )
            ).one()
            for key, value in options.items():
                setattr(download_task, key, value)
            session.add_all(download_task)
            await session.commit()

    async def get_global_option(self) -> dict:
        return self.config.model_dump()

    async def change_global_option(self, **options):
        for key, value in options.items():
            setattr(self.config, key, value)

    async def get_global_stat(self):
        ...

    async def purge_download_result(self):
        async with AsyncSession(self.db) as session:
            download_tasks = (
                await session.exec(
                    select(DownloadTask).where(
                        or_(
                            DownloadTask.status == "complete",
                            DownloadTask.status == "error",
                        )
                    )
                )
            ).all()
            for download_task in download_tasks:
                await session.delete(download_task)
            await session.commit()

    def get_version(self) -> str:
        return __version__

    async def dispatch(self, funcname: str, *args, **kwargs) -> Dict[str, Any]:
        names = []  # type: List[str]
        tasks = []
        for name, plugin in self.plugins.items():
            if plugin.enabled:
                if method := getattr(plugin, funcname, None):
                    if callable(method) and asyncio.iscoroutinefunction(method):
                        tasks.append(method(*args, **kwargs))
                        names.append(name)
        results = await asyncio.gather(*tasks)
        return dict(zip(names, results))

    def dispatch_nowait(self, funcname: str, *args, **kwargs) -> None:
        """
        如果不需要返回结果就用这个，直接在后台运行
        :param funcname:
        :param args:
        :param kwargs:
        :return:
        """
        task = asyncio.create_task(self.dispatch(funcname, *args, **kwargs))
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    async def _resume_one(self, download_task: DownloadTask) -> None:
        handlers = await self._check_handler(download_task.uri)
        if handlers:
            aiotask = asyncio.create_task(
                handlers[0].handle(download_task, resume=True)
            )
            self._pending_tasks[download_task.id] = aiotask  # type: ignore
            aiotask.add_done_callback(
                partial(self._on_download_task_complete, taskid=download_task.id)
            )
            self.dispatch_nowait("on_download_start", download_task.id)
            self._complete_event.clear()  # 现在不是处于完成状态了

    async def _resume_tasks(self) -> None:
        """
        断点续传的逻辑 从数据库中寻找downloading的任务，以resume=True调用handler
        :return:
        """
        resume_aiotasks: List[asyncio.Task] = []
        async with AsyncSession(self.db) as session:
            tasks: List[DownloadTask] = (
                await session.exec(
                    select(DownloadTask).where(DownloadTask.status == "downloading")
                )
            ).all()
            for download_task in tasks:
                resume_aiotasks.append(
                    asyncio.create_task(self._resume_one(download_task))
                )
            await asyncio.gather(*resume_aiotasks)

    async def wait(self):
        """
        等待全部下载任务完成
        :return:
        """
        await self._complete_event.wait()

    async def startup(self):
        await self._resume_tasks()
        await self.dispatch("on_startup")

    async def shutdown(self):
        await self.collector.close()
        await self.dispatch("on_shutdown")

    async def __aenter__(self):
        await self.startup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()
