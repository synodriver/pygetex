# -*- coding: utf-8 -*-
import asyncio
import sys
from contextvars import copy_context
from functools import partial, wraps
from importlib import import_module
from typing import Any, Callable, Coroutine, List, Optional, Union

from typing_extensions import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def run_sync(call: Callable[P, R]) -> Callable[P, Coroutine[None, None, R]]:
    """一个用于包装 sync function 为 async function 的装饰器

    参数:
        call: 被装饰的同步函数
    """

    @wraps(call)
    async def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        loop = asyncio.get_running_loop()
        pfunc = partial(call, *args, **kwargs)
        context = copy_context()
        result = await loop.run_in_executor(None, partial(context.run, pfunc))  # type: ignore
        return result  # type: ignore

    return _wrapper


def get_divisional_range(filesize: int, split=10) -> List[List[int]]:
    step = filesize // split
    arr = list(range(0, filesize, step))
    result = []
    for i in range(len(arr) - 1):  # n-1 0-(n-2)
        s_pos, e_pos = arr[i], arr[i + 1] - 1
        result.append([s_pos, e_pos])
    result[-1][-1] = filesize - 1
    return result


def get_unfinished_range(result: List[List[int]]) -> List[List[int]]:
    return list(filter(lambda x: x[0] <= x[1], result))


def get_remain_bytes(split_result: List[List[int]]) -> int:
    if split_result[-1][-1] == -1:
        if len(split_result) == 1:
            return sys.maxsize - split_result[0][0]  # unknown size
        else:
            # 2个block以上，说明大小已知才能分块，怎么可能最后一个block的大小未知？
            raise ValueError("last block must have a known size")
    size = 0
    for block_start, block_end in split_result:
        size += block_end - block_start + 1
    return size


def load_object(path: Union[str, Callable]) -> Any:
    """使用绝对路径加载并返回一个对象

    对象可以是类、函数、变量或实例的导入路径，例如"pygetex.downloader.aiohttpdownloader.AIOHTTPDownloader"
    如果```path```不是字符串，而是一个可调用的对象，如类或函数，则按原样返回。
    """

    if not isinstance(path, str):
        if callable(path):
            return path
        raise TypeError(
            f"Unexpected argument type, expected string or object, got: {type(path)}"
        )

    try:
        dot = path.rindex(".")
    except ValueError:
        raise ValueError(f"Error loading object '{path}': not a full path")

    module, name = path[:dot], path[dot + 1 :]
    mod = import_module(module)

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError(f"Module '{module}' doesn't define any object named '{name}'")

    return obj


# todo unitest them
