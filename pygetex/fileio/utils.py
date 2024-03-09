# -*- coding: utf-8 -*-
import os
from mmap import ACCESS_WRITE, mmap

if os.name == "nt":
    import msvcrt
    from ctypes import wintypes

from pygetex.config import Config


def pre_alloc_file(path: str, length: int, mode=0o666, exist_ok=True):
    if exist_ok:
        try:
            os.utime(path, None)
        except OSError:
            # Avoid exception chaining
            pass
        else:
            return
    flags = os.O_CREAT | os.O_WRONLY
    if not exist_ok:
        flags |= os.O_EXCL
    fd = os.open(path, flags, mode)
    os.truncate(fd, length)  # todo os.posix_fallocate on unix? SetFileValidData on win?
    os.close(fd)


def open_fd(path, mode: int = 0o777) -> int:
    if os.path.exists(path):
        flags = os.O_RDWR
    else:
        flags = os.O_RDWR | os.O_CREAT
    if os.name == "nt":
        flags |= os.O_BINARY
    fd = os.open(path, flags, mode)
    return fd


def open_fd_with_config(path: str, config: Config, mode: int = 0o777):
    """
    为下载任务准备fd，pathcunzai
    :param path:
    :param length:
    :param mode:
    :return: raw_fd, wrapped_fd
    """
    fd = open_fd(path, mode)
    if config.fileio == "mmapio":
        return fd, mmap(fd, 0, access=ACCESS_WRITE)
    elif config.fileio == "sysio":
        if os.name == "nt":
            return fd, wintypes.HANDLE(msvcrt.get_osfhandle(fd))
        else:
            return fd, fd
    elif config.fileio == "generalio":
        return fd, fd
