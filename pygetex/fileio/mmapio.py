# -*- coding: utf-8 -*-
from mmap import mmap


def pwrite(fd: mmap, data: bytes, offset: int) -> int:
    fd[offset : offset + len(data)] = data
    size = len(data)
    fd.flush(offset, size)
    return size
