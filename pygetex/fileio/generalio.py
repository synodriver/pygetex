# -*- coding: utf-8 -*-
import os


def pwrite(fd: int, data: bytes, offset: int) -> int:
    current_pos = os.lseek(fd, 0, os.SEEK_CUR)  # 当前的绝对位置
    try:
        os.lseek(fd, offset, os.SEEK_SET)
        return os.write(fd, data)
    finally:
        os.lseek(fd, current_pos, os.SEEK_SET)
