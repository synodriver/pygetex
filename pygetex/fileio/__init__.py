# -*- coding: utf-8 -*-
from pygetex.config import Config
from pygetex.fileio.generalio import pwrite as generalio_pwrite
from pygetex.fileio.mmapio import pwrite as mmapio_pwrite
from pygetex.fileio.sysio import pwrite as sysio_pwrite
from pygetex.utils.misc import run_sync


def pwrite(config, fd, data: bytes, offset: int) -> int:
    if config.fileio == "mmapio":
        return mmapio_pwrite(fd, data, offset)
    elif config.fileio == "sysio":
        return sysio_pwrite(fd, data, offset)
    elif config.fileio == "generalio":
        return generalio_pwrite(fd, data, offset)
    return 0


pwrite_async = run_sync(pwrite)
