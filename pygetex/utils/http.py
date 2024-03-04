# -*- coding: utf-8 -*-
import re
from typing import Mapping, Optional, Tuple, Type
from urllib.parse import unquote, urlparse

from pygetex.config import Config
from pygetex.downloader import HTTPDownloaderBase


def guess_filename(url: str, headers: Mapping) -> str:
    filename = urlparse(url).path.split("/")[-1]  # maybe ""
    if "Content-Disposition" in headers:
        try:
            filename = (
                headers["Content-Disposition"].split(";")[1].split("=")[1].strip("\"'")
            )
        except IndexError:
            pass
    if "content-disposition" in headers:
        try:
            filename = (
                headers["content-disposition"].split(";")[1].split("=")[1].strip("\"'")
            )
        except IndexError:
            pass
    return unquote(filename)


def guess_support_range(headers) -> bool:
    """支持断点续传"""
    if headers.get("Accept-Ranges", "none") == "bytes":
        return True
    if headers.get("accept-ranges", "none") == "bytes":
        return True
    return False


# todo retry on 4xx?
re_content_range_compiled = re.compile(r"bytes [^/]+/([0-9]+)")


async def guess_file_metadata(
    downloader: HTTPDownloaderBase,
    url,
    config: Config,
) -> Tuple[Optional[int], str, bool]:
    """

    :param downloader:
    :param url:
    :param method:
    :return: filesize, filename, support_range
    """
    status, headers, body = await downloader.download(
        url,
        getattr(config, "method", "GET"),
        headers=getattr(config, "headers", {}).update({"Range": "bytes=0-0"}),
    )
    await body.close()
    if status == 206:
        m = re_content_range_compiled.match(
            headers.get("Content-Range", None) or headers.get("content-range", None),
        )  # https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Headers/Content-Range
        if m is not None:
            filesize = int(m.group(1))
            return filesize, guess_filename(url, headers), guess_support_range(headers)

    status, headers, body = await downloader.download(
        url,
        "HEAD",
        headers=getattr(config, "headers", {}).update({"Range": "bytes=0-0"}),
    )
    await body.close()  # close body stream
    if "Content-Length" in headers:
        return (
            int(headers["Content-Length"]),
            guess_filename(url, headers),
            guess_support_range(headers),
        )
    elif "content-length" in headers:
        return (
            int(headers["content-length"]),
            guess_filename(url, headers),
            guess_support_range(headers),
        )
    else:
        return None, guess_filename(url, headers), guess_support_range(headers)
