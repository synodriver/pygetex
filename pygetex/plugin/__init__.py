# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict, List, Optional, Type

if TYPE_CHECKING:
    from pygetex.core import CoreProcess


class PluginMeta(type):
    plugins = {}  # type: Dict[str, Type[PluginBase]]

    def __new__(cls, name, bases, attrs, **kwargs):
        tp = super().__new__(cls, name, bases, attrs)
        name = getattr(tp, "name", None) or tp.__name__
        if name not in cls.plugins and name != "PluginBase":
            cls.plugins[name] = tp
        return tp


class PluginBase(metaclass=PluginMeta):
    enabled: bool = True
    name: str
    description: str

    def __init__(self, core: "CoreProcess"):
        ...

    async def on_startup(self):
        ...

    async def on_shutdown(self):
        ...

    async def on_add_uri(self, uri: str, **options) -> List[str]:
        return []
