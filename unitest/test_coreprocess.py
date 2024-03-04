# -*- coding: utf-8 -*-
from unittest import IsolatedAsyncioTestCase

from pygetex.config import Config
from pygetex.core import CoreProcess
from pygetex.handler import HandlerBase
from pygetex.plugin import PluginBase


class A(PluginBase):
    name = "a"

    async def on_test(self):
        return "A on_test"


class B(PluginBase):
    name = "b"

    async def on_test(self):
        return "B on_test"


class C(HandlerBase):
    name = "c"

    async def check_scope(self, uri: str) -> bool:
        return True


class D(HandlerBase):
    name = "d"

    async def check_scope(self, uri: str) -> bool:
        return False


class TestCoreProcess(IsolatedAsyncioTestCase):
    async def test_coreprocess(self):
        config = Config()
        async with CoreProcess(config) as process:
            plugin_names = process.get_plugins()
            self.assertEqual(len(plugin_names), 2)
            self.assertIn("a", plugin_names)
            self.assertIn("b", plugin_names)

            process.disable_plugin("a")
            self.assertFalse(process.plugins["a"].enabled)
            process.enable_plugin("a")
            self.assertTrue(process.plugins["a"].enabled)

            results = await process.dispatch("on_test")
            self.assertEqual(results["a"], "A on_test")
            self.assertEqual(results["b"], "B on_test")

    async def test_check_handler(self):
        config = Config()
        async with CoreProcess(config) as process:
            handlers = await process._check_handler("http://1.1.1.1")
            names = [h.name for h in handlers]
            self.assertIn("c", names)
            self.assertNotIn("d", names)
            # await process.add_uri("https://alpha.zrflie1.pw/PC-2/%E4%BD%8F%E5%9C%A8%E4%B8%8B%E4%BD%93%E5%8D%87%E7%BA%A7%E5%B2%9B%E4%B8%8A%E7%9A%84%E8%B4%AB%E4%B9%B3%E8%AF%A5%E5%A6%82%E4%BD%95%E6%98%AF%E5%A5%BD2(%E5%AE%98%E4%B8%AD).rar")


if __name__ == "__main__":
    import unittest

    unittest.main()
