# -*- coding: utf-8 -*-
import os
from mmap import ACCESS_READ, ACCESS_WRITE, mmap
from unittest import TestCase

from pygetex.fileio import generalio, mmapio, sysio
from pygetex.fileio.utils import pre_alloc_file


class TestFileIO(TestCase):
    def setUp(self) -> None:
        flags = os.O_WRONLY | os.O_CREAT
        if os.name == "nt":
            flags |= os.O_BINARY
        self.fd = os.open("./test.txt", flags, 0o777)
        os.truncate(self.fd, 100)
        os.close(self.fd)

    # def tearDown(self) -> None:
    #     os.close(self.fd)

    def test_mmapio(self):
        # self.fd = os.open("./test.txt", os.O_WRONLY | os.O_BINARY)
        with open("./test.txt", "r+b") as f:
            fd = mmap(f.fileno(), 0, access=ACCESS_WRITE)
            mmapio.pwrite(fd, b"foo bar", 10)
            mmapio.pwrite(fd, b"bar foo", 20)
            mmapio.pwrite(fd, b"foo", 30)
            self.assertEqual(fd[10:17], b"foo bar")
            self.assertEqual(fd[20:27], b"bar foo")
            self.assertEqual(fd[30:33], b"foo")

    def test_mmapio2(self):
        fd = os.open("./test.txt", os.O_RDWR)
        m = mmap(fd, 0, access=ACCESS_WRITE)
        mmapio.pwrite(m, b"foo bar", 10)
        mmapio.pwrite(m, b"bar foo", 20)
        mmapio.pwrite(m, b"foo", 30)
        os.close(fd)
        with open("./test.txt", "rb") as f:
            m = f.read()
            self.assertEqual(m[10:17], b"foo bar")
            self.assertEqual(m[20:27], b"bar foo")
            self.assertEqual(m[30:33], b"foo")

    def test_generalio(self):
        fd = os.open("./test.txt", os.O_RDWR)
        generalio.pwrite(fd, b"foo bar", 40)
        generalio.pwrite(fd, b"bar foo", 60)
        generalio.pwrite(fd, b"foo", 80)
        os.close(fd)
        with open("./test.txt", "rb") as f:
            data = f.read()
            self.assertEqual(data[40:47], b"foo bar")
            self.assertEqual(data[60:67], b"bar foo")
            self.assertEqual(data[80:83], b"foo")

    def test_sysio(self):
        fd = os.open("./test.txt", os.O_RDWR)
        sysio.pwrite(fd, b"foo bar", 40)
        sysio.pwrite(fd, b"bar foo foo", 60)
        sysio.pwrite(fd, b"foo", 80)
        os.close(fd)
        with open("./test.txt", "rb") as f:
            data = f.read()
            self.assertEqual(data[40:47], b"foo bar")
            self.assertEqual(data[60:71], b"bar foo foo")
            self.assertEqual(data[80:83], b"foo")


if __name__ == "__main__":
    import unittest

    unittest.main()
