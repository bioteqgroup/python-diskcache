"""Gzip compressed disk implementation."""

import gzip

from .core import UNKNOWN, Disk


class GzipDisk(Disk):
    """Stores data on disk compressed with gzip."""

    def __init__(self, directory, compress_level=7, **kwargs):
        self.compress_level = compress_level
        super().__init__(directory, **kwargs)

    def put(self, key):
        data = gzip.compress(key, compresslevel=self.compress_level)
        return super().put(data)

    def get(self, key, raw):
        data = super().get(key, raw)
        return gzip.decompress(data)

    async def store(self, value, read, key=UNKNOWN):
        if not read:
            value = gzip.compress(value, compresslevel=self.compress_level)
        return await super().store(value, read)

    async def fetch(self, mode, filename, value, read):
        data = await super().fetch(mode, filename, value, read)
        if not read:
            data = gzip.decompress(data)
        return data
