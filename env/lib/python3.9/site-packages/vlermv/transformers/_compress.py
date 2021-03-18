import importlib
from . import base64

class compress:
    def __init__(self, compressor):
        self.c = importlib.import_module(compressor)

    def to_path(self, key):
        if isinstance(key, tuple):
            key = key[0]
        return base64.to_path(self.c.compress(key.encode('utf-8')))

    def from_path(self, obj):
        return self.c.decompress(base64.from_path(obj)).decode('utf-8')

zlib = compress('zlib')
gzip = compress('gzip')
bz2 = compress('bz2')
