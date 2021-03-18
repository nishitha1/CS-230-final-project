import hashlib

class hashtransform:
    '''
    This works only in one direction; you can hash the filename,
    but you can't turn the hash back into a filename.
    '''
    def __init__(self, func):
        self.func = func

    def to_path(self, key):
        return self.func(key.encode('utf-8')).hexdigest(),

    def from_path(self, obj):
        return obj[0]

md5 = hashtransform(hashlib.md5)
sha1 = hashtransform(hashlib.sha1)
sha224 = hashtransform(hashlib.sha224)
sha256 = hashtransform(hashlib.sha256)
sha384 = hashtransform(hashlib.sha384)
sha512 = hashtransform(hashlib.sha512)
