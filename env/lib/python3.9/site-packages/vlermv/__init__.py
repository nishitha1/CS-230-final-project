# Vlermv backends
from ._fs import Vlermv
try:
    import boto
except ImportError:
    pass
else:
    del(boto)
    from ._s3 import S3Vlermv

# Helpers
from . import serializers, transformers
from ._versioned import versioned

# For backwards compatibility
cache = Vlermv.memoize
