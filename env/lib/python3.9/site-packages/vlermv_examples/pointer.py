'''
Here is an example of extending a Vlermv. PointerVlermv stores everything
as symlinks. Maybe this is useful if you want to store the same file under
several names, but I wouldn't use it; just see it as an example of how to extend
Vlermv.
'''

import os
import hashlib
import io
from functools import lru_cache
from . import serializers, transformers
from ._fs import Vlermv
from ._abstract import AbstractVlermv

class PointerVlermv(Vlermv):
    '''
    A :py:class:`dict` API to a filesystem
    '''
    def __init__(self, *directory, masterdir='.master', maxhashcachesize=1000,
                 **kwargs):
        '''
        :param str directory: Top-level directory of the vlermv
        :param serializer: A thing with dump and load functions for
            serializing and deserializing Python objects,
            like :py:mod:`json`, :py:mod:`yaml`, or
            anything in :py:mod:`vlermv.serializers`
        :type serializer: :py:mod:`serializer <vlermv.serializers>`
        :param transformer: A thing with to_path and from_path functions
            for transforming keys to file paths and back.
            Several are available in :py:mod:`vlermvtransformers`.
        :type transformer: :py:mod:`transformer <vlermvtransformers>`
        :param bool mutable: Whether values can be updated and deleted
        :param str masterdir: Subdirectory inside of base_directory to  use for data files
        :param int maxhashcachesize: Passed to :py:func:`functools.lru_cache`
            for caching the results of the hash function during setitem

        These are mostly relevant for initialization via :py:func:`PointerVlermv.memoize`.

        :param bool appendable: Whether new values can be added to the Vlermv
            (Set this to False to ensure that the decorated function is never
            run and that the all results are cached; this is useful for reviewing
            old data in a read-only mode.)
        :param bool cache_exceptions: If the decorated function raises
            an exception, should the failure and exception be cached?
            The exception is raised either way.
        :raises TypeError: If cache_exceptions is True but the serializer
            can't cache exceptions
        '''
        AbstractVlermv.__init__(self, **kwargs)
        self.base_directory = os.path.join(*directory)

        # Reusing tempdir is sloppy
        self.tempdir = os.path.join(*(directory +(masterdir,)))
        self.v = Vlermv(*(directory + (masterdir,)),
            transformer=transformers.simple,
            serializer=serializers.identity_mmap_bytes,
            mutable=False, appendable=True)

        @lru_cache(maxhashcachesize)
        def _hash(x):
            return hashlib.sha256(x).hexdigest()
        self.hash=_hash

    def __setitem__(self, index, obj):
        AbstractVlermv.__setitem__(self, index, obj)
        with io.BytesIO() as fp:
            self.serializer.dump(obj, fp)
            key = self.hash(fp.getvalue())
            self.v[key] = fp.getvalue()
        src = self.v.filename(key)
        dst = self.filename(index)
        os.makedirs(os.path.dirname(dst), exist_ok = True)
        os.symlink(src, dst)

    def vacuum(self):
        '''
        Free orphans.
        '''
        raise NotImplementedError
