import os
from random import randint
from string import ascii_letters
from abc import ABCMeta
from functools import partial

from ._exceptions import DeleteError, PermissionError, out_of_space
from ._abstract import AbstractVlermv
from ._exceptions import OpenError

def _rmdirs(base, leaf):
    for fn in _reversed_directories(base, leaf):
        if os.listdir(fn) == []:
            os.rmdir(fn)
        else:
            break

def _get_fn(fn, mode, load):
    '''
    Load a contents, checking that the file was not modified during the read.
    '''
    try:
        mtime_before = os.path.getmtime(fn)
    except OSError:
        mtime_before = None

    try:
        with open(fn, mode) as fp:
            item = load(fp)
    except OpenError:
        raise
    else:
        mtime_after = os.path.getmtime(fn)
        if mtime_before in {None, mtime_after}:
            return item
        else:
            raise EnvironmentError('File was edited during read: %s' % fn)


def _random_file_name():
    n = len(ascii_letters) - 1
    return ''.join(ascii_letters[randint(0, n)] for _ in range(10))

def _mkdirp(x):
    try:
        os.makedirs(x, exist_ok=True)
    except TypeError:
        # Python 2
        if not os.path.isdir(x):
            os.makedirs(x)

def _mktemp(tempdir, filename=_random_file_name):
    _mkdirp(tempdir)
    return os.path.join(tempdir, filename())

def _reversed_directories(outer, inner):
    while outer != inner:
        yield inner
        inner = os.path.dirname(inner)

class Vlermv(AbstractVlermv):
    '''
    A :py:class:`dict` API to a filesystem
    '''
    class LinkWrapper(metaclass=ABCMeta):
        '''
        This is a Wrapper for hard and symbolic links. Assign a key to an instance of
        Symlink to create a symlink from the key to the file referenced by the SymLink.

        :param key: Vlermv key to link to
        :returns: Object to be assigned as the value of different key in the
            same Vlermv
        '''
        def __init__(self, key):
            self.key = key
    class Link(LinkWrapper):
        func = os.link
    class Symlink(LinkWrapper):
        func = os.symlink

    def __init__(self, *directory, indexfile='.index', tempdir='.tmp', **kwargs):
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
        :param str tempdir: Subdirectory inside of base_directory to use for temporary files
        :param str indexfile: Unless this value is None, vlermv will allow you
            to to access your filesystem as if a file at any particular path can
            both reference files (like a directory) and contain data (like a
            regular file). When a file needs to be both a directory and a
            regular file, vlermv will create the directory and save the file
            contents in a file with this name (default is ".index") as a child
            of the directory.

        These are mostly relevant for initialization via :py:func:`Vlermv.memoize`.

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

        # For copying
        self._args = directory
        self._kwargs = dict(kwargs)
        self._kwargs['indexfile'] = indexfile
        self._kwargs['tempdir'] = tempdir

        super(Vlermv, self).__init__(**kwargs)
        if not directory:
            raise TypeError('Specify a directory for a vlermv.')
        self.base_directory = _base_directory(*directory)
        self.tempdir = tempdir
        self.indexfile = indexfile

    @property
    def allow_empty(self):
        return self.indexfile not in {'', None}

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.base_directory))

    def filename(self, key):
        self._check_path(key)
        return super(Vlermv, self).filename(key)

    def from_filename(self, filename):
        try:
            key = super(Vlermv, self).from_filename(filename)
            self._check_path(key)
        except KeyError:
            pass
        else:
            return key

    def _check_path(self, key):
        path = self.transformer.to_path(key)
        if self.tempdir in path:
            raise KeyError('Reserved for temporary files: %s' % repr(key))
        elif self.indexfile in path:
            raise KeyError('Reserved for index files: %s' % repr(key))
        return path
    
    def _make_index(self, fn):
        this = fn
        while True:
            init, last = os.path.split(this)
            if init == self.base_directory:
                break
            elif os.path.isfile(this):
                fn = os.path.join(this, self.indexfile)

                tmp = _mktemp(os.path.join(init, self.tempdir))
                os.rename(this, tmp)
                _mkdirp(this)
                os.rename(tmp, fn)
                return fn
            else:
                this = init

    def __setitem__(self, index, obj):
        super(Vlermv, self).__setitem__(index, obj)
        fn = self.filename(index)
        if os.path.isdir(fn):
            self._make_index(fn)
            fn = os.path.join(fn, self.indexfile)
        exists = os.path.exists(fn)

        if (not self.mutable) and exists:
            raise PermissionError('This vlermv is immutable, and %s already exists.' % fn)
        elif (not self.appendable) and (not exists):
            raise PermissionError('This vlermv not appendable, and %s does not exist.' % fn)
        elif isinstance(obj, self.LinkWrapper):
            src = self.filename(obj.key)
            dst = self.filename(index)
            os.makedirs(os.path.dirname(dst), exist_ok = True)
            obj.func(src, dst)
        else:
            self._make_index(fn)
            try:
                tmp = _mktemp(os.path.join(os.path.dirname(fn), self.tempdir))
            except NotADirectoryError:
                raise NotImplementedError('The path probably includes an ancestor file that needs to be converted to a directory with indexfile. This conversion is not yet performed automatically.')
            with open(tmp, 'w+' + self._b()) as fp:
                try:
                    self.serializer.dump(obj, fp)
                except Exception as e:
                    if out_of_space(e):
                        fp.close()
                        os.remove(tmp)
                        raise BufferError('Out of space')
                    else:
                        raise
            os.rename(tmp, fn)

    def __getitem__(self, index):
        fn = self.filename(index)
        if os.path.isdir(fn):
            fn = os.path.join(fn, self.indexfile)

        try:
            return _get_fn(fn, 'r+' + self._b(), self.serializer.load)
        except OpenError:
            raise KeyError(index)

    def __delitem__(self, index):
        super(Vlermv, self).__delitem__(index)
        fn = self.filename(index)
        try:
            os.remove(fn)
        except DeleteError as e:
            raise KeyError(*e.args)
        else:
            _rmdirs(self.base_directory, os.path.dirname(fn))

    def __len__(self):
        length = 0
        for dirpath, _, filenames in os.walk(self.base_directory):
            for filename in filenames:
                length += 1
        return length

    def keys(self):
        for filename in self.files():
            key = self.from_filename(filename)
            if key != None:
                yield key

    def ignored(self):
        for filename in self.files():
            if self.from_filename(filename) == None:
                yield filename

    def files(self):
        xs = os.walk(self.base_directory, topdown=True, followlinks=True)
        for dirpath, dirnames, filenames in xs:
            if os.path.basename(dirpath) in {self.tempdir, self.indexfile}:
                dirnames[:] = []
            else:
                for filename in filenames:
                    if filename == self.indexfile:
                        path = dirpath
                    else:
                        path = os.path.join(dirpath, filename)
                    yield path

    def rename(self, old, new):
        '''
        Rename a key/file atomically with :py:func:`os.rename`.

        :param old: Old key
        :param new: New key
        '''
        oldfn, newfn = self.filename(old), self.filename(new)
        if os.path.isdir(newfn):
            newfn = os.path.join(newfn, self.indexfile)
        _mkdirp(os.path.dirname(newfn))
        os.rename(oldfn, newfn)
        _rmdirs(self.base_directory, os.path.dirname(newfn))

def _base_directory(*directory):
    return os.path.expanduser(os.path.join(*directory))
