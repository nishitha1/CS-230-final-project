import datetime
from collections import Mapping

from .transformers import datetimes

now = datetime.datetime.now

class extended_transformer(object):
    def __init__(self, base, **kwargs):
        self.base = base
        self.d = datetimes(**kwargs)
    def from_path(self, path):
        base_path = path[:1]
        datetime_path = path[-1]
        return self.base.from_path(base_path), self.d.from_path((datetime_path,))
    def to_path(self, key):
        base_key, datetime_key = key
        return tuple(self.base.to_path(base_key)) + self.d.to_path(datetime_key)

def extend_transformer(base, **kwargs):
    extended = extended_transformer(base, **kwargs)
    extended.__name__ = 'versioned_%s' % base.__name__
    return extended

class versioned(Mapping):
    def __init__(self, vlermv, **kwargs):
        '''
        Create an append-only version log, without compression.

        This can be used as a decorator ::

            @versioned
            class MyVlermv(Vlermv):
                serializer = json

        or as a normal function. ::
        
            versioned(Vlermv('/tmp'))

        The decorators can be stacked, of course. ::

            @Vlermv.from_attributes
            @versioned
            class my_vlermv(Vlermv):
                serializer = json
        '''
        vlermv.transformer = extend_transformer(vlermv.transformer, **kwargs)
        vlermv.mutable = False
        self.vlermv = vlermv
        
    def append(self, key, value, as_of=None):
        self.vlermv[(key, as_of or now())] = value
        
    def get(self, key, default=None, as_of=None, _raise_on_missing=False):
        v = self.history(key)

        for date in sorted(v, reverse=True):
            if as_of == None or date < as_of:
                return date

        if _raise_on_missing:
            raise KeyError(key)
        else:
            return default

    def __getitem__(self, key):
        return self.get(key, _raise_on_missing=True)
    def __iter__(self):
        for key in self.vlermv:
            yield key[:-1]

    def history(self, key):
        '''
        Change directory to one key, and see the version history.
        '''
        sub = other = self.__copy__()
        sub.prefix = self.prefix + self.transformer.base.to_path(key)
        sub.transformer = self.transformer.d
        return sub
