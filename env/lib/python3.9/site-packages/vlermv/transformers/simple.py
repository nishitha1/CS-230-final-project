import os

error_msg = '''The default transformer cannot handle slashes (subdirectories);
try another transformer in vlermv.transformers.'''

def to_path(key):
    if isinstance(key, tuple):
        if len(key) == 1:
            key = key[0]
        else:
            raise ValueError(error_msg)

    if '/' in key or '\\' in key or os.path.sep in key:
        raise ValueError(error_msg)

    return (key,)

def from_path(path):
    if len(path) != 1:
        raise ValueError(error_msg)
    return path[0]
