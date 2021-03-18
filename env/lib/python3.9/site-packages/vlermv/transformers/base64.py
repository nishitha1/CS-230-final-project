from base64 import b64encode, b64decode

def to_path(path):
    if isinstance(path, tuple) and len(path) == 1:
        x = path[0].encode('utf-8')
    elif isinstance(path, str):
        x = path.encode('utf-8')
    elif isinstance(path, bytes):
        x = path
    else:
        raise ValueError('Bad path: %s' % path)

    return (b64encode(x).decode('ascii'),)

def from_path(key):
    if len(key) != 1:
        raise ValueError('Key must have exactly one element.')
    return b64decode(key[0]).decode('ascii')
