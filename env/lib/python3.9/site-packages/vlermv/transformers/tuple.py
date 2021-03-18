def to_path(key):
    ys = tuple(key)
    if not all(isinstance(y, str) for y in ys):
        raise TypeError('All key elements must be str.')
    return tuple(map(str, ys))

def from_path(x):
    return x
