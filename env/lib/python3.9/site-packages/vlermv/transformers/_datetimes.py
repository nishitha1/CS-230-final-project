import datetime

class datetimes(object):
    def __init__(self, format='%Y-%m-%dT%H:%M:%S'):
        self.format = format
    def from_path(self, path):
        assert len(path) == 1
        d, = path
        return datetime.datetime.strptime(d, self.format)
    def to_path(self, d):
        return d.strftime(self.format),
