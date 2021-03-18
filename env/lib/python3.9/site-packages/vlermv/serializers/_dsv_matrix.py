from logging import getLogger
from io import StringIO
from csv import reader, writer

logger = getLogger(__name__)

class dsv_matrix(object):
    def __init__(self, header=None, **kwargs):
        self.header = tuple(header)
        self.kwargs = kwargs

    def loads(self, text):
        return tuple(self.iterloads(text))
    def iterloads(self, text):
        fp = StringIO(text)
        r = reader(fp, **self.kwargs)

        if self.header:
            if not self.header == tuple(next(r)):
                msg = 'Headers do not match between %s.header and serialized data.'
                logger.warn(msg % self.__name__)
            ncol = len(self.header)
        else:
            ncol = None

        for row in r:
            if ncol == None:
                ncol = len(row)
            if len(row) != ncol:
                raise ValueError('Inconsistent column count')
            yield row

    def dumps(self, obj):
        fp = StringIO()
        w = writer(fp, **self.kwargs)

        if self.header:
            ncol = len(self.header)
            w.writerow(self.header)
        else:
            ncol = None

        for _row in obj:
            row = tuple(_row)
            if ncol == None:
                ncol = len(row)
            if len(row) != ncol:
                raise ValueError('Inconsistent column count')
            if not all(isinstance(cell, str) for cell in row):
                raise TypeError('All cells must be str type.')
            w.writerow(row)

        return fp.getvalue()
