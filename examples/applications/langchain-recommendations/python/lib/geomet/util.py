#  Copyright 2013 Lars Butler & individual contributors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import itertools
import six
if six.PY2:
    import collections
else:
    import collections.abc as collections


def block_splitter(data, block_size):
    """
    Creates a generator by slicing ``data`` into chunks of ``block_size``.

    >>> data = range(10)
    >>> list(block_splitter(data, 2))
    [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]

    If ``data`` cannot be evenly divided by ``block_size``, the last block will
    simply be the remainder of the data. Example:

    >>> data = range(10)
    >>> list(block_splitter(data, 3))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    If the ``block_size`` is greater than the total length of ``data``, a
    single block will be generated:

    >>> data = range(3)
    >>> list(block_splitter(data, 4))
    [[0, 1, 2]]

    :param data:
        Any iterable. If ``data`` is a generator, it will be exhausted,
        obviously.
    :param int block_site:
        Desired (maximum) block size.
    """
    buf = []
    for i, datum in enumerate(data):
        buf.append(datum)
        if len(buf) == block_size:
            yield buf
            buf = []

    # If there's anything leftover (a partial block),
    # yield it as well.
    if buf:
        yield buf


def take(n, iterable):
    """
    Return first n items of the iterable as a list

    Copied shamelessly from
    http://docs.python.org/2/library/itertools.html#recipes.
    """
    return list(itertools.islice(iterable, n))


def as_bin_str(a_list):
    if six.PY2:
        return b''.join(a_list)
    else:
        return bytes(a_list)


def round_geom(geom, precision=None):
    """Round coordinates of a geometric object to given precision."""
    if geom['type'] == 'Point':
        x, y = geom['coordinates']
        xp, yp = [x], [y]
        if precision is not None:
            xp = [round(v, precision) for v in xp]
            yp = [round(v, precision) for v in yp]
        new_coords = tuple(zip(xp, yp))[0]
    if geom['type'] in ['LineString', 'MultiPoint']:
        xp, yp = zip(*geom['coordinates'])
        if precision is not None:
            xp = [round(v, precision) for v in xp]
            yp = [round(v, precision) for v in yp]
        new_coords = tuple(zip(xp, yp))
    elif geom['type'] in ['Polygon', 'MultiLineString']:
        new_coords = []
        for piece in geom['coordinates']:
            xp, yp = zip(*piece)
            if precision is not None:
                xp = [round(v, precision) for v in xp]
                yp = [round(v, precision) for v in yp]
            new_coords.append(tuple(zip(xp, yp)))
    elif geom['type'] == 'MultiPolygon':
        parts = geom['coordinates']
        new_coords = []
        for part in parts:
            inner_coords = []
            for ring in part:
                xp, yp = zip(*ring)
                if precision is not None:
                    xp = [round(v, precision) for v in xp]
                    yp = [round(v, precision) for v in yp]
                inner_coords.append(tuple(zip(xp, yp)))
            new_coords.append(inner_coords)
    return {'type': geom['type'], 'coordinates': new_coords}


def flatten_multi_dim(sequence):
    """Flatten a multi-dimensional array-like to a single dimensional sequence
    (as a generator).
    """
    for x in sequence:
        if (isinstance(x, collections.Iterable)
                and not isinstance(x, six.string_types)):
            for y in flatten_multi_dim(x):
                yield y
        else:
            yield x
