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
import binascii
import six
import struct

from geomet.util import block_splitter
from geomet.util import take
from geomet.util import as_bin_str
from geomet.util import flatten_multi_dim
from itertools import chain

#: '\x00': The first byte of any WKB string. Indicates big endian byte
#: ordering for the data.
BIG_ENDIAN = b'\x00'
#: '\x01': The first byte of any WKB string. Indicates little endian byte
#: ordering for the data.
LITTLE_ENDIAN = b'\x01'
#: High byte in a 4-byte geometry type field to indicate that a 4-byte SRID
#: field follows.
SRID_FLAG = b'\x20'

#: Mapping of GeoJSON geometry types to the "2D" 4-byte binary string
#: representation for WKB. "2D" indicates that the geometry is 2-dimensional,
#: X and Y components.
#: NOTE: Byte ordering is big endian.
WKB_2D = {
    'Point': b'\x00\x00\x00\x01',
    'LineString': b'\x00\x00\x00\x02',
    'Polygon': b'\x00\x00\x00\x03',
    'MultiPoint': b'\x00\x00\x00\x04',
    'MultiLineString': b'\x00\x00\x00\x05',
    'MultiPolygon': b'\x00\x00\x00\x06',
    'GeometryCollection': b'\x00\x00\x00\x07',
}

#: Mapping of GeoJSON geometry types to the "Z" 4-byte binary string
#: representation for WKB. "Z" indicates that the geometry is 3-dimensional,
#: with X, Y, and Z components.
#: NOTE: Byte ordering is big endian.
WKB_Z = {
    'Point': b'\x00\x00\x03\xe9',
    'LineString': b'\x00\x00\x03\xea',
    'Polygon': b'\x00\x00\x03\xeb',
    'MultiPoint': b'\x00\x00\x03\xec',
    'MultiLineString': b'\x00\x00\x03\xed',
    'MultiPolygon': b'\x00\x00\x03\xee',
    'GeometryCollection': b'\x00\x00\x03\xef',
}

#: Mapping of GeoJSON geometry types to the "M" 4-byte binary string
#: representation for WKB. "M" indicates that the geometry is 2-dimensional,
#: with X, Y, and M ("Measure") components.
#: NOTE: Byte ordering is big endian.
WKB_M = {
    'Point': b'\x00\x00\x07\xd1',
    'LineString': b'\x00\x00\x07\xd2',
    'Polygon': b'\x00\x00\x07\xd3',
    'MultiPoint': b'\x00\x00\x07\xd4',
    'MultiLineString': b'\x00\x00\x07\xd5',
    'MultiPolygon': b'\x00\x00\x07\xd6',
    'GeometryCollection': b'\x00\x00\x07\xd7',
}

#: Mapping of GeoJSON geometry types to the "ZM" 4-byte binary string
#: representation for WKB. "ZM" indicates that the geometry is 4-dimensional,
#: with X, Y, Z, and M ("Measure") components.
#: NOTE: Byte ordering is big endian.
WKB_ZM = {
    'Point': b'\x00\x00\x0b\xb9',
    'LineString': b'\x00\x00\x0b\xba',
    'Polygon': b'\x00\x00\x0b\xbb',
    'MultiPoint': b'\x00\x00\x0b\xbc',
    'MultiLineString': b'\x00\x00\x0b\xbd',
    'MultiPolygon': b'\x00\x00\x0b\xbe',
    'GeometryCollection': b'\x00\x00\x0b\xbf',
}

#: Mapping of dimension types to maps of GeoJSON geometry type -> 4-byte binary
#: string representation for WKB.
_WKB = {
    '2D': WKB_2D,
    'Z': WKB_Z,
    'M': WKB_M,
    'ZM': WKB_ZM,
}

#: Mapping from binary geometry type (as a 4-byte binary string) to GeoJSON
#: geometry type.
#: NOTE: Byte ordering is big endian.
_BINARY_TO_GEOM_TYPE = dict(
    chain(*((reversed(x) for x in wkb_map.items())
            for wkb_map in _WKB.values()))
)

_INT_TO_DIM_LABEL = {2: '2D', 3: 'Z', 4: 'ZM'}


def _get_geom_type(type_bytes):
    """Get the GeoJSON geometry type label from a WKB type byte string.

    :param type_bytes:
        4 byte string in big endian byte order containing a WKB type number.
        It may also contain a "has SRID" flag in the high byte (the first type,
        since this is big endian byte order), indicated as 0x20. If the SRID
        flag is not set, the high byte will always be null (0x00).
    :returns:
        3-tuple ofGeoJSON geometry type label, the bytes resprenting the
        geometry type, and a separate "has SRID" flag. If the input
        `type_bytes` contains an SRID flag, it will be removed.

        >>> # Z Point, with SRID flag
        >>> _get_geom_type(b'\\x20\\x00\\x03\\xe9') == (
        ... 'Point', b'\\x00\\x00\\x03\\xe9', True)
        True

        >>> # 2D MultiLineString, without SRID flag
        >>> _get_geom_type(b'\\x00\\x00\\x00\\x05') == (
        ... 'MultiLineString', b'\\x00\\x00\\x00\\x05', False)
        True

    """
    # slice off the high byte, which may contain the SRID flag
    high_byte = type_bytes[0]
    if six.PY3:
        high_byte = bytes([high_byte])
    has_srid = high_byte == b'\x20'
    if has_srid:
        # replace the high byte with a null byte
        type_bytes = as_bin_str(b'\x00' + type_bytes[1:])
    else:
        type_bytes = as_bin_str(type_bytes)

    # look up the geometry type
    geom_type = _BINARY_TO_GEOM_TYPE.get(type_bytes)
    return geom_type, type_bytes, has_srid


def dump(obj, dest_file):
    """
    Dump GeoJSON-like `dict` to WKB and write it to the `dest_file`.

    :param dict obj:
        A GeoJSON-like dictionary. It must at least the keys 'type' and
        'coordinates'.
    :param dest_file:
        Open and writable file-like object.
    """
    dest_file.write(dumps(obj))


def load(source_file):
    """
    Load a GeoJSON `dict` object from a ``source_file`` containing WKB (as a
    byte string).

    :param source_file:
        Open and readable file-like object.

    :returns:
        A GeoJSON `dict` representing the geometry read from the file.
    """
    return loads(source_file.read())


def dumps(obj, big_endian=True):
    """
    Dump a GeoJSON-like `dict` to a WKB string.

    .. note::
        The dimensions of the generated WKB will be inferred from the first
        vertex in the GeoJSON `coordinates`. It will be assumed that all
        vertices are uniform. There are 4 types:

        - 2D (X, Y): 2-dimensional geometry
        - Z (X, Y, Z): 3-dimensional geometry
        - M (X, Y, M): 2-dimensional geometry with a "Measure"
        - ZM (X, Y, Z, M): 3-dimensional geometry with a "Measure"

        If the first vertex contains 2 values, we assume a 2D geometry.
        If the first vertex contains 3 values, this is slightly ambiguous and
        so the most common case is chosen: Z.
        If the first vertex contains 4 values, we assume a ZM geometry.

        The WKT/WKB standards provide a way of differentiating normal (2D), Z,
        M, and ZM geometries (http://en.wikipedia.org/wiki/Well-known_text),
        but the GeoJSON spec does not. Therefore, for the sake of interface
        simplicity, we assume that geometry that looks 3D contains XYZ
        components, instead of XYM.

        If the coordinates list has no coordinate values (this includes nested
        lists, for example, `[[[[],[]], []]]`, the geometry is considered to be
        empty. Geometries, with the exception of points, have a reasonable
        "empty" representation in WKB; however, without knowing the number of
        coordinate values per vertex, the type is ambigious, and thus we don't
        know if the geometry type is 2D, Z, M, or ZM. Therefore in this case
        we expect a `ValueError` to be raised.

    :param dict obj:
        GeoJson-like `dict` object.
    :param bool big_endian:
        Defaults to `True`. If `True`, data values in the generated WKB will
        be represented using big endian byte order. Else, little endian.

    TODO: remove this

    :param str dims:
        Indicates to WKB representation desired from converting the given
        GeoJSON `dict` ``obj``. The accepted values are:

        * '2D': 2-dimensional geometry (X, Y)
        * 'Z': 3-dimensional geometry (X, Y, Z)
        * 'M': 3-dimensional geometry (X, Y, M)
        * 'ZM': 4-dimensional geometry (X, Y, Z, M)

    :returns:
        A WKB binary string representing of the ``obj``.
    """
    geom_type = obj['type']
    meta = obj.get('meta', {})

    exporter = _dumps_registry.get(geom_type)
    if exporter is None:
        _unsupported_geom_type(geom_type)

    # Check for empty geometries. GeometryCollections have a slightly different
    # JSON/dict structure, but that's handled.
    coords_or_geoms = obj.get('coordinates', obj.get('geometries'))
    if len(list(flatten_multi_dim(coords_or_geoms))) == 0:
        raise ValueError(
            'Empty geometries cannot be represented in WKB. Reason: The '
            'dimensionality of the WKB would be ambiguous.'
        )

    return exporter(obj, big_endian, meta)


def loads(string):
    """
    Construct a GeoJSON `dict` from WKB (`string`).

    The resulting GeoJSON `dict` will include the SRID as an integer in the
    `meta` object. This was an arbitrary decision made by `geomet, the
    discussion of which took place here:
    https://github.com/geomet/geomet/issues/28.

    In order to be consistent with other libraries [1] and (deprecated)
    specifications [2], also include the same information in a `crs`
    object. This isn't ideal, but the `crs` member is no longer part of
    the GeoJSON standard, according to RFC7946 [3]. However, it's still
    useful to include this information in GeoJSON payloads because it
    supports conversion to EWKT/EWKB (which are canonical formats used by
    PostGIS and the like).

    Example:

        {'type': 'Point',
         'coordinates': [0.0, 1.0],
         'meta': {'srid': 4326},
         'crs': {'type': 'name', 'properties': {'name': 'EPSG4326'}}}

    NOTE(larsbutler): I'm not sure if it's valid to just prefix EPSG
    (European Petroluem Survey Group) to an SRID like this, but we'll
    stick with it for now until it becomes a problem.

    NOTE(larsbutler): Ideally, we should use URNs instead of this
    notation, according to the new GeoJSON spec [4]. However, in
    order to be consistent with [1], we'll stick with this approach
    for now.

    References:

    [1] - https://github.com/bryanjos/geo/issues/76
    [2] - http://geojson.org/geojson-spec.html#coordinate-reference-system-objects
    [3] - https://tools.ietf.org/html/rfc7946#appendix-B.1
    [4] - https://tools.ietf.org/html/rfc7946#section-4
    """  # noqa
    string = iter(string)
    # endianness = string[0:1]
    endianness = as_bin_str(take(1, string))
    if endianness == BIG_ENDIAN:
        big_endian = True
    elif endianness == LITTLE_ENDIAN:
        big_endian = False
    else:
        raise ValueError("Invalid endian byte: '0x%s'. Expected 0x00 or 0x01"
                         % binascii.hexlify(endianness.encode()).decode())

    endian_token = '>' if big_endian else '<'
    # type_bytes = string[1:5]
    type_bytes = as_bin_str(take(4, string))
    if not big_endian:
        # To identify the type, order the type bytes in big endian:
        type_bytes = type_bytes[::-1]

    geom_type, type_bytes, has_srid = _get_geom_type(type_bytes)
    srid = None
    if has_srid:
        srid_field = as_bin_str(take(4, string))
        [srid] = struct.unpack('%si' % endian_token, srid_field)

    # data_bytes = string[5:]  # FIXME: This won't work for GeometryCollections
    data_bytes = string

    importer = _loads_registry.get(geom_type)

    if importer is None:
        _unsupported_geom_type(geom_type)

    data_bytes = iter(data_bytes)
    result = importer(big_endian, type_bytes, data_bytes)
    if has_srid:
        # As mentioned in the docstring above, include both approaches to
        # indicating the SRID.
        result['meta'] = {'srid': int(srid)}
        result['crs'] = {
            'type': 'name',
            'properties': {'name': 'EPSG%s' % srid},
        }
    return result


def _unsupported_geom_type(geom_type):
    raise ValueError("Unsupported geometry type '%s'" % geom_type)


# TODO: dont default meta to none
def _header_bytefmt_byteorder(geom_type, num_dims, big_endian, meta=None):
    """
    Utility function to get the WKB header (endian byte + type header), byte
    format string, and byte order string.
    """
    dim = _INT_TO_DIM_LABEL.get(num_dims)
    if dim is None:
        pass  # TODO: raise

    type_byte_str = _WKB[dim][geom_type]
    srid = meta.get('srid')
    if srid is not None:
        # Add the srid flag
        type_byte_str = SRID_FLAG + type_byte_str[1:]

    if big_endian:
        header = BIG_ENDIAN
        byte_fmt = b'>'
        byte_order = '>'
    else:
        header = LITTLE_ENDIAN
        byte_fmt = b'<'
        byte_order = '<'
        # reverse the byte ordering for little endian
        type_byte_str = type_byte_str[::-1]

    header += type_byte_str
    if srid is not None:
        srid = int(srid)

        if big_endian:
            srid_header = struct.pack('>i', srid)
        else:
            srid_header = struct.pack('<i', srid)
        header += srid_header
    byte_fmt += b'd' * num_dims

    return header, byte_fmt, byte_order


def _dump_point(obj, big_endian, meta):
    """
    Dump a GeoJSON-like `dict` to a point WKB string.

    :param dict obj:
        GeoJson-like `dict` object.
    :param bool big_endian:
        If `True`, data values in the generated WKB will be represented using
        big endian byte order. Else, little endian.
    :param dict meta:
        Metadata associated with the GeoJSON object. Currently supported
        metadata:

        - srid: Used to support EWKT/EWKB. For example, ``meta`` equal to
          ``{'srid': '4326'}`` indicates that the geometry is defined using
          Extended WKT/WKB and that it bears a Spatial Reference System
          Identifier of 4326. This ID will be encoded into the resulting
          binary.

        Any other meta data objects will simply be ignored by this function.

    :returns:
        A WKB binary string representing of the Point ``obj``.
    """
    coords = obj['coordinates']
    num_dims = len(coords)

    wkb_string, byte_fmt, _ = _header_bytefmt_byteorder(
        'Point', num_dims, big_endian, meta
    )

    wkb_string += struct.pack(byte_fmt, *coords)
    return wkb_string


def _dump_linestring(obj, big_endian, meta):
    """
    Dump a GeoJSON-like `dict` to a linestring WKB string.

    Input parameters and output are similar to :func:`_dump_point`.
    """
    coords = obj['coordinates']
    vertex = coords[0]
    # Infer the number of dimensions from the first vertex
    num_dims = len(vertex)

    wkb_string, byte_fmt, byte_order = _header_bytefmt_byteorder(
        'LineString', num_dims, big_endian, meta
    )
    # append number of vertices in linestring
    wkb_string += struct.pack('%sl' % byte_order, len(coords))

    for vertex in coords:
        wkb_string += struct.pack(byte_fmt, *vertex)

    return wkb_string


def _dump_polygon(obj, big_endian, meta):
    """
    Dump a GeoJSON-like `dict` to a polygon WKB string.

    Input parameters and output are similar to :funct:`_dump_point`.
    """
    coords = obj['coordinates']
    vertex = coords[0][0]
    # Infer the number of dimensions from the first vertex
    num_dims = len(vertex)

    wkb_string, byte_fmt, byte_order = _header_bytefmt_byteorder(
        'Polygon', num_dims, big_endian, meta
    )

    # number of rings:
    wkb_string += struct.pack('%sl' % byte_order, len(coords))
    for ring in coords:
        # number of verts in this ring:
        wkb_string += struct.pack('%sl' % byte_order, len(ring))
        for vertex in ring:
            wkb_string += struct.pack(byte_fmt, *vertex)

    return wkb_string


def _dump_multipoint(obj, big_endian, meta):
    """
    Dump a GeoJSON-like `dict` to a multipoint WKB string.

    Input parameters and output are similar to :funct:`_dump_point`.
    """
    coords = obj['coordinates']
    vertex = coords[0]
    num_dims = len(vertex)

    wkb_string, byte_fmt, byte_order = _header_bytefmt_byteorder(
        'MultiPoint', num_dims, big_endian, meta
    )

    point_type = _WKB[_INT_TO_DIM_LABEL.get(num_dims)]['Point']
    if big_endian:
        point_type = BIG_ENDIAN + point_type
    else:
        point_type = LITTLE_ENDIAN + point_type[::-1]

    wkb_string += struct.pack('%sl' % byte_order, len(coords))
    for vertex in coords:
        # POINT type strings
        wkb_string += point_type
        wkb_string += struct.pack(byte_fmt, *vertex)

    return wkb_string


def _dump_multilinestring(obj, big_endian, meta):
    """
    Dump a GeoJSON-like `dict` to a multilinestring WKB string.

    Input parameters and output are similar to :funct:`_dump_point`.
    """
    coords = obj['coordinates']
    vertex = coords[0][0]
    num_dims = len(vertex)

    wkb_string, byte_fmt, byte_order = _header_bytefmt_byteorder(
        'MultiLineString', num_dims, big_endian, meta
    )

    ls_type = _WKB[_INT_TO_DIM_LABEL.get(num_dims)]['LineString']
    if big_endian:
        ls_type = BIG_ENDIAN + ls_type
    else:
        ls_type = LITTLE_ENDIAN + ls_type[::-1]

    # append the number of linestrings
    wkb_string += struct.pack('%sl' % byte_order, len(coords))

    for linestring in coords:
        wkb_string += ls_type
        # append the number of vertices in each linestring
        wkb_string += struct.pack('%sl' % byte_order, len(linestring))
        for vertex in linestring:
            wkb_string += struct.pack(byte_fmt, *vertex)

    return wkb_string


def _dump_multipolygon(obj, big_endian, meta):
    """
    Dump a GeoJSON-like `dict` to a multipolygon WKB string.

    Input parameters and output are similar to :funct:`_dump_point`.
    """
    coords = obj['coordinates']
    vertex = coords[0][0][0]
    num_dims = len(vertex)

    wkb_string, byte_fmt, byte_order = _header_bytefmt_byteorder(
        'MultiPolygon', num_dims, big_endian, meta
    )

    poly_type = _WKB[_INT_TO_DIM_LABEL.get(num_dims)]['Polygon']
    if big_endian:
        poly_type = BIG_ENDIAN + poly_type
    else:
        poly_type = LITTLE_ENDIAN + poly_type[::-1]

    # apped the number of polygons
    wkb_string += struct.pack('%sl' % byte_order, len(coords))

    for polygon in coords:
        # append polygon header
        wkb_string += poly_type
        # append the number of rings in this polygon
        wkb_string += struct.pack('%sl' % byte_order, len(polygon))
        for ring in polygon:
            # append the number of vertices in this ring
            wkb_string += struct.pack('%sl' % byte_order, len(ring))
            for vertex in ring:
                wkb_string += struct.pack(byte_fmt, *vertex)

    return wkb_string


def _dump_geometrycollection(obj, big_endian, meta):
    # TODO: handle empty collections
    geoms = obj['geometries']
    # determine the dimensionality (2d, 3d, 4d) of the collection
    # by sampling the first geometry
    first_geom = geoms[0]
    rest = geoms[1:]

    first_wkb = dumps(first_geom, big_endian=big_endian)
    first_type = first_wkb[1:5]
    if not big_endian:
        first_type = first_type[::-1]

    if first_type in WKB_2D.values():
        num_dims = 2
    elif first_type in WKB_Z.values():
        num_dims = 3
    elif first_type in WKB_ZM.values():
        num_dims = 4

    wkb_string, byte_fmt, byte_order = _header_bytefmt_byteorder(
        'GeometryCollection', num_dims, big_endian, meta
    )
    # append the number of geometries
    wkb_string += struct.pack('%sl' % byte_order, len(geoms))

    wkb_string += first_wkb
    for geom in rest:
        wkb_string += dumps(geom, big_endian=big_endian)

    return wkb_string


def _load_point(big_endian, type_bytes, data_bytes):
    """
    Convert byte data for a Point to a GeoJSON `dict`.

    :param bool big_endian:
        If `True`, interpret the ``data_bytes`` in big endian order, else
        little endian.
    :param str type_bytes:
        4-byte integer (as a binary string) indicating the geometry type
        (Point) and the dimensions (2D, Z, M or ZM). For consistency, these
        bytes are expected to always be in big endian order, regardless of the
        value of ``big_endian``.
    :param str data_bytes:
        Coordinate data in a binary string.

    :returns:
        GeoJSON `dict` representing the Point geometry.
    """
    endian_token = '>' if big_endian else '<'

    if type_bytes == WKB_2D['Point']:
        coords = struct.unpack('%sdd' % endian_token,
                               as_bin_str(take(16, data_bytes)))
    elif type_bytes == WKB_Z['Point']:
        coords = struct.unpack('%sddd' % endian_token,
                               as_bin_str(take(24, data_bytes)))
    elif type_bytes == WKB_M['Point']:
        # NOTE: The use of XYM types geometries is quite rare. In the interest
        # of removing ambiguity, we will treat all XYM geometries as XYZM when
        # generate the GeoJSON. A default Z value of `0.0` will be given in
        # this case.
        coords = list(struct.unpack('%sddd' % endian_token,
                                    as_bin_str(take(24, data_bytes))))
        coords.insert(2, 0.0)
    elif type_bytes == WKB_ZM['Point']:
        coords = struct.unpack('%sdddd' % endian_token,
                               as_bin_str(take(32, data_bytes)))

    return dict(type='Point', coordinates=list(coords))


def _load_linestring(big_endian, type_bytes, data_bytes):
    endian_token = '>' if big_endian else '<'

    is_m = False

    if type_bytes in WKB_2D.values():
        num_dims = 2
    elif type_bytes in WKB_Z.values():
        num_dims = 3
    elif type_bytes in WKB_M.values():
        num_dims = 3
        is_m = True
    elif type_bytes in WKB_ZM.values():
        num_dims = 4

    coords = []
    [num_verts] = struct.unpack('%sl' % endian_token,
                                as_bin_str(take(4, data_bytes)))

    while True:
        vert_wkb = as_bin_str(take(8 * num_dims, data_bytes))
        fmt = '%s' + 'd' * num_dims
        vert = list(struct.unpack(fmt % endian_token, vert_wkb))
        if is_m:
            vert.insert(2, 0.0)

        coords.append(vert)

        if len(coords) == num_verts:
            break

    return dict(type='LineString', coordinates=list(coords))


def _load_polygon(big_endian, type_bytes, data_bytes):
    endian_token = '>' if big_endian else '<'
    data_bytes = iter(data_bytes)

    is_m = False

    if type_bytes in WKB_2D.values():
        num_dims = 2
    elif type_bytes in WKB_Z.values():
        num_dims = 3
    elif type_bytes in WKB_M.values():
        num_dims = 3
        is_m = True
    elif type_bytes in WKB_ZM.values():
        num_dims = 4

    coords = []
    [num_rings] = struct.unpack('%sl' % endian_token,
                                as_bin_str(take(4, data_bytes)))

    while True:
        ring = []
        [num_verts] = struct.unpack('%sl' % endian_token,
                                    as_bin_str(take(4, data_bytes)))

        verts_wkb = as_bin_str(take(8 * num_verts * num_dims, data_bytes))
        verts = block_splitter(verts_wkb, 8)
        if six.PY2:
            verts = (b''.join(x) for x in verts)
        elif six.PY3:
            verts = (b''.join(bytes([y]) for y in x) for x in verts)
        for vert_wkb in block_splitter(verts, num_dims):
            values = [struct.unpack('%sd' % endian_token, x)[0]
                      for x in vert_wkb]
            if is_m:
                values.insert(2, 0.0)
            ring.append(values)
        coords.append(ring)
        if len(coords) == num_rings:
            break

    return dict(type='Polygon', coordinates=coords)


def _load_multipoint(big_endian, type_bytes, data_bytes):
    endian_token = '>' if big_endian else '<'
    data_bytes = iter(data_bytes)

    is_m = False

    if type_bytes in WKB_2D.values():
        num_dims = 2
    elif type_bytes in WKB_Z.values():
        num_dims = 3
    elif type_bytes in WKB_M.values():
        num_dims = 3
        is_m = True
    elif type_bytes in WKB_ZM.values():
        num_dims = 4

    if is_m:
        dim = 'M'
    else:
        dim = _INT_TO_DIM_LABEL[num_dims]

    coords = []
    [num_points] = struct.unpack('%sl' % endian_token,
                                 as_bin_str(take(4, data_bytes)))

    while True:
        point_endian = as_bin_str(take(1, data_bytes))
        point_type = as_bin_str(take(4, data_bytes))
        values = struct.unpack('%s%s' % (endian_token, 'd' * num_dims),
                               as_bin_str(take(8 * num_dims, data_bytes)))
        values = list(values)
        if is_m:
            values.insert(2, 0.0)

        if big_endian:
            assert point_endian == BIG_ENDIAN
            assert point_type == _WKB[dim]['Point']
        else:
            assert point_endian == LITTLE_ENDIAN
            assert point_type[::-1] == _WKB[dim]['Point']

        coords.append(list(values))
        if len(coords) == num_points:
            break

    return dict(type='MultiPoint', coordinates=coords)


def _load_multilinestring(big_endian, type_bytes, data_bytes):
    endian_token = '>' if big_endian else '<'
    data_bytes = iter(data_bytes)

    is_m = False

    if type_bytes in WKB_2D.values():
        num_dims = 2
    elif type_bytes in WKB_Z.values():
        num_dims = 3
    elif type_bytes in WKB_M.values():
        num_dims = 3
        is_m = True
    elif type_bytes in WKB_ZM.values():
        num_dims = 4

    if is_m:
        dim = 'M'
    else:
        dim = _INT_TO_DIM_LABEL[num_dims]

    [num_ls] = struct.unpack('%sl' % endian_token,
                             as_bin_str(take(4, data_bytes)))
    coords = []

    while True:
        ls_endian = as_bin_str(take(1, data_bytes))
        ls_type = as_bin_str(take(4, data_bytes))
        if big_endian:
            assert ls_endian == BIG_ENDIAN
            assert ls_type == _WKB[dim]['LineString']
        else:
            assert ls_endian == LITTLE_ENDIAN
            assert ls_type[::-1] == _WKB[dim]['LineString']

        [num_verts] = struct.unpack('%sl' % endian_token,
                                    as_bin_str(take(4, data_bytes)))
        num_values = num_dims * num_verts
        values = struct.unpack(endian_token + 'd' * num_values,
                               as_bin_str(take(8 * num_values, data_bytes)))

        values = list(block_splitter(values, num_dims))
        if is_m:
            for v in values:
                v.insert(2, 0.0)
        coords.append(values)
        if len(coords) == num_ls:
            break

    return dict(type='MultiLineString', coordinates=coords)


def _load_multipolygon(big_endian, type_bytes, data_bytes):
    endian_token = '>' if big_endian else '<'

    is_m = False

    if type_bytes in WKB_2D.values():
        num_dims = 2
    elif type_bytes in WKB_Z.values():
        num_dims = 3
    elif type_bytes in WKB_M.values():
        num_dims = 3
        is_m = True
    elif type_bytes in WKB_ZM.values():
        num_dims = 4

    if is_m:
        dim = 'M'
    else:
        dim = _INT_TO_DIM_LABEL[num_dims]

    [num_polys] = struct.unpack('%sl' % endian_token,
                                as_bin_str(take(4, data_bytes)))
    coords = []
    while True:
        polygon = []
        poly_endian = as_bin_str(take(1, data_bytes))
        poly_type = as_bin_str(take(4, data_bytes))
        if big_endian:
            assert poly_endian == BIG_ENDIAN
            assert poly_type == _WKB[dim]['Polygon']
        else:
            assert poly_endian == LITTLE_ENDIAN
            assert poly_type[::-1] == _WKB[dim]['Polygon']

        [num_rings] = struct.unpack('%sl' % endian_token,
                                    as_bin_str(take(4, data_bytes)))
        for _ in range(num_rings):
            ring = []
            [num_verts] = struct.unpack('%sl' % endian_token,
                                        as_bin_str(take(4, data_bytes)))
            for _ in range(num_verts):
                vert_wkb = as_bin_str(take(8 * num_dims, data_bytes))
                fmt = '%s' + 'd' * num_dims
                vert = list(struct.unpack(fmt % endian_token, vert_wkb))
                if is_m:
                    vert.insert(2, 0.0)
                ring.append(vert)

            polygon.append(ring)

        coords.append(polygon)

        if len(coords) == num_polys:
            break

    return dict(type='MultiPolygon', coordinates=coords)


def _check_dimensionality(geom, num_dims):
    def first_geom(gc):
        for g in gc['geometries']:
            if not g['type'] == 'GeometryCollection':
                return g

    first_vert = {
        'Point': lambda x: x['coordinates'],
        'LineString': lambda x: x['coordinates'][0],
        'Polygon': lambda x: x['coordinates'][0][0],
        'MultiLineString': lambda x: x['coordinates'][0][0],
        'MultiPolygon': lambda x: x['coordinates'][0][0][0],
        'GeometryCollection': first_geom,
    }
    if not len(first_vert[geom['type']](geom)) == num_dims:
        error = 'Cannot mix dimensionality in a geometry'
        raise Exception(error)


def _load_geometrycollection(big_endian, type_bytes, data_bytes):
    endian_token = '>' if big_endian else '<'

    is_m = False

    if type_bytes in WKB_2D.values():
        num_dims = 2
    elif type_bytes in WKB_Z.values():
        num_dims = 3
    elif type_bytes in WKB_M.values():
        num_dims = 3
        is_m = True
    elif type_bytes in WKB_ZM.values():
        num_dims = 4

    geometries = []
    [num_geoms] = struct.unpack('%sl' % endian_token,
                                as_bin_str(take(4, data_bytes)))
    while True:
        geometry = loads(data_bytes)
        if is_m:
            _check_dimensionality(geometry, 4)
        else:
            _check_dimensionality(geometry, num_dims)
        # TODO(LB): Add type assertions for the geometry; collections should
        # not mix 2d, 3d, 4d, etc.
        geometries.append(geometry)
        if len(geometries) == num_geoms:
            break

    return dict(type='GeometryCollection', geometries=geometries)


_dumps_registry = {
    'Point':  _dump_point,
    'LineString': _dump_linestring,
    'Polygon': _dump_polygon,
    'MultiPoint': _dump_multipoint,
    'MultiLineString': _dump_multilinestring,
    'MultiPolygon': _dump_multipolygon,
    'GeometryCollection': _dump_geometrycollection,
}


_loads_registry = {
    'Point': _load_point,
    'LineString': _load_linestring,
    'Polygon': _load_polygon,
    'MultiPoint': _load_multipoint,
    'MultiLineString': _load_multilinestring,
    'MultiPolygon': _load_multipolygon,
    'GeometryCollection': _load_geometrycollection,
}
