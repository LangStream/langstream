#!/usr/bin/env python

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

"""Simple CLI for converting between WKB/WKT and GeoJSON

Example usage:

  $ echo "POINT (0.9999999 0.9999999)" \
  > | geomet --wkb - \
  > | geomet --wkt --precision 7 -
  POINT (0.9999999 0.9999999)

"""

from binascii import a2b_hex
from binascii import b2a_hex
import json
import logging
import sys

import click

from geomet import util, wkb, wkt

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level)


def translate(text, output_format='json', indent=None, precision=-1):
    if text.startswith('{'):
        geom = json.loads(text)
    elif text.startswith(('G', 'L', 'M', 'P')):
        geom = wkt.loads(text)
    else:
        geom = wkb.loads(a2b_hex(text))
    if output_format == 'wkb':
        output = b2a_hex(wkb.dumps(geom))
    elif output_format == 'wkt':
        kwds = {}
        if precision >= 0:
            kwds['decimals'] = precision
        output = wkt.dumps(geom, **kwds)
    else:
        if precision >= 0:
            geom = util.round_geom(geom, precision)
        output = json.dumps(geom, indent=indent, sort_keys=True)
    return output


@click.command(
    short_help="Convert between WKT or hex-encoded WKB and GeoJSON.",
    context_settings=CONTEXT_SETTINGS)
@click.argument('input', default='-', required=False)
@click.option('--verbose', '-v', count=True, help="Increase verbosity.")
@click.option('--quiet', '-q', count=True, help="Decrease verbosity.")
@click.option('--json', 'output_format', flag_value='json', default=True,
              help="JSON output.")
@click.option('--wkb', 'output_format', flag_value='wkb',
              help="Hex-encoded WKB output.")
@click.option('--wkt', 'output_format', flag_value='wkt',
              help="WKT output.")
@click.option('--precision', type=int, default=-1,
              help="Decimal precision of JSON and WKT coordinates.")
@click.option('--indent', default=None, type=int,
              help="Indentation level for pretty printed output")
def cli(input, verbose, quiet, output_format, precision, indent):
    """Convert text read from the first positional argument, stdin, or
    a file to GeoJSON and write to stdout."""

    verbosity = verbose - quiet
    configure_logging(verbosity)
    logger = logging.getLogger('geomet')

    # Handle the case of file, stream, or string input.
    try:
        src = click.open_file(input).readlines()
    except IOError:
        src = [input]

    stdout = click.get_text_stream('stdout')

    # Read-write loop.
    try:
        for line in src:
            text = line.strip()
            logger.debug("Input: %r", text)
            output = translate(
                text,
                output_format=output_format,
                indent=indent,
                precision=precision
            )
            logger.debug("Output: %r", output)
            stdout.write(output)
            stdout.write('\n')
        sys.exit(0)
    except Exception:
        logger.exception("Failed. Exception caught")
        sys.exit(1)


if __name__ == '__main__':
    cli()
