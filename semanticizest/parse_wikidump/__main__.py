"""
Parse Wikidump

Reads in a Wikipedia snapshot file, or downloads it if it doesn't exist locally.
Then it attempts to parse it and store it in an SQL3 database, which it first
initializes.
"""
from __future__ import print_function

import logging
import re
import sqlite3
import sys
import errno

from six.moves.urllib.error import HTTPError
from six.moves.urllib.request import urlretrieve

import argparse
from docopt import docopt

from . import parse_dump, Db
from .._semanticizer import createtables_path


logger = logging.getLogger('semanticizest')
logger.addHandler(logging.StreamHandler(sys.stderr))
logger.setLevel('INFO')


class Progress(object):
    def __init__(self):
        self.threshold = .0

    def __call__(self, n_blocks, blocksize, totalsize):
        done = n_blocks * blocksize
        if done >= self.threshold * totalsize:
            logger.info("%3d%% done", int(100 * self.threshold))
            self.threshold += .05


DUMP_TEMPLATE = (
    "https://dumps.wikimedia.org/{0}/latest/{0}-latest-pages-articles.xml.bz2")


def die(msg):
    print("semanticizest.parse_wikidump: %s" % msg, file=sys.stderr)
    sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="semanticizer.parse_wikidump", description="Semanticizest Wiki parser")
    parser.add_argument('snapshot', 
                        help='Local Wikipedia snapshot to use.')
    parser.add_argument('model', 
                        help='File to store the model.')
    parser.add_argument('--download', dest='download', action="store_true",
                        help='Download snapshot if it does not exist as snapshot.xml.bz2. The corpus file name should match that of snapshot.')
    parser.add_argument('-N', '--ngram', dest='ngram', default=7, type=int,
                        help='Maximum order of ngrams, set to None to disable [default: 7].')
    args = parser.parse_args()

    try:
        fh = open(args.snapshot, 'r')
    except (IOError, OSError) as e:
        if e.errno == errno.ENOENT and args.download:
            m = re.match(r"(.+?)\.xml")
            if m:
                args.snapshot = m.group(1)
            url = DUMP_TEMPLATE.format(args.snapshot)
            print(url)
            args.snapshot = args.snapshot + ".xml.bz2"
            try:
                urlretrieve(url, args.snapshot, Progress())
            except HTTPError as e:
                die("Cannot download {0!r}: {1}".format(url, e))
        else:
            raise
    else:
        fh.close()
    
    # Init, connect to DB and setup db schema
    db = Db(args.model)
    db.connect()
    db.setup()
    
    # Parse wiki snapshot and store it to DB
    parse_dump(args.snapshot, db.db, N=args.ngram)
    
    # Close connection to DB and exit
    db.disconnect()
    
